// Copyright 2021-2026 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package draft7_test

import (
	"context"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft7"
	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	"github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// testPingServer implements the ping service across every RPC shape, with
// hooks the tests use to exercise metadata, errors, and cancellation.
type testPingServer struct {
	pingv1connect.UnimplementedPingServiceHandler

	respHeaders  map[string]string
	respTrailers map[string]string

	// countUpBlocks, when set, makes CountUp send one message and then block
	// until its context ends, recording why in cancelCause.
	countUpBlocks bool
	cancelled     chan error
	// sawRequestHeader records the request metadata the handler observed.
	mu               sync.Mutex
	sawRequestHeader http.Header
}

func (s *testPingServer) record(ctx context.Context) {
	info, ok := connect.CallInfoForServerContext(ctx)
	if !ok {
		return
	}
	seen := make(http.Header)
	maps.Insert(seen, info.RequestHeader().All())
	s.mu.Lock()
	s.sawRequestHeader = seen
	s.mu.Unlock()
	for key, value := range s.respHeaders {
		info.ResponseHeader().Set(key, value)
	}
	for key, value := range s.respTrailers {
		info.ResponseTrailer().Set(key, value)
	}
}

func (s *testPingServer) requestHeader() http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sawRequestHeader
}

func (s *testPingServer) Ping(ctx context.Context, req *pingv1.PingRequest) (*pingv1.PingResponse, error) {
	s.record(ctx)
	return &pingv1.PingResponse{Number: req.GetNumber(), Text: req.GetText()}, nil
}

func (s *testPingServer) Fail(ctx context.Context, _ *pingv1.FailRequest) (*pingv1.FailResponse, error) {
	s.record(ctx)
	return nil, connect.Errorf(connect.CodeResourceExhausted, "Fail")
}

func (s *testPingServer) Sum(ctx context.Context, stream pingv1connect.PingServiceSumServerStream) (*pingv1.SumResponse, error) {
	s.record(ctx)
	var total int64
	var count int64
	for {
		req, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		total += req.GetNumber()
		count++
	}
	if total < 0 {
		detail, err := connectproto.NewErrorDetail(wrapperspb.String("negative"))
		if err != nil {
			return nil, err
		}
		return nil, connect.Errorf(connect.CodeInvalidArgument, "negative sum").WithDetail(detail)
	}
	// The count rides in the text-free response so a test can tell how many
	// request messages arrived, including zero-length ones.
	return &pingv1.SumResponse{Sum: total*1000 + count}, nil
}

func (s *testPingServer) CountUp(ctx context.Context, req *pingv1.CountUpRequest, stream pingv1connect.PingServiceCountUpServerStream) error {
	s.record(ctx)
	if req.GetNumber() < 0 {
		return connect.Errorf(connect.CodeInvalidArgument, "count must not be negative")
	}
	if s.countUpBlocks {
		if err := stream.Send(&pingv1.CountUpResponse{Number: 1}); err != nil {
			return err
		}
		<-ctx.Done()
		if s.cancelled != nil {
			s.cancelled <- ctx.Err()
		}
		return ctx.Err()
	}
	for i := int64(1); i <= req.GetNumber(); i++ {
		if err := stream.Send(&pingv1.CountUpResponse{Number: i}); err != nil {
			return err
		}
	}
	return nil
}

func (s *testPingServer) CumSum(ctx context.Context, stream pingv1connect.PingServiceCumSumServerStream) error {
	s.record(ctx)
	var sum int64
	for {
		req, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		sum += req.GetNumber()
		if err := stream.Send(&pingv1.CumSumResponse{Sum: sum, Text: req.GetText()}); err != nil {
			return err
		}
	}
}

// newTestServer mounts the ping service on an httptest server and returns a
// client dispatching over it, plus the handler implementation so tests can
// inspect what it saw.
func newTestServer(tb testing.TB, opts ...draft7.Option) (*testPingServer, pingv1connect.PingServiceClient, *httptest.Server) {
	tb.Helper()
	impl := &testPingServer{
		respHeaders:  map[string]string{"X-Test-Header": "header-val"},
		respTrailers: map[string]string{"X-Test-Trailer": "trailer-val"},
	}
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, impl)

	mount := append([]draft7.Option{
		draft7.WithWebSocketAcceptOptions(&websocket.AcceptOptions{InsecureSkipVerify: true}),
	}, opts...)
	mux := http.NewServeMux()
	draft7.Mount(mux, connectServer, mount...)

	server := httptest.NewServer(mux)
	tb.Cleanup(server.Close)

	transport := draft7.NewTransport(server.Client(), server.URL, opts...)
	return impl, pingv1connect.NewPingServiceClient(connect.NewClient(transport)), server
}

// TestUnaryStaysOnHTTP checks the default: a unary call is an ordinary
// Connect POST, not a handshake.
func TestUnaryStaysOnHTTP(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t)

	ctx, callInfo := connect.NewClientContext(context.Background())
	resp, err := client.Ping(ctx, &pingv1.PingRequest{Number: 42, Text: "hello"})
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if resp.GetNumber() != 42 || resp.GetText() != "hello" {
		t.Errorf("unexpected response: %+v", resp)
	}
	if got, want := callInfo.Protocol, connect.ProtocolNameConnect; got != want {
		t.Errorf("CallInfo.Protocol = %q, want %q — unary must not upgrade by default", got, want)
	}
	if got := callInfo.ResponseHeader().Get("X-Test-Header"); got != "header-val" {
		t.Errorf("response header = %q, want header-val", got)
	}
	if got := callInfo.ResponseTrailer().Get("X-Test-Trailer"); got != "trailer-val" {
		t.Errorf("response trailer = %q, want trailer-val", got)
	}
}

// TestUnaryOverWebSocket is the other half: a server accepts an upgrade
// for a unary procedure, and a client asked to use one gets the spec's
// worked example — M, B, C one way; M, B, S the other.
func TestUnaryOverWebSocket(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t, draft7.WithUnaryOverWebSocket())

	ctx, callInfo := connect.NewClientContext(context.Background())
	resp, err := client.Ping(ctx, &pingv1.PingRequest{Number: 42, Text: "hello"})
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if resp.GetNumber() != 42 || resp.GetText() != "hello" {
		t.Errorf("unexpected response: %+v", resp)
	}
	if got, want := callInfo.Protocol, "websocket-draft7"; got != want {
		t.Errorf("CallInfo.Protocol = %q, want %q", got, want)
	}
	if got := callInfo.ResponseHeader().Get("X-Test-Header"); got != "header-val" {
		t.Errorf("response header = %q, want header-val", got)
	}
	if got := callInfo.ResponseTrailer().Get("X-Test-Trailer"); got != "trailer-val" {
		t.Errorf("response trailer = %q, want trailer-val", got)
	}

	if _, err := client.Fail(ctx, &pingv1.FailRequest{}); connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Errorf("Fail over WebSocket: code = %v, want resource_exhausted", connect.CodeOf(err))
	}
}

func TestUnaryError(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t)

	_, err := client.Fail(context.Background(), &pingv1.FailRequest{})
	if err == nil {
		t.Fatal("Fail returned no error")
	}
	if got, want := connect.CodeOf(err), connect.CodeResourceExhausted; got != want {
		t.Errorf("code = %v, want %v", got, want)
	}
}

func TestServerStreaming(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t)

	ctx, callInfo := connect.NewClientContext(context.Background())
	stream, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: 3})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	defer stream.Close()

	var numbers []int64
	for {
		resp, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		numbers = append(numbers, resp.GetNumber())
	}
	if len(numbers) != 3 || numbers[0] != 1 || numbers[2] != 3 {
		t.Errorf("numbers = %v, want [1 2 3]", numbers)
	}
	if got, want := callInfo.Protocol, "websocket-draft7"; got != want {
		t.Errorf("CallInfo.Protocol = %q, want %q", got, want)
	}
	if got := callInfo.ResponseHeader().Get("X-Test-Header"); got != "header-val" {
		t.Errorf("response header = %q, want header-val", got)
	}
	if got := callInfo.ResponseTrailer().Get("X-Test-Trailer"); got != "trailer-val" {
		t.Errorf("response trailer = %q, want trailer-val", got)
	}
}

func TestClientStreaming(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t)

	stream, err := client.Sum(context.Background())
	if err != nil {
		t.Fatalf("Sum: %v", err)
	}
	for _, number := range []int64{1, 2, 3, 4} {
		if err := stream.Send(&pingv1.SumRequest{Number: number}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	resp, err := stream.CloseAndReceive()
	if err != nil {
		t.Fatalf("CloseAndReceive: %v", err)
	}
	if got, want := resp.GetSum(), int64(10*1000+4); got != want {
		t.Errorf("sum = %d, want %d", got, want)
	}
}

// TestClientStreamingError checks that an error in the end-of-stream
// message reaches a client-streaming caller, which Receives exactly once.
func TestClientStreamingError(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t)

	stream, err := client.Sum(context.Background())
	if err != nil {
		t.Fatalf("Sum: %v", err)
	}
	if err := stream.Send(&pingv1.SumRequest{Number: -5}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.CloseAndReceive(); err == nil {
		t.Fatal("CloseAndReceive returned no error")
	} else if got, want := connect.CodeOf(err), connect.CodeInvalidArgument; got != want {
		t.Errorf("code = %v, want %v", got, want)
	}
}

func TestBidiStreaming(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t)

	stream, err := client.CumSum(context.Background())
	if err != nil {
		t.Fatalf("CumSum: %v", err)
	}
	defer stream.Close()

	want := []int64{1, 3, 6, 10}
	for i, number := range []int64{1, 2, 3, 4} {
		if err := stream.Send(&pingv1.CumSumRequest{Number: number}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
		resp, err := stream.Receive()
		if err != nil {
			t.Fatalf("Receive %d: %v", i, err)
		}
		if resp.GetSum() != want[i] {
			t.Errorf("sum %d = %d, want %d", i, resp.GetSum(), want[i])
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if _, err := stream.Receive(); !errors.Is(err, io.EOF) {
		t.Errorf("Receive after half-close = %v, want io.EOF", err)
	}
}

// TestSendAfterCloseSend pins the client API rule: a message after C would
// not be delivered, so the attempt is an error rather than a silent drop.
func TestSendAfterCloseSend(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t)

	stream, err := client.CumSum(context.Background())
	if err != nil {
		t.Fatalf("CumSum: %v", err)
	}
	defer stream.Close()
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if err := stream.Send(&pingv1.CumSumRequest{Number: 1}); err == nil {
		t.Error("Send after CloseSend succeeded; the message could never be delivered")
	}
}

// TestEmptyMessages sends messages that encode to zero bytes. Under the
// Protobuf codec a bare B is the empty message, so nothing is lost.
func TestEmptyMessages(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t)

	stream, err := client.Sum(context.Background())
	if err != nil {
		t.Fatalf("Sum: %v", err)
	}
	for range 3 {
		if err := stream.Send(&pingv1.SumRequest{}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	resp, err := stream.CloseAndReceive()
	if err != nil {
		t.Fatalf("CloseAndReceive: %v", err)
	}
	if got, want := resp.GetSum(), int64(3); got != want {
		t.Errorf("received %d request messages, want %d", got, want)
	}

	// And in the response direction: a cumulative sum of zero is empty.
	bidi, err := client.CumSum(context.Background())
	if err != nil {
		t.Fatalf("CumSum: %v", err)
	}
	defer bidi.Close()
	for range 3 {
		if err := bidi.Send(&pingv1.CumSumRequest{Number: 0}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		resp, err := bidi.Receive()
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		if resp.GetSum() != 0 {
			t.Errorf("sum = %d, want 0", resp.GetSum())
		}
	}
}

func TestStreamingError(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t)

	stream, err := client.CountUp(context.Background(), &pingv1.CountUpRequest{Number: -1})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	defer stream.Close()

	if _, err := stream.Receive(); err == nil {
		t.Fatal("Receive returned no error")
	} else if got, want := connect.CodeOf(err), connect.CodeInvalidArgument; got != want {
		t.Errorf("code = %v, want %v", got, want)
	}
}

// TestRequestMetadata checks that call metadata reaches the handler through
// the leading-metadata message, and that the handshake's own headers are
// visible too — every request header is made available.
func TestRequestMetadata(t *testing.T) {
	t.Parallel()
	impl, client, _ := newTestServer(t)

	ctx, callInfo := connect.NewClientContext(context.Background())
	callInfo.RequestHeader().Set("X-Custom", "custom-val")
	callInfo.RequestHeader().Set("X-Data-Bin", connect.EncodeBinaryHeader([]byte{0xff, 0x00, 0x01}))
	// A key the handshake also carries: the message replaces it.
	callInfo.RequestHeader().Set("User-Agent", "draft7-test")
	stream, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: 1})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	defer stream.Close()
	for {
		if _, err := stream.Receive(); err != nil {
			break
		}
	}

	header := impl.requestHeader()
	if got := header.Get("X-Custom"); got != "custom-val" {
		t.Errorf("X-Custom = %q, want custom-val", got)
	}
	if got, err := connect.DecodeBinaryHeader(header.Get("X-Data-Bin")); err != nil || string(got) != "\xff\x00\x01" {
		t.Errorf("X-Data-Bin = %q (%v), want the original bytes", header.Get("X-Data-Bin"), err)
	}
	if got := header.Get("User-Agent"); got != "draft7-test" {
		t.Errorf("User-Agent = %q, want the metadata message's value to replace the handshake's", got)
	}
	// The handshake's headers reach the handler as well.
	if got := header.Get("Sec-Websocket-Version"); got != "13" {
		t.Errorf("Sec-Websocket-Version = %q, want 13 from the handshake", got)
	}
	// The codec is the subprotocol's business, not a header's.
	if got := header.Get("Content-Type"); got != "" {
		t.Errorf("Content-Type = %q, want none: the subprotocol selects the codec", got)
	}
}

// TestReservedMetadataEndsTheRPC checks that a reserved key is an error,
// not an ignored key: the client believes it set a value the server does
// not have, and nothing on the wire would show it.
func TestReservedMetadataEndsTheRPC(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t, draft7.WithInfrastructureHeaders("X-Tenant-"))

	// Protocol-controlled names never leave the client — it strips them —
	// so the reserved names reaching the server are the other two kinds.
	for _, key := range []string{"Cookie", "Origin", "Sec-Custom", "Proxy-Thing", "X-Tenant-Id"} {
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			ctx, callInfo := connect.NewClientContext(context.Background())
			callInfo.RequestHeader().Set(key, "value")
			stream, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: 1})
			if err != nil {
				t.Fatalf("CountUp: %v", err)
			}
			defer stream.Close()
			_, err = stream.Receive()
			if got, want := connect.CodeOf(err), connect.CodeInvalidArgument; got != want {
				t.Errorf("code = %v (%v), want %v", got, err, want)
			}
		})
	}

	// The default deny list is replaced, not extended: X-Forwarded-For is
	// allowed on this server.
	ctx, callInfo := connect.NewClientContext(context.Background())
	callInfo.RequestHeader().Set("X-Forwarded-For", "203.0.113.7")
	stream, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: 1})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	defer stream.Close()
	if _, err := stream.Receive(); err != nil {
		t.Errorf("X-Forwarded-For was rejected with a replaced deny list: %v", err)
	}
}

func TestCancellationReachesHandler(t *testing.T) {
	t.Parallel()
	impl, client, _ := newTestServer(t)
	impl.countUpBlocks = true
	impl.cancelled = make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: 1})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	if _, err := stream.Receive(); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	// The handler is now blocked mid-stream with nothing left to read. One
	// WebSocket is one RPC, so cancelling closes the connection, and the
	// server's read pump is what turns that into a cancelled context.
	cancel()
	_ = stream.Close()

	select {
	case err := <-impl.cancelled:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("handler context ended with %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not cancelled when the client disconnected")
	}
}

// TestDeadline checks that a client deadline travels on the handshake URI
// and bounds the RPC on the server.
func TestDeadline(t *testing.T) {
	t.Parallel()
	impl, client, _ := newTestServer(t)
	impl.countUpBlocks = true
	impl.cancelled = make(chan error, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	stream, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: 1})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	defer stream.Close()
	if _, err := stream.Receive(); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if _, err := stream.Receive(); err == nil {
		t.Fatal("Receive returned no error after the deadline")
	}

	select {
	case err := <-impl.cancelled:
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Errorf("handler context ended with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler outlived the client's deadline")
	}
}

// TestServerTimeout checks that the server's own deadline bounds an RPC
// whose client asked for none.
func TestServerTimeout(t *testing.T) {
	t.Parallel()
	impl, client, _ := newTestServer(t, draft7.WithServerTimeout(300*time.Millisecond))
	impl.countUpBlocks = true
	impl.cancelled = make(chan error, 1)

	stream, err := client.CountUp(context.Background(), &pingv1.CountUpRequest{Number: 1})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	defer stream.Close()
	if _, err := stream.Receive(); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	_, err = stream.Receive()
	if got, want := connect.CodeOf(err), connect.CodeDeadlineExceeded; got != want {
		t.Errorf("code = %v (%v), want %v", got, err, want)
	}
	select {
	case <-impl.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler outlived the server's deadline")
	}
}

func TestJSONCodec(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t, draft7.WithProtoJSON())

	stream, err := client.CumSum(context.Background())
	if err != nil {
		t.Fatalf("CumSum: %v", err)
	}
	defer stream.Close()
	if err := stream.Send(&pingv1.CumSumRequest{Number: 7}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp, err := stream.Receive()
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if resp.GetSum() != 7 {
		t.Errorf("sum = %d, want 7", resp.GetSum())
	}
}

func TestWithoutCompression(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t, draft7.WithoutCompression())

	stream, err := client.CountUp(context.Background(), &pingv1.CountUpRequest{Number: 2})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	defer stream.Close()
	count := 0
	for {
		if _, err := stream.Receive(); err != nil {
			break
		}
		count++
	}
	if count != 2 {
		t.Errorf("received %d messages, want 2", count)
	}
}

func TestLargeMessages(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t)

	stream, err := client.CumSum(context.Background())
	if err != nil {
		t.Fatalf("CumSum: %v", err)
	}
	defer stream.Close()
	// 16 KiB of highly compressible text, to exercise permessage-deflate.
	text := strings.Repeat("a", 16*1024)
	for range 4 {
		if err := stream.Send(&pingv1.CumSumRequest{Number: 1, Text: text}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		resp, err := stream.Receive()
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		if resp.GetText() != text {
			t.Errorf("echoed %d bytes, want %d", len(resp.GetText()), len(text))
		}
	}
}

// TestReadMaxBytes checks the size limit in both directions: a server
// reports an oversized request on the socket as resource_exhausted, and a
// client reports an oversized response the same way after closing with
// 1009.
func TestReadMaxBytes(t *testing.T) {
	t.Parallel()
	big := strings.Repeat("a", 1024)

	t.Run("Server", func(t *testing.T) {
		t.Parallel()
		_, client, _ := newTestServer(t, draft7.WithReadMaxBytes(64))
		stream, err := client.CumSum(context.Background())
		if err != nil {
			t.Fatalf("CumSum: %v", err)
		}
		defer stream.Close()
		if err := stream.Send(&pingv1.CumSumRequest{Number: 1, Text: big}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		_, err = stream.Receive()
		if got, want := connect.CodeOf(err), connect.CodeResourceExhausted; got != want {
			t.Errorf("oversized request: code = %v (%v), want %v", got, err, want)
		}
	})

	t.Run("Client", func(t *testing.T) {
		t.Parallel()
		_, _, server := newTestServer(t)
		transport := draft7.NewTransport(server.Client(), server.URL, draft7.WithReadMaxBytes(64))
		client := pingv1connect.NewPingServiceClient(connect.NewClient(transport))
		stream, err := client.CumSum(context.Background())
		if err != nil {
			t.Fatalf("CumSum: %v", err)
		}
		defer stream.Close()
		if err := stream.Send(&pingv1.CumSumRequest{Number: 1, Text: big}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		_, err = stream.Receive()
		if got, want := connect.CodeOf(err), connect.CodeResourceExhausted; got != want {
			t.Errorf("oversized response: code = %v (%v), want %v", got, err, want)
		}
	})
}

// TestPathPrefix checks the routing a deployment gets from WithPathPrefix:
// upgrades under the prefix, plain RPCs at the bare paths, and the two
// refusals for getting it the wrong way round.
func TestPathPrefix(t *testing.T) {
	t.Parallel()
	_, client, server := newTestServer(t, draft7.WithPathPrefix("/ws"))

	// Unary stays on HTTP at the bare path; streaming dials the prefix.
	if _, err := client.Ping(context.Background(), &pingv1.PingRequest{Number: 1}); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	stream, err := client.CountUp(context.Background(), &pingv1.CountUpRequest{Number: 1})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	defer stream.Close()
	if _, err := stream.Receive(); err != nil {
		t.Fatalf("Receive: %v", err)
	}

	t.Run("NonUpgradeUnderPrefixIs426", func(t *testing.T) {
		t.Parallel()
		resp, err := server.Client().Get(server.URL + "/ws/connectbidi.ping.v1.PingService/CountUp")
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUpgradeRequired {
			t.Errorf("status = %d, want 426", resp.StatusCode)
		}
		if got := resp.Header.Get("Upgrade"); got != "websocket" {
			t.Errorf("Upgrade header = %q, want websocket", got)
		}
	})

	t.Run("UpgradeAtBarePathIs400", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
		conn, resp, err := websocket.Dial(ctx, wsURL+"/connectbidi.ping.v1.PingService/CountUp", //nolint:bodyclose
			&websocket.DialOptions{Subprotocols: []string{"connectrpc.1+proto"}})
		if err == nil {
			_ = conn.CloseNow()
			t.Fatal("an upgrade at the bare procedure path was accepted")
		}
		if resp == nil || resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status = %v, want 400", resp)
		}
	})
}

// TestUpgradeRejections covers the failures that are not about a particular
// RPC, and so are reported by refusing the handshake rather than on the
// socket.
func TestUpgradeRejections(t *testing.T) {
	t.Parallel()
	_, _, server := newTestServer(t, draft7.WithConditionalOptions(func(connect.Spec) []draft7.Option {
		return nil
	}))
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// dialStatus returns the status of a handshake that must fail.
	dialStatus := func(t *testing.T, path string, dialOpts *websocket.DialOptions) int {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, resp, err := websocket.Dial(ctx, wsURL+path, dialOpts) //nolint:bodyclose // coder/websocket closes the handshake response body itself
		if err == nil {
			_ = conn.CloseNow()
			t.Fatal("handshake succeeded")
		}
		if resp == nil {
			t.Fatalf("no response: %v", err)
		}
		return resp.StatusCode
	}

	t.Run("NoRecognizedSubprotocolIs400", func(t *testing.T) {
		t.Parallel()
		status := dialStatus(t, "/connectbidi.ping.v1.PingService/CumSum",
			&websocket.DialOptions{Subprotocols: []string{"connect.bidi.d5"}})
		if status != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", status)
		}
	})

	t.Run("UnknownProcedureIs404", func(t *testing.T) {
		t.Parallel()
		status := dialStatus(t, "/connectbidi.ping.v1.PingService/Nope",
			&websocket.DialOptions{Subprotocols: []string{"connectrpc.1+proto"}})
		if status != http.StatusNotFound {
			t.Errorf("status = %d, want 404", status)
		}
	})

	t.Run("CrossOriginIs403", func(t *testing.T) {
		t.Parallel()
		// The test server permits every origin; this one does not.
		connectServer := connect.NewServer()
		pingv1connect.RegisterPingServiceHandler(connectServer, &testPingServer{})
		mux := http.NewServeMux()
		draft7.Mount(mux, connectServer)
		strict := httptest.NewServer(mux)
		t.Cleanup(strict.Close)
		strictURL := "ws" + strings.TrimPrefix(strict.URL, "http")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, resp, err := websocket.Dial(ctx, strictURL+"/connectbidi.ping.v1.PingService/CumSum", //nolint:bodyclose
			&websocket.DialOptions{
				Subprotocols: []string{"connectrpc.1+proto"},
				HTTPHeader:   http.Header{"Origin": []string{"https://evil.example"}},
			})
		if err == nil {
			_ = conn.CloseNow()
			t.Fatal("a cross-origin handshake was accepted")
		}
		if resp == nil || resp.StatusCode != http.StatusForbidden {
			t.Errorf("status = %v, want 403", resp)
		}

		// Same host, different scheme: permitted, because the comparison
		// leaves the scheme out on purpose.
		conn, _, err = websocket.Dial(ctx, strictURL+"/connectbidi.ping.v1.PingService/CumSum", //nolint:bodyclose
			&websocket.DialOptions{
				Subprotocols: []string{"connectrpc.1+proto"},
				HTTPHeader:   http.Header{"Origin": []string{"https://" + strings.TrimPrefix(strict.URL, "http://")}},
			})
		if err != nil {
			t.Fatalf("a same-host handshake was refused: %v", err)
		}
		_ = conn.CloseNow()
	})
}

// TestWebSocketURLScheme checks that one endpoint really is configured with
// one URL, in either spelling.
func TestWebSocketURLScheme(t *testing.T) {
	t.Parallel()
	impl := &testPingServer{}
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, impl)
	mux := http.NewServeMux()
	draft7.Mount(mux, connectServer)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	transport := draft7.NewTransport(server.Client(), wsURL)
	client := pingv1connect.NewPingServiceClient(connect.NewClient(transport))

	// Unary over the ws:// spelling must still dispatch as HTTP.
	if _, err := client.Ping(context.Background(), &pingv1.PingRequest{Number: 1}); err != nil {
		t.Fatalf("Ping over a ws:// base URL: %v", err)
	}
	// ...and streaming must still upgrade.
	stream, err := client.CountUp(context.Background(), &pingv1.CountUpRequest{Number: 1})
	if err != nil {
		t.Fatalf("CountUp over a ws:// base URL: %v", err)
	}
	defer stream.Close()
	if _, err := stream.Receive(); err != nil {
		t.Fatalf("Receive: %v", err)
	}
}
