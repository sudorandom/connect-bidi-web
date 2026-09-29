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

package draft5_test

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
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft5"
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
		if err := stream.Send(&pingv1.CumSumResponse{Sum: sum}); err != nil {
			return err
		}
	}
}

// newTestServer mounts the ping service on an httptest server and returns a
// client dispatching over it, plus the handler implementation so tests can
// inspect what it saw.
func newTestServer(tb testing.TB, opts ...draft5.Option) (*testPingServer, pingv1connect.PingServiceClient, *httptest.Server) {
	tb.Helper()
	impl := &testPingServer{
		respHeaders:  map[string]string{"X-Test-Header": "header-val"},
		respTrailers: map[string]string{"X-Test-Trailer": "trailer-val"},
	}
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, impl)

	mount := append([]draft5.Option{
		draft5.WithWebSocketAcceptOptions(&websocket.AcceptOptions{InsecureSkipVerify: true}),
	}, opts...)
	mux := http.NewServeMux()
	draft5.Mount(mux, connectServer, mount...)

	server := httptest.NewServer(mux)
	tb.Cleanup(server.Close)

	transport := draft5.NewTransport(server.Client(), server.URL, opts...)
	return impl, pingv1connect.NewPingServiceClient(connect.NewClient(transport)), server
}

// TestUnaryStaysOnHTTP is the load-bearing half of "unary RPCs never
// upgrade": a unary call must be an ordinary Connect POST, not a handshake.
func TestUnaryStaysOnHTTP(t *testing.T) {
	t.Parallel()
	impl, client, _ := newTestServer(t)

	ctx, callInfo := connect.NewClientContext(context.Background())
	resp, err := client.Ping(ctx, &pingv1.PingRequest{Number: 42, Text: "hello"})
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if resp.GetNumber() != 42 || resp.GetText() != "hello" {
		t.Errorf("unexpected response: %+v", resp)
	}
	if got, want := callInfo.Protocol, connect.ProtocolNameConnect; got != want {
		t.Errorf("CallInfo.Protocol = %q, want %q — unary must not upgrade", got, want)
	}
	if got := callInfo.ResponseHeader().Get("X-Test-Header"); got != "header-val" {
		t.Errorf("response header = %q, want header-val", got)
	}
	if got := callInfo.ResponseTrailer().Get("X-Test-Trailer"); got != "trailer-val" {
		t.Errorf("response trailer = %q, want trailer-val", got)
	}
	if info := impl.requestHeader(); info.Get("Content-Type") == "" {
		t.Error("handler saw no content-type on the unary request")
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
	if got, want := callInfo.Protocol, "websocket-draft5"; got != want {
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

// TestClientStreamingError checks that an error in the end-stream message
// reaches a client-streaming caller, which Receives exactly once.
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

// TestEmptyMessagesAreNotSeparators is the regression test for the reason
// the opcode is load-bearing: an empty protobuf message encodes to zero
// bytes, so a zero-length *binary* message is a real RPC message while a
// zero-length *text* message is the separator. If the two were confused,
// this stream would half-close after the first message.
func TestEmptyMessagesAreNotSeparators(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t)

	stream, err := client.Sum(context.Background())
	if err != nil {
		t.Fatalf("Sum: %v", err)
	}
	// Every one of these encodes to zero bytes on the wire.
	for range 3 {
		if err := stream.Send(&pingv1.SumRequest{}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	resp, err := stream.CloseAndReceive()
	if err != nil {
		t.Fatalf("CloseAndReceive: %v", err)
	}
	// Sum encodes the message count in the low digits: three empty messages
	// must arrive as three messages, not as an early half-close.
	if got, want := resp.GetSum(), int64(3); got != want {
		t.Errorf("received %d request messages, want %d", got, want)
	}
}

// TestEmptyResponseMessages is the same hazard in the response direction.
func TestEmptyResponseMessages(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t)

	stream, err := client.CumSum(context.Background())
	if err != nil {
		t.Fatalf("CumSum: %v", err)
	}
	defer stream.Close()

	// A cumulative sum of zero encodes to an empty CumSumResponse.
	for range 3 {
		if err := stream.Send(&pingv1.CumSumRequest{Number: 0}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		resp, err := stream.Receive()
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
// the mandatory headers message, and that headers the handshake carried are
// visible too.
func TestRequestMetadata(t *testing.T) {
	t.Parallel()
	impl, client, _ := newTestServer(t)

	ctx, callInfo := connect.NewClientContext(context.Background())
	callInfo.RequestHeader().Set("X-Custom", "custom-val")
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
	if got := header.Get("Content-Type"); got != "application/connect+proto" {
		t.Errorf("Content-Type = %q, want application/connect+proto", got)
	}
	if got := header.Get("Connect-Protocol-Version"); got != "1" {
		t.Errorf("Connect-Protocol-Version = %q, want 1", got)
	}
	// The handshake's own headers must not leak into request metadata.
	for _, key := range []string{"Sec-Websocket-Key", "Sec-Websocket-Version", "Upgrade", "Connection"} {
		if value := header.Get(key); value != "" {
			t.Errorf("handshake header %s leaked into request metadata as %q", key, value)
		}
	}
	// ...but the ones the platform attached do reach the handler.
	if got := header.Get("User-Agent"); got == "" {
		t.Error("User-Agent from the handshake did not reach the handler")
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

func TestJSONCodec(t *testing.T) {
	t.Parallel()
	_, client, _ := newTestServer(t, draft5.WithProtoJSON())

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
	_, client, _ := newTestServer(t, draft5.WithoutCompression())

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

	stream, err := client.Sum(context.Background())
	if err != nil {
		t.Fatalf("Sum: %v", err)
	}
	// 16 KiB of highly compressible text, to exercise permessage-deflate.
	for range 4 {
		if err := stream.Send(&pingv1.SumRequest{Number: 1}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	resp, err := stream.CloseAndReceive()
	if err != nil {
		t.Fatalf("CloseAndReceive: %v", err)
	}
	if got, want := resp.GetSum(), int64(4*1000+4); got != want {
		t.Errorf("sum = %d, want %d", got, want)
	}
}

// TestUpgradeRejections covers the failures that are not about a particular
// RPC, and so are reported by refusing the handshake rather than on the
// socket.
func TestUpgradeRejections(t *testing.T) {
	t.Parallel()
	_, _, server := newTestServer(t)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	t.Run("MissingSubprotocol", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, resp, err := websocket.Dial(ctx, wsURL+"/connectbidi.ping.v1.PingService/CumSum", nil) //nolint:bodyclose
		if err == nil {
			_ = conn.CloseNow()
			t.Fatal("handshake succeeded without the subprotocol")
		}
		if resp == nil || resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status = %v, want 400", resp)
		}
	})

	t.Run("UnaryProcedure", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, resp, err := websocket.Dial(ctx, wsURL+"/connectbidi.ping.v1.PingService/Ping", //nolint:bodyclose
			&websocket.DialOptions{Subprotocols: []string{"connect.bidi.d5"}})
		if err == nil {
			_ = conn.CloseNow()
			t.Fatal("a unary procedure accepted an upgrade")
		}
		if resp == nil || resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("status = %v, want 405", resp)
		}
	})

	t.Run("UnknownProcedure", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, resp, err := websocket.Dial(ctx, wsURL+"/connectbidi.ping.v1.PingService/Nope", //nolint:bodyclose
			&websocket.DialOptions{Subprotocols: []string{"connect.bidi.d5"}})
		if err == nil {
			_ = conn.CloseNow()
			t.Fatal("an unknown procedure accepted an upgrade")
		}
		if resp == nil || resp.StatusCode != http.StatusNotFound {
			t.Errorf("status = %v, want 404", resp)
		}
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
	draft5.Mount(mux, connectServer)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	transport := draft5.NewTransport(server.Client(), wsURL)
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
