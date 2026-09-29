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

package draft1_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft1"
	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	"github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
)

type testPingServer struct {
	pingv1connect.UnimplementedPingServiceHandler

	respHeaders  map[string]string
	respTrailers map[string]string

	countUpBlocks bool
	cancelled     chan error

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

func (s *testPingServer) Sum(ctx context.Context, stream pingv1connect.PingServiceSumServerStream) (*pingv1.SumResponse, error) {
	s.record(ctx)
	var total, count int64
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
		return nil, connect.Errorf(connect.CodeInvalidArgument, "negative sum")
	}
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

// newTestServer mounts draft 1 under a prefix and counts the WebSocket
// handshakes that reach it, which is how the tests below check that each
// RPC really does get a connection of its own.
func newTestServer(tb testing.TB, opts ...draft1.Option) (*testPingServer, pingv1connect.PingServiceClient, *atomic.Int64, string) {
	tb.Helper()
	impl := &testPingServer{
		respHeaders:  map[string]string{"X-Test-Header": "header-val"},
		respTrailers: map[string]string{"X-Test-Trailer": "trailer-val"},
	}
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, impl)

	handlerOpts := append([]draft1.Option{
		draft1.WithAcceptOptions(&websocket.AcceptOptions{InsecureSkipVerify: true}),
	}, opts...)

	var handshakes atomic.Int64
	mux := http.NewServeMux()
	counting := http.NewServeMux()
	draft1.Mount(counting, connectServer, draft1.DefaultPathPrefix, handlerOpts...)
	mux.Handle(draft1.DefaultPathPrefix+"/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handshakes.Add(1)
		counting.ServeHTTP(w, r)
	}))
	server := httptest.NewServer(mux)
	tb.Cleanup(server.Close)

	baseURL := "ws" + strings.TrimPrefix(server.URL, "http") + draft1.DefaultPathPrefix
	transport := draft1.NewTransport(baseURL, opts...)
	return impl, pingv1connect.NewPingServiceClient(connect.NewClient(transport)), &handshakes, baseURL
}

// TestConnectionPerRPC is draft 1's defining decision: no multiplexing and
// no pooling, so N streaming RPCs cost N handshakes.
func TestConnectionPerRPC(t *testing.T) {
	t.Parallel()
	_, client, handshakes, _ := newTestServer(t)

	for round := range 3 {
		stream, err := client.CumSum(context.Background())
		if err != nil {
			t.Fatalf("round %d: CumSum: %v", round, err)
		}
		if err := stream.Send(&pingv1.CumSumRequest{Number: 1}); err != nil {
			t.Fatalf("round %d: Send: %v", round, err)
		}
		if _, err := stream.Receive(); err != nil {
			t.Fatalf("round %d: Receive: %v", round, err)
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("round %d: CloseSend: %v", round, err)
		}
		if _, err := stream.Receive(); !errors.Is(err, io.EOF) {
			t.Fatalf("round %d: Receive = %v, want io.EOF", round, err)
		}
		if err := stream.Close(); err != nil {
			t.Fatalf("round %d: Close: %v", round, err)
		}
	}

	if got := handshakes.Load(); got != 3 {
		t.Errorf("3 sequential RPCs cost %d handshakes, want 3", got)
	}
}

// TestConcurrentRPCs: overlapping streams must not interfere, each having
// a socket to itself.
func TestConcurrentRPCs(t *testing.T) {
	t.Parallel()
	_, client, handshakes, _ := newTestServer(t)

	const concurrency = 4
	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	for i := range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream, err := client.CountUp(context.Background(), &pingv1.CountUpRequest{Number: int64(i + 1)})
			if err != nil {
				errs[i] = err
				return
			}
			defer func() { _ = stream.Close() }()
			var count int64
			for {
				msg, err := stream.Receive()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					errs[i] = err
					return
				}
				count++
				if msg.GetNumber() != count {
					errs[i] = errors.New("out-of-order response")
					return
				}
			}
			if count != int64(i+1) {
				errs[i] = errors.New("wrong number of responses")
			}
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("stream %d: %v", i, err)
		}
	}
	if got := handshakes.Load(); got != concurrency {
		t.Errorf("%d concurrent RPCs cost %d handshakes, want %d", concurrency, got, concurrency)
	}
}

func TestServerStreaming(t *testing.T) {
	t.Parallel()
	impl, client, _, _ := newTestServer(t)

	ctx, callInfo := connect.NewClientContext(context.Background())
	stream, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: 3})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	var got []int64
	for {
		msg, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		got = append(got, msg.GetNumber())
	}
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("responses = %v, want [1 2 3]", got)
	}
	if got, want := callInfo.Protocol, "websocket-draft1"; got != want {
		t.Errorf("CallInfo.Protocol = %q, want %q", got, want)
	}
	if value := callInfo.ResponseHeader().Get("X-Test-Header"); value != "header-val" {
		t.Errorf("response header = %q, want %q", value, "header-val")
	}
	if value := callInfo.ResponseTrailer().Get("X-Test-Trailer"); value != "trailer-val" {
		t.Errorf("response trailer = %q, want %q", value, "trailer-val")
	}
	if err := stream.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if impl.requestHeader().Get("Content-Type") != "application/connect+proto" {
		t.Errorf("handler saw content-type %q", impl.requestHeader().Get("Content-Type"))
	}
}

func TestClientStreaming(t *testing.T) {
	t.Parallel()
	_, client, _, _ := newTestServer(t)

	ctx, callInfo := connect.NewClientContext(context.Background())
	stream, err := client.Sum(ctx)
	if err != nil {
		t.Fatalf("Sum: %v", err)
	}
	for i := range 3 {
		if err := stream.Send(&pingv1.SumRequest{Number: int64(i + 1)}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	resp, err := stream.CloseAndReceive()
	if err != nil {
		t.Fatalf("CloseAndReceive: %v", err)
	}
	if resp.GetSum() != 6*1000+3 {
		t.Errorf("sum = %d, want %d", resp.GetSum(), 6*1000+3)
	}
	if value := callInfo.ResponseTrailer().Get("X-Test-Trailer"); value != "trailer-val" {
		t.Errorf("response trailer = %q, want %q", value, "trailer-val")
	}
}

func TestBidiStreaming(t *testing.T) {
	t.Parallel()
	_, client, _, _ := newTestServer(t)

	stream, err := client.CumSum(context.Background())
	if err != nil {
		t.Fatalf("CumSum: %v", err)
	}
	want := []int64{1, 3, 6}
	for i, expected := range want {
		if err := stream.Send(&pingv1.CumSumRequest{Number: int64(i + 1)}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		msg, err := stream.Receive()
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		if msg.GetSum() != expected {
			t.Errorf("sum = %d, want %d", msg.GetSum(), expected)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if _, err := stream.Receive(); !errors.Is(err, io.EOF) {
		t.Fatalf("Receive = %v, want io.EOF", err)
	}
	if err := stream.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestEmptyMessagesRoundTrip: an empty protobuf message encodes to zero
// bytes. Draft 1 carries it in an envelope with a zero length, which is
// unambiguous — the drafts that frame with the opcode had to work to keep
// it from reading as a half-close.
func TestEmptyMessagesRoundTrip(t *testing.T) {
	t.Parallel()
	_, client, _, _ := newTestServer(t)

	stream, err := client.CumSum(context.Background())
	if err != nil {
		t.Fatalf("CumSum: %v", err)
	}
	// Number defaults to zero, so this marshals to nothing at all.
	if err := stream.Send(&pingv1.CumSumRequest{}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	msg, err := stream.Receive()
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if msg.GetSum() != 0 {
		t.Errorf("sum = %d, want 0", msg.GetSum())
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if _, err := stream.Receive(); !errors.Is(err, io.EOF) {
		t.Fatalf("Receive = %v, want io.EOF", err)
	}
	_ = stream.Close()
}

// TestUnaryIsRefused: draft 1 leaves unary RPCs on plain Connect HTTP, and
// says so rather than opening a socket to carry one.
func TestUnaryIsRefused(t *testing.T) {
	t.Parallel()
	_, client, handshakes, _ := newTestServer(t)

	_, err := client.Ping(context.Background(), &pingv1.PingRequest{Number: 1})
	if err == nil {
		t.Fatal("Ping succeeded over a draft 1 transport, want an error")
	}
	if got := connect.CodeOf(err); got != connect.CodeUnimplemented {
		t.Errorf("code = %v, want %v", got, connect.CodeUnimplemented)
	}
	if got := handshakes.Load(); got != 0 {
		t.Errorf("a refused unary RPC cost %d handshakes, want 0", got)
	}
}

// TestInterceptSharesTheProcedureURLs is the deployment shape draft 1
// proposes: one URL per procedure, answering POST with ordinary Connect
// over HTTP and GET+Upgrade with this protocol.
func TestInterceptSharesTheProcedureURLs(t *testing.T) {
	t.Parallel()
	impl := &testPingServer{}
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, impl)

	mux := http.NewServeMux()
	connecthttp.Mount(mux, connectServer)
	server := httptest.NewServer(draft1.Intercept(mux, connectServer,
		draft1.WithAcceptOptions(&websocket.AcceptOptions{InsecureSkipVerify: true}),
	))
	t.Cleanup(server.Close)

	client := pingv1connect.NewPingServiceClient(connect.NewClient(
		connectwebsocket.NewCompositeTransport(
			connecthttp.NewTransport(server.Client(), server.URL),
			draft1.NewTransport(server.URL),
		),
	))

	// Unary goes over HTTP on the same URL the stream upgrades on.
	ctx, unaryInfo := connect.NewClientContext(context.Background())
	resp, err := client.Ping(ctx, &pingv1.PingRequest{Number: 42, Text: "http"})
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if resp.GetNumber() != 42 || resp.GetText() != "http" {
		t.Errorf("unexpected response: %+v", resp)
	}
	if got := unaryInfo.Protocol; got != connect.ProtocolNameConnect {
		t.Errorf("unary CallInfo.Protocol = %q, want %q — unary must not upgrade", got, connect.ProtocolNameConnect)
	}

	streamCtx, streamInfo := connect.NewClientContext(context.Background())
	stream, err := client.CountUp(streamCtx, &pingv1.CountUpRequest{Number: 2})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	var count int
	for {
		if _, err := stream.Receive(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		count++
	}
	if count != 2 {
		t.Errorf("received %d messages, want 2", count)
	}
	if got := streamInfo.Protocol; got != "websocket-draft1" {
		t.Errorf("streaming CallInfo.Protocol = %q, want websocket-draft1", got)
	}
}

// TestUnknownProcedure: the URL names the procedure, so an unknown one is
// answered in the protocol with a Connect code rather than by dropping the
// connection. Spoken raw, because a generated client cannot ask for a
// procedure that does not exist.
func TestUnknownProcedure(t *testing.T) {
	t.Parallel()
	// Mounted as a subtree rather than with Mount, so a request for a
	// procedure that does not exist reaches the handler instead of being
	// 404ed by the router.
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, &testPingServer{})
	mux := http.NewServeMux()
	mux.Handle("/", draft1.NewHandler(connectServer,
		draft1.WithAcceptOptions(&websocket.AcceptOptions{InsecureSkipVerify: true}),
	))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	baseURL := "ws" + strings.TrimPrefix(server.URL, "http")

	conn := dialRaw(t, baseURL+"/connectbidi.ping.v1.PingService/NoSuchMethod")
	writeEnvelope(t, conn, flagHeaders, []byte(`{"metadata":{"content-type":["application/connect+proto"]}}`))

	if flag, _ := readEnvelope(t, conn); flag != flagHeaders {
		t.Fatalf("first response envelope flag = 0x%02x, want 0x%02x", flag, flagHeaders)
	}
	flag, payload := readEnvelope(t, conn)
	if flag != flagEndStream {
		t.Fatalf("second response envelope flag = 0x%02x, want 0x%02x", flag, flagEndStream)
	}
	if !strings.Contains(string(payload), "unimplemented") {
		t.Errorf("end-stream payload = %s, want an unimplemented error", payload)
	}
}

// TestWireShape pins the bytes a draft 1 exchange puts on the wire: one
// envelope per binary WebSocket message, headers then data then end-stream,
// in both directions.
func TestWireShape(t *testing.T) {
	t.Parallel()
	_, _, _, baseURL := newTestServer(t)

	conn := dialRaw(t, baseURL+"/connectbidi.ping.v1.PingService/CumSum")
	writeEnvelope(t, conn, flagHeaders, []byte(`{"metadata":{"content-type":["application/connect+json"]}}`))
	writeEnvelope(t, conn, flagData, []byte(`{"number":"7"}`))
	writeEnvelope(t, conn, flagEndStream, nil)

	if flag, payload := readEnvelope(t, conn); flag != flagHeaders {
		t.Fatalf("response envelope 1: flag = 0x%02x, payload = %s", flag, payload)
	} else if !strings.Contains(string(payload), "application/connect+json") {
		t.Errorf("response headers = %s, want the negotiated content type", payload)
	}
	if flag, payload := readEnvelope(t, conn); flag != flagData {
		t.Fatalf("response envelope 2: flag = 0x%02x, payload = %s", flag, payload)
	} else if !strings.Contains(string(payload), `"7"`) {
		t.Errorf("response data = %s, want the cumulative sum", payload)
	}
	if flag, payload := readEnvelope(t, conn); flag != flagEndStream {
		t.Fatalf("response envelope 3: flag = 0x%02x, payload = %s", flag, payload)
	} else if strings.Contains(string(payload), "error") {
		t.Errorf("end-stream payload = %s, want a successful end", payload)
	}
}

// The flag values, restated here so the tests fail if the protocol changes
// rather than following it.
const (
	flagData      byte = 0x00
	flagEndStream byte = 0x02
	flagHeaders   byte = 0x06
)

func dialRaw(tb testing.TB, url string) *websocket.Conn {
	tb.Helper()
	conn, _, err := websocket.Dial(context.Background(), url, &websocket.DialOptions{ //nolint:bodyclose // coder/websocket closes the handshake response body itself
		Subprotocols: []string{"connect.bidi.d1"},
	})
	if err != nil {
		tb.Fatalf("dial %s: %v", url, err)
	}
	tb.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func writeEnvelope(tb testing.TB, conn *websocket.Conn, flag byte, payload []byte) {
	tb.Helper()
	message := make([]byte, 5+len(payload))
	message[0] = flag
	binary.BigEndian.PutUint32(message[1:5], uint32(len(payload)))
	copy(message[5:], payload)
	if err := conn.Write(context.Background(), websocket.MessageBinary, message); err != nil {
		tb.Fatalf("write envelope 0x%02x: %v", flag, err)
	}
}

func readEnvelope(tb testing.TB, conn *websocket.Conn) (byte, []byte) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msgType, message, err := conn.Read(ctx)
	if err != nil {
		tb.Fatalf("read envelope: %v", err)
	}
	if msgType != websocket.MessageBinary {
		tb.Fatalf("message type = %v, want binary", msgType)
	}
	if len(message) < 5 {
		tb.Fatalf("message is %d bytes, too short for an envelope head", len(message))
	}
	declared := binary.BigEndian.Uint32(message[1:5])
	payload := message[5:]
	if uint64(declared) != uint64(len(payload)) {
		tb.Fatalf("envelope declares %d payload bytes but the message carries %d", declared, len(payload))
	}
	return message[0], payload
}

// TestCancellationClosesTheSocket: with one RPC per connection there is no
// reset envelope, so cancelling is closing — and the handler's context must
// still end.
func TestCancellationClosesTheSocket(t *testing.T) {
	t.Parallel()
	impl, client, _, _ := newTestServer(t)
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
	cancel()
	select {
	case err := <-impl.cancelled:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("handler context ended with %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler context did not end after the client cancelled")
	}
	_ = stream.Close()
}

// TestAbandonedStreamReachesHandler: closing without reading to the end
// tears the socket down, which is the only cancellation signal draft 1 has.
func TestAbandonedStreamReachesHandler(t *testing.T) {
	t.Parallel()
	impl, client, _, _ := newTestServer(t)
	impl.countUpBlocks = true
	impl.cancelled = make(chan error, 1)

	stream, err := client.CountUp(context.Background(), &pingv1.CountUpRequest{Number: 1})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	if _, err := stream.Receive(); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-impl.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler context did not end after the client closed the socket")
	}
}

// TestRequestMetadata: the headers envelope is what carries metadata a
// browser cannot put on the handshake.
func TestRequestMetadata(t *testing.T) {
	t.Parallel()
	impl, client, _, _ := newTestServer(t)

	ctx, _ := connect.NewClientContext(context.Background())
	callInfo, _ := connect.CallInfoForClientContext(ctx)
	callInfo.RequestHeader().Set("X-Custom", "custom-val")
	stream, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: 1})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	for {
		if _, err := stream.Receive(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("Receive: %v", err)
		}
	}
	_ = stream.Close()

	seen := impl.requestHeader()
	if got := seen.Get("X-Custom"); got != "custom-val" {
		t.Errorf("handler saw x-custom = %q, want %q", got, "custom-val")
	}
	if got := seen.Get("Connect-Protocol-Version"); got != "1" {
		t.Errorf("handler saw connect-protocol-version = %q, want %q", got, "1")
	}
}

func TestJSONCodec(t *testing.T) {
	t.Parallel()
	impl, client, _, _ := newTestServer(t, draft1.WithSendCodec(connect.CodecNameJSON))

	stream, err := client.CountUp(context.Background(), &pingv1.CountUpRequest{Number: 2})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	var count int
	for {
		if _, err := stream.Receive(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		count++
	}
	_ = stream.Close()
	if count != 2 {
		t.Errorf("received %d messages, want 2", count)
	}
	if got := impl.requestHeader().Get("Content-Type"); got != "application/connect+json" {
		t.Errorf("handler saw content-type %q, want application/connect+json", got)
	}
}

func TestWithoutCompression(t *testing.T) {
	t.Parallel()
	_, client, _, _ := newTestServer(t, draft1.WithoutCompression())

	stream, err := client.CountUp(context.Background(), &pingv1.CountUpRequest{Number: 2})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	var count int
	for {
		if _, err := stream.Receive(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		count++
	}
	_ = stream.Close()
	if count != 2 {
		t.Errorf("received %d messages, want 2", count)
	}
}

// TestRPCErrorReachesClient: a failing handler produces an end-stream
// envelope carrying the Connect code, not a dropped connection.
func TestRPCErrorReachesClient(t *testing.T) {
	t.Parallel()
	_, client, _, _ := newTestServer(t)

	stream, err := client.CountUp(context.Background(), &pingv1.CountUpRequest{Number: -1})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	if _, err := stream.Receive(); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("Receive error = %v (code %v), want %v", err, connect.CodeOf(err), connect.CodeInvalidArgument)
	}
}

func TestUpgradeRejections(t *testing.T) {
	t.Parallel()
	_, _, _, baseURL := newTestServer(t)
	httpURL := "http" + strings.TrimPrefix(baseURL, "ws") + "/connectbidi.ping.v1.PingService/CountUp"

	t.Run("plain GET", func(t *testing.T) {
		t.Parallel()
		resp, err := http.Get(httpURL) //nolint:noctx // a test request
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusMethodNotAllowed)
		}
	})

	t.Run("upgrade without the subprotocol", func(t *testing.T) {
		t.Parallel()
		_, _, err := websocket.Dial(context.Background(), //nolint:bodyclose // coder/websocket closes the handshake response body itself
			strings.Replace(httpURL, "http", "ws", 1), nil)
		if err == nil {
			t.Fatal("dial without the subprotocol succeeded, want a rejection")
		}
	})
}
