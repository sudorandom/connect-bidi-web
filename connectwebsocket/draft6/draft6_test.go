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

package draft6_test

import (
	"context"
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
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft6"
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

// newTestServer serves draft 6 on one endpoint and counts the WebSocket
// handshakes that reach it, which is how the reuse tests tell a pooled
// connection from a fresh one.
func newTestServer(tb testing.TB, opts ...draft6.Option) (*testPingServer, pingv1connect.PingServiceClient, *atomic.Int64, string) {
	tb.Helper()
	impl := &testPingServer{
		respHeaders:  map[string]string{"X-Test-Header": "header-val"},
		respTrailers: map[string]string{"X-Test-Trailer": "trailer-val"},
	}
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, impl)

	handlerOpts := append([]draft6.Option{
		draft6.WithAcceptOptions(&websocket.AcceptOptions{InsecureSkipVerify: true}),
	}, opts...)
	handler := draft6.NewHandler(connectServer, handlerOpts...)

	var handshakes atomic.Int64
	mux := http.NewServeMux()
	mux.Handle(draft6.DefaultPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handshakes.Add(1)
		handler.ServeHTTP(w, r)
	}))
	server := httptest.NewServer(mux)
	tb.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + draft6.DefaultPath
	transport := draft6.NewTransport(wsURL, opts...)
	tb.Cleanup(func() {
		if closer, ok := transport.(io.Closer); ok {
			_ = closer.Close()
		}
	})
	return impl, pingv1connect.NewPingServiceClient(connect.NewClient(transport)), &handshakes, wsURL
}

// TestConnectionReuse is the whole point of draft 6: a second RPC must not
// cost a second handshake.
func TestConnectionReuse(t *testing.T) {
	t.Parallel()
	_, client, handshakes, _ := newTestServer(t)

	for round := range 5 {
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

	if got := handshakes.Load(); got != 1 {
		t.Errorf("5 sequential RPCs cost %d handshakes, want 1", got)
	}
}

// TestConcurrentRPCsEachTakeAConnection: draft 6 does not multiplex, so
// overlapping RPCs need a connection each — and then those connections are
// pooled and reused by whatever runs next.
func TestConcurrentRPCsEachTakeAConnection(t *testing.T) {
	t.Parallel()
	_, client, handshakes, _ := newTestServer(t)

	const concurrency = 4
	streams := make([]pingv1connect.PingServiceCumSumClientStream, 0, concurrency)
	for i := range concurrency {
		stream, err := client.CumSum(context.Background())
		if err != nil {
			t.Fatalf("stream %d: CumSum: %v", i, err)
		}
		// Send on each stream so every connection is genuinely in use.
		if err := stream.Send(&pingv1.CumSumRequest{Number: 1}); err != nil {
			t.Fatalf("stream %d: Send: %v", i, err)
		}
		if _, err := stream.Receive(); err != nil {
			t.Fatalf("stream %d: Receive: %v", i, err)
		}
		streams = append(streams, stream)
	}
	if got := handshakes.Load(); got != concurrency {
		t.Errorf("%d concurrent RPCs cost %d handshakes, want %d", concurrency, got, concurrency)
	}

	for i, stream := range streams {
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("stream %d: CloseSend: %v", i, err)
		}
		if _, err := stream.Receive(); !errors.Is(err, io.EOF) {
			t.Fatalf("stream %d: Receive = %v, want io.EOF", i, err)
		}
		if err := stream.Close(); err != nil {
			t.Fatalf("stream %d: Close: %v", i, err)
		}
	}

	// All four are back in the pool now, so the next four RPCs are free.
	for i := range concurrency {
		stream, err := client.CumSum(context.Background())
		if err != nil {
			t.Fatalf("reuse %d: CumSum: %v", i, err)
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("reuse %d: CloseSend: %v", i, err)
		}
		for {
			if _, err := stream.Receive(); err != nil {
				break
			}
		}
		_ = stream.Close()
	}
	if got := handshakes.Load(); got != concurrency {
		t.Errorf("reusing %d pooled connections cost %d handshakes, want %d", concurrency, got, concurrency)
	}
}

// TestWithoutPooling pins the opt-out: with no idle connections kept, every
// RPC dials, which is draft 5's cost model.
func TestWithoutPooling(t *testing.T) {
	t.Parallel()
	_, client, handshakes, _ := newTestServer(t, draft6.WithMaxIdleConns(0))

	for range 3 {
		stream, err := client.CumSum(context.Background())
		if err != nil {
			t.Fatalf("CumSum: %v", err)
		}
		_ = stream.CloseSend()
		for {
			if _, err := stream.Receive(); err != nil {
				break
			}
		}
		_ = stream.Close()
	}
	if got := handshakes.Load(); got != 3 {
		t.Errorf("3 RPCs without pooling cost %d handshakes, want 3", got)
	}
}

// TestFailedRPCDoesNotPoisonThePool: an RPC that ends in an application
// error still ended exactly as the protocol says, so its connection is
// still good.
func TestFailedRPCDoesNotPoisonThePool(t *testing.T) {
	t.Parallel()
	_, client, handshakes, _ := newTestServer(t)

	stream, err := client.CountUp(context.Background(), &pingv1.CountUpRequest{Number: -1})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	if _, err := stream.Receive(); err == nil {
		t.Fatal("expected an error")
	} else if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("code = %v, want invalid_argument", connect.CodeOf(err))
	}
	_ = stream.Close()

	// The next RPC should find that connection waiting.
	next, err := client.CumSum(context.Background())
	if err != nil {
		t.Fatalf("CumSum: %v", err)
	}
	_ = next.CloseSend()
	for {
		if _, err := next.Receive(); err != nil {
			break
		}
	}
	_ = next.Close()

	if got := handshakes.Load(); got != 1 {
		t.Errorf("an application error cost the pool its connection: %d handshakes, want 1", got)
	}
}

func TestServerStreaming(t *testing.T) {
	t.Parallel()
	_, client, _, _ := newTestServer(t)

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
	if len(numbers) != 3 {
		t.Errorf("numbers = %v, want three", numbers)
	}
	if got, want := callInfo.Protocol, "websocket-draft6"; got != want {
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
	_, client, _, _ := newTestServer(t)

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

func TestBidiStreaming(t *testing.T) {
	t.Parallel()
	_, client, _, _ := newTestServer(t)

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
}

// TestEmptyMessagesAreNotSeparators: inherited from draft 5, and worth
// re-pinning here because draft 6 reuses connections — a stray half-close
// would corrupt the *next* RPC too.
func TestEmptyMessagesAreNotSeparators(t *testing.T) {
	t.Parallel()
	_, client, _, _ := newTestServer(t)

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
}

// TestUnaryIsRefused: draft 6 leaves unary RPCs on plain Connect HTTP, and
// says so rather than half-working.
func TestUnaryIsRefused(t *testing.T) {
	t.Parallel()
	_, client, _, _ := newTestServer(t)

	_, err := client.Ping(context.Background(), &pingv1.PingRequest{Number: 1})
	if err == nil {
		t.Fatal("a unary RPC was carried over the WebSocket")
	}
	if got := connect.CodeOf(err); got != connect.CodeUnimplemented {
		t.Errorf("code = %v, want unimplemented", got)
	}
	if !strings.Contains(err.Error(), "CompositeTransport") {
		t.Errorf("error should point at the composite transport, got %q", err)
	}
}

func TestCancellationReachesHandler(t *testing.T) {
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
	// The pseudo-header names the procedure but is protocol metadata, so it
	// must not reach the handler as a request header.
	if got := header.Get(":path"); got != "" {
		t.Errorf(":path leaked into request metadata as %q", got)
	}
}

func TestJSONCodec(t *testing.T) {
	t.Parallel()
	_, client, _, _ := newTestServer(t, draft6.WithSendCodec(connect.CodecNameJSON))

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

func TestUpgradeRejections(t *testing.T) {
	t.Parallel()
	_, _, _, wsURL := newTestServer(t)

	t.Run("MissingSubprotocol", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, resp, err := websocket.Dial(ctx, wsURL, nil) //nolint:bodyclose
		if err == nil {
			_ = conn.CloseNow()
			t.Fatal("handshake succeeded without the subprotocol")
		}
		if resp == nil || resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status = %v, want 400", resp)
		}
	})
}
