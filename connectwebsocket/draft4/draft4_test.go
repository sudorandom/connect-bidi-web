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

package draft4_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft4"
	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	pingv1connect "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type testPingServer struct {
	respHeaders  map[string]string
	respTrailers map[string]string
}

func (s testPingServer) Ping(ctx context.Context, req *pingv1.PingRequest) (*pingv1.PingResponse, error) {
	if info, ok := connect.CallInfoForServerContext(ctx); ok {
		for k, v := range s.respHeaders {
			info.ResponseHeader().Set(k, v)
		}
		for k, v := range s.respTrailers {
			info.ResponseTrailer().Set(k, v)
		}
	}
	return &pingv1.PingResponse{Number: req.GetNumber(), Text: req.GetText()}, nil
}

func (testPingServer) Fail(_ context.Context, _ *pingv1.FailRequest) (*pingv1.FailResponse, error) {
	detail, err := connectproto.NewErrorDetail(wrapperspb.String("detail"))
	if err != nil {
		return nil, err
	}
	return nil, connect.NewError(connect.CodeUnimplemented, "Fail").WithDetail(detail)
}

func (testPingServer) Sum(_ context.Context, stream pingv1connect.PingServiceSumServerStream) (*pingv1.SumResponse, error) {
	var total int64
	for {
		req, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		total += req.GetNumber()
	}
	return &pingv1.SumResponse{Sum: total}, nil
}

func (testPingServer) CountUp(_ context.Context, req *pingv1.CountUpRequest, stream pingv1connect.PingServiceCountUpServerStream) error {
	for i := int64(1); i <= req.GetNumber(); i++ {
		if err := stream.Send(&pingv1.CountUpResponse{Number: i}); err != nil {
			return err
		}
	}
	return nil
}

func (testPingServer) CumSum(_ context.Context, stream pingv1connect.PingServiceCumSumServerStream) error {
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

func TestWebSocketDraft4(t *testing.T) {
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, testPingServer{
		respHeaders:  map[string]string{"X-Test-Header": "header-val"},
		respTrailers: map[string]string{"X-Test-Trailer": "trailer-val"},
	})

	wsHandler := draft4.NewHandler(connectServer, draft4.WithAcceptOptions(&websocket.AcceptOptions{
		InsecureSkipVerify: true,
	}))

	var connections atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connections.Add(1)
		wsHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	wsTransport := draft4.NewTransport(wsURL)
	client := pingv1connect.NewPingServiceClient(connect.NewClient(wsTransport))

	t.Run("Unary", func(t *testing.T) {
		ctx, callInfo := connect.NewClientContext(context.Background())
		resp, err := client.Ping(ctx, &pingv1.PingRequest{Number: 42, Text: "hello"})
		if err != nil {
			t.Fatalf("Ping failed: %v", err)
		}
		if resp.GetNumber() != 42 || resp.GetText() != "hello" {
			t.Errorf("unexpected response: %+v", resp)
		}

		if got := callInfo.ResponseHeader().Get("X-Test-Header"); got != "header-val" {
			t.Errorf("expected X-Test-Header 'header-val', got %q", got)
		}
		if got := callInfo.ResponseTrailer().Get("X-Test-Trailer"); got != "trailer-val" {
			t.Errorf("expected X-Test-Trailer 'trailer-val', got %q", got)
		}
		if got, want := callInfo.Protocol, "websocket-draft4"; got != want {
			t.Errorf("CallInfo.Protocol = %q, want %q", got, want)
		}
	})

	t.Run("ServerStreaming", func(t *testing.T) {
		stream, err := client.CountUp(context.Background(), &pingv1.CountUpRequest{Number: 3})
		if err != nil {
			t.Fatalf("CountUp failed: %v", err)
		}
		defer stream.Close()

		var numbers []int64
		for {
			res, err := stream.Receive()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("Receive failed: %v", err)
			}
			numbers = append(numbers, res.GetNumber())
		}

		if len(numbers) != 3 || numbers[0] != 1 || numbers[1] != 2 || numbers[2] != 3 {
			t.Errorf("unexpected numbers: %v", numbers)
		}
	})

	t.Run("BidiStreaming", func(t *testing.T) {
		stream, err := client.CumSum(context.Background())
		if err != nil {
			t.Fatalf("CumSum failed: %v", err)
		}
		defer stream.Close()

		inputs := []int64{1, 2, 3, 4}
		expectedSums := []int64{1, 3, 6, 10}

		for i, val := range inputs {
			if err := stream.Send(&pingv1.CumSumRequest{Number: val}); err != nil {
				t.Fatalf("Send failed on item %d: %v", i, err)
			}
			res, err := stream.Receive()
			if err != nil {
				t.Fatalf("Receive failed on item %d: %v", i, err)
			}
			if res.GetSum() != expectedSums[i] {
				t.Errorf("expected sum %d, got %d", expectedSums[i], res.GetSum())
			}
		}

		if err := stream.CloseSend(); err != nil {
			t.Fatalf("CloseSend failed: %v", err)
		}
	})

	t.Run("ErrorDetails", func(t *testing.T) {
		_, err := client.Fail(context.Background(), &pingv1.FailRequest{})
		var connectErr *connect.Error
		if !errors.As(err, &connectErr) {
			t.Fatalf("Fail error = %v, want *connect.Error", err)
		}
		if got, want := connectErr.Code(), connect.CodeUnimplemented; got != want {
			t.Errorf("error code = %v, want %v", got, want)
		}
		details := connectErr.Details()
		if len(details) != 1 {
			t.Fatalf("error detail count = %d, want 1", len(details))
		}
		message, err := connectproto.UnmarshalErrorDetail(details[0])
		if err != nil {
			t.Fatalf("unmarshal error detail: %v", err)
		}
		if got, want := message.(*wrapperspb.StringValue).GetValue(), "detail"; got != want {
			t.Errorf("error detail = %q, want %q", got, want)
		}
	})

	// The field separator is not escaped anywhere, so a payload full of
	// separators must survive the round trip untouched -- the parser splits
	// on the first two only.
	t.Run("PayloadContainingSeparators", func(t *testing.T) {
		text := "a|b||c|" + strings.Repeat("|", 32)
		resp, err := client.Ping(context.Background(), &pingv1.PingRequest{Text: text})
		if err != nil {
			t.Fatalf("Ping failed: %v", err)
		}
		if resp.GetText() != text {
			t.Errorf("text = %q, want %q", resp.GetText(), text)
		}
	})

	if got, want := connections.Load(), int64(1); got != want {
		t.Errorf("WebSocket connections = %d, want %d (all RPCs multiplexed onto one connection)", got, want)
	}
}

// newTestServer starts an httptest server that counts WebSocket upgrades and
// serves the given ping service implementation.
func newTestServer(t *testing.T, impl pingv1connect.PingServiceHandler, opts ...draft4.Option) (wsURL string, connections *atomic.Int64) {
	t.Helper()
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, impl)
	opts = append(opts, draft4.WithAcceptOptions(&websocket.AcceptOptions{
		InsecureSkipVerify: true,
	}))
	wsHandler := draft4.NewHandler(connectServer, opts...)

	connections = &atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connections.Add(1)
		wsHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http"), connections
}

func TestWebSocketDraft4ConnectionPerStream(t *testing.T) {
	wsURL, connections := newTestServer(t, testPingServer{})
	wsTransport := draft4.NewTransport(wsURL, draft4.WithConnectionPerStream())
	client := pingv1connect.NewPingServiceClient(connect.NewClient(wsTransport))

	// Unary RPCs share the multiplexed connection even with
	// WithConnectionPerStream.
	for range 2 {
		if _, err := client.Ping(context.Background(), &pingv1.PingRequest{Number: 1}); err != nil {
			t.Fatalf("Ping failed: %v", err)
		}
	}
	if got, want := connections.Load(), int64(1); got != want {
		t.Fatalf("connections after unary calls = %d, want %d", got, want)
	}

	// Each streaming RPC dials a dedicated connection.
	for i := int64(1); i <= 2; i++ {
		stream, err := client.CountUp(context.Background(), &pingv1.CountUpRequest{Number: 2})
		if err != nil {
			t.Fatalf("CountUp failed: %v", err)
		}
		for {
			if _, err := stream.Receive(); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatalf("Receive failed: %v", err)
			}
		}
		if err := stream.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
		if got, want := connections.Load(), 1+i; got != want {
			t.Fatalf("connections after %d streaming calls = %d, want %d", i, got, want)
		}
	}
}

func TestWebSocketDraft4ConcurrentStreams(t *testing.T) {
	wsURL, connections := newTestServer(t, testPingServer{})
	wsTransport := draft4.NewTransport(wsURL)
	client := pingv1connect.NewPingServiceClient(connect.NewClient(wsTransport))

	var group sync.WaitGroup
	for range 3 {
		group.Add(1)
		go func() {
			defer group.Done()
			stream, err := client.CumSum(context.Background())
			if err != nil {
				t.Errorf("CumSum failed: %v", err)
				return
			}
			defer stream.Close()
			var sum int64
			for i := int64(1); i <= 10; i++ {
				if err := stream.Send(&pingv1.CumSumRequest{Number: i}); err != nil {
					t.Errorf("Send failed: %v", err)
					return
				}
				res, err := stream.Receive()
				if err != nil {
					t.Errorf("Receive failed: %v", err)
					return
				}
				sum += i
				if res.GetSum() != sum {
					t.Errorf("sum = %d, want %d", res.GetSum(), sum)
					return
				}
			}
			if err := stream.CloseSend(); err != nil {
				t.Errorf("CloseSend failed: %v", err)
			}
		}()
	}
	group.Wait()

	if got, want := connections.Load(), int64(1); got != want {
		t.Errorf("WebSocket connections = %d, want %d (concurrent streams multiplexed)", got, want)
	}
}

// cancelPingServer blocks its CumSum handler until the RPC context is
// canceled, recording how the handler ended.
type cancelPingServer struct {
	testPingServer
	handlerDone chan error
}

func (s *cancelPingServer) CumSum(ctx context.Context, stream pingv1connect.PingServiceCumSumServerStream) error {
	req, err := stream.Receive()
	if err != nil {
		s.handlerDone <- err
		return err
	}
	if err := stream.Send(&pingv1.CumSumResponse{Sum: req.GetNumber()}); err != nil {
		s.handlerDone <- err
		return err
	}
	<-ctx.Done()
	s.handlerDone <- ctx.Err()
	return ctx.Err()
}

func TestWebSocketDraft4ClientCancelResetsStream(t *testing.T) {
	impl := &cancelPingServer{handlerDone: make(chan error, 1)}
	wsURL, connections := newTestServer(t, impl)
	wsTransport := draft4.NewTransport(wsURL)
	client := pingv1connect.NewPingServiceClient(connect.NewClient(wsTransport))

	stream, err := client.CumSum(context.Background())
	if err != nil {
		t.Fatalf("CumSum failed: %v", err)
	}
	if err := stream.Send(&pingv1.CumSumRequest{Number: 5}); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if _, err := stream.Receive(); err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	// Abandon the RPC mid-stream. The client sends a reset frame, which must
	// cancel the handler's context on the server.
	if err := stream.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	select {
	case handlerErr := <-impl.handlerDone:
		if !errors.Is(handlerErr, context.Canceled) {
			t.Errorf("handler ended with %v, want context.Canceled", handlerErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server handler was not canceled after client reset")
	}

	// The shared connection must survive the reset and remain usable.
	if _, err := client.Ping(context.Background(), &pingv1.PingRequest{Number: 1}); err != nil {
		t.Fatalf("Ping after reset failed: %v", err)
	}
	if got, want := connections.Load(), int64(1); got != want {
		t.Errorf("WebSocket connections = %d, want %d (reset must not tear down the connection)", got, want)
	}
}

// dialRaw opens a WebSocket to url with compression disabled, so the frames
// the assertions below read are the frames that went on the wire.
func dialRaw(ctx context.Context, t *testing.T, url string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{ //nolint:bodyclose // coder/websocket closes the handshake response body itself
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.CloseNow()
	})
	conn.SetReadLimit(-1)
	return conn
}

// TestWebSocketDraft4WireFormat exercises the wire protocol with hand-built
// frames. Every message is an ASCII head, "<stream ID>|<flags>|", followed
// by the payload, and requires no decoding to read.
func TestWebSocketDraft4WireFormat(t *testing.T) {
	wsURL, _ := newTestServer(t, testPingServer{}, draft4.WithoutCompression())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn := dialRaw(ctx, t, wsURL)

	headersJSON := `{"metadata":{":path":["` + pingv1connect.PingServicePingProcedure + `"],"content-type":["application/connect+proto"]}}`
	// An empty PingRequest encodes to zero bytes, so the data payload is empty.
	for _, frame := range []string{
		"7|1|" + headersJSON, // headers
		"7|0|",               // data
		"7|2|",               // end-stream (half-close)
	} {
		if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
			t.Fatalf("write frame %q failed: %v", frame, err)
		}
	}

	for _, want := range []struct {
		name    string
		head    string
		msgType websocket.MessageType
	}{
		// Control payloads are JSON, so their frames are text; a proto data
		// payload is not UTF-8, so its frame is binary.
		{name: "headers", head: "7|1|", msgType: websocket.MessageText},
		{name: "data", head: "7|0|", msgType: websocket.MessageBinary},
		{name: "end-stream", head: "7|2|", msgType: websocket.MessageText},
	} {
		msgType, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read %s frame failed: %v", want.name, err)
		}
		if msgType != want.msgType {
			t.Errorf("%s frame message type = %v, want %v", want.name, msgType, want.msgType)
		}
		if !strings.HasPrefix(string(data), want.head) {
			t.Errorf("%s frame = %q, want the head %q", want.name, data, want.head)
		}
		if want.name == "data" {
			if got := string(data); got != want.head {
				t.Errorf("data frame = %q, want %q (empty PingResponse)", got, want.head)
			}
		}
	}
}

// TestWebSocketDraft4AllTextWithJSONCodec is the property draft 4 exists
// for: with the JSON codec every frame on the connection, data included, is
// a text WebSocket message whose whole content is readable.
func TestWebSocketDraft4AllTextWithJSONCodec(t *testing.T) {
	wsURL, _ := newTestServer(t, testPingServer{}, draft4.WithoutCompression())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn := dialRaw(ctx, t, wsURL)

	headersJSON := `{"metadata":{":path":["` + pingv1connect.PingServicePingProcedure + `"],"content-type":["application/connect+json"]}}`
	for _, frame := range []string{
		"1|1|" + headersJSON,
		`1|0|{"text":"hello"}`,
		"1|2|",
	} {
		if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
			t.Fatalf("write frame %q failed: %v", frame, err)
		}
	}

	var sawData bool
	for range 3 {
		msgType, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read frame failed: %v", err)
		}
		if msgType != websocket.MessageText {
			t.Errorf("frame %q message type = %v, want text", data, msgType)
		}
		if strings.HasPrefix(string(data), "1|0|") {
			sawData = true
			if got, want := string(data), `1|0|{"text":"hello"}`; got != want {
				t.Errorf("data frame = %q, want %q", got, want)
			}
		}
	}
	if !sawData {
		t.Error("never saw a data frame")
	}
}

// TestWebSocketDraft4IgnoresUnknownFlags is the forward-compatibility
// guarantee: a peer built against a later revision may set flag bits this
// one has never heard of, and its frames must still be served. The type
// lives in the low 3 bits; everything above is ignored.
func TestWebSocketDraft4IgnoresUnknownFlags(t *testing.T) {
	wsURL, _ := newTestServer(t, testPingServer{}, draft4.WithoutCompression())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn := dialRaw(ctx, t, wsURL)

	headersJSON := `{"metadata":{":path":["` + pingv1connect.PingServicePingProcedure + `"],"content-type":["application/connect+json"]}}`
	// Every frame carries all five undefined flag bits (0xF8 = 248) ORed
	// onto its type: headers 1|248 = 249, data 0|248 = 248, end-stream
	// 2|248 = 250.
	for _, frame := range []string{
		"1|249|" + headersJSON,
		`1|248|{"text":"flagged"}`,
		"1|250|",
	} {
		if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
			t.Fatalf("write frame %q failed: %v", frame, err)
		}
	}

	var sawData bool
	for range 3 {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read failed (server rejected an unknown flag?): %v", err)
		}
		if strings.HasPrefix(string(data), "1|0|") {
			sawData = true
			if got, want := string(data), `1|0|{"text":"flagged"}`; got != want {
				t.Errorf("data frame = %q, want %q", got, want)
			}
		}
	}
	if !sawData {
		t.Error("never saw a data frame; the flagged request was not served")
	}
}

// TestWebSocketDraft4RejectsReservedFrameType is the other half of the
// contract: an unknown *type* can't be guessed at, so the stream fails
// rather than silently mishandling the payload.
func TestWebSocketDraft4RejectsReservedFrameType(t *testing.T) {
	wsURL, _ := newTestServer(t, testPingServer{}, draft4.WithoutCompression())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn := dialRaw(ctx, t, wsURL)

	headersJSON := `{"metadata":{":path":["` + pingv1connect.PingServiceCumSumProcedure + `"],"content-type":["application/connect+json"]}}`
	if err := conn.Write(ctx, websocket.MessageText, []byte("1|1|"+headersJSON)); err != nil {
		t.Fatalf("write headers failed: %v", err)
	}
	// Frame type 5 is reserved: within the type mask, but undefined.
	if err := conn.Write(ctx, websocket.MessageText, []byte("1|5|{}")); err != nil {
		t.Fatalf("write reserved-type frame failed: %v", err)
	}

	// The server answers with headers, then finishes the stream with an
	// error end-stream rather than treating the frame as data.
	var end string
	for range 2 {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
		if strings.HasPrefix(string(data), "1|2|") {
			end = string(data)
		}
	}
	if end == "" {
		t.Fatal("no end-stream frame; the reserved type was not rejected")
	}
	if !strings.Contains(end, "unknown frame type") {
		t.Errorf("end-stream = %q, want it to report an unknown frame type", end)
	}
}

// TestWebSocketDraft4MalformedFrameEndsConnection verifies that a frame
// whose head can't be parsed takes the connection down rather than being
// skipped: framing has been lost, so nothing after it can be trusted.
func TestWebSocketDraft4MalformedFrameEndsConnection(t *testing.T) {
	wsURL, _ := newTestServer(t, testPingServer{}, draft4.WithoutCompression())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn := dialRaw(ctx, t, wsURL)

	if err := conn.Write(ctx, websocket.MessageText, []byte("this frame has no separators")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if _, _, err := conn.Read(ctx); err == nil {
		t.Error("read after a malformed frame succeeded, want the connection closed")
	}
}

// TestWebSocketDraft4PermessageDeflate performs a raw handshake offering
// permessage-deflate, as browsers do, and verifies the server accepts it:
// unlike draft 3, draft 4 has no compression of its own and relies on the
// extension.
func TestWebSocketDraft4PermessageDeflate(t *testing.T) {
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, testPingServer{})
	server := httptest.NewServer(draft4.NewHandler(connectServer))
	t.Cleanup(server.Close)

	host := strings.TrimPrefix(server.URL, "http://")
	conn, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	request := "GET / HTTP/1.1\r\n" +
		"Host: " + host + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
		"Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n" +
		"\r\n"
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatalf("write handshake failed: %v", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read handshake response failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("handshake status = %d, want %d", resp.StatusCode, http.StatusSwitchingProtocols)
	}
	if got := resp.Header.Get("Sec-WebSocket-Extensions"); !strings.Contains(got, "permessage-deflate") {
		t.Errorf("Sec-WebSocket-Extensions = %q, want permessage-deflate accepted", got)
	}
	// Draft 4 defines no subprotocol: it is identified by path, like drafts
	// the low three bits.
	if got := resp.Header.Get("Sec-WebSocket-Protocol"); got != "" {
		t.Errorf("Sec-WebSocket-Protocol = %q, want empty", got)
	}
}

// TestWebSocketDraft4WithoutCompression verifies the option reaches the
// handshake, and that RPCs still work with the extension declined.
func TestWebSocketDraft4WithoutCompression(t *testing.T) {
	wsURL, _ := newTestServer(t, testPingServer{}, draft4.WithoutCompression())

	client := pingv1connect.NewPingServiceClient(connect.NewClient(
		draft4.NewTransport(wsURL, draft4.WithoutCompression()),
	))
	resp, err := client.Ping(context.Background(), &pingv1.PingRequest{Number: 7, Text: "plain"})
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
	if resp.GetText() != "plain" {
		t.Errorf("unexpected response: %+v", resp)
	}
}

// TestWebSocketDraft4JSONCodecRoundTrip runs the full client stack with the
// JSON codec, the configuration that makes every frame legible.
func TestWebSocketDraft4JSONCodecRoundTrip(t *testing.T) {
	wsURL, _ := newTestServer(t, testPingServer{})
	client := pingv1connect.NewPingServiceClient(connect.NewClient(
		draft4.NewTransport(wsURL, draft4.WithSendCodec(connect.CodecNameJSON)),
	))

	ctx, callInfo := connect.NewClientContext(context.Background())
	resp, err := client.Ping(ctx, &pingv1.PingRequest{Number: 3, Text: "json"})
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
	if resp.GetNumber() != 3 || resp.GetText() != "json" {
		t.Errorf("unexpected response: %+v", resp)
	}
	// The server echoes the negotiated codec in the response content type,
	// which is how we know the JSON codec actually reached the wire.
	if got, want := callInfo.ResponseHeader().Get("Content-Type"), "application/connect+json"; got != want {
		t.Errorf("response Content-Type = %q, want %q", got, want)
	}

	stream, err := client.CumSum(context.Background())
	if err != nil {
		t.Fatalf("CumSum failed: %v", err)
	}
	defer stream.Close()
	if err := stream.Send(&pingv1.CumSumRequest{Number: 4}); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	res, err := stream.Receive()
	if err != nil {
		t.Fatalf("Receive failed: %v", err)
	}
	if res.GetSum() != 4 {
		t.Errorf("sum = %d, want 4", res.GetSum())
	}
}
