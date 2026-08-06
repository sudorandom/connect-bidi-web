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

package connectwebsocket_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
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
	"github.com/sudorandom/connect-bidi-web/connectwebsocket"
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

func TestWebSocket(t *testing.T) {
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, testPingServer{
		respHeaders:  map[string]string{"X-Test-Header": "header-val"},
		respTrailers: map[string]string{"X-Test-Trailer": "trailer-val"},
	})

	wsHandler := connectwebsocket.NewHandler(connectServer, connectwebsocket.WithAcceptOptions(&websocket.AcceptOptions{
		InsecureSkipVerify: true,
	}))

	var connections atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connections.Add(1)
		wsHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	wsTransport := connectwebsocket.NewTransport(wsURL)
	client := pingv1connect.NewPingServiceClient(connect.NewClient(wsTransport))

	// 1. Unary call
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
	})

	// 2. Server streaming call
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

	// 3. Bidirectional streaming call
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

	if got, want := connections.Load(), int64(1); got != want {
		t.Errorf("WebSocket connections = %d, want %d (all RPCs multiplexed onto one connection)", got, want)
	}
}

// newTestServer starts an httptest server that counts WebSocket upgrades and
// serves the given ping service implementation.
func newTestServer(t *testing.T, impl pingv1connect.PingServiceHandler) (wsURL string, connections *atomic.Int64) {
	t.Helper()
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, impl)
	wsHandler := connectwebsocket.NewHandler(connectServer, connectwebsocket.WithAcceptOptions(&websocket.AcceptOptions{
		InsecureSkipVerify: true,
	}))

	connections = &atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connections.Add(1)
		wsHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http"), connections
}

func TestWebSocketConnectionPerStream(t *testing.T) {
	wsURL, connections := newTestServer(t, testPingServer{})
	wsTransport := connectwebsocket.NewTransport(wsURL, connectwebsocket.WithConnectionPerStream())
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

func TestWebSocketConcurrentStreams(t *testing.T) {
	wsURL, connections := newTestServer(t, testPingServer{})
	wsTransport := connectwebsocket.NewTransport(wsURL)
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

func TestWebSocketClientCancelResetsStream(t *testing.T) {
	impl := &cancelPingServer{handlerDone: make(chan error, 1)}
	wsURL, connections := newTestServer(t, impl)
	wsTransport := connectwebsocket.NewTransport(wsURL)
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

// TestWebSocketWireFormat exercises the wire protocol with hand-built
// frames: every binary message is a 4-byte big-endian stream ID followed by
// one Connect envelope, request and response frames of an RPC carry the same
// stream ID, and the headers envelope uses flag 0x07.
func TestWebSocketWireFormat(t *testing.T) {
	wsURL, _ := newTestServer(t, testPingServer{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil) //nolint:bodyclose // coder/websocket closes the handshake response body itself
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer func() {
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}()
	conn.SetReadLimit(-1)

	const streamID = uint32(7)
	buildFrame := func(id uint32, flag byte, payload []byte) []byte {
		frame := make([]byte, 9+len(payload))
		binary.BigEndian.PutUint32(frame[0:4], id)
		frame[4] = flag
		binary.BigEndian.PutUint32(frame[5:9], uint32(len(payload)))
		copy(frame[9:], payload)
		return frame
	}

	headersJSON := []byte(`{"metadata":{":path":["` + pingv1connect.PingServicePingProcedure + `"],"content-type":["application/connect+proto"]}}`)
	// An empty PingRequest encodes to zero bytes, so the data payload is empty.
	for _, frame := range [][]byte{
		buildFrame(streamID, 0x07, headersJSON), // headers
		buildFrame(streamID, 0x00, nil),         // data
		buildFrame(streamID, 0x02, nil),         // end-stream (half-close)
	} {
		if err := conn.Write(ctx, websocket.MessageBinary, frame); err != nil {
			t.Fatalf("write frame failed: %v", err)
		}
	}

	readFrame := func() (uint32, byte, []byte) {
		t.Helper()
		msgType, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read frame failed: %v", err)
		}
		if msgType != websocket.MessageBinary {
			t.Fatalf("message type = %v, want binary", msgType)
		}
		if len(data) < 9 {
			t.Fatalf("frame too short: %d bytes", len(data))
		}
		if got, want := binary.BigEndian.Uint32(data[5:9]), uint32(len(data)-9); got != want {
			t.Fatalf("envelope length = %d, want %d", got, want)
		}
		return binary.BigEndian.Uint32(data[0:4]), data[4], data[9:]
	}

	for _, want := range []struct {
		name string
		flag byte
	}{
		{name: "headers", flag: 0x07},
		{name: "data", flag: 0x00},
		{name: "end-stream", flag: 0x02},
	} {
		gotID, gotFlag, payload := readFrame()
		if gotID != streamID {
			t.Errorf("%s frame stream ID = %d, want %d", want.name, gotID, streamID)
		}
		if gotFlag != want.flag {
			t.Errorf("%s frame flag = 0x%02x, want 0x%02x", want.name, gotFlag, want.flag)
		}
		if want.name == "data" && len(payload) != 0 {
			t.Errorf("data frame payload = %d bytes, want empty (empty PingResponse)", len(payload))
		}
	}
}
