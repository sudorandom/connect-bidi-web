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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

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

	if got, want := connections.Load(), int64(4); got != want {
		t.Errorf("WebSocket connections = %d, want %d (one per RPC)", got, want)
	}
}
