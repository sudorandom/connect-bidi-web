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

package connectfallback_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect/v2"
	"github.com/sudorandom/connect-bidi-web/connectfallback"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft2"
	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	pingv1connect "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
	"golang.org/x/net/http2"
)

// fakeStream is a no-op connect.ClientStream, used only as a non-nil
// success value from fake rungs.
type fakeStream struct{}

func (fakeStream) SendHeaders() error { return nil }
func (fakeStream) Send(any) error     { return nil }
func (fakeStream) CloseSend() error   { return nil }
func (fakeStream) Receive(any) error  { return io.EOF }
func (fakeStream) Close() error       { return nil }

// fakeRung fails with err when set, and counts calls either way.
type fakeRung struct {
	err   error
	calls int
}

func (r *fakeRung) NewClientStream(context.Context, connect.Spec) (connect.ClientStream, error) {
	r.calls++
	if r.err != nil {
		return nil, r.err
	}
	return fakeStream{}, nil
}

func unavailable() error {
	return connect.Errorf(connect.CodeUnavailable, "transport down")
}

func TestFallbackDescendsAndSticks(t *testing.T) {
	first := &fakeRung{err: unavailable()}
	second := &fakeRung{err: unavailable()}
	third := &fakeRung{}
	transport := connectfallback.New(first, second, third)

	if _, err := transport.NewClientStream(context.Background(), connect.Spec{}); err != nil {
		t.Fatalf("NewClientStream failed: %v", err)
	}
	if first.calls != 1 || second.calls != 1 || third.calls != 1 {
		t.Errorf("calls = %d/%d/%d, want 1/1/1", first.calls, second.calls, third.calls)
	}

	// The working rung is remembered: the failed rungs above it are not
	// retried on the next RPC.
	if _, err := transport.NewClientStream(context.Background(), connect.Spec{}); err != nil {
		t.Fatalf("second NewClientStream failed: %v", err)
	}
	if first.calls != 1 || second.calls != 1 || third.calls != 2 {
		t.Errorf("calls = %d/%d/%d, want 1/1/2", first.calls, second.calls, third.calls)
	}
}

func TestFallbackWrapsAroundWhenCurrentRungFails(t *testing.T) {
	first := &fakeRung{}
	second := &fakeRung{err: unavailable()}
	transport := connectfallback.New(second, first)

	// Settle on the second rung (index 1).
	if _, err := transport.NewClientStream(context.Background(), connect.Spec{}); err != nil {
		t.Fatalf("NewClientStream failed: %v", err)
	}

	// It starts failing; the ladder wraps around to the top, where the
	// first rung has recovered.
	first.err = unavailable()
	second.err = nil
	first.calls, second.calls = 0, 0
	if _, err := transport.NewClientStream(context.Background(), connect.Spec{}); err != nil {
		t.Fatalf("NewClientStream after failure failed: %v", err)
	}
	if first.calls != 1 || second.calls != 1 {
		t.Errorf("calls = %d/%d, want 1/1 (wrap around to the top)", second.calls, first.calls)
	}
}

func TestFallbackSurfacesNonUnavailableErrors(t *testing.T) {
	rpcErr := connect.Errorf(connect.CodeInvalidArgument, "bad request")
	first := &fakeRung{err: rpcErr}
	second := &fakeRung{}
	transport := connectfallback.New(first, second)

	_, err := transport.NewClientStream(context.Background(), connect.Spec{})
	if !errors.Is(err, rpcErr) {
		t.Fatalf("error = %v, want the rung's own error", err)
	}
	if second.calls != 0 {
		t.Errorf("second rung called %d times, want 0 (no laddering on RPC-level errors)", second.calls)
	}
}

func TestFallbackAllRungsFail(t *testing.T) {
	first := &fakeRung{err: unavailable()}
	second := &fakeRung{err: unavailable()}
	transport := connectfallback.New(first, second)

	_, err := transport.NewClientStream(context.Background(), connect.Spec{})
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("error = %v, want CodeUnavailable", err)
	}
}

func TestFallbackStopsOnCanceledContext(t *testing.T) {
	first := &fakeRung{err: unavailable()}
	second := &fakeRung{}
	transport := connectfallback.New(first, second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := transport.NewClientStream(ctx, connect.Spec{}); err == nil {
		t.Fatal("expected an error on a canceled context")
	}
	if second.calls != 0 {
		t.Errorf("second rung called %d times, want 0 (no laddering after cancellation)", second.calls)
	}
}

// pingServer echoes, for the integration test.
type pingServer struct {
	pingv1connect.UnimplementedPingServiceHandler
}

func (pingServer) Ping(_ context.Context, req *pingv1.PingRequest) (*pingv1.PingResponse, error) {
	return &pingv1.PingResponse{Number: req.GetNumber(), Text: req.GetText()}, nil
}

// TestFallbackWebSocketH2ToH1 runs the real ladder against an
// HTTP/1.1-only server: the WebSocket-over-HTTP/2 rung cannot establish
// (no h2 ALPN), so RPCs degrade to the HTTP/1.1 upgrade and succeed.
func TestFallbackWebSocketH2ToH1(t *testing.T) {
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, pingServer{})
	server := httptest.NewServer(draft2.NewHandler(connectServer))
	t.Cleanup(server.Close)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	h2 := &http2.Transport{
		// The server speaks plain HTTP/1.1; this dial cannot succeed, which
		// is the point.
		AllowHTTP: false,
	}
	transport := connectfallback.New(
		draft2.NewH2Transport(server.URL, h2),
		draft2.NewTransport(wsURL),
	)
	t.Cleanup(func() { _ = transport.Close() })

	client := pingv1connect.NewPingServiceClient(connect.NewClient(transport))
	resp, err := client.Ping(context.Background(), &pingv1.PingRequest{Number: 7, Text: "ladder"})
	if err != nil {
		t.Fatalf("Ping over the ladder failed: %v", err)
	}
	if resp.GetNumber() != 7 || resp.GetText() != "ladder" {
		t.Errorf("unexpected response: %+v", resp)
	}

	resp2, err := http.Get(server.URL + "/healthz")
	if err != nil {
		t.Fatalf("server became unreachable: %v", err)
	}
	_ = resp2.Body.Close()
}
