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

package draft3_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft3"
	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	pingv1connect "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
	"golang.org/x/net/http2"
)

// requireExtendedConnect skips the test unless the process was started with
// extended CONNECT support enabled. Go's HTTP/2 stack reads GODEBUG once at
// init, so this cannot be toggled per test; the justfile sets it for the
// whole test run.
func requireExtendedConnect(t *testing.T) {
	t.Helper()
	if !strings.Contains(os.Getenv("GODEBUG"), "http2xconnect=1") {
		t.Skip("extended CONNECT disabled; run with GODEBUG=http2xconnect=1")
	}
}

// newH2TestServer serves the draft 3 handler over HTTP/2 (TLS, ALPN) and
// counts extended CONNECT requests, i.e. WebSocket-over-HTTP/2 connections.
func newH2TestServer(t *testing.T, impl pingv1connect.PingServiceHandler) (url string, connects *atomic.Int64) {
	t.Helper()
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, impl)
	wsHandler := draft3.NewHandler(connectServer)

	connects = &atomic.Int64{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			connects.Add(1)
		}
		wsHandler.ServeHTTP(w, r)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	return server.URL, connects
}

func newH2Client(t *testing.T, url string) pingv1connect.PingServiceClient {
	t.Helper()
	h2 := &http2.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{http2.NextProtoTLS},
		},
	}
	t.Cleanup(h2.CloseIdleConnections)
	transport := draft3.NewH2Transport(url, h2)
	// Unlike a hijacked HTTP/1.1 upgrade, the CONNECT stream is an active
	// request that httptest.Server.Close waits for; close the shared
	// connection so cleanup can finish.
	t.Cleanup(func() {
		if closer, ok := transport.(io.Closer); ok {
			_ = closer.Close()
		}
	})
	return pingv1connect.NewPingServiceClient(connect.NewClient(transport))
}

func TestWebSocketDraft3OverH2(t *testing.T) {
	requireExtendedConnect(t)
	url, connects := newH2TestServer(t, testPingServer{
		respHeaders:  map[string]string{"X-Test-Header": "header-val"},
		respTrailers: map[string]string{"X-Test-Trailer": "trailer-val"},
	})
	client := newH2Client(t, url)

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

	// Large payloads exercise HTTP/2 flow control and frame fragmentation.
	t.Run("LargeMessage", func(t *testing.T) {
		text := strings.Repeat("payload!", 512*1024/8) // 512 KiB
		resp, err := client.Ping(context.Background(), &pingv1.PingRequest{Text: text})
		if err != nil {
			t.Fatalf("Ping failed: %v", err)
		}
		if resp.GetText() != text {
			t.Errorf("large payload did not round-trip intact (%d bytes back)", len(resp.GetText()))
		}
	})

	if got, want := connects.Load(), int64(1); got != want {
		t.Errorf("extended CONNECT streams = %d, want %d (all RPCs multiplexed onto one WebSocket)", got, want)
	}
}

func TestWebSocketDraft3OverH2ClientCancelResetsStream(t *testing.T) {
	requireExtendedConnect(t)
	impl := &cancelPingServer{handlerDone: make(chan error, 1)}
	url, connects := newH2TestServer(t, impl)
	client := newH2Client(t, url)

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

	// The shared WebSocket (one CONNECT stream) must survive the reset.
	if _, err := client.Ping(context.Background(), &pingv1.PingRequest{Number: 1}); err != nil {
		t.Fatalf("Ping after reset failed: %v", err)
	}
	if got, want := connects.Load(), int64(1); got != want {
		t.Errorf("extended CONNECT streams = %d, want %d (reset must not tear down the connection)", got, want)
	}
}
