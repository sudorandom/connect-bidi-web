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
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft4"
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

// newH2TestServer serves the draft 4 handler over HTTP/2 (TLS, ALPN) and
// counts extended CONNECT requests, i.e. WebSocket-over-HTTP/2 connections.
func newH2TestServer(t *testing.T, impl pingv1connect.PingServiceHandler) (url string, connects *atomic.Int64) {
	t.Helper()
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, impl)
	wsHandler := draft4.NewHandler(connectServer)

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
	transport := draft4.NewH2Transport(url, h2)
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

func TestWebSocketDraft4OverH2(t *testing.T) {
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

func TestWebSocketDraft4OverH2ClientCancelResetsStream(t *testing.T) {
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

// TestWebSocketDraft4OverH2Compresses is the regression test for the gap
// this path used to have: draft 4 delegates compression to the WebSocket
// layer, and the HTTP/2 layer had no implementation of it, so a highly
// compressible payload went out whole with no error. RFC 8441 §5 keeps
// Sec-WebSocket-Extensions, so the extension is negotiable here; this
// asserts it is negotiated and that it actually shrinks the wire.
func TestWebSocketDraft4OverH2Compresses(t *testing.T) {
	requireExtendedConnect(t)

	// A payload big and repetitive enough that deflate is unmistakable.
	text := strings.Repeat("all work and no play makes jack a dull boy. ", 400)

	measure := func(t *testing.T, opts ...draft4.Option) int64 {
		t.Helper()
		connectServer := connect.NewServer()
		pingv1connect.RegisterPingServiceHandler(connectServer, testPingServer{})
		handler := draft4.NewHandler(connectServer, opts...)

		var rx atomic.Int64
		server := httptest.NewUnstartedServer(handler)
		server.Listener = &countingListener{Listener: server.Listener, rx: &rx}
		server.EnableHTTP2 = true
		server.StartTLS()
		t.Cleanup(server.Close)

		h2 := &http2.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{http2.NextProtoTLS},
			},
		}
		t.Cleanup(h2.CloseIdleConnections)
		transport := draft4.NewH2Transport(server.URL, h2, opts...)
		t.Cleanup(func() {
			if closer, ok := transport.(io.Closer); ok {
				_ = closer.Close()
			}
		})
		client := pingv1connect.NewPingServiceClient(connect.NewClient(transport))

		// Warm up the connection, then measure one request.
		if _, err := client.Ping(context.Background(), &pingv1.PingRequest{}); err != nil {
			t.Fatalf("warmup Ping failed: %v", err)
		}
		rx.Store(0)
		resp, err := client.Ping(context.Background(), &pingv1.PingRequest{Text: text})
		if err != nil {
			t.Fatalf("Ping failed: %v", err)
		}
		if resp.GetText() != text {
			t.Fatal("payload did not round-trip")
		}
		return rx.Load()
	}

	compressed := measure(t)
	plain := measure(t, draft4.WithoutCompression())

	t.Logf("16 KiB compressible payload over HTTP/2: %d bytes compressed, %d uncompressed", compressed, plain)
	if compressed >= plain/2 {
		t.Errorf("compression barely helped: %d bytes vs %d uncompressed; expected a large reduction", compressed, plain)
	}
	if plain < int64(len(text)) {
		t.Errorf("WithoutCompression sent %d bytes for a %d-byte payload; it should not be compressing", plain, len(text))
	}
}

// countingListener totals the bytes a client sends, so a test can assert on
// what actually crossed the wire.
type countingListener struct {
	net.Listener
	rx *atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: conn, rx: l.rx}, nil
}

type countingConn struct {
	net.Conn
	rx *atomic.Int64
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.rx.Add(int64(n))
	return n, err
}
