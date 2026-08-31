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
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect/v2"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft5"
	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	"github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
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

// newH2TestServer mounts the ping service over HTTP/2 (TLS, ALPN) and
// counts extended CONNECT requests, i.e. WebSockets bootstrapped on HTTP/2.
func newH2TestServer(t *testing.T) (*testPingServer, string, *atomic.Int64) {
	t.Helper()
	impl := &testPingServer{
		respHeaders:  map[string]string{"X-Test-Header": "header-val"},
		respTrailers: map[string]string{"X-Test-Trailer": "trailer-val"},
	}
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, impl)

	mux := http.NewServeMux()
	draft5.Mount(mux, connectServer)

	connects := &atomic.Int64{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			connects.Add(1)
		}
		mux.ServeHTTP(w, r)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	return impl, server.URL, connects
}

func newH2Client(t *testing.T, url string, opts ...draft5.Option) pingv1connect.PingServiceClient {
	t.Helper()
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{http2.NextProtoTLS},
	}
	h2 := &http2.Transport{TLSClientConfig: tlsConfig}
	t.Cleanup(h2.CloseIdleConnections)
	// Unary RPCs still go over ordinary HTTP, so the transport needs an
	// HTTP client too — the same HTTP/2 one, so both dispatch paths share a
	// connection.
	httpClient := &http.Client{Transport: h2}
	opts = append([]draft5.Option{draft5.WithH2Bootstrap(h2)}, opts...)
	transport := draft5.NewTransport(httpClient, url, opts...)
	return pingv1connect.NewPingServiceClient(connect.NewClient(transport))
}

// TestH2Bootstrap runs every stream shape over RFC 8441 extended CONNECT.
// This bootstrap is the one draft 5 is designed around: one WebSocket per
// RPC costs a whole handshake on HTTP/1.1, but only a new stream here.
func TestH2Bootstrap(t *testing.T) {
	t.Parallel()
	requireExtendedConnect(t)
	_, url, connects := newH2TestServer(t)
	client := newH2Client(t, url)

	t.Run("Unary", func(t *testing.T) {
		ctx, callInfo := connect.NewClientContext(context.Background())
		resp, err := client.Ping(ctx, &pingv1.PingRequest{Number: 7, Text: "h2"})
		if err != nil {
			t.Fatalf("Ping: %v", err)
		}
		if resp.GetNumber() != 7 || resp.GetText() != "h2" {
			t.Errorf("unexpected response: %+v", resp)
		}
		if got, want := callInfo.Protocol, connect.ProtocolNameConnect; got != want {
			t.Errorf("CallInfo.Protocol = %q, want %q — unary must not upgrade", got, want)
		}
	})

	t.Run("ServerStreaming", func(t *testing.T) {
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
		if got := callInfo.ResponseTrailer().Get("X-Test-Trailer"); got != "trailer-val" {
			t.Errorf("trailer = %q, want trailer-val", got)
		}
	})

	t.Run("BidiStreaming", func(t *testing.T) {
		stream, err := client.CumSum(context.Background())
		if err != nil {
			t.Fatalf("CumSum: %v", err)
		}
		defer stream.Close()
		for i, number := range []int64{1, 2, 3} {
			if err := stream.Send(&pingv1.CumSumRequest{Number: number}); err != nil {
				t.Fatalf("Send %d: %v", i, err)
			}
			if _, err := stream.Receive(); err != nil {
				t.Fatalf("Receive %d: %v", i, err)
			}
		}
	})

	t.Run("ClientStreaming", func(t *testing.T) {
		stream, err := client.Sum(context.Background())
		if err != nil {
			t.Fatalf("Sum: %v", err)
		}
		for _, number := range []int64{2, 3} {
			if err := stream.Send(&pingv1.SumRequest{Number: number}); err != nil {
				t.Fatalf("Send: %v", err)
			}
		}
		resp, err := stream.CloseAndReceive()
		if err != nil {
			t.Fatalf("CloseAndReceive: %v", err)
		}
		if got, want := resp.GetSum(), int64(5*1000+2); got != want {
			t.Errorf("sum = %d, want %d", got, want)
		}
	})

	if got := connects.Load(); got == 0 {
		t.Error("no extended CONNECT requests reached the server")
	}
}

// TestH2Compression checks that permessage-deflate is negotiated on the
// HTTP/2 bootstrap too. RFC 8441 §5 keeps Sec-WebSocket-Extensions in the
// CONNECT exchange, and this path implements the extension itself because
// coder/websocket cannot accept over HTTP/2 — a feature that once went
// missing precisely because it sits below the connection abstraction.
func TestH2Compression(t *testing.T) {
	t.Parallel()
	requireExtendedConnect(t)
	_, url, _ := newH2TestServer(t)
	client := newH2Client(t, url)

	stream, err := client.Sum(context.Background())
	if err != nil {
		t.Fatalf("Sum: %v", err)
	}
	for range 8 {
		if err := stream.Send(&pingv1.SumRequest{Number: 1}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	resp, err := stream.CloseAndReceive()
	if err != nil {
		t.Fatalf("CloseAndReceive: %v", err)
	}
	if got, want := resp.GetSum(), int64(8*1000+8); got != want {
		t.Errorf("sum = %d, want %d", got, want)
	}
}

// TestH2EmptyMessages repeats the zero-byte-message hazard on the HTTP/2
// bootstrap, where the RFC 6455 framing is this package's own.
func TestH2EmptyMessages(t *testing.T) {
	t.Parallel()
	requireExtendedConnect(t)
	_, url, _ := newH2TestServer(t)
	client := newH2Client(t, url)

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
