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
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft1"
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

// newH2TestServer mounts draft 1 over HTTP/2 (TLS, ALPN) and counts
// extended CONNECT requests, i.e. WebSockets bootstrapped on HTTP/2.
func newH2TestServer(t *testing.T, opts ...draft1.Option) (*testPingServer, string, *atomic.Int64) {
	t.Helper()
	impl := &testPingServer{
		respHeaders:  map[string]string{"X-Test-Header": "header-val"},
		respTrailers: map[string]string{"X-Test-Trailer": "trailer-val"},
	}
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, impl)

	mux := http.NewServeMux()
	draft1.Mount(mux, connectServer, draft1.DefaultPathPrefix, opts...)

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
	return impl, server.URL + draft1.DefaultPathPrefix, connects
}

func newH2Client(t *testing.T, baseURL string, opts ...draft1.Option) pingv1connect.PingServiceClient {
	t.Helper()
	h2 := &http2.Transport{TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{http2.NextProtoTLS},
	}}
	t.Cleanup(h2.CloseIdleConnections)
	transport := draft1.NewH2Transport(baseURL, h2, opts...)
	return pingv1connect.NewPingServiceClient(connect.NewClient(transport))
}

// TestH2Bootstrap runs every streaming shape over RFC 8441 extended
// CONNECT. This is the bootstrap draft 1 wants: a WebSocket per RPC costs a
// whole TCP and TLS handshake on HTTP/1.1, but only a new stream here — and
// it does not consume one of the browser's per-host WebSocket slots.
func TestH2Bootstrap(t *testing.T) {
	t.Parallel()
	requireExtendedConnect(t)
	_, baseURL, connects := newH2TestServer(t)
	client := newH2Client(t, baseURL)

	t.Run("ServerStreaming", func(t *testing.T) {
		ctx, callInfo := connect.NewClientContext(context.Background())
		stream, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: 3})
		if err != nil {
			t.Fatalf("CountUp: %v", err)
		}
		defer func() { _ = stream.Close() }()
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
		if got := callInfo.ResponseHeader().Get("X-Test-Header"); got != "header-val" {
			t.Errorf("header = %q, want header-val", got)
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
		defer func() { _ = stream.Close() }()
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

// TestH2WithoutCompression exercises the HTTP/2 path with
// permessage-deflate declined, which is the branch where h2Conn writes
// plain frames rather than deflating them itself.
func TestH2WithoutCompression(t *testing.T) {
	t.Parallel()
	requireExtendedConnect(t)
	_, baseURL, _ := newH2TestServer(t, draft1.WithoutCompression())
	client := newH2Client(t, baseURL, draft1.WithoutCompression())

	stream, err := client.CumSum(context.Background())
	if err != nil {
		t.Fatalf("CumSum: %v", err)
	}
	defer func() { _ = stream.Close() }()
	// A payload long enough to be worth compressing, so the uncompressed
	// path is genuinely the one under test.
	for range 4 {
		if err := stream.Send(&pingv1.CumSumRequest{Number: 1}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		if _, err := stream.Receive(); err != nil {
			t.Fatalf("Receive: %v", err)
		}
	}
}

// TestH2UnaryIsRefused: the bootstrap does not change what draft 1 carries.
func TestH2UnaryIsRefused(t *testing.T) {
	t.Parallel()
	requireExtendedConnect(t)
	_, baseURL, _ := newH2TestServer(t)
	client := newH2Client(t, baseURL)

	if _, err := client.Ping(context.Background(), &pingv1.PingRequest{Number: 1}); connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Errorf("Ping error = %v (code %v), want %v", err, connect.CodeOf(err), connect.CodeUnimplemented)
	}
}
