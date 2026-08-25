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
	pingv1connect "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
	"golang.org/x/net/http2"
)

// requireExtendedConnect skips the test unless the process was started with
// extended CONNECT support enabled (GODEBUG is read once at init; the
// justfile sets it for the whole test run).
func requireExtendedConnect(t *testing.T) {
	t.Helper()
	if !strings.Contains(os.Getenv("GODEBUG"), "http2xconnect=1") {
		t.Skip("extended CONNECT disabled; run with GODEBUG=http2xconnect=1")
	}
}

// TestWebSocketOverH2 runs draft 1's full RPC matrix over the RFC 8441
// extended-CONNECT bootstrap: one CONNECT stream carries the multiplexed
// wire protocol, exactly as over an HTTP/1.1 upgrade.
func TestWebSocketOverH2(t *testing.T) {
	requireExtendedConnect(t)

	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, testPingServer{
		respHeaders:  map[string]string{"X-Test-Header": "header-val"},
		respTrailers: map[string]string{"X-Test-Trailer": "trailer-val"},
	})
	wsHandler := draft1.NewHandler(connectServer)

	var connects atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			connects.Add(1)
		}
		wsHandler.ServeHTTP(w, r)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)

	client := newDraft1H2Client(t, server.URL)

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

	t.Run("BidiStreaming", func(t *testing.T) {
		stream, err := client.CumSum(context.Background())
		if err != nil {
			t.Fatalf("CumSum failed: %v", err)
		}
		defer stream.Close()
		var want int64
		for i := int64(1); i <= 4; i++ {
			if err := stream.Send(&pingv1.CumSumRequest{Number: i}); err != nil {
				t.Fatalf("Send failed: %v", err)
			}
			res, err := stream.Receive()
			if err != nil {
				t.Fatalf("Receive failed: %v", err)
			}
			want += i
			if res.GetSum() != want {
				t.Errorf("sum = %d, want %d", res.GetSum(), want)
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("CloseSend failed: %v", err)
		}
		if _, err := stream.Receive(); !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF after CloseSend, got %v", err)
		}
	})

	// Large payloads exercise the parts-based h2 frame writes and draft 1's
	// Connect-level gzip response compression over the h2 bootstrap.
	t.Run("LargeMessage", func(t *testing.T) {
		text := strings.Repeat("payload!", 256*1024/8) // 256 KiB
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

// newDraft1H2Client builds a draft 1 client whose WebSocket is
// bootstrapped over HTTP/2 extended CONNECT.
func newDraft1H2Client(t *testing.T, url string) pingv1connect.PingServiceClient {
	t.Helper()
	h2 := &http2.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{http2.NextProtoTLS},
		},
	}
	t.Cleanup(h2.CloseIdleConnections)
	transport := draft1.NewH2Transport(url, h2)
	t.Cleanup(func() {
		if closer, ok := transport.(io.Closer); ok {
			_ = closer.Close()
		}
	})
	return pingv1connect.NewPingServiceClient(connect.NewClient(transport))
}
