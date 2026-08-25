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

package draft1

import (
	"context"
	"io"
	"net/http"
	"strings"

	"connectrpc.com/connect/v2"
	"github.com/sudorandom/connect-bidi-web/internal/bidiprotocol"
	"golang.org/x/net/http2"
)

// WebSocket over HTTP/2 (RFC 8441, "Bootstrapping WebSockets with
// HTTP/2"): instead of an HTTP/1.1 Upgrade that consumes a whole TCP
// connection, each WebSocket is one extended CONNECT stream
// (:method=CONNECT, :protocol=websocket) on a shared HTTP/2 connection.
// There is no Sec-WebSocket-Key handshake; the server accepts with a 200
// and the stream then carries ordinary RFC 6455 frames. Go's HTTP/2 stack
// supports extended CONNECT only when the *server* process runs with
// GODEBUG=http2xconnect=1 (off by default as of Go 1.26); without it the
// server rejects :protocol streams and never advertises
// SETTINGS_ENABLE_CONNECT_PROTOCOL, so clients — browsers included —
// never attempt this bootstrap. The Go client transport dials only the
// HTTP/1.1 upgrade; for a Go client over HTTP/2, use draft 3 or 4.
//
// The wire protocol is identical on both bootstraps. One difference in
// practice: no WebSocket extensions exist on extended CONNECT, so
// permessage-deflate never applies there — draft 1's own Connect-metadata
// compression is unaffected.

// isExtendedConnectWebSocket reports whether r is an RFC 8441 extended
// CONNECT request opening a WebSocket. Such requests reach handlers only
// over HTTP/2 with extended CONNECT enabled; net/http surfaces the
// :protocol pseudo-header through r.Header.
func isExtendedConnectWebSocket(r *http.Request) bool {
	return r.Method == http.MethodConnect &&
		r.ProtoMajor == 2 &&
		r.Header.Get(":protocol") == "websocket"
}

// serveH2 accepts an extended CONNECT WebSocket and serves the same
// multiplexed draft 1 protocol on it that the HTTP/1.1 bootstrap carries.
func (h *Handler) serveH2(w http.ResponseWriter, r *http.Request) {
	if version := r.Header.Get("Sec-Websocket-Version"); version != "13" {
		w.Header().Set("Sec-Websocket-Version", "13")
		http.Error(w, "unsupported websocket version", http.StatusBadRequest)
		return
	}
	// Accept by writing a 200 (RFC 8441 has no 101) and flushing it onto
	// the stream. Nothing is echoed for Sec-WebSocket-Protocol or
	// -Extensions: no subprotocol and no extensions are negotiated.
	control := http.NewResponseController(w)
	w.WriteHeader(http.StatusOK)
	if err := control.Flush(); err != nil {
		return
	}
	body := r.Body
	conn := newH2Conn(body, func() { _ = body.Close() }, w, control.Flush, false)
	h.serveConn(r.Context(), conn, r.RemoteAddr)
}

// NewH2Transport returns a connect.Transport that carries draft 1 over
// WebSockets bootstrapped with RFC 8441 extended CONNECT on HTTP/2,
// instead of an HTTP/1.1 upgrade. The wire protocol is identical; what
// changes is the plumbing underneath: the WebSocket connection is a
// stream on a shared HTTP/2 connection, so it shares TCP and TLS state
// with other streams to the same origin.
//
// url uses the https scheme (wss:// is accepted and rewritten). h2
// configures the underlying HTTP/2 client — TLS settings in particular;
// nil is equivalent to a zero http2.Transport. The plain *http.Client
// cannot send the :protocol pseudo-header, which is why this takes an
// *http2.Transport directly.
//
// The server must run with GODEBUG=http2xconnect=1. Dialing a server that
// hasn't advertised extended CONNECT support fails with CodeUnavailable
// ("extended connect not supported by peer"), which callers can use to
// fall back to NewTransport's HTTP/1.1 bootstrap.
func NewH2Transport(url string, h2 *http2.Transport, opts ...Option) connect.Transport {
	tOpts := transportOptions{Options: bidiprotocol.NewClientOptions()}
	for _, opt := range opts {
		opt.applyTransport(&tOpts)
	}
	tOpts.Finalize()
	if h2 == nil {
		h2 = &http2.Transport{}
	}
	t := &transport{
		url:  normalizeH2URL(url),
		opts: tOpts,
		h2:   h2,
	}
	t.dialConn = t.dialH2
	return t
}

func normalizeH2URL(rawURL string) string {
	if rest, ok := strings.CutPrefix(rawURL, "wss://"); ok {
		return "https://" + rest
	}
	if rest, ok := strings.CutPrefix(rawURL, "ws://"); ok {
		return "http://" + rest
	}
	return rawURL
}

// dialH2 opens one extended CONNECT stream and hands it to the shared mux
// machinery. The request stays open for the connection's lifetime: the
// request body pipe carries client-to-server frames, the response body
// carries server-to-client frames.
func (t *transport) dialH2(ctx context.Context) (*muxConn, error) {
	pipeReader, pipeWriter := io.Pipe()
	// The stream outlives the RPC that dialed it; its own context ends it.
	connCtx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(connCtx, http.MethodConnect, t.url, pipeReader) //nolint:contextcheck // see above
	if err != nil {
		cancel()
		return nil, connect.Errorf(connect.CodeUnavailable, "invalid WebSocket over HTTP/2 URL: %v", err)
	}
	req.Header.Set(":protocol", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")

	// RoundTrip returns when the response headers arrive, while the
	// request body keeps streaming — full duplex on one stream. It must
	// not outlive the dialing RPC's patience, though, so wait under ctx.
	type roundTripResult struct {
		resp *http.Response
		err  error
	}
	resultChan := make(chan roundTripResult, 1)
	go func() {
		resp, err := t.h2.RoundTrip(req) //nolint:bodyclose // the connection owns the response body
		resultChan <- roundTripResult{resp: resp, err: err}
	}()
	var resp *http.Response
	select {
	case result := <-resultChan:
		if result.err != nil {
			cancel()
			return nil, connect.Errorf(connect.CodeUnavailable, "failed to dial WebSocket over HTTP/2: %v", result.err)
		}
		resp = result.resp
	case <-ctx.Done():
		cancel()
		go func() {
			if result := <-resultChan; result.resp != nil {
				_ = result.resp.Body.Close()
			}
		}()
		return nil, ctx.Err()
	}
	if resp.StatusCode != http.StatusOK {
		cancel()
		_ = resp.Body.Close()
		return nil, connect.Errorf(connect.CodeUnavailable, "WebSocket over HTTP/2 handshake failed: status %d", resp.StatusCode)
	}

	closeRead := func() {
		_ = resp.Body.Close()
		_ = pipeWriter.Close()
		cancel()
	}
	conn := newH2Conn(resp.Body, closeRead, pipeWriter, nil, true)
	mc := newMuxConn(context.Background(), conn) //nolint:contextcheck // the connection deliberately outlives the RPC that dialed it
	go mc.readLoopClient(context.Background())   //nolint:contextcheck,gosec // G118: see above
	return mc, nil
}
