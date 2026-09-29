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

package draft6

import (
	"context"
	"io"
	"net/http"
	"strings"

	"connectrpc.com/connect/v2"
	"golang.org/x/net/http2"
)

// WebSocket over HTTP/2 (RFC 8441, "Bootstrapping WebSockets with
// HTTP/2"): each WebSocket is one extended CONNECT stream
// (:method=CONNECT, :protocol=websocket) on a shared HTTP/2 connection.
// Go's HTTP/2 stack supports it only when the *server* process runs with
// GODEBUG=http2xconnect=1.
//
// Draft 6 gets less out of this bootstrap than draft 5 does, and that is
// the point of having both. Draft 5 needed it, because a handshake per RPC
// is only affordable when a handshake is one more stream on an open
// connection. Draft 6 pools connections instead, so it has already stopped
// paying per RPC and reaches for HTTP/2 for the ordinary reasons: sharing
// TCP and TLS state with the page's other traffic.

// isExtendedConnectWebSocket reports whether r is an RFC 8441 extended
// CONNECT request opening a WebSocket.
func isExtendedConnectWebSocket(request *http.Request) bool {
	return request.Method == http.MethodConnect &&
		request.ProtoMajor == 2 &&
		request.Header.Get(":protocol") == "websocket"
}

// NewH2Transport returns a connect.Transport that carries draft 6 over
// WebSockets bootstrapped with RFC 8441 extended CONNECT on HTTP/2. The
// wire protocol and the connection pooling are identical; only the dial
// changes.
//
// url uses the https scheme (wss:// is accepted and rewritten). h2
// configures the underlying HTTP/2 client; nil is equivalent to a zero
// http2.Transport. The plain *http.Client cannot send the :protocol
// pseudo-header, which is why this takes an *http2.Transport directly.
func NewH2Transport(url string, h2 *http2.Transport, opts ...Option) connect.Transport {
	tOpts := transportOptions{
		protocolOptions: newClientProtocolOptions(),
		maxIdleConns:    DefaultMaxIdleConns,
	}
	for _, opt := range opts {
		opt.applyTransport(&tOpts)
	}
	tOpts.finalize()
	if h2 == nil {
		h2 = &http2.Transport{}
	}
	t := &transport{url: normalizeH2URL(url), opts: tOpts, h2: h2}
	t.dial = t.dialH2
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

// dialH2 opens one extended CONNECT stream. The stream outlives the RPC
// that dialed it, because the connection goes back to the pool.
func (t *transport) dialH2(ctx context.Context) (messageConn, error) {
	pipeReader, pipeWriter := io.Pipe()
	// A pooled connection must not die with the RPC that opened it, so the
	// stream's context is deliberately detached from ctx.
	connCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	request, err := http.NewRequestWithContext(connCtx, http.MethodConnect, t.url, pipeReader)
	if err != nil {
		cancel()
		return nil, connect.Errorf(connect.CodeUnavailable, "invalid WebSocket over HTTP/2 URL: %v", err)
	}
	request.Header.Set(":protocol", "websocket")
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.Header.Set("Sec-WebSocket-Protocol", subprotocol)
	if !t.opts.withoutCompression {
		request.Header.Set(extensionHeader, extensionOffer)
	}

	type roundTripResult struct {
		response *http.Response
		err      error
	}
	results := make(chan roundTripResult, 1)
	go func() {
		response, err := t.h2.RoundTrip(request) //nolint:bodyclose // the connection owns the response body
		results <- roundTripResult{response: response, err: err}
	}()
	var response *http.Response
	select {
	case result := <-results:
		if result.err != nil {
			cancel()
			return nil, connect.Errorf(connect.CodeUnavailable, "failed to dial WebSocket over HTTP/2: %v", result.err)
		}
		response = result.response
	case <-ctx.Done():
		cancel()
		go func() {
			if result := <-results; result.response != nil {
				_ = result.response.Body.Close()
			}
		}()
		return nil, ctx.Err()
	}
	if response.StatusCode != http.StatusOK {
		cancel()
		_ = response.Body.Close()
		return nil, connect.Errorf(
			connect.CodeUnavailable,
			"WebSocket over HTTP/2 handshake failed: status %d", response.StatusCode,
		)
	}
	if selected := response.Header.Get("Sec-WebSocket-Protocol"); selected != subprotocol {
		cancel()
		_ = response.Body.Close()
		return nil, connect.Errorf(
			connect.CodeUnavailable,
			"server did not select the %q subprotocol; it may not serve this protocol",
			subprotocol,
		)
	}

	deflate := acceptsDeflate(response.Header.Get(extensionHeader))
	closeRead := func() {
		_ = response.Body.Close()
		_ = pipeWriter.Close()
		cancel()
	}
	return newH2Conn(response.Body, closeRead, pipeWriter, nil, true, deflate), nil
}

// acceptH2 accepts an extended CONNECT WebSocket. It reports false when it
// has already written a response declining the upgrade.
func (h *Handler) acceptH2(responseWriter http.ResponseWriter, request *http.Request) (messageConn, bool) {
	if version := request.Header.Get("Sec-Websocket-Version"); version != "13" {
		responseWriter.Header().Set("Sec-Websocket-Version", "13")
		http.Error(responseWriter, "unsupported websocket version", http.StatusBadRequest)
		return nil, false
	}
	responseWriter.Header().Set("Sec-WebSocket-Protocol", subprotocol)
	deflate := !h.opts.withoutCompression && acceptsDeflate(request.Header.Get(extensionHeader))
	if deflate {
		responseWriter.Header().Set(extensionHeader, extensionOffer)
	}
	// RFC 8441 has no 101: accept with a 200 flushed onto the stream.
	control := http.NewResponseController(responseWriter)
	responseWriter.WriteHeader(http.StatusOK)
	if err := control.Flush(); err != nil {
		return nil, false
	}
	body := request.Body
	return newH2Conn(body, func() { _ = body.Close() }, responseWriter, control.Flush, false, deflate), true
}
