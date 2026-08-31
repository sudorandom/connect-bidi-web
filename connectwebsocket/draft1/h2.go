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
	"golang.org/x/net/http2"
)

// WebSocket over HTTP/2 (RFC 8441, "Bootstrapping WebSockets with
// HTTP/2"): each WebSocket is one extended CONNECT stream
// (:method=CONNECT, :protocol=websocket) on a shared HTTP/2 connection.
// Go's HTTP/2 stack supports it only when the *server* process runs with
// GODEBUG=http2xconnect=1.
//
// This bootstrap is what makes draft 1's "one WebSocket per RPC" affordable.
// Over HTTP/1.1 every streaming call pays a TCP connection, a TLS
// handshake, and a browser connection slot. Over HTTP/2 it is one more
// stream on a connection the page already has, with HPACK compressing the
// repeated request headers down to a few bytes.

// isExtendedConnectWebSocket reports whether r is an RFC 8441 extended
// CONNECT request opening a WebSocket.
func isExtendedConnectWebSocket(request *http.Request) bool {
	return request.Method == http.MethodConnect &&
		request.ProtoMajor == 2 &&
		request.Header.Get(":protocol") == "websocket"
}

// NewH2Transport returns a connect.Transport that carries draft 1 over
// WebSockets bootstrapped with RFC 8441 extended CONNECT on HTTP/2. The
// wire protocol is identical; only the dial changes.
//
// baseURL uses the https scheme (wss:// is accepted and rewritten). h2
// configures the underlying HTTP/2 client; nil is equivalent to a zero
// http2.Transport. The plain *http.Client cannot send the :protocol
// pseudo-header, which is why this takes an *http2.Transport directly.
func NewH2Transport(baseURL string, h2 *http2.Transport, opts ...Option) connect.Transport {
	tOpts := transportOptions{protocolOptions: newClientProtocolOptions()}
	for _, opt := range opts {
		opt.applyTransport(&tOpts)
	}
	tOpts.finalize()
	if h2 == nil {
		h2 = &http2.Transport{}
	}
	t := &transport{baseURL: normalizeH2URL(baseURL), opts: tOpts, h2: h2}
	t.dial = t.dialH2
	return t
}

func normalizeH2URL(rawURL string) string {
	trimmed := strings.TrimSuffix(rawURL, "/")
	if rest, ok := strings.CutPrefix(trimmed, "wss://"); ok {
		return "https://" + rest
	}
	if rest, ok := strings.CutPrefix(trimmed, "ws://"); ok {
		return "http://" + rest
	}
	return trimmed
}

// dialH2 opens one extended CONNECT stream for one RPC.
func (t *transport) dialH2(ctx context.Context, target string) (messageConn, error) {
	pipeReader, pipeWriter := io.Pipe()
	request, err := http.NewRequestWithContext(ctx, http.MethodConnect, target, pipeReader)
	if err != nil {
		return nil, connect.Errorf(connect.CodeUnavailable, "invalid WebSocket over HTTP/2 URL: %v", err)
	}
	request.Header.Set(":protocol", "websocket")
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.Header.Set("Sec-WebSocket-Protocol", subprotocol)
	if !t.opts.withoutCompression {
		request.Header.Set(extensionHeader, extensionOffer)
	}

	response, err := t.h2.RoundTrip(request)
	if err != nil {
		return nil, connect.Errorf(connect.CodeUnavailable, "failed to dial WebSocket over HTTP/2: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		_ = response.Body.Close()
		return nil, connect.Errorf(
			connect.CodeUnavailable,
			"WebSocket over HTTP/2 handshake failed: status %d", response.StatusCode,
		)
	}
	if selected := response.Header.Get("Sec-WebSocket-Protocol"); selected != subprotocol {
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
