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

package draft7

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"

	"connectrpc.com/connect/v2"
)

// WebSocket over HTTP/2 (RFC 8441, "Bootstrapping WebSockets with
// HTTP/2"): instead of an HTTP/1.1 Upgrade that consumes a whole TCP
// connection, each WebSocket is one extended CONNECT stream
// (:method=CONNECT, :protocol=websocket) on a shared HTTP/2 connection.
// There is no Sec-WebSocket-Key handshake; the server accepts with a 200
// and the stream then carries ordinary RFC 6455 frames. Go's HTTP/2 stack
// supports extended CONNECT only when the *server* process runs with
// GODEBUG=http2xconnect=1 (off by default as of Go 1.26); without it the
// server never advertises SETTINGS_ENABLE_CONNECT_PROTOCOL, so clients —
// browsers included — never attempt this bootstrap.
//
// The Connect-over-WebSocket specification (§4.1) does not adopt RFC 8441
// and asks a server to refuse an extended CONNECT. This implementation
// deliberately departs from that: a browser that has seen the setting on
// an existing HTTP/2 connection sends its handshake as an extended CONNECT
// with no fallback to HTTP/1.1, and a page cannot override the choice, so
// refusing it means a deployment that enables the setting for anything
// else loses the protocol in browsers entirely. The frames on the stream
// are identical to the HTTP/1.1 path's, and a per-RPC handshake is far
// cheaper here — one more stream on an open connection — which is the
// protocol's whole cost model. The negotiation is otherwise the ordinary
// one: RFC 8441 §5 keeps Sec-WebSocket-Protocol and
// Sec-WebSocket-Extensions in the CONNECT exchange.

// dialH2 opens one extended CONNECT stream carrying one RPC, offering the
// subprotocol for the codec in use. The request stays open for the RPC's
// lifetime: the request body pipe carries client-to-server messages and
// the response body carries the reverse.
func dialH2(ctx context.Context, dialURL *url.URL, token string, opts *options) (messageConn, error) {
	pipeReader, pipeWriter := io.Pipe()
	// The stream is torn down by the RPC ending, which the client stream
	// watches for; a context of its own would end it twice.
	connCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	requestURL := *dialURL
	switch requestURL.Scheme {
	case "ws":
		requestURL.Scheme = "http"
	case "wss":
		requestURL.Scheme = "https"
	}
	request, err := http.NewRequestWithContext(connCtx, http.MethodConnect, requestURL.String(), pipeReader)
	if err != nil {
		cancel()
		return nil, connect.Errorf(connect.CodeUnavailable, "invalid WebSocket over HTTP/2 URL: %v", err)
	}
	request.Header.Set(":protocol", "websocket")
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.Header.Set("Sec-WebSocket-Protocol", token)
	if !opts.withoutCompression {
		request.Header.Set(extensionHeader, extensionOffer)
	}

	// RoundTrip returns when the response headers arrive, while the request
	// body keeps streaming — full duplex on one stream. It must not outlive
	// the dialing RPC's patience, though, so wait under ctx.
	type roundTripResult struct {
		response *http.Response
		err      error
	}
	results := make(chan roundTripResult, 1)
	go func() {
		response, err := opts.h2.RoundTrip(request) //nolint:bodyclose // the connection owns the response body
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
	closeAll := func() {
		cancel()
		_ = response.Body.Close()
	}
	if response.StatusCode != http.StatusOK {
		closeAll()
		return nil, handshakeError(response, errors.New("extended CONNECT was refused"))
	}
	if selected := response.Header.Get("Sec-WebSocket-Protocol"); selected != token {
		closeAll()
		return nil, connect.Errorf(connect.CodeUnavailable,
			"server selected subprotocol %q, which was not offered; it may not serve this protocol", selected)
	}
	if err := verifyCompression(response.Header); err != nil {
		closeAll()
		return nil, err
	}

	// The server compresses only if it echoed the extension back.
	deflate := acceptsDeflate(response.Header.Get(extensionHeader))
	closeRead := func() {
		_ = response.Body.Close()
		_ = pipeWriter.Close()
		cancel()
	}
	return newH2Conn(response.Body, closeRead, pipeWriter, nil, true, deflate), nil
}

// acceptH2 accepts an extended CONNECT WebSocket for the already-selected
// subprotocol and returns the connection carrying the RPC. It reports
// false when it has already written a response declining the upgrade.
//
// The origin rule is enforced here because coder/websocket, which does it
// on the HTTP/1.1 path, is not involved: a cross-origin handshake is
// refused with 403 unless the accept options permit it, and one with no
// Origin is not from a browser.
func acceptH2(responseWriter http.ResponseWriter, request *http.Request, opts *options, token string) (messageConn, bool) {
	if !originPermitted(request, opts) {
		http.Error(responseWriter, "origin not permitted", http.StatusForbidden)
		return nil, false
	}
	if version := request.Header.Get("Sec-Websocket-Version"); version != "13" {
		responseWriter.Header().Set("Sec-Websocket-Version", "13")
		http.Error(responseWriter, "unsupported websocket version", http.StatusBadRequest)
		return nil, false
	}
	responseWriter.Header().Set("Sec-WebSocket-Protocol", token)
	// permessage-deflate is negotiated in the CONNECT exchange, the same way
	// it would be in an Upgrade handshake, and no_context_takeover is
	// imposed in both directions whatever the client offered.
	deflate := !opts.withoutCompression && acceptsDeflate(request.Header.Get(extensionHeader))
	if deflate {
		responseWriter.Header().Set(extensionHeader, extensionOffer)
	}
	// Accept by writing a 200 (RFC 8441 has no 101) and flushing it onto
	// the stream.
	control := http.NewResponseController(responseWriter)
	responseWriter.WriteHeader(http.StatusOK)
	if err := control.Flush(); err != nil {
		return nil, false
	}
	body := request.Body
	return newH2Conn(body, func() { _ = body.Close() }, responseWriter, control.Flush, false, deflate), true
}

// originPermitted applies the specification's origin rule, plus whatever
// the accept options permit: coder/websocket's InsecureSkipVerify and
// OriginPatterns, so that one configuration governs both bootstraps.
func originPermitted(request *http.Request, opts *options) bool {
	origin := request.Header.Get("Origin")
	if origin == "" {
		return true
	}
	if opts.webSocketAcceptOptions != nil && opts.webSocketAcceptOptions.InsecureSkipVerify {
		return true
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Host == "" {
		return false
	}
	if strings.EqualFold(parsed.Host, request.Host) {
		return true
	}
	if opts.webSocketAcceptOptions == nil {
		return false
	}
	for _, pattern := range opts.webSocketAcceptOptions.OriginPatterns {
		target := parsed.Host
		if strings.Contains(pattern, "://") {
			target = parsed.Scheme + "://" + parsed.Host
		}
		if matched, err := path.Match(strings.ToLower(pattern), strings.ToLower(target)); err == nil && matched {
			return true
		}
	}
	return false
}
