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

package draft5

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"

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
// server rejects :protocol streams and never advertises
// SETTINGS_ENABLE_CONNECT_PROTOCOL, so clients — browsers included —
// never attempt this bootstrap.
//
// This bootstrap matters more to draft 5 than to any other draft. One
// WebSocket per RPC means a handshake per RPC, which on HTTP/1.1 is an
// expensive thing to repeat. Here it is one more stream on a connection
// that is already open: no TCP connection, no TLS handshake, and HPACK
// compresses the repeated request headers down to a few bytes. Draft 5 is
// the draft that wants this path, where the others merely tolerate it.
//
// Compression works here too. RFC 8441 §5 keeps Sec-WebSocket-Extensions
// ("used in the CONNECT request and response-header fields as defined in
// [RFC6455]"), so permessage-deflate is negotiated in the CONNECT exchange
// exactly as it would be in an Upgrade handshake. The framing underneath is
// ours because coder/websocket cannot accept over HTTP/2; see
// ws_compress.go for the extension and h2Conn for the frames.

// dialH2 opens one extended CONNECT stream carrying one RPC. The request
// stays open for the RPC's lifetime: the request body pipe carries
// client-to-server messages and the response body carries the reverse.
func dialH2(ctx context.Context, procedureURL *url.URL, opts *options) (messageConn, error) {
	pipeReader, pipeWriter := io.Pipe()
	// The stream is torn down by the RPC ending, which the client stream
	// watches for; a context of its own would end it twice.
	connCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	request, err := http.NewRequestWithContext(connCtx, http.MethodConnect, procedureURL.String(), pipeReader)
	if err != nil {
		cancel()
		return nil, connect.Errorf(connect.CodeUnavailable, "invalid WebSocket over HTTP/2 URL: %v", err)
	}
	request.Header.Set(":protocol", "websocket")
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.Header.Set("Sec-WebSocket-Protocol", subprotocol)
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
	if response.StatusCode != http.StatusOK {
		cancel()
		_ = response.Body.Close()
		return nil, handshakeError(response, errors.New("extended CONNECT was refused"))
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

	// The server compresses only if it echoed the extension back.
	deflate := acceptsDeflate(response.Header.Get(extensionHeader))
	closeRead := func() {
		_ = response.Body.Close()
		_ = pipeWriter.Close()
		cancel()
	}
	return newH2Conn(response.Body, closeRead, pipeWriter, nil, true, deflate), nil
}

// acceptH2 accepts an extended CONNECT WebSocket and returns the connection
// carrying the RPC. It reports false when it has already written a response
// declining the upgrade.
func acceptH2(responseWriter http.ResponseWriter, request *http.Request, opts *options) (messageConn, bool) {
	if version := request.Header.Get("Sec-Websocket-Version"); version != "13" {
		responseWriter.Header().Set("Sec-Websocket-Version", "13")
		http.Error(responseWriter, "unsupported websocket version", http.StatusBadRequest)
		return nil, false
	}
	responseWriter.Header().Set("Sec-WebSocket-Protocol", subprotocol)
	// permessage-deflate is negotiated in the CONNECT exchange, the same way
	// it would be in an Upgrade handshake.
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
