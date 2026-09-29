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
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/net/http2"
)

// The options draft 7 adds to the ones it inherits from connecthttp. They
// configure the WebSocket that RPCs upgrade to; the HTTP dispatch path
// ignores them.

// WithoutCompression disables permessage-deflate, which is the protocol's
// only compression: there is no message-level compression and no marker
// for it. It applies to both [NewTransport] and [Mount]: a client stops
// offering the extension, a server stops accepting it.
func WithoutCompression() Option {
	return withoutCompressionOption{}
}

type withoutCompressionOption struct{}

func (o withoutCompressionOption) apply(opts *options) { opts.withoutCompression = true }

// WithH2Bootstrap configures a client to open WebSockets with RFC 8441
// extended CONNECT on HTTP/2 rather than an HTTP/1.1 Upgrade handshake. The
// protocol on the connection is identical; what changes is the plumbing
// underneath, and for a protocol with one WebSocket per RPC that plumbing
// is the whole cost model: here a handshake is one more stream on a
// connection that is already open.
//
// The specification does not adopt RFC 8441. This implementation serves
// and dials it anyway, because a browser that has seen
// SETTINGS_ENABLE_CONNECT_PROTOCOL on an existing HTTP/2 connection sends
// its handshake this way with no fallback, and a page cannot override
// that; see ws_h2.go.
//
// The transport takes an [*http2.Transport] directly because the plain
// [*http.Client] cannot send the :protocol pseudo-header — net/http rejects
// it before HTTP/2 sees it. A nil transport is equivalent to a zero
// [http2.Transport]. The server process must run with
// GODEBUG=http2xconnect=1; dialing a server that hasn't advertised support
// fails with CodeUnavailable. [Mount] ignores this option: a handler
// serves both bootstraps automatically.
func WithH2Bootstrap(transport *http2.Transport) Option {
	if transport == nil {
		transport = &http2.Transport{}
	}
	return h2BootstrapOption{transport: transport}
}

type h2BootstrapOption struct{ transport *http2.Transport }

func (o h2BootstrapOption) apply(opts *options) { opts.h2 = o.transport }

// WithPathPrefix places every WebSocket handshake under a common path
// prefix, so that an upgrade is distinguishable from an ordinary RPC by
// URL alone — which some load balancers need to route WebSocket traffic
// differently. Both peers must agree on the value.
//
// A client dials prefix + procedure for the WebSocket and keeps the bare
// procedure path for plain HTTP RPCs. A server answers a non-upgrade
// request under the prefix with 426 and an upgrade at a bare procedure
// path with 400. The prefix must begin with "/" and must not end with one.
func WithPathPrefix(prefix string) Option {
	return pathPrefixOption(prefix)
}

type pathPrefixOption string

func (o pathPrefixOption) apply(opts *options) {
	prefix := strings.TrimSuffix(string(o), "/")
	if prefix != "" && !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	opts.pathPrefix = prefix
}

// WithUnaryOverWebSocket makes a client carry unary RPCs over a WebSocket
// too, rather than as ordinary Connect HTTP requests. Every server accepts
// an upgrade for a unary procedure; a client merely defaults to the POST
// the endpoint already answers, because a handshake to carry one request
// and one response is usually a bad trade. [Mount] ignores this option.
func WithUnaryOverWebSocket() Option {
	return unaryOverWebSocketOption{}
}

type unaryOverWebSocketOption struct{}

func (o unaryOverWebSocketOption) apply(opts *options) { opts.unaryOverWebSocket = true }

// WithServerTimeout bounds every RPC a handler serves over a WebSocket,
// from the moment the handshake is accepted. The effective deadline is the
// shorter of this and the client's connect-timeout-ms. It defaults to
// [DefaultServerTimeout]; zero disables it, leaving an RPC without a
// client deadline unbounded. [NewTransport] ignores this option.
func WithServerTimeout(timeout time.Duration) Option {
	return serverTimeoutOption(timeout)
}

type serverTimeoutOption time.Duration

func (o serverTimeoutOption) apply(opts *options) { opts.serverTimeout = time.Duration(o) }

// WithInfrastructureHeaders replaces the server's default deny list of
// header names its own infrastructure sets — Forwarded, X-Forwarded-*, and
// X-Real-IP — with names. A client's leading-metadata message must not
// carry any of them; the RPC ends with an error if it does. A name ending
// in "-" is a prefix. [NewTransport] ignores this option.
func WithInfrastructureHeaders(names ...string) Option {
	canonical := make([]string, 0, len(names))
	for _, name := range names {
		if strings.HasSuffix(name, "-") {
			canonical = append(canonical, http.CanonicalHeaderKey(name[:len(name)-1])+"-")
			continue
		}
		canonical = append(canonical, http.CanonicalHeaderKey(name))
	}
	return infrastructureHeadersOption(canonical)
}

type infrastructureHeadersOption []string

func (o infrastructureHeadersOption) apply(opts *options) {
	opts.forbiddenRequestHeaders = []string(o)
}

// WithWebSocketDialOptions sets custom [websocket.DialOptions] for the
// connections a client opens. Subprotocols and CompressionMode are
// overridden regardless: they are the protocol's own, so custom options
// cannot change what is spoken on the wire or silently disable the only
// compression there is. Use [WithoutCompression] for that.
//
// [Mount] ignores this option.
func WithWebSocketDialOptions(dialOptions *websocket.DialOptions) Option {
	return dialOptionsOption{dialOptions: dialOptions}
}

type dialOptionsOption struct{ dialOptions *websocket.DialOptions }

func (o dialOptionsOption) apply(opts *options) {
	if o.dialOptions == nil {
		opts.webSocketDialOptions = nil
		return
	}
	cloned := *o.dialOptions
	opts.webSocketDialOptions = &cloned
}

// WithWebSocketAcceptOptions sets custom [websocket.AcceptOptions] for the
// connections a handler accepts. OriginPatterns is how a deployment permits
// cross-origin handshakes: by default a handshake whose Origin host is not
// the request's own Host is refused with 403, and one with no Origin at
// all is not from a browser and is never refused on origin grounds.
// Subprotocols and CompressionMode are overridden regardless, for the
// reasons given on [WithWebSocketDialOptions].
//
// [NewTransport] ignores this option.
func WithWebSocketAcceptOptions(acceptOptions *websocket.AcceptOptions) Option {
	return acceptOptionsOption{acceptOptions: acceptOptions}
}

type acceptOptionsOption struct{ acceptOptions *websocket.AcceptOptions }

func (o acceptOptionsOption) apply(opts *options) {
	if o.acceptOptions == nil {
		opts.webSocketAcceptOptions = nil
		return
	}
	cloned := *o.acceptOptions
	opts.webSocketAcceptOptions = &cloned
}
