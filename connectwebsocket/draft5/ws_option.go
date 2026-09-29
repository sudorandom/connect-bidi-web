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
	"github.com/coder/websocket"
	"golang.org/x/net/http2"
)

// The options draft 5 adds to the ones it inherits from connecthttp. They
// configure the WebSocket that streaming RPCs upgrade to; the HTTP dispatch
// path that carries unary RPCs ignores them.

// WithoutCompression disables permessage-deflate, which is draft 5's only
// compression. With no envelope, a data message has nowhere to carry a
// "this one is compressed" bit, so the Connect protocol's own
// connect-content-encoding mechanism has no mapping here and the WebSocket
// extension is all there is.
//
// It applies to both [NewTransport] and [Mount]: a client stops offering
// the extension, a server stops accepting it.
func WithoutCompression() Option {
	return withoutCompressionOption{}
}

type withoutCompressionOption struct{}

func (o withoutCompressionOption) apply(opts *options) { opts.withoutCompression = true }

// WithH2Bootstrap configures a client to open WebSockets with RFC 8441
// extended CONNECT on HTTP/2 rather than an HTTP/1.1 Upgrade handshake. The
// protocol on the connection is identical; what changes is the plumbing
// underneath, and for draft 5 that plumbing is the whole cost model. One
// WebSocket per RPC means a handshake per RPC, and here a handshake is one
// more stream on a connection that is already open.
//
// The transport takes an [*http2.Transport] directly because the plain
// [*http.Client] cannot send the :protocol pseudo-header — net/http rejects
// it before HTTP/2 sees it. A nil transport is equivalent to a zero
// [http2.Transport].
//
// The server process must run with GODEBUG=http2xconnect=1; Go's HTTP/2
// stack has extended CONNECT off by default as of Go 1.26. Dialing a server
// that hasn't advertised support fails with CodeUnavailable, which callers
// can use to fall back to the HTTP/1.1 bootstrap.
//
// [Mount] ignores this option: a handler serves both bootstraps
// automatically.
func WithH2Bootstrap(transport *http2.Transport) Option {
	if transport == nil {
		transport = &http2.Transport{}
	}
	return h2BootstrapOption{transport: transport}
}

type h2BootstrapOption struct{ transport *http2.Transport }

func (o h2BootstrapOption) apply(opts *options) { opts.h2 = o.transport }

// WithWebSocketDialOptions sets custom [websocket.DialOptions] for the
// connections a client opens. Subprotocols and CompressionMode are
// overridden regardless: they are the protocol's own, so custom options
// cannot change what is spoken on the wire or silently disable the only
// compression draft 5 has. Use [WithoutCompression] for that.
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
// connections a handler accepts — OriginPatterns in particular, which
// browsers need. Subprotocols and CompressionMode are overridden
// regardless, for the reasons given on [WithWebSocketDialOptions].
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
