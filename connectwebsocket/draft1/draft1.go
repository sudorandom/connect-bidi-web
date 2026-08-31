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

// Package draft1 provides a connect.Transport and an http.Handler that
// carry streaming Connect RPCs over WebSocket connections, enabling full
// bidirectional streaming from environments such as web browsers.
//
// This is draft 1 of the WebSocket wire protocol, and the shape the other
// drafts are variations on. Four decisions define it:
//
// One WebSocket per streaming RPC. Multiplexing every RPC onto a shared
// connection was considered and rejected as too complicated and too easy to
// get wrong; drafts [3] and [4] show what it costs, in stream IDs on every
// frame and a reset frame in the protocol. Here there is nothing to
// multiplex, so cancelling an RPC is closing its socket. The price is a
// handshake per streaming call, and a browser-enforced ceiling on how many
// WebSockets a page may hold open to one host at once — 255 in Chrome, and
// worth knowing before a page opens streams in a loop.
//
// Unary RPCs stay on HTTP. This transport carries streaming RPCs only; pair
// it with an ordinary Connect HTTP transport through
// [github.com/sudorandom/connect-bidi-web/connectwebsocket.NewCompositeTransport]
// so unary calls keep the caching, proxies, and observability they already
// have.
//
// Headers travel as the first message. Browser APIs cannot attach custom
// headers to the upgrade request and cannot read them off the response, so
// each direction opens with a metadata envelope rather than relying on the
// handshake — the server's as well as the client's.
//
// Every message is a Connect envelope. One envelope per WebSocket message,
// with Connect's own flag byte and length prefix. The length restates the
// message boundary, which is redundant but keeps the envelope exactly
// Connect's. Compression is the native permessage-deflate extension rather
// than Connect's per-message compression: it is transparent, browsers all
// have it, and it leaves the payload legible in devtools.
//
// [3]: https://pkg.go.dev/github.com/sudorandom/connect-bidi-web/connectwebsocket/draft3
// [4]: https://pkg.go.dev/github.com/sudorandom/connect-bidi-web/connectwebsocket/draft4
package draft1

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"golang.org/x/net/http2"
)

// Option configures NewTransport, NewH2Transport, NewHandler, and Mount.
type Option interface {
	applyTransport(*transportOptions)
	applyServer(*serverOptions)
}

type transportOptions struct {
	protocolOptions

	dialOptions        *websocket.DialOptions
	withoutCompression bool
}

type serverOptions struct {
	protocolOptions

	acceptOptions      *websocket.AcceptOptions
	withoutCompression bool
}

type optionFunc func(*transportOptions, *serverOptions)

func (f optionFunc) applyTransport(opts *transportOptions) { f(opts, nil) }
func (f optionFunc) applyServer(opts *serverOptions)       { f(nil, opts) }

// WithSendCodec selects the codec (by name) used for outgoing client
// requests.
func WithSendCodec(name string) Option {
	return optionFunc(func(topts *transportOptions, _ *serverOptions) {
		if topts != nil {
			topts.SendCodecName = name
		}
	})
}

// WithCodecs registers connect.Codec values.
func WithCodecs(codecs ...connect.Codec) Option {
	return optionFunc(func(topts *transportOptions, sopts *serverOptions) {
		if topts != nil {
			topts.addCodecs(codecs...)
		}
		if sopts != nil {
			sopts.addCodecs(codecs...)
		}
	})
}

// WithReadMaxBytes limits the size of a message that can be read.
func WithReadMaxBytes(maxBytes int) Option {
	return optionFunc(func(topts *transportOptions, sopts *serverOptions) {
		if topts != nil {
			topts.ReadMaxBytes = maxBytes
		}
		if sopts != nil {
			sopts.ReadMaxBytes = maxBytes
		}
	})
}

// WithSendMaxBytes limits the size of a message that can be sent.
func WithSendMaxBytes(maxBytes int) Option {
	return optionFunc(func(topts *transportOptions, sopts *serverOptions) {
		if topts != nil {
			topts.SendMaxBytes = maxBytes
		}
		if sopts != nil {
			sopts.SendMaxBytes = maxBytes
		}
	})
}

// WithDialOptions sets custom websocket.DialOptions, used for every
// connection the transport opens. Subprotocols and CompressionMode are
// overridden regardless: they are the protocol's, so custom options cannot
// silently change what is spoken on the wire.
func WithDialOptions(dialOpts *websocket.DialOptions) Option {
	return optionFunc(func(topts *transportOptions, _ *serverOptions) {
		if topts == nil {
			return
		}
		if dialOpts == nil {
			topts.dialOptions = nil
			return
		}
		cloned := *dialOpts
		topts.dialOptions = &cloned
	})
}

// WithAcceptOptions sets custom websocket.AcceptOptions — OriginPatterns in
// particular, which browsers need. Subprotocols and CompressionMode are
// overridden regardless, for the reason given on [WithDialOptions].
func WithAcceptOptions(acceptOpts *websocket.AcceptOptions) Option {
	return optionFunc(func(_ *transportOptions, sopts *serverOptions) {
		if sopts == nil {
			return
		}
		if acceptOpts == nil {
			sopts.acceptOptions = nil
			return
		}
		cloned := *acceptOpts
		sopts.acceptOptions = &cloned
	})
}

// WithoutCompression disables permessage-deflate, which is draft 1's only
// compression: the envelope has a compressed-data flag, but this draft
// deliberately leaves it unused rather than compressing payloads a browser
// would then show as noise.
//
// Worth considering for streams of small messages. Nothing under a few
// hundred bytes compresses, so all that arrives is the negotiation in the
// handshake — which draft 1 pays once per RPC, having no connection reuse
// to amortize it over.
func WithoutCompression() Option {
	return optionFunc(func(topts *transportOptions, sopts *serverOptions) {
		if topts != nil {
			topts.withoutCompression = true
		}
		if sopts != nil {
			sopts.withoutCompression = true
		}
	})
}

// transport dispatches streaming RPCs over WebSocket connections, one per
// RPC.
type transport struct {
	baseURL string
	opts    transportOptions
	// dial opens one connection for the given procedure: an HTTP/1.1
	// upgrade (NewTransport) or an RFC 8441 extended CONNECT stream
	// (NewH2Transport).
	dial func(ctx context.Context, url string) (messageConn, error)
	// h2 is the HTTP/2 client used by dialH2; nil for NewTransport.
	h2 *http2.Transport
}

// NewTransport returns a connect.Transport that dispatches streaming RPCs
// over WebSocket using draft 1 of the wire protocol. Each RPC dials its own
// connection against baseURL, with the procedure appended — so a call to
// /connectrpc.eliza.v1.ElizaService/Converse against a base URL of
// wss://example.com opens
// wss://example.com/connectrpc.eliza.v1.ElizaService/Converse.
//
// Unary RPCs are refused; compose this with an HTTP transport using
// [github.com/sudorandom/connect-bidi-web/connectwebsocket.NewCompositeTransport].
func NewTransport(baseURL string, opts ...Option) connect.Transport {
	tOpts := transportOptions{protocolOptions: newClientProtocolOptions()}
	for _, opt := range opts {
		opt.applyTransport(&tOpts)
	}
	tOpts.finalize()

	t := &transport{baseURL: normalizeWebSocketURL(baseURL), opts: tOpts}
	t.dial = t.dialWebSocket
	return t
}

// normalizeWebSocketURL accepts an http(s) base URL for the WebSocket dial,
// so a caller configuring one endpoint for both dispatch paths does not
// have to spell it twice.
func normalizeWebSocketURL(rawURL string) string {
	trimmed := strings.TrimSuffix(rawURL, "/")
	if rest, ok := strings.CutPrefix(trimmed, "https://"); ok {
		return "wss://" + rest
	}
	if rest, ok := strings.CutPrefix(trimmed, "http://"); ok {
		return "ws://" + rest
	}
	return trimmed
}

// procedureURL returns the URL an RPC dials: the base URL with the
// procedure appended.
func (t *transport) procedureURL(procedure string) (string, error) {
	if t.baseURL == "" {
		return "", errors.New("connectwebsocket/draft1: empty base URL")
	}
	if !strings.HasPrefix(procedure, "/") {
		procedure = "/" + procedure
	}
	full := t.baseURL + procedure
	if _, err := url.Parse(full); err != nil {
		return "", connect.Errorf(connect.CodeInternal, "invalid URL %q: %v", full, err)
	}
	return full, nil
}

// NewClientStream implements connect.Transport.
func (t *transport) NewClientStream(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
	if spec.StreamType == connect.StreamTypeUnary {
		// A WebSocket handshake to carry one request and one response is a
		// bad trade, and it costs one of the browser's limited connection
		// slots. Unary RPCs belong on plain Connect over HTTP.
		return nil, connect.Errorf(
			connect.CodeUnimplemented,
			"connectwebsocket/draft1: unary RPCs are not carried over WebSocket; "+
				"pair this transport with an HTTP one using connectwebsocket.NewCompositeTransport",
		)
	}
	target, err := t.procedureURL(spec.Procedure)
	if err != nil {
		return nil, err
	}

	callInfo, _ := connect.CallInfoForClientContext(ctx)
	if callInfo != nil {
		callInfo.Protocol = protocolName
	}

	conn, err := t.dial(ctx, target)
	if err != nil {
		return nil, err
	}
	return newClientStream(ctx, spec, conn, callInfo, t), nil
}

// dialWebSocket opens a WebSocket with an HTTP/1.1 Upgrade handshake.
func (t *transport) dialWebSocket(ctx context.Context, target string) (messageConn, error) {
	dialOpts := &websocket.DialOptions{}
	if t.opts.dialOptions != nil {
		cloned := *t.opts.dialOptions
		dialOpts = &cloned
	}
	dialOpts.Subprotocols = []string{subprotocol}
	// Context takeover is refused in both directions: a shared compression
	// window across messages is what makes an attacker-influenced payload
	// leak the size of a secret one, and every browser that offers
	// permessage-deflate offers no_context_takeover with it.
	dialOpts.CompressionMode = websocket.CompressionNoContextTakeover
	if t.opts.withoutCompression {
		dialOpts.CompressionMode = websocket.CompressionDisabled
	}

	conn, _, err := websocket.Dial(ctx, target, dialOpts) //nolint:bodyclose // coder/websocket closes the handshake response body itself
	if err != nil {
		return nil, connect.Errorf(connect.CodeUnavailable, "failed to dial WebSocket: %v", err)
	}
	if conn.Subprotocol() != subprotocol {
		_ = conn.CloseNow()
		return nil, connect.Errorf(
			connect.CodeUnavailable,
			"server did not select the %q subprotocol; it may not serve this protocol",
			subprotocol,
		)
	}
	return newCoderConn(conn), nil
}
