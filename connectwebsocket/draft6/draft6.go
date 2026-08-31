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

// Package draft6 provides a connect.Transport and an http.Handler that
// carry Connect RPCs over WebSocket connections, enabling full
// bidirectional streaming from environments such as web browsers.
//
// This is draft 6 of the WebSocket wire protocol. On the wire it is
// [draft 5]: no framing, no multiplexing, one RPC at a time, with the
// WebSocket opcode carrying the only distinction the protocol needs. It
// changes two things about how that protocol is deployed and paid for.
//
// One endpoint, not one per procedure. Draft 5 mounts on the Connect
// procedure URLs themselves and reads the procedure off the URL. Draft 6
// takes a single path ([DefaultPath] unless the deployment picks another)
// and names the procedure in a ":path" pseudo-header in the request
// headers message, the way HTTP/2 names it in a pseudo-header rather than
// in the request line. One route to configure, one handler to mount, and
// nothing about the deployment has to change when a service gains a method.
//
// Connections outlive their RPCs. Draft 5's connection carries exactly one
// RPC and then closes, so every streaming call pays a handshake. A draft 6
// connection returns to an idle pool when its RPC ends and carries the next
// one, so the handshake is paid once per connection rather than once per
// call — while a connection still carries a single RPC at a time, so there
// is still no multiplexing and still no stream ID.
//
// Unary RPCs do not upgrade. This transport carries streaming RPCs; pair it
// with an HTTP transport through
// [github.com/sudorandom/connect-bidi-web/connectwebsocket.NewCompositeTransport]
// to keep unary calls on plain Connect over HTTP, where caching, proxies,
// and observability already work.
//
// [draft 5]: https://pkg.go.dev/github.com/sudorandom/connect-bidi-web/connectwebsocket/draft5
package draft6

import (
	"context"
	"errors"
	"sync"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"golang.org/x/net/http2"
)

// DefaultMaxIdleConns is how many idle connections a transport keeps for
// reuse when the caller does not say. Idle connections cost a socket and a
// little server memory each; the point of pooling is to avoid a handshake
// on the next call, and a handful covers the concurrency most callers have.
const DefaultMaxIdleConns = 8

// Option configures NewTransport and NewHandler.
type Option interface {
	applyTransport(*transportOptions)
	applyServer(*serverOptions)
}

type transportOptions struct {
	protocolOptions

	dialOptions        *websocket.DialOptions
	withoutCompression bool
	maxIdleConns       int
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

// WithMaxIdleConns caps how many idle connections the transport keeps for
// reuse, defaulting to [DefaultMaxIdleConns]. Zero disables pooling, which
// makes every RPC pay a handshake — draft 5's behaviour, and a useful thing
// to measure against. Negative values are treated as zero.
func WithMaxIdleConns(maxIdle int) Option {
	return optionFunc(func(topts *transportOptions, _ *serverOptions) {
		if topts != nil {
			topts.maxIdleConns = max(maxIdle, 0)
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

// WithoutCompression disables permessage-deflate, which is the protocol's
// only compression: with no envelope, a data message has nowhere to carry a
// "this one is compressed" bit.
//
// Worth considering for streams of small messages. Nothing under a few
// hundred bytes compresses, so all that arrives is the negotiation in the
// handshake — though draft 6 pays that once per connection rather than
// once per RPC, which is exactly the cost draft 5 could not amortize.
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

// transport dispatches streaming RPCs over a pool of WebSocket
// connections, each carrying one RPC at a time.
type transport struct {
	url  string
	opts transportOptions
	// dial opens one new connection: an HTTP/1.1 upgrade (NewTransport) or
	// an RFC 8441 extended CONNECT stream (NewH2Transport).
	dial func(ctx context.Context) (messageConn, error)
	// h2 is the HTTP/2 client used by dialH2; nil for NewTransport.
	h2 *http2.Transport

	mu   sync.Mutex
	idle []messageConn
}

// NewTransport returns a connect.Transport that dispatches streaming RPCs
// over WebSocket using draft 6 of the wire protocol. Connections are
// pooled: an RPC takes an idle one if there is one and dials otherwise, and
// hands it back when it finishes.
//
// The returned Transport also implements io.Closer. Close drops every idle
// connection; RPCs still running are unaffected, and the transport stays
// usable.
func NewTransport(url string, opts ...Option) connect.Transport {
	tOpts := transportOptions{
		protocolOptions: newClientProtocolOptions(),
		maxIdleConns:    DefaultMaxIdleConns,
	}
	for _, opt := range opts {
		opt.applyTransport(&tOpts)
	}
	tOpts.finalize()

	t := &transport{url: url, opts: tOpts}
	t.dial = t.dialWebSocket
	return t
}

// NewClientStream implements connect.Transport.
func (t *transport) NewClientStream(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
	if t.url == "" {
		return nil, errors.New("connectwebsocket/draft6: empty URL")
	}
	if spec.StreamType == connect.StreamTypeUnary {
		// Unary RPCs stay on plain Connect over HTTP, as in draft 5. Compose
		// this transport with an HTTP one rather than upgrading for a single
		// request and response.
		return nil, connect.Errorf(
			connect.CodeUnimplemented,
			"connectwebsocket/draft6: unary RPCs are not carried over WebSocket; "+
				"pair this transport with an HTTP one using connectwebsocket.NewCompositeTransport",
		)
	}

	callInfo, _ := connect.CallInfoForClientContext(ctx)
	if callInfo != nil {
		callInfo.Protocol = protocolName
	}

	conn, reused, err := t.acquire(ctx)
	if err != nil {
		return nil, err
	}
	return newClientStream(ctx, spec, conn, reused, callInfo, t), nil
}

// Close drops every idle connection. RPCs in flight keep theirs.
func (t *transport) Close() error {
	t.mu.Lock()
	idle := t.idle
	t.idle = nil
	t.mu.Unlock()
	for _, conn := range idle {
		_ = conn.Close()
	}
	return nil
}

// acquire returns a connection to carry one RPC, and whether it came from
// the pool. A pooled connection may have been closed by the peer while it
// sat idle — nobody was reading it — so the caller retries once on a fresh
// connection if the first write fails. That is the same bargain net/http
// makes with its own keep-alives.
func (t *transport) acquire(ctx context.Context) (messageConn, bool, error) {
	t.mu.Lock()
	if n := len(t.idle); n > 0 {
		conn := t.idle[n-1]
		t.idle = t.idle[:n-1]
		t.mu.Unlock()
		return conn, true, nil
	}
	t.mu.Unlock()

	conn, err := t.dial(ctx)
	if err != nil {
		return nil, false, err
	}
	return conn, false, nil
}

// release returns a connection to the pool once its RPC has finished
// cleanly. A connection whose RPC failed is never pooled: after a protocol
// error or a transport failure neither side can be sure where the other
// thinks the message boundary is.
func (t *transport) release(conn messageConn) {
	t.mu.Lock()
	if len(t.idle) >= t.opts.maxIdleConns {
		t.mu.Unlock()
		_ = conn.Close()
		return
	}
	t.idle = append(t.idle, conn)
	t.mu.Unlock()
}

// dialWebSocket opens a WebSocket with an HTTP/1.1 Upgrade handshake.
func (t *transport) dialWebSocket(ctx context.Context) (messageConn, error) {
	dialOpts := &websocket.DialOptions{}
	if t.opts.dialOptions != nil {
		cloned := *t.opts.dialOptions
		dialOpts = &cloned
	}
	dialOpts.Subprotocols = []string{subprotocol}
	dialOpts.CompressionMode = websocket.CompressionNoContextTakeover
	if t.opts.withoutCompression {
		dialOpts.CompressionMode = websocket.CompressionDisabled
	}

	conn, _, err := websocket.Dial(ctx, t.url, dialOpts) //nolint:bodyclose // coder/websocket closes the handshake response body itself
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
