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

// Package draft3 provides a connect.Transport and an http.Handler that
// carry Connect RPCs over WebSocket connections, enabling full
// bidirectional streaming from environments such as web browsers.
//
// This is draft 3 of the WebSocket wire protocol, wire-incompatible with
// drafts 1 and 4 in the sibling packages. Its framing is a stream ID, one
// descriptor byte, and the payload, delimited by the WebSocket message; it
// moves compression into the protocol: a WebSocket subprotocol
// (connect.bidi.d3.deflate) negotiates per-frame raw DEFLATE once per
// connection, signaled by one bit in the descriptor.
//
// Compression therefore behaves identically over every bootstrap, and in
// both directions, without depending on the WebSocket's permessage-deflate
// extension being implemented on the path — or on the peer choosing to use
// it, which that extension leaves entirely to the sender. All drafts
// coexist so their implementations can be compared; serve them on
// different paths.
//
// Every frame carries a stream ID, so any number of concurrent RPCs are
// multiplexed onto one shared WebSocket connection; WithConnectionPerStream
// gives each streaming RPC a dedicated connection instead.
package draft3

import (
	"context"
	"errors"
	"sync"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"golang.org/x/net/http2"
)

// Option configures NewTransport and NewHandler.
type Option interface {
	applyTransport(*transportOptions)
	applyServer(*serverOptions)
}

type transportOptions struct {
	protocolOptions

	dialOptions         *websocket.DialOptions
	connectionPerStream bool
	// withoutCompression makes the client offer only the identity
	// subprotocol, so nothing is ever compressed.
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

// WithSendCodec selects the codec (by name) used for outgoing client requests.
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
// connection opened by the transport. Subprotocols and CompressionMode
// are overridden regardless: the draft 3 subprotocols are the protocol's
// negotiation surface, and the permessage-deflate extension is never
// offered (the deflate subprotocol replaces it).
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

// WithoutCompression restricts this side to the identity subprotocol
// (connect.bidi.d3), so no frame is ever compressed in either direction:
// a client stops offering connect.bidi.d3.deflate, a server stops
// selecting it (clients that offer only deflate are then refused). The
// default supports connect.bidi.d3.deflate: per-frame raw DEFLATE for
// payloads of 512 bytes and up, in both directions, negotiated once per
// connection.
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

// WithConnectionPerStream configures the transport to dial a dedicated
// WebSocket connection for each streaming RPC instead of multiplexing all
// RPCs onto one shared connection. A shared connection is subject to
// head-of-line blocking: one stream with a large message or a slow consumer
// delays every other stream behind it. Dedicated connections trade a
// WebSocket handshake per streaming RPC for full isolation. Unary RPCs
// always use the shared multiplexed connection.
func WithConnectionPerStream() Option {
	return optionFunc(func(topts *transportOptions, _ *serverOptions) {
		if topts != nil {
			topts.connectionPerStream = true
		}
	})
}

// WithAcceptOptions sets custom websocket.AcceptOptions. Subprotocols and
// CompressionMode are overridden regardless: the draft 3 subprotocols are
// the protocol's negotiation surface, and the permessage-deflate
// extension is never accepted (the deflate subprotocol replaces it).
func WithAcceptOptions(acceptOpts *websocket.AcceptOptions) Option {
	return optionFunc(func(_ *transportOptions, sopts *serverOptions) {
		if sopts != nil {
			sopts.acceptOptions = acceptOpts
		}
	})
}

type transport struct {
	url  string
	opts transportOptions
	// dialConn opens a new multiplexed connection: an HTTP/1.1 upgrade
	// (NewTransport) or an RFC 8441 extended CONNECT stream on HTTP/2
	// (NewH2Transport). The protocol above the connection is identical.
	dialConn func(ctx context.Context) (*muxConn, error)
	// h2 is the HTTP/2 client used by dialH2; nil for NewTransport.
	h2 *http2.Transport

	mu     sync.Mutex
	shared *muxConn
}

// NewTransport returns a connect.Transport that dispatches RPCs over
// WebSocket using draft 3 of the wire protocol. RPCs are multiplexed onto
// one shared WebSocket connection, dialed lazily on first use and re-dialed
// if it fails; every frame carries the stream ID of the RPC it belongs to.
// With WithConnectionPerStream, each streaming RPC dials a dedicated
// connection instead.
//
// The returned Transport also implements io.Closer: Close closes the shared
// connection, terminating any RPCs still running on it. The transport
// remains usable afterwards.
func NewTransport(url string, opts ...Option) connect.Transport {
	tOpts := transportOptions{protocolOptions: newClientProtocolOptions()}
	for _, opt := range opts {
		opt.applyTransport(&tOpts)
	}
	tOpts.finalize()

	t := &transport{
		url:  url,
		opts: tOpts,
	}
	t.dialConn = t.dialWebSocket
	return t
}

func (t *transport) NewClientStream(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
	if t.url == "" {
		return nil, errors.New("connectwebsocket/draft3: empty URL")
	}

	callInfo, _ := connect.CallInfoForClientContext(ctx)
	if callInfo != nil {
		callInfo.Protocol = protocolName
	}

	var stream *muxStream
	if t.opts.connectionPerStream && spec.StreamType != connect.StreamTypeUnary {
		mc, err := t.dialConn(ctx)
		if err != nil {
			return nil, err
		}
		stream, err = mc.newStream(ctx, true)
		if err != nil {
			_ = mc.shutdown()
			return nil, err
		}
	} else {
		var err error
		stream, err = t.sharedStream(ctx)
		if err != nil {
			return nil, err
		}
	}

	return newClientStream(
		ctx,
		spec,
		stream,
		callInfo,
		t.opts.protocolOptions,
	), nil
}

// Close closes the shared multiplexed connection, if one is open,
// terminating any RPCs still running on it. The next RPC dials a new
// connection.
func (t *transport) Close() error {
	t.mu.Lock()
	shared := t.shared
	t.shared = nil
	t.mu.Unlock()
	if shared == nil {
		return nil
	}
	return shared.shutdown()
}

// offeredSubprotocols lists the draft 3 subprotocol tokens this transport
// offers, most preferred first.
func (t *transport) offeredSubprotocols() []string {
	if t.opts.withoutCompression {
		return []string{subprotocolIdentity}
	}
	return []string{subprotocolDeflate, subprotocolIdentity}
}

// subprotocolCompression maps the server's subprotocol selection to the
// connection's compression choice. A selection outside the draft 3 tokens
// (including none at all) is a failed negotiation.
func subprotocolCompression(selected string) (bool, error) {
	switch selected {
	case subprotocolDeflate:
		return true, nil
	case subprotocolIdentity:
		return false, nil
	default:
		return false, connect.Errorf(connect.CodeUnavailable, "server did not negotiate a draft 3 subprotocol (got %q)", selected)
	}
}

func (t *transport) dialWebSocket(ctx context.Context) (*muxConn, error) {
	dialOpts := &websocket.DialOptions{}
	if t.opts.dialOptions != nil {
		cloned := *t.opts.dialOptions
		dialOpts = &cloned
	}
	// The subprotocols are the protocol's negotiation surface, so they are
	// not configurable, and the permessage-deflate extension is never
	// offered: compression is the deflate subprotocol's job.
	dialOpts.Subprotocols = t.offeredSubprotocols()
	dialOpts.CompressionMode = websocket.CompressionDisabled
	conn, _, err := websocket.Dial(ctx, t.url, dialOpts) //nolint:bodyclose // coder/websocket closes the handshake response body itself
	if err != nil {
		return nil, connect.Errorf(connect.CodeUnavailable, "failed to dial WebSocket: %v", err)
	}
	compress, err := subprotocolCompression(conn.Subprotocol())
	if err != nil {
		_ = conn.Close(websocket.StatusPolicyViolation, "missing draft 3 subprotocol")
		return nil, err
	}
	// The connection outlives any single RPC, so neither the read loop nor
	// writes use an RPC context; closing the connection ends them.
	mc := newMuxConn(context.Background(), newCoderConn(conn), compress)
	go mc.readLoopClient(context.Background()) //nolint:contextcheck,gosec // G118: the shared connection deliberately outlives the RPC that dialed it
	return mc, nil
}

// sharedStream opens a stream on the shared multiplexed connection, dialing
// it on first use and replacing it if it has failed. Dialing holds the
// transport lock, so concurrent RPCs wait for one shared connection instead
// of racing to dial several.
func (t *transport) sharedStream(ctx context.Context) (*muxStream, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.shared != nil {
		stream, err := t.shared.newStream(ctx, false)
		if err == nil {
			return stream, nil
		}
		// The shared connection failed; dial a replacement.
	}
	mc, err := t.dialConn(ctx)
	if err != nil {
		return nil, err
	}
	t.shared = mc
	return mc.newStream(ctx, false)
}
