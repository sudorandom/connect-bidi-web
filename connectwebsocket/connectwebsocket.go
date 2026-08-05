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

// Package connectwebsocket provides a connect.Transport and an http.Handler
// that carry Connect RPCs over WebSocket connections, enabling full
// bidirectional streaming from environments such as web browsers. Each RPC
// uses a dedicated WebSocket connection.
package connectwebsocket

import (
	"context"
	"errors"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/internal/bidiprotocol"
)

// Option configures NewTransport and NewHandler.
type Option interface {
	applyTransport(*transportOptions)
	applyServer(*serverOptions)
}

type transportOptions struct {
	bidiprotocol.Options

	dialOptions *websocket.DialOptions
}

type serverOptions struct {
	bidiprotocol.Options

	acceptOptions *websocket.AcceptOptions
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
			topts.AddCodecs(codecs...)
		}
		if sopts != nil {
			sopts.AddCodecs(codecs...)
		}
	})
}

// WithSendCompressor selects the compressor (by name) to apply to outgoing
// client payloads.
func WithSendCompressor(name string) Option {
	return optionFunc(func(topts *transportOptions, _ *serverOptions) {
		if topts != nil {
			topts.SendCompressor = name
		}
	})
}

// WithCompressors registers connect.Compressor values.
func WithCompressors(compressors ...connect.Compressor) Option {
	return optionFunc(func(topts *transportOptions, sopts *serverOptions) {
		if topts != nil {
			topts.AddCompressors(compressors...)
		}
		if sopts != nil {
			sopts.AddCompressors(compressors...)
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

// WithDialOptions sets custom websocket.DialOptions. The options are used for
// every connection opened by the transport.
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

// WithAcceptOptions sets custom websocket.AcceptOptions.
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
}

// NewTransport returns a connect.Transport that dispatches RPCs over
// WebSocket. It opens a dedicated WebSocket connection for each RPC.
func NewTransport(url string, opts ...Option) connect.Transport {
	tOpts := transportOptions{Options: bidiprotocol.NewClientOptions()}
	for _, opt := range opts {
		opt.applyTransport(&tOpts)
	}
	tOpts.Finalize()

	return &transport{
		url:  url,
		opts: tOpts,
	}
}

func (t *transport) NewClientStream(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
	if t.url == "" {
		return nil, errors.New("connectwebsocket: empty URL")
	}
	conn, _, err := websocket.Dial(ctx, t.url, t.opts.dialOptions) //nolint:bodyclose // coder/websocket closes the handshake response body itself
	if err != nil {
		return nil, connect.Errorf(connect.CodeUnavailable, "failed to dial WebSocket: %v", err)
	}
	conn.SetReadLimit(-1)

	callInfo, _ := connect.CallInfoForClientContext(ctx)
	if callInfo != nil {
		callInfo.Protocol = "websocket"
	}

	rwc := websocket.NetConn(ctx, conn, websocket.MessageBinary)
	return bidiprotocol.NewClientStream(
		ctx,
		spec,
		bidiprotocol.NewNetConn(rwc),
		callInfo,
		t.opts.Options,
	), nil
}
