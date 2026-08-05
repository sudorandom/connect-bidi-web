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

// Package connectwebtransport provides a connect.Transport and a session
// handler that carry Connect RPCs over WebTransport (HTTP/3), enabling full
// bidirectional streaming from environments such as web browsers. RPCs share
// one WebTransport session, with a dedicated bidirectional stream per RPC.
package connectwebtransport

import (
	"context"
	"errors"

	"connectrpc.com/connect/v2"
	"github.com/quic-go/webtransport-go"
	"github.com/sudorandom/connect-bidi-web/internal/bidiprotocol"
)

// Option configures NewTransport and NewHandler.
type Option interface {
	applyTransport(*transportOptions)
	applyServer(*serverOptions)
}

type transportOptions struct {
	bidiprotocol.Options
}

type serverOptions struct {
	bidiprotocol.Options
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

type transport struct {
	session *webtransport.Session
	opts    transportOptions
}

// NewTransport returns a connect.Transport that dispatches RPCs over
// WebTransport, opening a new bidirectional stream on the session for each
// RPC.
func NewTransport(session *webtransport.Session, opts ...Option) connect.Transport {
	tOpts := transportOptions{Options: bidiprotocol.NewClientOptions()}
	for _, opt := range opts {
		opt.applyTransport(&tOpts)
	}
	tOpts.Finalize()

	return &transport{
		session: session,
		opts:    tOpts,
	}
}

func (t *transport) NewClientStream(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
	if t.session == nil {
		return nil, errors.New("connectwebtransport: nil session")
	}
	stream, err := t.session.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}

	callInfo, _ := connect.CallInfoForClientContext(ctx)
	if callInfo != nil {
		callInfo.Protocol = "webtransport"
	}

	return bidiprotocol.NewClientStream(
		ctx,
		spec,
		&streamConn{stream: stream},
		callInfo,
		t.opts.Options,
	), nil
}

// streamConn adapts a WebTransport bidirectional stream to
// bidiprotocol.Conn. Half-close is expressed by closing the write side of
// the stream (FIN), and envelopes are written head-then-payload since QUIC
// streams have no message boundaries to preserve.
type streamConn struct {
	stream *webtransport.Stream
}

func (c *streamConn) ReadEnvelope() (uint8, []byte, error) {
	return bidiprotocol.ReadEnvelope(c.stream)
}

func (c *streamConn) WriteEnvelope(flag uint8, payload []byte) error {
	return bidiprotocol.WriteEnvelopeChunked(c.stream, flag, payload)
}

func (c *streamConn) CloseSend() error {
	return c.stream.Close()
}

func (c *streamConn) Close() error {
	c.stream.CancelRead(0)
	return nil
}
