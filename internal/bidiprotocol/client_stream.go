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

package bidiprotocol

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect/v2"
)

// ClientStream implements connect.ClientStream on top of a Conn. It is shared
// by the WebSocket and WebTransport transports.
type ClientStream struct {
	spec     connect.Spec
	conn     Conn
	callInfo *connect.CallInfo
	opts     Options
	codec    connect.Codec
	ctx      context.Context

	sendHeadersOnce sync.Once
	sendHeadersErr  error

	closeSendOnce sync.Once

	recvHeadersOnce sync.Once
	recvHeadersErr  error

	rxEnd bool
}

// NewClientStream returns a ClientStream for a single RPC carried by conn.
func NewClientStream(
	ctx context.Context,
	spec connect.Spec,
	conn Conn,
	callInfo *connect.CallInfo,
	opts Options,
) *ClientStream {
	codec := opts.SendCodec
	if codec == nil {
		codec = opts.Codecs[connect.CodecNameProto]
	}
	return &ClientStream{
		spec:     spec,
		conn:     conn,
		callInfo: callInfo,
		opts:     opts,
		codec:    codec,
		ctx:      ctx,
	}
}

// SendHeaders writes the request headers envelope exactly once.
func (cs *ClientStream) SendHeaders() error {
	cs.sendHeadersOnce.Do(func() {
		cs.sendHeadersErr = cs.sendHeaders()
	})
	return cs.sendHeadersErr
}

func (cs *ClientStream) sendHeaders() error {
	header := make(http.Header)
	if cs.callInfo != nil && cs.callInfo.RequestHeader() != nil {
		maps.Insert(header, cs.callInfo.RequestHeader().All())
	}
	header.Set(":path", cs.spec.Procedure)
	header.Set("Content-Type", "application/connect+"+cs.codec.Name())
	if cs.opts.SendCompressor != "" && cs.opts.SendCompressor != connect.CompressionNameIdentity {
		header.Set("Connect-Content-Encoding", cs.opts.SendCompressor)
	}
	if cs.opts.AcceptCompression != "" {
		header.Set("Connect-Accept-Encoding", cs.opts.AcceptCompression)
	}
	if deadline, ok := cs.ctx.Deadline(); ok {
		if ms := time.Until(deadline).Milliseconds(); ms > 0 {
			header.Set("Connect-Timeout-Ms", strconv.FormatInt(ms, 10))
		}
	}

	data, err := MarshalHeaders(header)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal headers: %v", err)
	}

	return cs.conn.WriteEnvelope(FlagEnvelopeHeaders, data)
}

// Send marshals and writes a request message.
func (cs *ClientStream) Send(msg any) error {
	if err := cs.SendHeaders(); err != nil {
		return err
	}

	var buf bytes.Buffer
	if err := cs.codec.MarshalWrite(cs.ctx, &buf, msg); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal request: %v", err)
	}
	payload := buf.Bytes()

	flag := FlagEnvelopeData
	var compressor connect.Compressor
	if cs.opts.SendCompressor != "" && cs.opts.SendCompressor != connect.CompressionNameIdentity {
		compressor = cs.opts.Compressors[cs.opts.SendCompressor]
	}

	if compressor != nil {
		compressed, err := compress(compressor, payload)
		if err != nil {
			return err
		}
		payload = compressed
		flag = FlagEnvelopeCompressed
	}

	if cs.opts.SendMaxBytes > 0 && len(payload) > cs.opts.SendMaxBytes {
		return connect.Errorf(connect.CodeResourceExhausted, "message size %d exceeds send limit %d", len(payload), cs.opts.SendMaxBytes)
	}

	return cs.conn.WriteEnvelope(flag, payload)
}

// CloseSend half-closes the stream in the send direction exactly once.
func (cs *ClientStream) CloseSend() error {
	var err error
	cs.closeSendOnce.Do(func() {
		err = cs.conn.CloseSend()
	})
	return err
}

// Receive reads the next response message.
func (cs *ClientStream) Receive(msg any) error {
	// The request headers must precede everything, even for receive-first RPCs.
	if err := cs.SendHeaders(); err != nil {
		return err
	}

	cs.recvHeadersOnce.Do(func() {
		cs.recvHeadersErr = cs.recvHeaders()
	})
	if cs.recvHeadersErr != nil {
		return cs.recvHeadersErr
	}

	if cs.rxEnd {
		return io.EOF
	}

	flag, payload, err := cs.conn.ReadEnvelope()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return connect.Errorf(connect.CodeInternal, "protocol error: unexpected EOF before end stream envelope")
		}
		return err
	}

	if flag == FlagEnvelopeHeaders {
		return connect.Errorf(connect.CodeInternal, "protocol error: unexpected headers frame")
	}

	if flag == FlagEnvelopeEndStream {
		cs.rxEnd = true
		cerr, trailers, err := UnmarshalEndStream(payload)
		if err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to unmarshal end stream envelope: %v", err)
		}
		cs.setResponseTrailers(trailers)
		if cerr != nil {
			return cerr
		}
		return io.EOF
	}

	if flag != FlagEnvelopeData && flag != FlagEnvelopeCompressed {
		return connect.Errorf(connect.CodeInternal, "protocol error: unknown frame flag: 0x%x", flag)
	}

	if cs.opts.ReadMaxBytes > 0 && len(payload) > cs.opts.ReadMaxBytes {
		return connect.Errorf(connect.CodeResourceExhausted, "message size %d exceeds read limit %d", len(payload), cs.opts.ReadMaxBytes)
	}

	if flag == FlagEnvelopeCompressed {
		encoding := cs.callInfo.ResponseHeader().Get("Connect-Content-Encoding")
		compressor := cs.opts.Compressors[encoding]
		if compressor == nil {
			return connect.Errorf(connect.CodeInternal, "protocol error: compressed message without Connect-Content-Encoding")
		}
		decompressed, err := decompress(compressor, payload)
		if err != nil {
			return err
		}
		payload = decompressed
	}

	if err := cs.codec.UnmarshalRead(cs.ctx, bytes.NewReader(payload), msg); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to unmarshal response: %v", err)
	}

	if cs.spec.StreamType == connect.StreamTypeUnary {
		if err := cs.readUnaryEndStream(); err != nil {
			return err
		}
	}
	return nil
}

func (cs *ClientStream) recvHeaders() error {
	flag, payload, err := cs.conn.ReadEnvelope()
	if err != nil {
		return err
	}
	if flag != FlagEnvelopeHeaders {
		return connect.Errorf(connect.CodeInternal, "protocol error: expected headers frame first, got 0x%x", flag)
	}
	headers, err := UnmarshalHeaders(payload)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to unmarshal response headers: %v", err)
	}
	if cs.callInfo != nil && cs.callInfo.ResponseHeader() != nil {
		for k, vs := range headers {
			// Pseudo-headers aren't surfaced to the application.
			if strings.HasPrefix(k, ":") {
				continue
			}
			cs.callInfo.ResponseHeader().SetValues(k, vs)
		}
	}
	return nil
}

func (cs *ClientStream) readUnaryEndStream() error {
	nextFlag, nextPayload, nextErr := cs.conn.ReadEnvelope()
	if nextErr != nil {
		return nextErr
	}
	if nextFlag != FlagEnvelopeEndStream {
		return connect.Errorf(connect.CodeInternal, "protocol error: expected end stream envelope after unary response message, got 0x%x", nextFlag)
	}
	cs.rxEnd = true
	cerr, trailers, err := UnmarshalEndStream(nextPayload)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to unmarshal end stream envelope: %v", err)
	}
	cs.setResponseTrailers(trailers)
	if cerr != nil {
		return cerr
	}
	return nil
}

func (cs *ClientStream) setResponseTrailers(trailers http.Header) {
	if cs.callInfo == nil || cs.callInfo.ResponseTrailer() == nil {
		return
	}
	for k, vs := range trailers {
		cs.callInfo.ResponseTrailer().SetValues(k, vs)
	}
}

// Close half-closes the send direction if necessary and releases the
// underlying connection.
func (cs *ClientStream) Close() error {
	_ = cs.CloseSend()
	return cs.conn.Close()
}
