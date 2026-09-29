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

package draft3

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
	"github.com/sudorandom/connect-bidi-web/internal/connectprotocol"
)

// clientStream implements connect.ClientStream on top of a frameConn.
type clientStream struct {
	spec     connect.Spec
	conn     frameConn
	callInfo *connect.CallInfo
	opts     protocolOptions
	codec    connect.Codec
	ctx      context.Context

	sendHeadersOnce sync.Once
	sendHeadersErr  error

	closeSendOnce sync.Once

	recvHeadersOnce sync.Once
	recvHeadersErr  error

	rxEnd bool
}

// newClientStream returns a clientStream for a single RPC carried by conn.
func newClientStream(
	ctx context.Context,
	spec connect.Spec,
	conn frameConn,
	callInfo *connect.CallInfo,
	opts protocolOptions,
) *clientStream {
	codec := opts.SendCodec
	if codec == nil {
		codec = opts.Codecs[connect.CodecNameProto]
	}
	return &clientStream{
		spec:     spec,
		conn:     conn,
		callInfo: callInfo,
		opts:     opts,
		codec:    codec,
		ctx:      ctx,
	}
}

// SendHeaders writes the request headers frame exactly once.
func (cs *clientStream) SendHeaders() error {
	cs.sendHeadersOnce.Do(func() {
		cs.sendHeadersErr = cs.sendHeaders()
	})
	return cs.sendHeadersErr
}

func (cs *clientStream) sendHeaders() error {
	header := make(http.Header)
	if cs.callInfo != nil && cs.callInfo.RequestHeader() != nil {
		maps.Insert(header, cs.callInfo.RequestHeader().All())
	}
	header.Set(":path", cs.spec.Procedure)
	header.Set("Content-Type", "application/connect+"+cs.codec.Name())
	if deadline, ok := cs.ctx.Deadline(); ok {
		if ms := time.Until(deadline).Milliseconds(); ms > 0 {
			header.Set("Connect-Timeout-Ms", strconv.FormatInt(ms, 10))
		}
	}

	data, err := connectprotocol.MarshalHeaders(header)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal headers: %v", err)
	}

	return cs.conn.WriteFrame(frameTypeHeaders, data)
}

// Send marshals and writes a request message.
func (cs *clientStream) Send(msg any) error {
	if err := cs.SendHeaders(); err != nil {
		return err
	}

	var buf bytes.Buffer
	if err := cs.codec.MarshalWrite(cs.ctx, &buf, msg); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal request: %v", err)
	}
	payload := buf.Bytes()

	if cs.opts.SendMaxBytes > 0 && len(payload) > cs.opts.SendMaxBytes {
		return connect.Errorf(connect.CodeResourceExhausted, "message size %d exceeds send limit %d", len(payload), cs.opts.SendMaxBytes)
	}

	return cs.conn.WriteFrame(frameTypeData, payload)
}

// CloseSend half-closes the stream in the send direction exactly once.
func (cs *clientStream) CloseSend() error {
	var err error
	cs.closeSendOnce.Do(func() {
		err = cs.conn.CloseSend()
	})
	return err
}

// Receive reads the next response message.
func (cs *clientStream) Receive(msg any) error {
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

	frameType, payload, err := cs.conn.ReadFrame()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return connect.Errorf(connect.CodeInternal, "protocol error: unexpected EOF before end-stream frame")
		}
		return err
	}

	if frameType == frameTypeHeaders {
		return connect.Errorf(connect.CodeInternal, "protocol error: unexpected headers frame")
	}

	if frameType == frameTypeEndStream {
		if err := cs.processEndStream(payload); err != nil {
			return err
		}
		return io.EOF
	}

	if frameType != frameTypeData {
		return connect.Errorf(connect.CodeInternal, "protocol error: unknown frame type: 0x%x", frameType)
	}

	if cs.opts.ReadMaxBytes > 0 && len(payload) > cs.opts.ReadMaxBytes {
		return connect.Errorf(connect.CodeResourceExhausted, "message size %d exceeds read limit %d", len(payload), cs.opts.ReadMaxBytes)
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

func (cs *clientStream) recvHeaders() error {
	frameType, payload, err := cs.conn.ReadFrame()
	if err != nil {
		return err
	}
	if frameType != frameTypeHeaders {
		return connect.Errorf(connect.CodeInternal, "protocol error: expected headers frame first, got 0x%x", frameType)
	}
	headers, err := connectprotocol.UnmarshalHeaders(payload)
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

func (cs *clientStream) readUnaryEndStream() error {
	nextType, nextPayload, nextErr := cs.conn.ReadFrame()
	if nextErr != nil {
		return nextErr
	}
	if nextType != frameTypeEndStream {
		return connect.Errorf(connect.CodeInternal, "protocol error: expected end-stream frame after unary response message, got 0x%x", nextType)
	}
	return cs.processEndStream(nextPayload)
}

// processEndStream records the end of the response stream: it unmarshals
// the end-stream payload, surfaces the trailers, and returns the RPC's
// error, if any.
func (cs *clientStream) processEndStream(payload []byte) error {
	cs.rxEnd = true
	cerr, trailers, err := connectprotocol.UnmarshalEndStream(payload)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to unmarshal end-stream frame: %v", err)
	}
	cs.setResponseTrailers(trailers)
	if cerr != nil {
		return cerr
	}
	return nil
}

func (cs *clientStream) setResponseTrailers(trailers http.Header) {
	if cs.callInfo == nil || cs.callInfo.ResponseTrailer() == nil {
		return
	}
	for k, vs := range trailers {
		cs.callInfo.ResponseTrailer().SetValues(k, vs)
	}
}

// Close half-closes the send direction if necessary and releases the
// underlying connection.
func (cs *clientStream) Close() error {
	_ = cs.CloseSend()
	return cs.conn.Close()
}
