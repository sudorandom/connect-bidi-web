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

package draft1

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
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/internal/connectprotocol"
)

// clientStream implements connect.ClientStream over one WebSocket, which
// exists for exactly this RPC and closes with it.
type clientStream struct {
	spec  connect.Spec
	conn  messageConn
	info  *connect.CallInfo
	codec connect.Codec
	opts  *transportOptions
	ctx   context.Context //nolint:containedctx // the stream's lifetime is the RPC's

	sendHeadersOnce sync.Once
	sendHeadersErr  error
	closeSendOnce   sync.Once
	closeSendErr    error
	recvHeadersOnce sync.Once
	recvHeadersErr  error
	closeOnce       sync.Once

	// rxEnd records that the end-stream envelope has been read, so the
	// connection can be closed with a closing handshake rather than torn
	// down.
	rxEnd bool

	cancelWatch context.CancelFunc
}

func newClientStream(
	ctx context.Context,
	spec connect.Spec,
	conn messageConn,
	info *connect.CallInfo,
	trans *transport,
) *clientStream {
	codec := trans.opts.SendCodec
	if info != nil {
		info.Spec = spec
		info.Protocol = protocolName
		info.Codec = codec.Name()
		// Draft 1 leaves the envelope's compressed flag unused: compression
		// is permessage-deflate, one layer down, and applies to the whole
		// message rather than to the payload inside the envelope.
		info.RequestEncoding = connect.CompressionNameIdentity
	}
	stream := &clientStream{
		spec:  spec,
		conn:  conn,
		info:  info,
		codec: codec,
		opts:  &trans.opts,
		ctx:   ctx,
	}
	stream.watchCancel()
	return stream
}

// watchCancel tears the connection down when the RPC's context ends. There
// is no reset envelope to send: the connection carries this RPC and nothing
// else, so closing it says everything a reset would.
func (cs *clientStream) watchCancel() {
	watchCtx, stop := context.WithCancel(context.Background())
	cs.cancelWatch = stop
	go func() {
		select {
		case <-cs.ctx.Done():
			_ = cs.conn.CloseNow()
		case <-watchCtx.Done():
		}
	}()
}

// SendHeaders writes the request headers envelope, the first message of the
// RPC. It carries the metadata a browser cannot put on the handshake; the
// procedure is not among it, because the URL already named it.
func (cs *clientStream) SendHeaders() error {
	cs.sendHeadersOnce.Do(func() {
		cs.sendHeadersErr = cs.sendHeaders()
	})
	return cs.sendHeadersErr
}

func (cs *clientStream) sendHeaders() error {
	header := make(http.Header)
	if cs.info != nil && cs.info.RequestHeader() != nil {
		maps.Insert(header, cs.info.RequestHeader().All())
	}
	header.Set("Content-Type", contentTypePrefix+cs.codec.Name())
	header.Set("Connect-Protocol-Version", "1")
	if deadline, ok := cs.ctx.Deadline(); ok {
		if ms := time.Until(deadline).Milliseconds(); ms > 0 {
			header.Set("Connect-Timeout-Ms", strconv.FormatInt(ms, 10))
		}
	}

	payload, err := connectprotocol.MarshalHeaders(header)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal headers: %v", err)
	}
	return cs.writeEnvelope(flagHeaders, payload)
}

// Send marshals and writes one request message.
func (cs *clientStream) Send(msg any) error {
	if err := cs.SendHeaders(); err != nil {
		return err
	}
	payload, err := marshalMessage(cs.ctx, cs.codec, msg, cs.opts.SendMaxBytes)
	if err != nil {
		return err
	}
	return cs.writeEnvelope(flagData, payload)
}

// CloseSend writes the empty end-stream envelope, half-closing the request
// direction. The envelope is necessary because neither the stream nor the
// WebSocket has a send-direction close of its own.
func (cs *clientStream) CloseSend() error {
	cs.closeSendOnce.Do(func() {
		if err := cs.SendHeaders(); err != nil {
			cs.closeSendErr = err
			return
		}
		cs.closeSendErr = cs.writeEnvelope(flagEndStream, nil)
	})
	return cs.closeSendErr
}

func (cs *clientStream) writeEnvelope(flag uint8, payload []byte) error {
	message, err := encodeEnvelope(flag, payload)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to encode envelope: %v", err)
	}
	if err := cs.conn.WriteMessage(cs.ctx, message); err != nil {
		return wrapWriteError(err)
	}
	return nil
}

// Receive reads the next response message.
func (cs *clientStream) Receive(msg any) error {
	if err := cs.SendHeaders(); err != nil {
		return err
	}
	cs.recvHeadersOnce.Do(func() {
		cs.recvHeadersErr = cs.receiveHeaders()
	})
	if cs.recvHeadersErr != nil {
		return cs.recvHeadersErr
	}
	if cs.rxEnd {
		return io.EOF
	}

	flag, payload, err := cs.readEnvelope()
	if err != nil {
		return err
	}
	switch flag {
	case flagEndStream:
		return cs.finishEndStream(payload)
	case flagData:
		if err := unmarshalMessage(cs.ctx, cs.codec, payload, msg, cs.opts.ReadMaxBytes); err != nil {
			return err
		}
	case flagHeaders:
		return connect.Errorf(connect.CodeInternal, "protocol error: a second response headers envelope")
	default:
		return connect.Errorf(connect.CodeInternal, "protocol error: unexpected envelope flag 0x%02x", flag)
	}
	if cs.spec.StreamType == connect.StreamTypeClient {
		// A client-streaming RPC has one response message and its caller
		// Receives once, so the rest of the stream has to be read here or
		// an error in the trailers would never surface.
		if err := cs.receiveEndStream(); !errors.Is(err, io.EOF) {
			return err
		}
	}
	return nil
}

func (cs *clientStream) receiveHeaders() error {
	flag, payload, err := cs.readEnvelope()
	if err != nil {
		return err
	}
	if flag != flagHeaders {
		return connect.Errorf(
			connect.CodeInternal,
			"protocol error: expected a response headers envelope first, got flag 0x%02x", flag,
		)
	}
	headers, err := connectprotocol.UnmarshalHeaders(payload)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to unmarshal response headers: %v", err)
	}
	if cs.info != nil && cs.info.ResponseHeader() != nil {
		for key, values := range headers {
			if strings.HasPrefix(key, ":") {
				continue
			}
			cs.info.ResponseHeader().SetValues(key, values)
		}
	}
	return nil
}

// receiveEndStream reads what must be the end-stream envelope.
func (cs *clientStream) receiveEndStream() error {
	flag, payload, err := cs.readEnvelope()
	if err != nil {
		return err
	}
	if flag != flagEndStream {
		return connect.Errorf(
			connect.CodeInternal,
			"protocol error: expected an end-stream envelope, got flag 0x%02x", flag,
		)
	}
	return cs.finishEndStream(payload)
}

// finishEndStream surfaces an end-stream payload's trailers and returns the
// RPC's error — or io.EOF when it succeeded.
func (cs *clientStream) finishEndStream(payload []byte) error {
	cs.rxEnd = true
	rpcErr, trailers, err := connectprotocol.UnmarshalEndStream(payload)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to unmarshal end-stream envelope: %v", err)
	}
	if cs.info != nil && cs.info.ResponseTrailer() != nil {
		for key, values := range trailers {
			cs.info.ResponseTrailer().SetValues(key, values)
		}
	}
	if rpcErr != nil {
		return rpcErr
	}
	return io.EOF
}

// readEnvelope reads one message and splits it into flag and payload.
func (cs *clientStream) readEnvelope() (uint8, []byte, error) {
	message, err := cs.conn.ReadMessage(cs.ctx)
	if err != nil {
		return 0, nil, cs.readError(err)
	}
	flag, payload, err := decodeEnvelope(message)
	if err != nil {
		return 0, nil, connect.Errorf(connect.CodeInternal, "protocol error: %v", err)
	}
	if cs.opts.ReadMaxBytes > 0 && len(payload) > cs.opts.ReadMaxBytes {
		return 0, nil, connect.Errorf(
			connect.CodeResourceExhausted,
			"message size %d exceeds read limit %d", len(payload), cs.opts.ReadMaxBytes,
		)
	}
	return flag, payload, nil
}

func (cs *clientStream) readError(err error) error {
	if ctxErr := cs.ctx.Err(); ctxErr != nil {
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			return connect.Errorf(connect.CodeDeadlineExceeded, "RPC deadline exceeded")
		}
		return connect.Errorf(connect.CodeCanceled, "RPC canceled")
	}
	if errors.Is(err, io.EOF) || websocket.CloseStatus(err) != -1 {
		return connect.Errorf(connect.CodeUnavailable, "connection closed before the end-stream envelope")
	}
	if connectErr := new(connect.Error); errors.As(err, &connectErr) {
		return err
	}
	return connect.Errorf(connect.CodeUnavailable, "failed to read from WebSocket: %v", err)
}

// Close ends the RPC by closing its connection, which is all draft 1 has to
// do: the socket carries this call and nothing else.
func (cs *clientStream) Close() error {
	var err error
	cs.closeOnce.Do(func() {
		if cs.cancelWatch != nil {
			cs.cancelWatch()
		}
		if cs.rxEnd {
			err = cs.conn.Close()
			return
		}
		// Abandoned before the end-stream envelope: the server is still
		// mid-RPC as far as it knows, and tearing the socket down is how it
		// finds out.
		err = cs.conn.CloseNow()
	})
	return err
}

// marshalMessage encodes one RPC message and enforces the send limit.
func marshalMessage(ctx context.Context, codec connect.Codec, msg any, sendMaxBytes int) ([]byte, error) {
	var buf bytes.Buffer
	if err := codec.MarshalWrite(ctx, &buf, msg); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to marshal message: %v", err)
	}
	payload := buf.Bytes()
	if sendMaxBytes > 0 && len(payload) > sendMaxBytes {
		return nil, connect.Errorf(
			connect.CodeResourceExhausted,
			"message size %d exceeds send limit %d", len(payload), sendMaxBytes,
		)
	}
	return payload, nil
}

// unmarshalMessage decodes one RPC message.
func unmarshalMessage(ctx context.Context, codec connect.Codec, data []byte, msg any, readMaxBytes int) error {
	if readMaxBytes > 0 && len(data) > readMaxBytes {
		return connect.Errorf(
			connect.CodeResourceExhausted,
			"message size %d exceeds read limit %d", len(data), readMaxBytes,
		)
	}
	if err := codec.UnmarshalRead(ctx, bytes.NewReader(data), msg); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to unmarshal message: %v", err)
	}
	return nil
}

// wrapWriteError maps a transport-level write failure onto an RPC error.
func wrapWriteError(err error) error {
	if errors.Is(err, context.Canceled) {
		return connect.Errorf(connect.CodeCanceled, "RPC canceled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return connect.Errorf(connect.CodeDeadlineExceeded, "RPC deadline exceeded")
	}
	return connect.Errorf(connect.CodeUnavailable, "failed to write to WebSocket: %v", err)
}
