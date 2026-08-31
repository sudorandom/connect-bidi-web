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

package draft6

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

// clientStream implements connect.ClientStream over one WebSocket, which it
// borrows from the transport's pool for the RPC's lifetime and returns when
// the RPC finishes cleanly.
type clientStream struct {
	spec      connect.Spec
	conn      messageConn
	transport *transport
	info      *connect.CallInfo
	codec     connect.Codec
	opts      *transportOptions
	ctx       context.Context //nolint:containedctx // the stream's lifetime is the RPC's

	// reused records that the connection came from the pool, so a failure
	// writing the first message can be retried once on a fresh one.
	reused bool

	sendHeadersOnce sync.Once
	sendHeadersErr  error
	closeSendOnce   sync.Once
	closeSendErr    error
	recvHeadersOnce sync.Once
	recvHeadersErr  error
	closeOnce       sync.Once

	// rxEnd records that the end-stream message has been read, which is
	// also what makes the connection reusable.
	rxEnd bool
	// failed records that something went wrong, so the connection is
	// dropped rather than pooled: after a protocol error neither side can
	// be sure where the other thinks the message boundary is.
	failed bool

	cancelWatch context.CancelFunc
}

func newClientStream(
	ctx context.Context,
	spec connect.Spec,
	conn messageConn,
	reused bool,
	info *connect.CallInfo,
	trans *transport,
) *clientStream {
	codec := trans.opts.SendCodec
	if info != nil {
		info.Spec = spec
		info.Protocol = protocolName
		info.Codec = codec.Name()
		// Draft 6 has no per-message compression: with no envelope, a data
		// message has nowhere to carry a "this one is compressed" bit.
		info.RequestEncoding = connect.CompressionNameIdentity
	}
	stream := &clientStream{
		spec:      spec,
		conn:      conn,
		transport: trans,
		info:      info,
		codec:     codec,
		opts:      &trans.opts,
		ctx:       ctx,
		reused:    reused,
	}
	stream.watchCancel()
	return stream
}

// watchCancel tears the connection down when the RPC's context ends. There
// is no reset frame to send — draft 6 inherits draft 5's "cancellation is
// closing the socket" — so cancelling costs the connection, which is the
// one place pooling and cancellation pull against each other.
func (cs *clientStream) watchCancel() {
	watchCtx, stop := context.WithCancel(context.Background())
	cs.cancelWatch = stop
	go func() {
		select {
		case <-cs.ctx.Done():
			cs.failed = true
			_ = cs.conn.CloseNow()
		case <-watchCtx.Done():
		}
	}()
}

// SendHeaders writes the request headers message, naming the procedure in
// the ":path" pseudo-header. It is the first message of the RPC, and on a
// pooled connection it is also the first thing to discover that the peer
// closed while the connection sat idle — so a failure here retries once on
// a fresh connection.
func (cs *clientStream) SendHeaders() error {
	cs.sendHeadersOnce.Do(func() {
		cs.sendHeadersErr = cs.sendHeaders()
		if cs.sendHeadersErr != nil && cs.reused {
			cs.sendHeadersErr = cs.retryOnFreshConn()
		}
		if cs.sendHeadersErr != nil {
			cs.failed = true
		}
	})
	return cs.sendHeadersErr
}

// retryOnFreshConn replaces a pooled connection that turned out to be dead
// and re-sends the headers. Nothing of the RPC has reached the peer yet, so
// this is safe: no request message has been written, and no response has
// been read.
func (cs *clientStream) retryOnFreshConn() error {
	_ = cs.conn.CloseNow()
	conn, err := cs.transport.dial(cs.ctx)
	if err != nil {
		return err
	}
	cs.conn = conn
	cs.reused = false
	// The cancel watcher holds the old connection; point it at the new one.
	if cs.cancelWatch != nil {
		cs.cancelWatch()
	}
	cs.watchCancel()
	return cs.sendHeaders()
}

func (cs *clientStream) sendHeaders() error {
	header := make(http.Header)
	if cs.info != nil && cs.info.RequestHeader() != nil {
		maps.Insert(header, cs.info.RequestHeader().All())
	}
	header.Set(pseudoHeaderPath, cs.spec.Procedure)
	header.Set("Content-Type", contentTypePrefix+cs.codec.Name())
	header.Set("Connect-Protocol-Version", "1")
	if deadline, ok := cs.ctx.Deadline(); ok {
		if ms := time.Until(deadline).Milliseconds(); ms > 0 {
			header.Set("Connect-Timeout-Ms", strconv.FormatInt(ms, 10))
		}
	}

	data, err := connectprotocol.MarshalHeaders(header)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal headers: %v", err)
	}
	if err := cs.conn.WriteMessage(cs.ctx, data, true); err != nil {
		return wrapWriteError(err)
	}
	return nil
}

// Send marshals and writes one request message.
func (cs *clientStream) Send(msg any) error {
	if err := cs.SendHeaders(); err != nil {
		return err
	}
	payload, err := marshalMessage(cs.ctx, cs.codec, msg, cs.opts.SendMaxBytes)
	if err != nil {
		cs.failed = true
		return err
	}
	if err := cs.conn.WriteMessage(cs.ctx, payload, dataIsText(payload)); err != nil {
		cs.failed = true
		return wrapWriteError(err)
	}
	return nil
}

// CloseSend writes the separator, half-closing the request direction.
func (cs *clientStream) CloseSend() error {
	cs.closeSendOnce.Do(func() {
		if err := cs.SendHeaders(); err != nil {
			cs.closeSendErr = err
			return
		}
		if err := cs.conn.WriteMessage(cs.ctx, nil, true); err != nil {
			cs.failed = true
			cs.closeSendErr = wrapWriteError(err)
		}
	})
	return cs.closeSendErr
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

	data, text, err := cs.conn.ReadMessage(cs.ctx)
	if err != nil {
		return cs.readError(err)
	}
	if text && len(data) == 0 {
		return cs.receiveEndStream()
	}
	if err := unmarshalMessage(cs.ctx, cs.codec, data, msg, cs.opts.ReadMaxBytes); err != nil {
		cs.failed = true
		return err
	}
	if cs.spec.StreamType == connect.StreamTypeClient {
		// A client-streaming RPC has one response message and its caller
		// Receives once, so the rest of the stream has to be read here or
		// an error in the trailers would never surface.
		if err := cs.receiveSeparator(); err != nil {
			return err
		}
		if err := cs.receiveEndStream(); !errors.Is(err, io.EOF) {
			return err
		}
	}
	return nil
}

func (cs *clientStream) receiveHeaders() error {
	data, text, err := cs.conn.ReadMessage(cs.ctx)
	if err != nil {
		return cs.readError(err)
	}
	if !text || len(data) == 0 {
		cs.failed = true
		return connect.Errorf(connect.CodeInternal, "protocol error: expected a response headers message first")
	}
	headers, err := connectprotocol.UnmarshalHeaders(data)
	if err != nil {
		cs.failed = true
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

func (cs *clientStream) receiveSeparator() error {
	data, text, err := cs.conn.ReadMessage(cs.ctx)
	if err != nil {
		return cs.readError(err)
	}
	if !text || len(data) > 0 {
		cs.failed = true
		return connect.Errorf(connect.CodeInternal, "protocol error: expected the separator after the response message")
	}
	return nil
}

// receiveEndStream reads the end-stream message, surfaces its trailers, and
// returns the RPC's error — or io.EOF when it succeeded. Reaching it
// cleanly is what makes the connection reusable.
func (cs *clientStream) receiveEndStream() error {
	cs.rxEnd = true
	data, text, err := cs.conn.ReadMessage(cs.ctx)
	if err != nil {
		return cs.readError(err)
	}
	if !text {
		cs.failed = true
		return connect.Errorf(connect.CodeInternal, "protocol error: expected an end-stream message after the separator")
	}
	rpcErr, trailers, err := connectprotocol.UnmarshalEndStream(data)
	if err != nil {
		cs.failed = true
		return connect.Errorf(connect.CodeInternal, "failed to unmarshal end-stream message: %v", err)
	}
	if cs.info != nil && cs.info.ResponseTrailer() != nil {
		for key, values := range trailers {
			cs.info.ResponseTrailer().SetValues(key, values)
		}
	}
	if rpcErr != nil {
		// An RPC-level error is not a connection-level one: the stream
		// finished exactly as the protocol says, so the connection is still
		// good and goes back in the pool.
		return rpcErr
	}
	return io.EOF
}

func (cs *clientStream) readError(err error) error {
	cs.failed = true
	if ctxErr := cs.ctx.Err(); ctxErr != nil {
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			return connect.Errorf(connect.CodeDeadlineExceeded, "RPC deadline exceeded")
		}
		return connect.Errorf(connect.CodeCanceled, "RPC canceled")
	}
	if errors.Is(err, io.EOF) || websocket.CloseStatus(err) != -1 {
		return connect.Errorf(connect.CodeUnavailable, "connection closed before the end-stream message")
	}
	if connectErr := new(connect.Error); errors.As(err, &connectErr) {
		return err
	}
	return connect.Errorf(connect.CodeUnavailable, "failed to read from WebSocket: %v", err)
}

// Close finishes with the connection: back to the pool if the RPC ran to
// its end-stream message, torn down otherwise.
func (cs *clientStream) Close() error {
	var err error
	cs.closeOnce.Do(func() {
		if cs.cancelWatch != nil {
			cs.cancelWatch()
		}
		if cs.rxEnd && !cs.failed {
			// The connection can only be reused if the server has seen this
			// RPC's separator: without it the server is still reading
			// request messages, and would take the next RPC's headers for
			// one of them. CloseSend is idempotent, so this is a no-op for
			// callers that already half-closed.
			if closeErr := cs.CloseSend(); closeErr != nil {
				err = cs.conn.CloseNow()
				return
			}
			cs.transport.release(cs.conn)
			return
		}
		// Abandoned early, or broken: the peer is still mid-RPC as far as
		// it knows, so the connection cannot be handed to another call.
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

// unmarshalMessage decodes one RPC message and enforces the read limit.
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
