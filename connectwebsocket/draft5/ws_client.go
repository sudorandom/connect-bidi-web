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
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/internal/connectprotocol"
)

// newWebSocketStream dials one WebSocket for one streaming RPC and returns
// the client's view of it.
//
// Unary RPCs never reach here: they are dispatched as ordinary Connect HTTP
// requests, because a handshake to carry one request and one response is a
// bad trade.
func (t *transport) newWebSocketStream(
	ctx context.Context,
	spec connect.Spec,
	opts *options,
	info *connect.CallInfo,
) (connect.ClientStream, error) {
	codec := opts.codecs[opts.sendCodecName]
	if codec == nil {
		return nil, connect.Errorf(connect.CodeUnknown, "unknown codec %q", opts.sendCodecName)
	}
	procedureURL := t.urlForProcedure(spec.Procedure)

	var (
		conn messageConn
		err  error
	)
	if opts.h2 != nil {
		conn, err = dialH2(ctx, procedureURL, opts)
	} else {
		conn, err = dialWebSocket(ctx, procedureURL, opts)
	}
	if err != nil {
		return nil, err
	}

	if info != nil {
		info.Spec = spec
		info.PeerAddr = procedureURL.Host
		info.Protocol = protocolName
		info.Codec = codec.Name()
		// Draft 5 has no per-message compression: with no envelope, a data
		// message has nowhere to carry a "this one is compressed" bit.
		// permessage-deflate covers the whole connection instead.
		info.RequestEncoding = connect.CompressionNameIdentity
	}

	stream := &wsClientStream{
		spec:  spec,
		conn:  conn,
		info:  info,
		codec: codec,
		opts:  opts,
		ctx:   ctx,
	}
	// One WebSocket is one RPC, so cancelling the RPC is closing the
	// connection — there is nothing else on it to leave running. The h2
	// bootstrap needs this explicitly: its reads do not consult a context.
	stream.watchCancel()
	return stream, nil
}

// dialWebSocket opens a WebSocket with an HTTP/1.1 Upgrade handshake.
func dialWebSocket(ctx context.Context, procedureURL *url.URL, opts *options) (messageConn, error) {
	dialOpts := &websocket.DialOptions{}
	if opts.webSocketDialOptions != nil {
		cloned := *opts.webSocketDialOptions
		dialOpts = &cloned
	}
	// The subprotocol identifies the protocol and the compression mode is
	// the protocol's only compression, so neither is a caller knob: custom
	// dial options can't silently change what is spoken on the wire.
	dialOpts.Subprotocols = []string{subprotocol}
	dialOpts.CompressionMode = websocket.CompressionNoContextTakeover
	if opts.withoutCompression {
		dialOpts.CompressionMode = websocket.CompressionDisabled
	}

	conn, response, err := websocket.Dial(ctx, webSocketURL(procedureURL), dialOpts) //nolint:bodyclose // coder/websocket closes the handshake response body itself
	if err != nil {
		return nil, handshakeError(response, err)
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

// handshakeError maps a failed upgrade onto an RPC error. The handshake is
// the request, so its status code is the RPC's status: a procedure that is
// not mounted 404s, and a unary one refuses the method.
func handshakeError(response *http.Response, err error) error {
	if response == nil {
		return connect.Errorf(connect.CodeUnavailable, "failed to dial WebSocket: %v", err)
	}
	code := httpToCode(response.StatusCode)
	if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusMethodNotAllowed {
		code = connect.CodeUnimplemented
	}
	return connect.Errorf(code, "WebSocket handshake failed with status %d: %v", response.StatusCode, err)
}

// webSocketURL rewrites an RPC URL's scheme for a WebSocket dial. The two
// schemes name the same endpoint, so callers configure only one of them.
func webSocketURL(procedureURL *url.URL) string {
	dialURL := *procedureURL
	switch dialURL.Scheme {
	case "http":
		dialURL.Scheme = "ws"
	case "https":
		dialURL.Scheme = "wss"
	}
	return dialURL.String()
}

// normalizeBaseURL rewrites a ws:// or wss:// base URL for the HTTP
// dispatch path. NewTransport accepts either spelling because one endpoint
// serves both.
func normalizeBaseURL(baseURL string) string {
	if rest, ok := strings.CutPrefix(baseURL, "wss://"); ok {
		return "https://" + rest
	}
	if rest, ok := strings.CutPrefix(baseURL, "ws://"); ok {
		return "http://" + rest
	}
	return baseURL
}

// wsClientStream implements connect.ClientStream over one WebSocket.
type wsClientStream struct {
	spec  connect.Spec
	conn  messageConn
	info  *connect.CallInfo
	codec connect.Codec
	opts  *options
	ctx   context.Context //nolint:containedctx // the stream's lifetime is the RPC's

	sendHeadersOnce sync.Once
	sendHeadersErr  error
	closeSendOnce   sync.Once
	closeSendErr    error
	recvHeadersOnce sync.Once
	recvHeadersErr  error
	closeOnce       sync.Once

	// rxEnd records that the end-stream message has been processed, so
	// further Receives report EOF rather than reading a closed connection.
	rxEnd bool

	// cancelWatch stops the goroutine that closes the connection when the
	// RPC's context ends.
	cancelWatch context.CancelFunc
}

// watchCancel closes the WebSocket when the RPC's context ends. The
// HTTP/1.1 bootstrap could rely on the context passed to each read, but the
// HTTP/2 one cannot: an extended CONNECT stream read is unblocked by the
// stream ending, not by a context.
func (cs *wsClientStream) watchCancel() {
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

// SendHeaders writes the request headers message, which always precedes
// everything else in its direction.
func (cs *wsClientStream) SendHeaders() error {
	cs.sendHeadersOnce.Do(func() {
		cs.sendHeadersErr = cs.sendHeaders()
	})
	return cs.sendHeadersErr
}

func (cs *wsClientStream) sendHeaders() error {
	header := make(http.Header)
	if cs.info != nil && cs.info.RequestHeader() != nil {
		maps.Insert(header, cs.info.RequestHeader().All())
	}
	// The procedure is the URL's path, so it is not repeated here: a second
	// copy that could disagree with the first is a bug waiting to happen.
	header.Set("Content-Type", contentTypePrefix+cs.codec.Name())
	header.Set(connectHeaderProtocolVersion, connectProtocolVersion)
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
func (cs *wsClientStream) Send(msg any) error {
	if err := cs.SendHeaders(); err != nil {
		return err
	}
	payload, err := marshalMessage(cs.ctx, cs.codec, msg, cs.opts.sendMaxBytes)
	if err != nil {
		return err
	}
	if err := cs.conn.WriteMessage(cs.ctx, payload, dataIsText(payload)); err != nil {
		return wrapWriteError(err)
	}
	return nil
}

// dataIsText reports whether a data message may travel as a text WebSocket
// message: it must be valid UTF-8, so tooling can render it, and it must be
// non-empty, because an empty text message is the separator. An empty
// protobuf message encodes to zero bytes, so that second condition is not
// hypothetical — it is what keeps a legitimate empty message from reading
// as a half-close.
func dataIsText(payload []byte) bool {
	return len(payload) > 0 && utf8.Valid(payload)
}

// CloseSend writes the separator, half-closing the request direction.
// WebSocket has no half-close of its own — the close handshake tears down
// both directions — which is why the signal has to be in band.
func (cs *wsClientStream) CloseSend() error {
	cs.closeSendOnce.Do(func() {
		if err := cs.SendHeaders(); err != nil {
			cs.closeSendErr = err
			return
		}
		if err := cs.conn.WriteMessage(cs.ctx, nil, true); err != nil {
			cs.closeSendErr = wrapWriteError(err)
		}
	})
	return cs.closeSendErr
}

// Receive reads the next response message, reporting clean completion as
// io.EOF and an RPC failure as the error carried in the end-stream message.
func (cs *wsClientStream) Receive(msg any) error {
	// The request headers precede everything, even on a receive-first RPC.
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
		// The separator: the data phase is over and the next message is the
		// end-stream metadata. Every other message here is a data message,
		// text or binary alike.
		return cs.receiveEndStream()
	}

	if err := unmarshalMessage(cs.ctx, cs.codec, data, msg, cs.opts.readMaxBytes); err != nil {
		return err
	}
	if cs.spec.StreamType == connect.StreamTypeClient {
		// A client-streaming RPC has exactly one response message, and its
		// caller Receives exactly once, so the rest of the response stream
		// has to be read here or an error in the trailers would never
		// surface.
		if err := cs.receiveSeparator(); err != nil {
			return err
		}
		if err := cs.receiveEndStream(); !errors.Is(err, io.EOF) {
			return err
		}
	}
	return nil
}

// receiveSeparator reads the empty text message that ends the response data
// phase.
func (cs *wsClientStream) receiveSeparator() error {
	data, text, err := cs.conn.ReadMessage(cs.ctx)
	if err != nil {
		return cs.readError(err)
	}
	if !text || len(data) > 0 {
		return connect.Errorf(connect.CodeInternal, "protocol error: expected the separator after the response message")
	}
	return nil
}

// receiveHeaders reads the mandatory response headers message.
func (cs *wsClientStream) receiveHeaders() error {
	data, text, err := cs.conn.ReadMessage(cs.ctx)
	if err != nil {
		return cs.readError(err)
	}
	if !text || len(data) == 0 {
		return connect.Errorf(connect.CodeInternal, "protocol error: expected a response headers message first")
	}
	headers, err := connectprotocol.UnmarshalHeaders(data)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to unmarshal response headers: %v", err)
	}
	if cs.info != nil && cs.info.ResponseHeader() != nil {
		for key, values := range headers {
			if strings.HasPrefix(key, ":") {
				// Pseudo-headers aren't surfaced to the application.
				continue
			}
			cs.info.ResponseHeader().SetValues(key, values)
		}
	}
	return nil
}

// receiveEndStream reads the end-stream message that follows the server's
// separator, surfaces its trailers, and returns the RPC's error — or io.EOF
// when the RPC succeeded.
func (cs *wsClientStream) receiveEndStream() error {
	cs.rxEnd = true
	data, text, err := cs.conn.ReadMessage(cs.ctx)
	if err != nil {
		return cs.readError(err)
	}
	if !text {
		return connect.Errorf(connect.CodeInternal, "protocol error: expected an end-stream message after the separator")
	}
	rpcErr, trailers, err := connectprotocol.UnmarshalEndStream(data)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to unmarshal end-stream message: %v", err)
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

// readError maps a transport-level read failure onto an RPC error. A close
// before the end-stream message is never an implicit success: the trailers
// are the only place a status can appear, so their absence is a broken
// stream.
func (cs *wsClientStream) readError(err error) error {
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

// Close releases the WebSocket, ending the RPC.
func (cs *wsClientStream) Close() error {
	var err error
	cs.closeOnce.Do(func() {
		if cs.cancelWatch != nil {
			cs.cancelWatch()
		}
		if cs.rxEnd {
			// The RPC finished; close politely.
			err = cs.conn.Close()
			return
		}
		// Abandoning the stream early is a cancellation: tear the
		// connection down rather than waiting for a closing handshake the
		// peer may never answer.
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
