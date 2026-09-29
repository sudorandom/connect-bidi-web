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

package draft7

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/internal/connectprotocol"
)

// newWebSocketStream dials one WebSocket for one RPC and returns the
// client's view of it.
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
	token := subprotocolForCodec(codec.Name())
	if token == "" {
		return nil, connect.Errorf(connect.CodeUnknown,
			"codec %q has no WebSocket subprotocol; use proto or json", codec.Name())
	}
	dialURL, err := t.dialURL(ctx, spec.Procedure, opts)
	if err != nil {
		return nil, err
	}
	var conn messageConn
	if opts.h2 != nil {
		conn, err = dialH2(ctx, dialURL, token, opts)
	} else {
		conn, err = dialWebSocket(ctx, dialURL, token, opts)
	}
	if err != nil {
		return nil, err
	}

	if info != nil {
		info.Spec = spec
		info.PeerAddr = dialURL.Host
		info.Protocol = protocolName
		info.Codec = codec.Name()
		// There is no message-level compression: permessage-deflate covers
		// the whole connection instead.
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
	// connection — there is nothing else on it to leave running.
	stream.watchCancel()
	return stream, nil
}

// dialURL forms the handshake URI: base + prefix + procedure, with the
// deadline as a query parameter. The deadline goes on the URI because a
// browser cannot set headers on a handshake, and because metadata cannot
// bound the read of the message that carries it.
func (t *transport) dialURL(ctx context.Context, procedure string, opts *options) (*url.URL, error) {
	procedureURL := *t.urlForProcedure(procedure)
	if opts.pathPrefix != "" {
		procedureURL.Path = joinURLPath(t.baseURLPtr.Path, opts.pathPrefix+procedure)
	}
	switch procedureURL.Scheme {
	case "http":
		procedureURL.Scheme = "ws"
	case "https":
		procedureURL.Scheme = "wss"
	}
	if deadline, ok := ctx.Deadline(); ok {
		ms := time.Until(deadline).Milliseconds()
		if ms <= 0 {
			return nil, connect.Errorf(connect.CodeDeadlineExceeded, "RPC deadline exceeded before dialing")
		}
		query := procedureURL.Query()
		query.Set(timeoutQueryParameter, strconv.FormatInt(ms, 10))
		procedureURL.RawQuery = query.Encode()
	}
	return &procedureURL, nil
}

// dialWebSocket opens a WebSocket with an HTTP/1.1 Upgrade handshake,
// offering exactly the subprotocol for the codec in use, and verifies what
// the server negotiated: the echoed token, and — when compression is on —
// that no context is kept in either direction.
func dialWebSocket(ctx context.Context, dialURL *url.URL, token string, opts *options) (messageConn, error) {
	dialOpts := &websocket.DialOptions{}
	if opts.webSocketDialOptions != nil {
		cloned := *opts.webSocketDialOptions
		dialOpts = &cloned
	}
	// The subprotocol selects the codec and the compression mode is the
	// protocol's only compression, so neither is a caller knob: custom dial
	// options can't silently change what is spoken on the wire.
	dialOpts.Subprotocols = []string{token}
	dialOpts.CompressionMode = websocket.CompressionNoContextTakeover
	if opts.withoutCompression {
		dialOpts.CompressionMode = websocket.CompressionDisabled
	}

	conn, response, err := websocket.Dial(ctx, dialURL.String(), dialOpts) //nolint:bodyclose // coder/websocket closes the handshake response body itself
	if err != nil {
		return nil, handshakeError(response, err)
	}
	if conn.Subprotocol() != token {
		_ = conn.CloseNow()
		return nil, connect.Errorf(connect.CodeUnavailable,
			"server selected subprotocol %q, which was not offered; it may not serve this protocol", conn.Subprotocol())
	}
	if response != nil {
		if err := verifyCompression(response.Header); err != nil {
			_ = conn.CloseNow()
			return nil, err
		}
	}
	return newCoderConn(conn), nil
}

// verifyCompression checks the handshake response's negotiated extension.
// permessage-deflate is acceptable only with no_context_takeover in both
// directions: a compression context shared across messages leaks plaintext
// across trust boundaries whenever attacker-influenced and secret data
// travel on one connection.
func verifyCompression(header http.Header) error {
	for _, entry := range header.Values("Sec-WebSocket-Extensions") {
		for extension := range strings.SplitSeq(entry, ",") {
			fields := strings.Split(extension, ";")
			if strings.TrimSpace(fields[0]) != "permessage-deflate" {
				continue
			}
			client, server := false, false
			for _, param := range fields[1:] {
				switch strings.TrimSpace(param) {
				case "client_no_context_takeover":
					client = true
				case "server_no_context_takeover":
					server = true
				}
			}
			if !client || !server {
				return connect.Errorf(connect.CodeUnavailable,
					"server negotiated permessage-deflate without no_context_takeover in both directions: %q",
					strings.TrimSpace(extension))
			}
		}
	}
	return nil
}

// handshakeError maps a failed upgrade onto an RPC error. The handshake is
// the request, so its status code is the RPC's status: a procedure that is
// not mounted 404s, an unsupported codec 415s, a forbidden origin 403s.
func handshakeError(response *http.Response, err error) error {
	if response == nil {
		return connect.Errorf(connect.CodeUnavailable, "failed to dial WebSocket: %v", err)
	}
	code := httpToCode(response.StatusCode)
	switch response.StatusCode {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusUpgradeRequired:
		code = connect.CodeUnimplemented
	case http.StatusUnsupportedMediaType:
		code = connect.CodeUnimplemented
	case http.StatusForbidden:
		code = connect.CodePermissionDenied
	}
	return connect.Errorf(code, "WebSocket handshake failed with status %d: %v", response.StatusCode, err)
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
	sendClosed      bool
	recvHeadersOnce sync.Once
	recvHeadersErr  error
	closeOnce       sync.Once

	// rxEnd records that the end-of-stream message has been processed, so
	// further Receives report EOF rather than reading a closed connection.
	rxEnd bool
	// failed records a protocol violation by the server, which tears the
	// connection down rather than closing it politely.
	failed bool

	// cancelWatch stops the goroutine that closes the connection when the
	// RPC's context ends.
	cancelWatch context.CancelFunc
}

// watchCancel closes the WebSocket when the RPC's context ends.
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

// SendHeaders writes the leading-metadata message, which always precedes
// everything else in its direction.
func (cs *wsClientStream) SendHeaders() error {
	cs.sendHeadersOnce.Do(func() {
		cs.sendHeadersErr = cs.sendHeaders()
	})
	return cs.sendHeadersErr
}

func (cs *wsClientStream) sendHeaders() error {
	var header http.Header
	if cs.info != nil {
		header = clientMetadataHeaders(cs.info.RequestHeader())
	}
	// The procedure is the URL's path, the codec is the subprotocol, and
	// the deadline is on the URI: none of them is repeated here, so there
	// is no second copy that could disagree with the first.
	data, err := marshalMetadata(header)
	if err != nil {
		return connect.Errorf(connect.CodeInvalidArgument, "invalid request metadata: %v", err)
	}
	if err := cs.conn.WriteMessage(cs.ctx, markerLeadingMetadata, data, true); err != nil {
		return wrapWriteError(err)
	}
	return nil
}

// Send marshals and writes one request message. Sending after CloseSend
// is an error, because the message would not be delivered.
func (cs *wsClientStream) Send(msg any) error {
	if cs.sendClosed {
		return connect.Errorf(connect.CodeInternal, "cannot send after the request stream is closed")
	}
	if err := cs.SendHeaders(); err != nil {
		return err
	}
	payload, err := marshalMessage(cs.ctx, cs.codec, msg, cs.opts.sendMaxBytes)
	if err != nil {
		return err
	}
	if err := cs.conn.WriteMessage(cs.ctx, markerBody, payload, !codecIsBinary(cs.codec.Name())); err != nil {
		return wrapWriteError(err)
	}
	return nil
}

// CloseSend writes a bare client end-of-stream message. WebSocket has no
// half-close of its own — the close handshake tears down both directions —
// which is why the signal has to be in band. It is skipped once the server
// has already ended the RPC: a C is not required after an S.
func (cs *wsClientStream) CloseSend() error {
	cs.closeSendOnce.Do(func() {
		cs.sendClosed = true
		if cs.rxEnd {
			return
		}
		if err := cs.SendHeaders(); err != nil {
			cs.closeSendErr = err
			return
		}
		if err := cs.conn.WriteMessage(cs.ctx, markerClientEndStream, nil, true); err != nil {
			cs.closeSendErr = wrapWriteError(err)
		}
	})
	return cs.closeSendErr
}

// Receive reads the next response message, reporting clean completion as
// io.EOF and an RPC failure as the error carried in the end-of-stream
// message.
func (cs *wsClientStream) Receive(msg any) error {
	// The request metadata precedes everything, even on a receive-first RPC.
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

	next, err := cs.conn.ReadMessage(cs.ctx, cs.opts.readMaxBytes)
	if err != nil {
		return cs.readError(err)
	}
	switch next.marker {
	case markerBody:
		if err := checkBodyFrame(next, cs.codec.Name()); err != nil {
			return cs.protocolError(err)
		}
	case markerServerEndStream:
		return cs.receiveEndStream(next)
	case markerLeadingMetadata:
		return cs.protocolError(connect.Errorf(connect.CodeInternal,
			"protocol error: a second leading-metadata message from the server"))
	case markerClientEndStream:
		return cs.protocolError(connect.Errorf(connect.CodeInternal,
			"protocol error: client end-of-stream marker from a server"))
	default:
		return cs.protocolError(unknownMarkerError(next.marker))
	}

	if err := unmarshalMessage(cs.ctx, cs.codec, next.payload, msg); err != nil {
		return err
	}
	if cs.spec.StreamType == connect.StreamTypeClient || cs.spec.StreamType == connect.StreamTypeUnary {
		// A single-response RPC has exactly one response message, and its
		// caller Receives exactly once, so the end-of-stream message has to
		// be read here or an error in the trailers would never surface.
		end, err := cs.conn.ReadMessage(cs.ctx, cs.opts.readMaxBytes)
		if err != nil {
			return cs.readError(err)
		}
		if end.marker != markerServerEndStream {
			return cs.protocolError(connect.Errorf(connect.CodeInternal,
				"protocol error: expected the end-of-stream message after the single response, got marker %q", end.marker))
		}
		if err := cs.receiveEndStream(end); !errors.Is(err, io.EOF) {
			return err
		}
	}
	return nil
}

// receiveHeaders reads the mandatory leading-metadata message.
func (cs *wsClientStream) receiveHeaders() error {
	msg, err := cs.conn.ReadMessage(cs.ctx, cs.opts.readMaxBytes)
	if err != nil {
		return cs.readError(err)
	}
	if msg.marker != markerLeadingMetadata {
		return cs.protocolError(connect.Errorf(connect.CodeInternal,
			"protocol error: expected the leading-metadata message first, got marker %q", msg.marker))
	}
	if !msg.text || len(msg.payload) == 0 {
		return cs.protocolError(connect.Errorf(connect.CodeInternal,
			"protocol error: the leading-metadata message must be a non-empty text frame"))
	}
	headers, err := unmarshalMetadata(msg.payload)
	if err != nil {
		return cs.protocolError(connect.Errorf(connect.CodeInternal, "protocol error: response metadata: %v", err))
	}
	if cs.info != nil && cs.info.ResponseHeader() != nil {
		for key, values := range headers {
			cs.info.ResponseHeader().SetValues(key, values)
		}
	}
	return nil
}

// receiveEndStream processes the end-of-stream message, surfaces its
// trailers, and returns the RPC's error — or io.EOF when the RPC succeeded.
func (cs *wsClientStream) receiveEndStream(msg message) error {
	if !msg.text || len(msg.payload) == 0 {
		return cs.protocolError(connect.Errorf(connect.CodeInternal,
			"protocol error: the end-of-stream message must be a non-empty text frame"))
	}
	rpcErr, trailers, err := connectprotocol.UnmarshalEndStream(msg.payload)
	if err != nil {
		return cs.protocolError(connect.Errorf(connect.CodeInternal, "protocol error: end-of-stream message: %v", err))
	}
	cs.rxEnd = true
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

// protocolError fails the RPC for a violation by the server. There is no
// client-to-server error message in this protocol, so the connection is
// simply dropped.
func (cs *wsClientStream) protocolError(err error) error {
	cs.failed = true
	_ = cs.conn.CloseNow()
	return err
}

// readError maps a transport-level read failure onto an RPC error. A close
// before the end-of-stream message is never an implicit success: the
// trailers are the only place a status can appear, so their absence is a
// broken stream — whatever the close code said.
func (cs *wsClientStream) readError(err error) error {
	if ctxErr := cs.ctx.Err(); ctxErr != nil {
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			return connect.Errorf(connect.CodeDeadlineExceeded, "RPC deadline exceeded")
		}
		return connect.Errorf(connect.CodeCanceled, "RPC canceled")
	}
	switch {
	case errors.Is(err, errMessageTooBig):
		// The only channel a client has for this is the close frame.
		cs.failed = true
		closeTooBig(cs.conn, err.Error())
		return connect.Errorf(connect.CodeResourceExhausted, "%v", err)
	case errors.Is(err, errEmptyMessage):
		return cs.protocolError(connect.Errorf(connect.CodeInternal, "protocol error: %v", err))
	case errors.Is(err, io.EOF), websocket.CloseStatus(err) != -1:
		return connect.Errorf(connect.CodeUnavailable, "connection closed before the end-of-stream message")
	}
	if connectErr := new(connect.Error); errors.As(err, &connectErr) {
		return err
	}
	return connect.Errorf(connect.CodeUnavailable, "failed to read from WebSocket: %v", err)
}

// closeTooBig sends a 1009 close frame naming the limit, then drops the
// connection without waiting for the peer's answer: a closing handshake
// would mean draining the very message that was too big to read.
func closeTooBig(conn messageConn, reason string) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = conn.Close(websocket.StatusMessageTooBig, reason)
	}()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
	}
	_ = conn.CloseNow()
}

// Close releases the WebSocket, ending the RPC.
func (cs *wsClientStream) Close() error {
	var err error
	cs.closeOnce.Do(func() {
		if cs.cancelWatch != nil {
			cs.cancelWatch()
		}
		if cs.rxEnd && !cs.failed {
			// The RPC finished; close politely.
			err = cs.conn.Close(websocket.StatusNormalClosure, "")
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

// unmarshalMessage decodes one RPC message. The read limit was enforced
// while the message was read, so only decoding remains.
func unmarshalMessage(ctx context.Context, codec connect.Codec, data []byte, msg any) error {
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
