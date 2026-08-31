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
	"context"
	"errors"
	"io"
	"log/slog"
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

// withWebSocket wraps a procedure's ordinary Connect HTTP handler so the
// same URL also answers a WebSocket upgrade. This is the whole of draft 5's
// server-side routing: a draft 5 endpoint is a Connect endpoint that
// happens to accept an upgrade, so there is no path of its own to register.
func withWebSocket(server *connect.Server, spec connect.Spec, opts *options, next http.Handler) http.Handler {
	return http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		upgrade := isWebSocketUpgrade(request)
		extendedConnect := isExtendedConnectWebSocket(request)
		if !upgrade && !extendedConnect {
			next.ServeHTTP(responseWriter, request)
			return
		}
		if spec.StreamType == connect.StreamTypeUnary {
			// Unary RPCs never upgrade: a handshake to carry one request
			// and one response is a bad trade, and the POST this endpoint
			// already answers is the better one.
			responseWriter.Header().Set("Allow", http.MethodPost)
			responseWriter.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if !offersSubprotocol(request.Header) {
			http.Error(responseWriter, "expected the "+subprotocol+" websocket subprotocol", http.StatusBadRequest)
			return
		}

		var conn messageConn
		if extendedConnect {
			accepted, ok := acceptH2(responseWriter, request, opts)
			if !ok {
				return
			}
			conn = accepted
		} else {
			accepted, ok := acceptWebSocket(responseWriter, request, opts)
			if !ok {
				return
			}
			conn = accepted
		}
		serveWebSocketRPC(request, conn, server, spec, opts)
	})
}

// acceptWebSocket completes an HTTP/1.1 Upgrade handshake. It reports false
// when the handshake failed, in which case coder/websocket has already
// written the response.
func acceptWebSocket(responseWriter http.ResponseWriter, request *http.Request, opts *options) (messageConn, bool) {
	acceptOpts := &websocket.AcceptOptions{}
	if opts.webSocketAcceptOptions != nil {
		cloned := *opts.webSocketAcceptOptions
		acceptOpts = &cloned
	}
	// The subprotocol and the compression mode are the protocol's, not the
	// caller's: custom accept options can't change what is spoken on the
	// wire, or silently turn off the only compression draft 5 has.
	acceptOpts.Subprotocols = []string{subprotocol}
	acceptOpts.CompressionMode = websocket.CompressionNoContextTakeover
	if opts.withoutCompression {
		acceptOpts.CompressionMode = websocket.CompressionDisabled
	}
	conn, err := websocket.Accept(responseWriter, request, acceptOpts)
	if err != nil {
		return nil, false
	}
	return newCoderConn(conn), true
}

// serveWebSocketRPC runs one RPC on one accepted connection, from the
// mandatory request headers message to the end-stream message that closes
// it. Every RPC-level failure is reported there rather than by refusing the
// handshake: JavaScript cannot read the status of a failed WebSocket
// handshake, so an error a client must handle has to arrive on the socket.
func serveWebSocketRPC(
	request *http.Request,
	conn messageConn,
	server *connect.Server,
	spec connect.Spec,
	opts *options,
) {
	ctx, cancel := context.WithCancel(request.Context())
	defer cancel()

	stream := &wsServerStream{
		conn:     conn,
		opts:     opts,
		ctx:      ctx,
		messages: make(chan []byte),
	}
	defer func() {
		if err := conn.Close(); err != nil {
			slog.DebugContext(ctx, "draft5: closing websocket failed", "error", err)
		}
	}()

	header, callErr := readRequestHeaders(ctx, conn, request)
	if callErr == nil {
		var codec connect.Codec
		codec, callErr = negotiateCodec(header, opts)
		if callErr == nil {
			stream.codec = codec
			callErr = serveCall(ctx, cancel, request, header, spec, server, stream)
		}
	}
	if stream.codec == nil {
		// The RPC failed before a codec was agreed. The end-stream message
		// is JSON either way, so the failure still reaches the client; the
		// response headers just name the default content type.
		stream.codec = opts.codecs[connect.CodecNameProto]
	}

	// The headers message precedes everything in its direction, even when
	// the handler never sent a message.
	if err := stream.SendHeaders(); err != nil {
		slog.DebugContext(ctx, "draft5: writing response headers failed", "error", err)
		return
	}
	if err := stream.writeSeparator(); err != nil {
		slog.DebugContext(ctx, "draft5: writing the separator failed", "error", err)
		return
	}
	if err := stream.writeEndStream(callErr); err != nil {
		slog.DebugContext(ctx, "draft5: writing end-stream failed", "error", err)
	}
}

// serveCall dispatches the RPC once its metadata is known.
func serveCall(
	ctx context.Context,
	cancel context.CancelFunc,
	request *http.Request,
	header http.Header,
	spec connect.Spec,
	server *connect.Server,
	stream *wsServerStream,
) error {
	if timeout := header.Get("Connect-Timeout-Ms"); timeout != "" {
		ms, err := strconv.ParseInt(timeout, 10, 64)
		if err != nil || ms < 0 {
			return connect.Errorf(connect.CodeInvalidArgument, "protocol error: invalid connect-timeout-ms %q", timeout)
		}
		var timeoutCancel context.CancelFunc
		ctx, timeoutCancel = context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
		defer timeoutCancel()
	}

	info := &connect.CallInfo{
		Spec:            spec,
		PeerAddr:        request.RemoteAddr,
		Protocol:        protocolName,
		Codec:           stream.codec.Name(),
		RequestEncoding: connect.CompressionNameIdentity,
		TransportInfo:   &ServerInfo{request: request},
	}
	for key, values := range header {
		if !strings.HasPrefix(key, ":") {
			info.RequestHeader().SetValues(key, values)
		}
	}
	stream.info = info
	stream.ctx = ctx
	// The pump owns every read from here on, so a client that disappears
	// mid-response cancels the handler instead of being noticed only by
	// whoever next calls Receive.
	go stream.pump(ctx, cancel)

	return server.Call(ctx, spec.Procedure, info, stream)
}

// readRequestHeaders reads the mandatory first message and merges it over
// the metadata the upgrade request carried. The handshake headers are the
// base and the message overrides them key by key, so a Cookie the platform
// attached by itself still reaches the handler while anything the Connect
// client set explicitly wins.
func readRequestHeaders(ctx context.Context, conn messageConn, request *http.Request) (http.Header, error) {
	data, text, err := conn.ReadMessage(ctx)
	if err != nil {
		return nil, connect.Errorf(connect.CodeUnavailable, "failed to read the request headers message: %v", err)
	}
	if !text || len(data) == 0 {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "protocol error: expected a request headers message first")
	}
	message, err := connectprotocol.UnmarshalHeaders(data)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "failed to unmarshal the request headers message: %v", err)
	}
	header := requestMetadata(request.Header)
	maps.Copy(header, message)
	return header, nil
}

// negotiateCodec resolves the codec named by the request's content type.
// The content type is required: one socket carries one RPC, so the codec is
// genuinely a property of the connection here, and guessing it would make a
// mismatch look like a corrupt message much later.
func negotiateCodec(header http.Header, opts *options) (connect.Codec, error) {
	contentType := header.Get("Content-Type")
	if contentType == "" {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "protocol error: the request headers message must set content-type")
	}
	name := codecNameForContentType(contentType)
	if name == "" {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "invalid content-type %q: expected %s{codec}", contentType, contentTypePrefix)
	}
	codec := opts.codecs[name]
	if codec == nil {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "unknown codec %q in content-type %q", name, contentType)
	}
	return codec, nil
}

// wsServerStream implements connect.ServerStream over one WebSocket.
type wsServerStream struct {
	conn  messageConn
	info  *connect.CallInfo
	codec connect.Codec
	opts  *options
	ctx   context.Context //nolint:containedctx // the stream's lifetime is the RPC's

	// messages carries request messages from the pump. It is closed when
	// the client half-closes or the connection fails.
	messages     chan []byte
	messagesOnce sync.Once

	readMu  sync.Mutex
	readErr error

	sendHeadersOnce sync.Once
	sendHeadersErr  error
}

// pump reads the request direction to its end. It exists so that the
// connection is being read even while the handler is only sending: a client
// that goes away mid-response then cancels the RPC's context instead of
// going unnoticed until someone calls Receive.
func (ss *wsServerStream) pump(ctx context.Context, cancel context.CancelFunc) {
	defer ss.endRequestStream()
	halfClosed := false
	for {
		data, text, err := ss.conn.ReadMessage(ctx)
		if err != nil {
			// A clean close after the client half-closed is how a finished
			// client leaves; anything else ended the RPC early.
			if !halfClosed || !isCleanClose(err) {
				ss.setReadErr(connect.Errorf(connect.CodeUnavailable, "failed to read from WebSocket: %v", err))
			}
			cancel()
			return
		}
		switch {
		case text && len(data) == 0 && !halfClosed:
			// The separator: no more request messages. Receive reports EOF
			// from here, but the loop keeps reading so that a client which
			// then disappears mid-response still cancels the handler.
			halfClosed = true
			ss.endRequestStream()
			continue
		case halfClosed:
			ss.setReadErr(connect.Errorf(connect.CodeInvalidArgument, "protocol error: request message after the separator"))
			cancel()
			return
		}
		// Everything else is a data message. The opcode says only whether the
		// payload was UTF-8 worth rendering as text; it is the same message
		// either way.
		select {
		case ss.messages <- data:
		case <-ctx.Done():
			return
		}
	}
}

// endRequestStream closes the message channel, which is what Receive reads
// as the end of the request stream. The pump reaches it twice on a
// well-behaved RPC — once at the separator, once when the loop exits.
func (ss *wsServerStream) endRequestStream() {
	ss.messagesOnce.Do(func() { close(ss.messages) })
}

func (ss *wsServerStream) setReadErr(err error) {
	ss.readMu.Lock()
	defer ss.readMu.Unlock()
	if ss.readErr == nil {
		ss.readErr = err
	}
}

func (ss *wsServerStream) takeReadErr() error {
	ss.readMu.Lock()
	defer ss.readMu.Unlock()
	return ss.readErr
}

// Receive reads the next request message, reporting the client's
// half-close as io.EOF.
func (ss *wsServerStream) Receive(msg any) error {
	select {
	case data, ok := <-ss.messages:
		if !ok {
			if err := ss.takeReadErr(); err != nil {
				return err
			}
			return io.EOF
		}
		return unmarshalMessage(ss.ctx, ss.codec, data, msg, ss.opts.readMaxBytes)
	case <-ss.ctx.Done():
		return ss.ctx.Err()
	}
}

// SendHeaders writes the response headers message exactly once.
func (ss *wsServerStream) SendHeaders() error {
	ss.sendHeadersOnce.Do(func() {
		ss.sendHeadersErr = ss.sendHeaders()
	})
	return ss.sendHeadersErr
}

func (ss *wsServerStream) sendHeaders() error {
	header := make(http.Header)
	if ss.info != nil && ss.info.ResponseHeader() != nil {
		maps.Insert(header, ss.info.ResponseHeader().All())
	}
	header.Set("Content-Type", contentTypePrefix+ss.codec.Name())

	data, err := connectprotocol.MarshalHeaders(header)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal headers: %v", err)
	}
	// Writes use a context that outlives a cancelled RPC: the end-stream
	// message explaining the cancellation still has to reach the client.
	return ss.conn.WriteMessage(context.WithoutCancel(ss.ctx), data, true)
}

// Send marshals and writes one response message.
func (ss *wsServerStream) Send(msg any) error {
	if err := ss.SendHeaders(); err != nil {
		return err
	}
	payload, err := marshalMessage(ss.ctx, ss.codec, msg, ss.opts.sendMaxBytes)
	if err != nil {
		return err
	}
	if err := ss.conn.WriteMessage(ss.ctx, payload, dataIsText(payload)); err != nil {
		return wrapWriteError(err)
	}
	return nil
}

// writeSeparator ends the response data phase: the next message is the
// end-stream metadata.
func (ss *wsServerStream) writeSeparator() error {
	return ss.conn.WriteMessage(context.WithoutCancel(ss.ctx), nil, true)
}

// writeEndStream writes the Connect EndStreamResponse that finishes the
// RPC, carrying the handler's error and any trailers.
func (ss *wsServerStream) writeEndStream(callErr error) error {
	var wireErr error
	if callErr != nil {
		wireErr = connectprotocol.ErrorForWire(callErr)
	}
	trailers := make(http.Header)
	if ss.info != nil && ss.info.ResponseTrailer() != nil {
		maps.Insert(trailers, ss.info.ResponseTrailer().All())
	}
	data, err := connectprotocol.MarshalEndStream(wireErr, trailers)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal end-stream: %v", err)
	}
	return ss.conn.WriteMessage(context.WithoutCancel(ss.ctx), data, true)
}

// isCleanClose reports whether err is the ordinary end of a connection the
// peer closed politely.
func isCleanClose(err error) bool {
	if errors.Is(err, io.EOF) {
		return true
	}
	switch websocket.CloseStatus(err) {
	case websocket.StatusNormalClosure, websocket.StatusGoingAway:
		return true
	default:
		return false
	}
}
