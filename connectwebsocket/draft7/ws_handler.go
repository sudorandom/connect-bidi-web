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
	"context"
	"errors"
	"fmt"
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
// same URL also answers a WebSocket upgrade. This is the routing for a
// deployment without a path prefix: a draft 7 endpoint is a Connect
// endpoint that happens to accept an upgrade.
func withWebSocket(server *connect.Server, spec connect.Spec, opts *options, next http.Handler) http.Handler {
	return http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		if !isWebSocketUpgrade(request) && !isExtendedConnectWebSocket(request) {
			next.ServeHTTP(responseWriter, request)
			return
		}
		serveUpgrade(responseWriter, request, server, spec, opts)
	})
}

// webSocketOnly serves prefix + procedure when a path prefix is
// configured: upgrades are served, and anything else is told to upgrade.
func webSocketOnly(server *connect.Server, spec connect.Spec, opts *options) http.Handler {
	return http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		if !isWebSocketUpgrade(request) && !isExtendedConnectWebSocket(request) {
			responseWriter.Header().Set("Connection", "Upgrade")
			responseWriter.Header().Set("Upgrade", "websocket")
			http.Error(responseWriter, "this path serves WebSocket upgrades only", http.StatusUpgradeRequired)
			return
		}
		serveUpgrade(responseWriter, request, server, spec, opts)
	})
}

// httpOnly serves the bare procedure URL when a path prefix is configured:
// an upgrade there is an error, because both peers agreed the prefix is
// where upgrades go.
func httpOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		if isWebSocketUpgrade(request) || isExtendedConnectWebSocket(request) {
			http.Error(responseWriter, "WebSocket upgrades are served under the configured path prefix", http.StatusBadRequest)
			return
		}
		next.ServeHTTP(responseWriter, request)
	})
}

// serveUpgrade negotiates and accepts one WebSocket handshake — an
// HTTP/1.1 Upgrade, or an RFC 8441 extended CONNECT on HTTP/2 (see
// ws_h2.go for why the latter is served) — then runs the RPC on it. The
// handshake's own failures — the codec is not offered or not served, the
// origin is not permitted — are HTTP statuses; every failure of the RPC
// itself is reported on the socket.
func serveUpgrade(
	responseWriter http.ResponseWriter,
	request *http.Request,
	server *connect.Server,
	spec connect.Spec,
	opts *options,
) {
	token, recognized := selectSubprotocol(offeredSubprotocols(request.Header), opts.codecs)
	if token == "" {
		if !recognized {
			http.Error(responseWriter,
				"expected one of the "+subprotocolBase+", "+subprotocolProto+", or "+subprotocolJSON+" websocket subprotocols",
				http.StatusBadRequest)
			return
		}
		http.Error(responseWriter,
			"none of the offered codecs is supported; this server serves "+strings.Join(supportedSubprotocols(opts.codecs), ", "),
			http.StatusUnsupportedMediaType)
		return
	}
	codec := opts.codecs[subprotocolCodecs[token]]

	var conn messageConn
	if isExtendedConnectWebSocket(request) {
		accepted, ok := acceptH2(responseWriter, request, opts, token)
		if !ok {
			return
		}
		conn = accepted
	} else {
		accepted, ok := acceptWebSocket(responseWriter, request, opts, token)
		if !ok {
			return
		}
		conn = accepted
	}
	serveWebSocketRPC(request, conn, server, spec, opts, codec)
}

// acceptWebSocket completes the HTTP/1.1 Upgrade handshake. It reports
// false when the handshake failed, in which case coder/websocket has
// already written the response: a 403 for a cross-origin request the
// accept options do not permit, in particular.
func acceptWebSocket(responseWriter http.ResponseWriter, request *http.Request, opts *options, token string) (messageConn, bool) {
	acceptOpts := &websocket.AcceptOptions{}
	if opts.webSocketAcceptOptions != nil {
		cloned := *opts.webSocketAcceptOptions
		acceptOpts = &cloned
	}
	// The subprotocol and the compression mode are the protocol's, not the
	// caller's: custom accept options can't change what is spoken on the
	// wire. No-context-takeover is required in both directions, and
	// coder/websocket imposes it on the client if the client did not offer
	// it, exactly as RFC 7692 §7.1.1 allows.
	acceptOpts.Subprotocols = []string{token}
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
// mandatory leading-metadata message to the end-of-stream message that
// closes it. Every RPC-level failure is reported there rather than by
// refusing the handshake: JavaScript cannot read the status of a failed
// WebSocket handshake, so an error a client must handle has to arrive on
// the socket.
func serveWebSocketRPC(
	request *http.Request,
	conn messageConn,
	server *connect.Server,
	spec connect.Spec,
	opts *options,
	codec connect.Codec,
) {
	// The deadline is the shorter of the server's own and the client's,
	// and it starts now: it bounds the wait for the opening message too.
	ctx, cancel := context.WithCancel(request.Context())
	defer cancel()
	if opts.serverTimeout > 0 {
		var timeoutCancel context.CancelFunc
		ctx, timeoutCancel = context.WithTimeout(ctx, opts.serverTimeout)
		defer timeoutCancel()
	}
	ctx, clientCancel, callErr := applyClientTimeout(ctx, request)
	defer clientCancel()

	stream := &wsServerStream{
		conn:     conn,
		opts:     opts,
		codec:    codec,
		ctx:      ctx,
		messages: make(chan []byte),
	}

	if callErr == nil {
		var header http.Header
		header, callErr = readLeadingMetadata(ctx, conn, request, opts)
		if callErr == nil {
			callErr = serveCall(ctx, cancel, request, header, spec, server, stream)
		}
	}
	// A protocol violation the read pump caught outranks whatever the
	// handler returned when its context was cancelled underneath it.
	if protocolErr := stream.protocolError(); protocolErr != nil {
		callErr = protocolErr
	}

	// The leading-metadata message precedes everything in its direction,
	// even when the handler never sent a message and even when the RPC
	// failed before it was dispatched.
	if err := stream.SendHeaders(); err != nil {
		slog.DebugContext(ctx, "draft7: writing leading metadata failed", "error", err)
		_ = conn.CloseNow()
		return
	}
	if err := stream.writeEndStream(callErr); err != nil {
		slog.DebugContext(ctx, "draft7: writing end-of-stream failed", "error", err)
		_ = conn.Close(websocket.StatusInternalError, "failed to marshal the end-of-stream message")
		return
	}
	if stream.protocolError() != nil {
		// The peer violated the protocol, and may have queued more of the
		// message that did it: drop the connection rather than drain it
		// through a closing handshake.
		_ = conn.CloseNow()
		return
	}
	if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
		slog.DebugContext(ctx, "draft7: closing websocket failed", "error", err)
	}
}

// applyClientTimeout narrows ctx to the deadline the handshake URI asked
// for, if it asked for one. The value is a positive integer of at most ten
// digits, in milliseconds; anything else is invalid_argument, as in
// Connect over HTTP. A non-positive value is a deadline that has already
// passed, matching connect-go.
func applyClientTimeout(ctx context.Context, request *http.Request) (context.Context, context.CancelFunc, error) {
	values, ok := request.URL.Query()[timeoutQueryParameter]
	if !ok {
		return ctx, func() {}, nil
	}
	if len(values) != 1 {
		return ctx, func() {}, connect.Errorf(connect.CodeInvalidArgument,
			"protocol error: %s must be given once", timeoutQueryParameter)
	}
	raw := values[0]
	if len(raw) > maxTimeoutDigits {
		return ctx, func() {}, connect.Errorf(connect.CodeInvalidArgument,
			"protocol error: %s %q has more than %d digits", timeoutQueryParameter, raw, maxTimeoutDigits)
	}
	ms, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return ctx, func() {}, connect.Errorf(connect.CodeInvalidArgument,
			"protocol error: invalid %s %q", timeoutQueryParameter, raw)
	}
	// context.WithTimeout keeps the shorter of this and any deadline
	// already on ctx, which is how the server's own timeout wins when it
	// is the shorter one.
	ctx, cancel := context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
	return ctx, cancel, nil
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
	info := &connect.CallInfo{
		Spec:            spec,
		PeerAddr:        request.RemoteAddr,
		Protocol:        protocolName,
		Codec:           stream.codec.Name(),
		RequestEncoding: connect.CompressionNameIdentity,
		TransportInfo:   &ServerInfo{request: request},
	}
	for key, values := range header {
		info.RequestHeader().SetValues(key, values)
	}
	stream.info = info
	// The pump owns every read from here on, so a client that disappears
	// mid-response cancels the handler instead of being noticed only by
	// whoever next calls Receive.
	go stream.pump(ctx, cancel)

	return server.Call(ctx, spec.Procedure, info, stream)
}

// readLeadingMetadata reads the mandatory first message and merges it over
// the metadata the upgrade request carried. The handshake headers are the
// base and the message replaces them key by key, so a Cookie the browser
// attached by itself still reaches the handler while anything the Connect
// client set explicitly wins — unless the key is reserved, which ends the
// RPC rather than being ignored.
func readLeadingMetadata(ctx context.Context, conn messageConn, request *http.Request, opts *options) (http.Header, error) {
	msg, err := readBeforeDeadline(ctx, conn, opts.readMaxBytes)
	if err != nil {
		return nil, readFailure(err, "the leading-metadata message")
	}
	if msg.marker != markerLeadingMetadata {
		return nil, connect.Errorf(connect.CodeInvalidArgument,
			"protocol error: expected the leading-metadata message first, got marker %q", msg.marker)
	}
	if !msg.text {
		return nil, connect.Errorf(connect.CodeInvalidArgument,
			"protocol error: the leading-metadata message must be a text frame")
	}
	if len(msg.payload) == 0 {
		return nil, connect.Errorf(connect.CodeInvalidArgument,
			"protocol error: the leading-metadata message has no payload; send {} for no metadata")
	}
	fromMessage, err := unmarshalMetadata(msg.payload)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "protocol error: %v", err)
	}
	for key := range fromMessage {
		if reason := reservedHeaderReason(key, opts.forbiddenRequestHeaders); reason != "" {
			return nil, connect.Errorf(connect.CodeInvalidArgument,
				"protocol error: metadata key %q is reserved: %s", strings.ToLower(key), reason)
		}
	}
	header := requestMetadata(request.Header)
	maps.Copy(header, fromMessage)
	return header, nil
}

// readBeforeDeadline reads one message, or gives up when ctx ends. The
// read itself is not bound to ctx: coder/websocket closes a connection
// whose read context expires, and a deadline that passes while the server
// waits for the opening message still has to be reported on the socket
// with an M and an S. The abandoned read returns once the connection is
// closed after that.
func readBeforeDeadline(ctx context.Context, conn messageConn, readMaxBytes int) (message, error) {
	type result struct {
		msg message
		err error
	}
	results := make(chan result, 1)
	go func() {
		msg, err := conn.ReadMessage(context.WithoutCancel(ctx), readMaxBytes)
		results <- result{msg: msg, err: err}
	}()
	select {
	case res := <-results:
		return res.msg, res.err
	case <-ctx.Done():
		return message{}, ctx.Err()
	}
}

// readFailure maps a failed read to the RPC error that reports it.
func readFailure(err error, what string) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return connect.Errorf(connect.CodeDeadlineExceeded, "deadline exceeded before %s", what)
	case errors.Is(err, context.Canceled):
		return connect.Errorf(connect.CodeCanceled, "RPC canceled before %s", what)
	case errors.Is(err, errMessageTooBig):
		return connect.Errorf(connect.CodeResourceExhausted, "%s: %v", what, err)
	case errors.Is(err, errEmptyMessage):
		return connect.Errorf(connect.CodeInvalidArgument, "protocol error: %s: %v", what, err)
	case websocket.CloseStatus(err) != -1 || errors.Is(err, io.EOF):
		return connect.Errorf(connect.CodeCanceled, "connection closed before %s", what)
	default:
		return connect.Errorf(connect.CodeUnavailable, "failed to read %s: %v", what, err)
	}
}

// wsServerStream implements connect.ServerStream over one WebSocket.
type wsServerStream struct {
	conn  messageConn
	info  *connect.CallInfo
	codec connect.Codec
	opts  *options
	ctx   context.Context //nolint:containedctx // the stream's lifetime is the RPC's

	// messages carries request bodies from the pump. It is closed when the
	// client ends its stream or the connection fails.
	messages     chan []byte
	messagesOnce sync.Once

	readMu sync.Mutex
	// readErr is what Receive reports instead of io.EOF: the connection
	// dropped, or the peer broke the protocol.
	readErr error
	// protoErr is set when readErr is a protocol violation rather than a
	// dropped connection; it becomes the end-of-stream error.
	protoErr error

	sendHeadersOnce sync.Once
	sendHeadersErr  error
	writeMu         sync.Mutex
}

// pump reads the request direction to its end. It exists so that the
// connection is being read even while the handler is only sending: a client
// that goes away mid-response then cancels the RPC's context instead of
// going unnoticed until someone calls Receive.
func (ss *wsServerStream) pump(ctx context.Context, cancel context.CancelFunc) {
	defer ss.endRequestStream()
	// Reads are not bound to the RPC's context, because coder/websocket
	// closes a connection whose read context ends and the end-of-stream
	// message explaining a deadline still has to get out. A read that
	// outlives the RPC returns when serveWebSocketRPC closes the connection.
	readCtx := context.WithoutCancel(ctx)
	ended := false
	for {
		msg, err := ss.conn.ReadMessage(readCtx, ss.opts.readMaxBytes)
		if err != nil {
			switch {
			case errors.Is(err, errMessageTooBig), errors.Is(err, errEmptyMessage):
				ss.setProtocolErr(readFailure(err, "a request message"))
			case ended && isCleanClose(err), ctx.Err() != nil:
				// A clean close after the client ended its stream is how
				// a finished client leaves; a read failing after the RPC
				// ended is the connection being closed underneath it.
			default:
				// The stream was still open and no C arrived: the client
				// is gone, and the RPC is cancelled.
				ss.setReadErr(connect.Errorf(connect.CodeCanceled,
					"connection closed before the client ended its stream: %v", err))
			}
			cancel()
			return
		}
		if ended {
			ss.setProtocolErr(connect.Errorf(connect.CodeInvalidArgument,
				"protocol error: message with marker %q after the client end-of-stream", msg.marker))
			cancel()
			return
		}
		switch msg.marker {
		case markerBody:
			if err := ss.checkBodyFrame(msg); err != nil {
				ss.setProtocolErr(err)
				cancel()
				return
			}
			if !ss.deliver(ctx, msg.payload) {
				return
			}
		case markerClientEndStream:
			ended = true
			if len(msg.payload) > 0 {
				if err := ss.checkBodyFrame(msg); err != nil {
					ss.setProtocolErr(err)
					cancel()
					return
				}
				if !ss.deliver(ctx, msg.payload) {
					return
				}
			} else if !msg.text {
				ss.setProtocolErr(connect.Errorf(connect.CodeInvalidArgument,
					"protocol error: a bare client end-of-stream must be a text frame"))
				cancel()
				return
			}
			// Receive reports EOF from here, but the loop keeps reading so
			// that a client which then disappears mid-response still
			// cancels the handler, and a message after C is caught.
			ss.endRequestStream()
		case markerLeadingMetadata:
			ss.setProtocolErr(connect.Errorf(connect.CodeInvalidArgument,
				"protocol error: a second leading-metadata message"))
			cancel()
			return
		case markerServerEndStream:
			ss.setProtocolErr(connect.Errorf(connect.CodeInvalidArgument,
				"protocol error: server end-of-stream marker from a client"))
			cancel()
			return
		default:
			ss.setProtocolErr(unknownMarkerError(msg.marker))
			cancel()
			return
		}
	}
}

// checkBodyFrame enforces that a body's frame type names the negotiated
// codec: binary for Protobuf, text for JSON, and never an empty text body.
func (ss *wsServerStream) checkBodyFrame(msg message) error {
	return checkBodyFrame(msg, ss.codec.Name())
}

// checkBodyFrame is the frame-type rule for bodies, shared by both peers.
func checkBodyFrame(msg message, codecName string) error {
	binary := codecIsBinary(codecName)
	switch {
	case msg.text && binary:
		return connect.Errorf(connect.CodeInvalidArgument,
			"protocol error: body arrived as a text frame (JSON) but the negotiated codec is %s", codecName)
	case !msg.text && !binary:
		return connect.Errorf(connect.CodeInvalidArgument,
			"protocol error: body arrived as a binary frame (Protobuf) but the negotiated codec is %s", codecName)
	case msg.text && len(msg.payload) == 0:
		return connect.Errorf(connect.CodeInvalidArgument,
			"protocol error: an empty text body; an empty JSON message is {}")
	}
	return nil
}

// unknownMarkerError is the protocol error for a marker this revision does
// not define, including one with the reserved high bit set.
func unknownMarkerError(marker byte) error {
	if marker >= 0x80 {
		return connect.Errorf(connect.CodeInvalidArgument,
			"protocol error: marker 0x%02x has the reserved high bit set", marker)
	}
	return connect.Errorf(connect.CodeInvalidArgument, "protocol error: unknown marker %q", marker)
}

// deliver hands one request body to Receive. It reports false when the RPC
// ended first.
func (ss *wsServerStream) deliver(ctx context.Context, payload []byte) bool {
	select {
	case ss.messages <- payload:
		return true
	case <-ctx.Done():
		return false
	}
}

// endRequestStream closes the message channel, which is what Receive reads
// as the end of the request stream. The pump reaches it twice on a
// well-behaved RPC — once at C, once when the loop exits.
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

func (ss *wsServerStream) setProtocolErr(err error) {
	ss.readMu.Lock()
	defer ss.readMu.Unlock()
	if ss.readErr == nil {
		ss.readErr = err
	}
	if ss.protoErr == nil {
		ss.protoErr = err
	}
}

func (ss *wsServerStream) takeReadErr() error {
	ss.readMu.Lock()
	defer ss.readMu.Unlock()
	return ss.readErr
}

func (ss *wsServerStream) protocolError() error {
	ss.readMu.Lock()
	defer ss.readMu.Unlock()
	return ss.protoErr
}

// Receive reads the next request message. The client's end-of-stream is
// io.EOF; a connection that ended without one is an error, so a handler
// can tell "the client finished" from "the client is gone".
func (ss *wsServerStream) Receive(msg any) error {
	select {
	case data, ok := <-ss.messages:
		if !ok {
			if err := ss.takeReadErr(); err != nil {
				return err
			}
			return io.EOF
		}
		return unmarshalMessage(ss.ctx, ss.codec, data, msg)
	case <-ss.ctx.Done():
		if err := ss.takeReadErr(); err != nil {
			return err
		}
		return ss.ctx.Err()
	}
}

// SendHeaders writes the leading-metadata message exactly once. Response
// headers set after it are late, and are dropped.
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
	data, err := marshalMetadata(header)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal response metadata: %v", err)
	}
	// Writes use a context that outlives a cancelled RPC: the end-of-stream
	// message explaining the cancellation still has to reach the client.
	return ss.write(context.WithoutCancel(ss.ctx), markerLeadingMetadata, data, true)
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
	if err := ss.write(ss.ctx, markerBody, payload, !codecIsBinary(ss.codec.Name())); err != nil {
		return wrapWriteError(err)
	}
	return nil
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
		return fmt.Errorf("marshal end-of-stream: %w", err)
	}
	return ss.write(context.WithoutCancel(ss.ctx), markerServerEndStream, data, true)
}

// write serializes writes: the handler's Send and the RPC's own S can race
// when a handler returns while another goroutine is still sending.
func (ss *wsServerStream) write(ctx context.Context, marker byte, payload []byte, text bool) error {
	ss.writeMu.Lock()
	defer ss.writeMu.Unlock()
	return ss.conn.WriteMessage(ctx, marker, payload, text)
}
