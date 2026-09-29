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

// Handler serves Connect RPCs over WebSocket connections using draft 6 of
// the wire protocol. One handler serves every procedure: the request
// headers message names which one, so the endpoint is a deployment choice
// rather than part of the protocol.
//
// Each accepted connection carries a sequence of RPCs, one at a time. When
// an RPC ends the handler goes back to waiting for the next headers
// message, so a client that pools connections pays one handshake for many
// calls.
type Handler struct {
	server *connect.Server
	opts   serverOptions
	// streamTypes records which procedures are streaming, so a unary one
	// can be refused: draft 6 leaves unary on plain Connect over HTTP.
	streamTypes map[string]connect.StreamType
}

// NewHandler creates a Handler for serving Connect RPCs over WebSockets
// using draft 6 of the wire protocol. Mount it wherever you like;
// [DefaultPath] is the convention.
func NewHandler(server *connect.Server, opts ...Option) *Handler {
	sOpts := serverOptions{protocolOptions: newServerProtocolOptions()}
	for _, opt := range opts {
		opt.applyServer(&sOpts)
	}
	sOpts.finalize()

	streamTypes := make(map[string]connect.StreamType)
	for spec := range server.Specs() {
		streamTypes[spec.Procedure] = spec.StreamType
	}
	return &Handler{server: server, opts: sOpts, streamTypes: streamTypes}
}

// ServeHTTP implements http.Handler by turning the request into a WebSocket
// connection and serving RPCs on it until the client goes away. Two
// bootstraps are supported: an HTTP/1.1 Upgrade handshake, and an RFC 8441
// extended CONNECT stream on HTTP/2 (which reaches handlers only when the
// server runs with GODEBUG=http2xconnect=1).
func (h *Handler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	extendedConnect := isExtendedConnectWebSocket(request)
	if !extendedConnect && !isWebSocketUpgrade(request) {
		responseWriter.Header().Set("Allow", http.MethodGet)
		http.Error(responseWriter, "expected a websocket upgrade", http.StatusMethodNotAllowed)
		return
	}
	if !offersSubprotocol(request.Header) {
		http.Error(responseWriter, "expected the "+subprotocol+" websocket subprotocol", http.StatusBadRequest)
		return
	}

	var conn messageConn
	if extendedConnect {
		accepted, ok := h.acceptH2(responseWriter, request)
		if !ok {
			return
		}
		conn = accepted
	} else {
		acceptOpts := &websocket.AcceptOptions{}
		if h.opts.acceptOptions != nil {
			cloned := *h.opts.acceptOptions
			acceptOpts = &cloned
		}
		acceptOpts.Subprotocols = []string{subprotocol}
		acceptOpts.CompressionMode = websocket.CompressionNoContextTakeover
		if h.opts.withoutCompression {
			acceptOpts.CompressionMode = websocket.CompressionDisabled
		}
		accepted, err := websocket.Accept(responseWriter, request, acceptOpts)
		if err != nil {
			return
		}
		conn = newCoderConn(accepted)
	}
	h.serveConn(request, conn)
}

// serveConn runs RPCs on one accepted connection until the client stops
// starting them. This loop is the whole of draft 6's connection reuse: an
// RPC ends with the end-stream message, and then the handler simply waits
// for the next request headers message instead of closing.
func (h *Handler) serveConn(request *http.Request, conn messageConn) {
	ctx := request.Context()
	defer func() {
		if err := conn.Close(); err != nil {
			slog.DebugContext(ctx, "draft6: closing websocket failed", "error", err)
		}
	}()

	// The first RPC's headers are read here; every later one arrives as the
	// read-ahead of the RPC before it.
	next := readMessage(ctx, conn)
	for {
		if next.err != nil {
			// The client hung up, which is the ordinary end of a pooled
			// connection rather than a failure.
			return
		}
		if !next.text || len(next.data) == 0 {
			slog.DebugContext(ctx, "draft6: expected a request headers message between RPCs")
			return
		}
		following, reusable := h.serveRPC(ctx, request, conn, next.data)
		if !reusable {
			// The RPC ended in a way that leaves the connection's framing
			// in doubt, so it cannot carry another.
			return
		}
		next = following
	}
}

// nextMessage is the message that follows an RPC on a reused connection:
// the headers of whatever comes next, or the failure that ended the
// connection.
type nextMessage struct {
	data []byte
	text bool
	err  error
}

func readMessage(ctx context.Context, conn messageConn) nextMessage {
	data, text, err := conn.ReadMessage(ctx)
	return nextMessage{data: data, text: text, err: err}
}

// serveRPC runs one RPC, given the request headers message that opened it.
// It returns the next message on the connection and whether the connection
// is still fit to carry another RPC.
func (h *Handler) serveRPC(
	ctx context.Context,
	request *http.Request,
	conn messageConn,
	headersPayload []byte,
) (nextMessage, bool) {
	rpcCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream := &serverStream{
		conn:     conn,
		opts:     &h.opts,
		ctx:      rpcCtx,
		messages: make(chan []byte),
		next:     make(chan nextMessage, 1),
	}
	pumped := false

	header, callErr := parseRequestHeaders(headersPayload)
	procedure := ""
	if callErr == nil {
		procedure = header.Get(pseudoHeaderPath)
		callErr = h.checkProcedure(procedure)
	}
	if callErr == nil {
		var codec connect.Codec
		codec, callErr = negotiateCodec(header, &h.opts)
		if callErr == nil {
			stream.codec = codec
			pumped = true
			callErr = h.dispatch(ctx, rpcCtx, cancel, request, header, procedure, stream)
		}
	}
	if stream.codec == nil {
		stream.codec = h.opts.Codecs[connect.CodecNameProto]
	}

	// The response is always well formed, even when the request was not:
	// headers, separator, end-stream. That is what lets the connection
	// survive a bad request and carry the next one.
	if err := stream.SendHeaders(); err != nil {
		return nextMessage{err: err}, false
	}
	if err := stream.writeSeparator(); err != nil {
		return nextMessage{err: err}, false
	}
	if err := stream.writeEndStream(callErr); err != nil {
		return nextMessage{err: err}, false
	}

	if !pumped {
		// Nothing ever read this connection for us, so read the next
		// message here.
		return readMessage(ctx, conn), true
	}
	// Release the pump if the handler stopped receiving mid-stream, then
	// take what it read after this RPC's separator.
	cancel()
	next := <-stream.next
	// A protocol error means the peer and the handler disagree about where
	// the next message starts; anything else leaves the connection clean.
	return next, !stream.protocolError()
}

// checkProcedure rejects requests this handler will not serve: unknown
// procedures, and unary ones, which draft 6 leaves on plain Connect HTTP.
func (h *Handler) checkProcedure(procedure string) error {
	if procedure == "" {
		return connect.Errorf(connect.CodeInvalidArgument, "protocol error: the request headers message must set %s", pseudoHeaderPath)
	}
	streamType, ok := h.streamTypes[procedure]
	if !ok {
		return connect.Errorf(connect.CodeUnimplemented, "unknown procedure %q", procedure)
	}
	if streamType == connect.StreamTypeUnary {
		return connect.Errorf(
			connect.CodeUnimplemented,
			"%q is a unary procedure; draft 6 carries unary RPCs over plain Connect HTTP", procedure,
		)
	}
	return nil
}

// dispatch runs the RPC once its metadata is known.
func (h *Handler) dispatch(
	connCtx context.Context,
	ctx context.Context,
	cancel context.CancelFunc,
	request *http.Request,
	header http.Header,
	procedure string,
	stream *serverStream,
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
		PeerAddr:        request.RemoteAddr,
		Protocol:        protocolName,
		Codec:           stream.codec.Name(),
		RequestEncoding: connect.CompressionNameIdentity,
	}
	for key, values := range header {
		if !strings.HasPrefix(key, ":") {
			info.RequestHeader().SetValues(key, values)
		}
	}
	stream.info = info
	stream.ctx = ctx
	// The pump reads on the *connection's* context, not the RPC's: it has to
	// outlive this RPC by one message, and it is what notices a client that
	// disappears while the handler is still streaming.
	go stream.pump(connCtx, ctx, cancel)

	return h.server.Call(ctx, procedure, info, stream)
}

// parseRequestHeaders decodes the JSON metadata that opens an RPC.
func parseRequestHeaders(payload []byte) (http.Header, error) {
	header, err := connectprotocol.UnmarshalHeaders(payload)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "failed to unmarshal the request headers message: %v", err)
	}
	return header, nil
}

// negotiateCodec resolves the codec named by the request's content type.
func negotiateCodec(header http.Header, opts *serverOptions) (connect.Codec, error) {
	contentType := header.Get("Content-Type")
	if contentType == "" {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "protocol error: the request headers message must set content-type")
	}
	name := codecNameForContentType(contentType)
	if name == "" {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "invalid content-type %q: expected %s{codec}", contentType, contentTypePrefix)
	}
	codec := opts.Codecs[name]
	if codec == nil {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "unknown codec %q in content-type %q", name, contentType)
	}
	return codec, nil
}

// serverStream implements connect.ServerStream for one RPC on a connection
// that will carry more.
type serverStream struct {
	conn  messageConn
	info  *connect.CallInfo
	codec connect.Codec
	opts  *serverOptions
	ctx   context.Context //nolint:containedctx // the stream's lifetime is the RPC's

	messages     chan []byte
	messagesOnce sync.Once
	// next carries the first message read after this RPC's separator. The
	// serve loop takes it instead of reading itself, which is what keeps
	// something reading the connection while a handler streams.
	next chan nextMessage

	readMu sync.Mutex
	// readErr is the first read failure, and protocolErr records whether it
	// was the client breaking the protocol — which is what decides if the
	// connection can carry another RPC.
	readErr     error
	protocolErr bool

	sendHeadersOnce sync.Once
	sendHeadersErr  error
}

// pump reads this RPC's request messages, and then one message more.
//
// That last read is what makes reuse work without going blind. Draft 5's
// pump could keep reading forever, because nothing else would ever arrive
// on the connection. Here the message after the separator belongs to the
// *next* RPC — so the pump reads exactly one and hands it to the serve
// loop. Reading it early is not just tidiness: while a handler streams a
// long response, this is the only read outstanding, and so the only thing
// that will notice a client that has gone away.
func (ss *serverStream) pump(connCtx, rpcCtx context.Context, cancel context.CancelFunc) {
	for {
		data, text, err := ss.conn.ReadMessage(connCtx)
		if err != nil {
			ss.setReadErr(connect.Errorf(connect.CodeUnavailable, "failed to read from WebSocket: %v", err), false)
			ss.endRequestStream()
			cancel()
			ss.next <- nextMessage{err: err}
			return
		}
		if text && len(data) == 0 {
			// The separator: this RPC's requests are done.
			ss.endRequestStream()
			break
		}
		select {
		case ss.messages <- data:
		case <-rpcCtx.Done():
			// The handler stopped receiving, but the connection still has
			// to reach the separator or the next RPC would start reading
			// mid-stream. Drop the message and keep going.
		}
	}

	// One message beyond this RPC: the next one's headers, or the failure
	// that ended the connection.
	next := readMessage(connCtx, ss.conn)
	if next.err != nil {
		// The client is gone. Ending the RPC's context is what unblocks a
		// handler that is waiting rather than writing.
		cancel()
	}
	ss.next <- next
}

func (ss *serverStream) endRequestStream() {
	ss.messagesOnce.Do(func() { close(ss.messages) })
}

func (ss *serverStream) setReadErr(err error, protocol bool) {
	ss.readMu.Lock()
	defer ss.readMu.Unlock()
	if ss.readErr == nil {
		ss.readErr = err
		ss.protocolErr = protocol
	}
}

func (ss *serverStream) takeReadErr() error {
	ss.readMu.Lock()
	defer ss.readMu.Unlock()
	return ss.readErr
}

func (ss *serverStream) protocolError() bool {
	ss.readMu.Lock()
	defer ss.readMu.Unlock()
	return ss.readErr != nil
}

// Receive reads the next request message, reporting the client's
// half-close as io.EOF.
func (ss *serverStream) Receive(msg any) error {
	select {
	case data, ok := <-ss.messages:
		if !ok {
			if err := ss.takeReadErr(); err != nil {
				return err
			}
			return io.EOF
		}
		return unmarshalMessage(ss.ctx, ss.codec, data, msg, ss.opts.ReadMaxBytes)
	case <-ss.ctx.Done():
		return ss.ctx.Err()
	}
}

// SendHeaders writes the response headers message exactly once.
func (ss *serverStream) SendHeaders() error {
	ss.sendHeadersOnce.Do(func() {
		ss.sendHeadersErr = ss.sendHeaders()
	})
	return ss.sendHeadersErr
}

func (ss *serverStream) sendHeaders() error {
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
func (ss *serverStream) Send(msg any) error {
	if err := ss.SendHeaders(); err != nil {
		return err
	}
	payload, err := marshalMessage(ss.ctx, ss.codec, msg, ss.opts.SendMaxBytes)
	if err != nil {
		return err
	}
	if err := ss.conn.WriteMessage(ss.ctx, payload, dataIsText(payload)); err != nil {
		return wrapWriteError(err)
	}
	return nil
}

// writeSeparator ends the response data phase.
func (ss *serverStream) writeSeparator() error {
	return ss.conn.WriteMessage(context.WithoutCancel(ss.ctx), nil, true)
}

// writeEndStream writes the Connect EndStreamResponse that finishes the
// RPC. On a draft 6 connection it is not the last thing on the wire — the
// next RPC's headers message follows it.
func (ss *serverStream) writeEndStream(callErr error) error {
	var wireErr error
	if callErr != nil && !errors.Is(callErr, io.EOF) {
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
