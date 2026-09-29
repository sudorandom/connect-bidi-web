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

// DefaultPathPrefix is where [Mount] registers procedures unless the
// deployment picks somewhere else.
//
// A deployment actually following this draft mounts on the Connect
// procedure URLs themselves — pass "" to [Mount] — so that
// /connectrpc.eliza.v1.ElizaService/Converse answers POST with ordinary
// Connect over HTTP and GET+Upgrade with this protocol. The prefix exists
// because this repository serves several wire-incompatible drafts from one
// origin, and they cannot all own the same URLs.
const DefaultPathPrefix = "/websocket-draft1"

// ServeMux is the subset of [http.ServeMux] used by [Mount]. Any router
// satisfying it can host these routes.
type ServeMux interface {
	Handle(pattern string, handler http.Handler)
}

// Handler serves streaming Connect RPCs over WebSocket connections using
// draft 1 of the wire protocol. One connection carries one RPC and closes
// with it.
//
// The procedure comes from the URL — the request path's last two segments —
// so one Handler serves every procedure whether it is mounted on the
// Connect procedure URLs themselves or under a prefix.
type Handler struct {
	server *connect.Server
	opts   serverOptions
	// streamTypes records which procedures are streaming, so a unary one
	// can be refused: draft 1 leaves unary on plain Connect over HTTP.
	streamTypes map[string]connect.StreamType
}

// NewHandler creates a Handler for serving streaming Connect RPCs over
// WebSockets using draft 1 of the wire protocol. Mount it on a subtree, or
// use [Mount] to register one route per streaming procedure.
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

// Mount registers a draft 1 handler on mux for each of server's streaming
// procedures, at prefix + procedure. Unary procedures are skipped: draft 1
// does not carry them.
//
// This is for serving draft 1 on URLs of its own — [DefaultPathPrefix] is
// the convention, and an empty prefix takes the Connect procedure URLs for
// a host that serves nothing else. To put draft 1 *beside* an existing
// Connect HTTP mount on those same URLs, which is what a deployment
// following this draft does, use [Intercept]: two handlers cannot register
// the same pattern on one [http.ServeMux].
func Mount(mux ServeMux, server *connect.Server, prefix string, opts ...Option) {
	handler := NewHandler(server, opts...)
	prefix = strings.TrimSuffix(prefix, "/")
	for spec := range server.Specs() {
		if spec.StreamType == connect.StreamTypeUnary {
			continue
		}
		mux.Handle(prefix+spec.Procedure, handler)
	}
}

// Intercept returns an [http.Handler] that serves draft 1 WebSocket
// upgrades addressed to one of server's streaming procedures and passes
// everything else through to next.
//
// This is how a deployment following this draft mounts it: draft 1 has no
// path of its own, so the Connect procedure URLs answer POST with ordinary
// Connect over HTTP and GET+Upgrade with this protocol.
//
//	mux := http.NewServeMux()
//	connecthttp.Mount(mux, connectServer)
//	http.ListenAndServe(addr, draft1.Intercept(mux, connectServer))
//
// Anything that is not a draft 1 upgrade reaches next untouched, including
// upgrades that do not offer the subprotocol and upgrades addressed to a
// unary procedure — neither is draft 1's to answer, and next is free to
// serve some other WebSocket protocol on the same origin.
func Intercept(next http.Handler, server *connect.Server, opts ...Option) http.Handler {
	handler := NewHandler(server, opts...)
	return http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		if !handler.claims(request) {
			next.ServeHTTP(responseWriter, request)
			return
		}
		handler.ServeHTTP(responseWriter, request)
	})
}

// claims reports whether a request is a draft 1 upgrade this handler should
// answer: a WebSocket handshake, offering the subprotocol, for a procedure
// this server streams.
func (h *Handler) claims(request *http.Request) bool {
	if !isExtendedConnectWebSocket(request) && !isWebSocketUpgrade(request) {
		return false
	}
	if !offersSubprotocol(request.Header) {
		return false
	}
	streamType, ok := h.streamTypes[procedureFromPath(request.URL.Path)]
	return ok && streamType != connect.StreamTypeUnary
}

// ServeHTTP implements http.Handler by turning the request into a WebSocket
// connection and serving the one RPC its URL names. Two bootstraps are
// supported: an HTTP/1.1 Upgrade handshake, and an RFC 8441 extended
// CONNECT stream on HTTP/2 (which reaches handlers only when the server
// runs with GODEBUG=http2xconnect=1).
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
	h.serveRPC(request, conn)
}

// serveRPC runs the connection's one RPC and then closes.
func (h *Handler) serveRPC(request *http.Request, conn messageConn) {
	connCtx := request.Context()
	defer func() {
		if err := conn.Close(); err != nil {
			slog.DebugContext(connCtx, "draft1: closing websocket failed", "error", err)
		}
	}()

	rpcCtx, cancel := context.WithCancel(connCtx)
	defer cancel()

	stream := &serverStream{
		conn:     conn,
		opts:     &h.opts,
		ctx:      rpcCtx,
		messages: make(chan []byte),
	}
	pumped := false

	header, callErr := h.readRequestHeaders(rpcCtx, conn)
	procedure := procedureFromPath(request.URL.Path)
	if callErr == nil {
		callErr = h.checkProcedure(procedure)
	}
	if callErr == nil {
		var codec connect.Codec
		codec, callErr = negotiateCodec(header, &h.opts)
		if callErr == nil {
			stream.codec = codec
			pumped = true
			callErr = h.dispatch(connCtx, rpcCtx, cancel, request, header, procedure, stream)
		}
	}
	if stream.codec == nil {
		stream.codec = h.opts.Codecs[connect.CodecNameProto]
	}

	// The response is always well formed, even when the request was not:
	// headers, then end-stream carrying the error.
	if err := stream.SendHeaders(); err != nil {
		slog.DebugContext(connCtx, "draft1: writing response headers failed", "error", err)
		return
	}
	if err := stream.writeEndStream(callErr); err != nil {
		slog.DebugContext(connCtx, "draft1: writing end-stream failed", "error", err)
	}
	if pumped {
		// Release a pump still waiting on a handler that stopped receiving.
		cancel()
	}
}

// readRequestHeaders reads the envelope that opens the RPC.
func (h *Handler) readRequestHeaders(ctx context.Context, conn messageConn) (http.Header, error) {
	message, err := conn.ReadMessage(ctx)
	if err != nil {
		return nil, connect.Errorf(connect.CodeUnavailable, "failed to read the request headers envelope: %v", err)
	}
	flag, payload, err := decodeEnvelope(message)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "protocol error: %v", err)
	}
	if flag != flagHeaders {
		return nil, connect.Errorf(
			connect.CodeInvalidArgument,
			"protocol error: expected a request headers envelope first, got flag 0x%02x", flag,
		)
	}
	header, err := connectprotocol.UnmarshalHeaders(payload)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "failed to unmarshal the request headers envelope: %v", err)
	}
	return header, nil
}

// checkProcedure rejects requests this handler will not serve: unknown
// procedures, and unary ones, which draft 1 leaves on plain Connect HTTP.
func (h *Handler) checkProcedure(procedure string) error {
	if procedure == "" {
		return connect.Errorf(connect.CodeUnimplemented, "the request URL does not name a procedure")
	}
	streamType, ok := h.streamTypes[procedure]
	if !ok {
		return connect.Errorf(connect.CodeUnimplemented, "unknown procedure %q", procedure)
	}
	if streamType == connect.StreamTypeUnary {
		return connect.Errorf(
			connect.CodeUnimplemented,
			"%q is a unary procedure; draft 1 carries unary RPCs over plain Connect HTTP", procedure,
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
	// The upgrade request's own headers are the base of the request
	// metadata; the headers envelope overrides them key by key, so a Cookie
	// the browser attached by itself still reaches the handler while
	// anything the Connect client set explicitly wins.
	for key, values := range request.Header {
		if isHandshakeHeader(key) {
			continue
		}
		info.RequestHeader().SetValues(key, values)
	}
	for key, values := range header {
		if !strings.HasPrefix(key, ":") {
			info.RequestHeader().SetValues(key, values)
		}
	}
	stream.info = info
	stream.ctx = ctx
	// The pump reads on the connection's context, not the RPC's: it is what
	// notices a client that disappears while the handler is still
	// streaming, and it must outlive a cancelled RPC to do so.
	go stream.pump(connCtx, ctx, cancel)

	return h.server.Call(ctx, procedure, info, stream)
}

// handshakeHeaders are headers belonging to the WebSocket handshake rather
// than to the RPC. They are stripped before the upgrade request's headers
// become request metadata, along with every sec-websocket-* header.
var handshakeHeaders = map[string]struct{}{
	"Connection":          {},
	"Upgrade":             {},
	"Keep-Alive":          {},
	"Proxy-Authenticate":  {},
	"Proxy-Authorization": {},
	"Te":                  {},
	"Trailer":             {},
	"Transfer-Encoding":   {},
}

func isHandshakeHeader(key string) bool {
	if _, ok := handshakeHeaders[key]; ok {
		return true
	}
	return strings.HasPrefix(strings.ToLower(key), "sec-websocket-")
}

// negotiateCodec resolves the codec named by the request's content type.
func negotiateCodec(header http.Header, opts *serverOptions) (connect.Codec, error) {
	contentType := header.Get("Content-Type")
	if contentType == "" {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "protocol error: the request headers envelope must set content-type")
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

// serverStream implements connect.ServerStream for the one RPC on a
// connection.
type serverStream struct {
	conn  messageConn
	info  *connect.CallInfo
	codec connect.Codec
	opts  *serverOptions
	ctx   context.Context //nolint:containedctx // the stream's lifetime is the RPC's

	messages     chan []byte
	messagesOnce sync.Once

	readMu  sync.Mutex
	readErr error

	sendHeadersOnce sync.Once
	sendHeadersErr  error
}

// pump reads the RPC's request envelopes, and then keeps reading.
//
// It does not stop at the client's half-close, because after that read this
// is the only thing left watching the connection — and draft 1's only
// cancellation signal is the client closing it. A server-streaming RPC
// half-closes immediately and then streams for as long as it likes; if the
// pump returned there, a browser tab closing mid-stream would go unnoticed
// until the next write failed.
func (ss *serverStream) pump(connCtx, rpcCtx context.Context, cancel context.CancelFunc) {
	defer ss.endRequestStream()
	halfClosed := false
	for {
		message, err := ss.conn.ReadMessage(connCtx)
		if err != nil {
			// The client is gone. After the half-close that is the ordinary
			// end of the request direction rather than a failure, so it is
			// not recorded as a read error — but the RPC's context ends
			// either way, which is what unblocks a handler that is waiting
			// rather than writing.
			if !halfClosed {
				ss.setReadErr(connect.Errorf(connect.CodeUnavailable, "failed to read from WebSocket: %v", err))
			}
			cancel()
			return
		}
		flag, payload, err := decodeEnvelope(message)
		if err != nil {
			ss.setReadErr(connect.Errorf(connect.CodeInvalidArgument, "protocol error: %v", err))
			cancel()
			return
		}
		if halfClosed {
			ss.setReadErr(connect.Errorf(connect.CodeInvalidArgument, "protocol error: an envelope after the request end-stream"))
			cancel()
			return
		}
		switch flag {
		case flagEndStream:
			// The client's half-close: no more request messages, but the
			// connection stays open for the response.
			halfClosed = true
			ss.endRequestStream()
			continue
		case flagData:
			if limit := ss.opts.ReadMaxBytes; limit > 0 && len(payload) > limit {
				ss.setReadErr(connect.Errorf(
					connect.CodeResourceExhausted,
					"message size %d exceeds read limit %d", len(payload), limit,
				))
				cancel()
				return
			}
		case flagHeaders:
			ss.setReadErr(connect.Errorf(connect.CodeInvalidArgument, "protocol error: a second request headers envelope"))
			cancel()
			return
		default:
			ss.setReadErr(connect.Errorf(connect.CodeInvalidArgument, "protocol error: unexpected envelope flag 0x%02x", flag))
			cancel()
			return
		}
		select {
		case ss.messages <- payload:
		case <-rpcCtx.Done():
			// The handler stopped receiving. Nothing else will arrive for
			// this connection, so there is no framing to stay aligned with.
			return
		}
	}
}

func (ss *serverStream) endRequestStream() {
	ss.messagesOnce.Do(func() { close(ss.messages) })
}

func (ss *serverStream) setReadErr(err error) {
	ss.readMu.Lock()
	defer ss.readMu.Unlock()
	if ss.readErr == nil {
		ss.readErr = err
	}
}

func (ss *serverStream) takeReadErr() error {
	ss.readMu.Lock()
	defer ss.readMu.Unlock()
	return ss.readErr
}

// Receive reads the next request message, reporting the client's
// half-close as io.EOF.
func (ss *serverStream) Receive(msg any) error {
	select {
	case payload, ok := <-ss.messages:
		if !ok {
			if err := ss.takeReadErr(); err != nil {
				return err
			}
			return io.EOF
		}
		return unmarshalMessage(ss.ctx, ss.codec, payload, msg, ss.opts.ReadMaxBytes)
	case <-ss.ctx.Done():
		return ss.ctx.Err()
	}
}

// SendHeaders writes the response headers envelope exactly once. Draft 1
// always sends one, even for an RPC that failed before producing a message:
// a browser cannot read response headers off the handshake, so this is the
// only place they can arrive.
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

	payload, err := connectprotocol.MarshalHeaders(header)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal headers: %v", err)
	}
	return ss.writeEnvelope(flagHeaders, payload)
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
	return ss.writeEnvelope(flagData, payload)
}

// writeEndStream writes the Connect EndStreamResponse that finishes the
// RPC, and is the last thing on the connection before it closes.
func (ss *serverStream) writeEndStream(callErr error) error {
	var wireErr error
	if callErr != nil && !errors.Is(callErr, io.EOF) {
		wireErr = connectprotocol.ErrorForWire(callErr)
	}
	trailers := make(http.Header)
	if ss.info != nil && ss.info.ResponseTrailer() != nil {
		maps.Insert(trailers, ss.info.ResponseTrailer().All())
	}
	payload, err := connectprotocol.MarshalEndStream(wireErr, trailers)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal end-stream: %v", err)
	}
	return ss.writeEnvelope(flagEndStream, payload)
}

func (ss *serverStream) writeEnvelope(flag uint8, payload []byte) error {
	message, err := encodeEnvelope(flag, payload)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to encode envelope: %v", err)
	}
	// Writes use a context that outlives a cancelled RPC: the end-stream
	// envelope explaining the cancellation still has to reach the client.
	if err := ss.conn.WriteMessage(context.WithoutCancel(ss.ctx), message); err != nil {
		return wrapWriteError(err)
	}
	return nil
}
