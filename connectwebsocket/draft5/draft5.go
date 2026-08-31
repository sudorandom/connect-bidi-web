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

// Package draft5 provides a connect.Transport and http.Handlers that carry
// Connect RPCs over WebSocket connections, enabling full bidirectional
// streaming from environments such as web browsers.
//
// This is draft 5 of the WebSocket wire protocol, and it shares nothing
// with drafts 1, 3, and 4 in the sibling packages — not their framing, not
// their connection mapping, and not their dedicated path. Draft 5 asks what
// is left to design once you admit that a WebSocket handshake is already an
// HTTP request. The answer is nearly nothing: one WebSocket carries one
// RPC, so there are no stream IDs and no reset frames; the upgrade request
// carries the URL and headers, so there is no envelope; and the WebSocket's
// own message boundaries delimit each message, so there are no length
// fields. A data message is the codec's output and nothing else.
//
// The one bit of framing is the WebSocket opcode, and draft 5 does not add
// it either — RFC 6455 sends it on every frame regardless. Exactly one
// shape is reserved: an empty text message is the separator that ends a
// direction's data phase. Everything else is a message of the RPC, with
// position telling metadata from data (the first message is the headers,
// the one after the server's separator is the end-stream). Senders use a
// text message whenever the payload is non-empty and valid UTF-8, so a
// JSON stream is readable on the wire, and a binary one otherwise —
// including whenever the payload is empty, because an empty protobuf
// message encodes to zero bytes and must not read as a half-close. See
// README.md for the wire protocol.
//
// Unary RPCs never upgrade. They are dispatched as ordinary Connect HTTP
// requests, so a draft 5 endpoint is a Connect endpoint that also answers
// GET with an upgrade. [Mount] registers both on each procedure URL, and
// [NewTransport] chooses between them per RPC.
//
// The package is a fork of connectrpc.com/connect/v2/connecthttp, kept as
// close to verbatim as possible so it can be re-synced; the draft 5
// behaviour lives in the files with no upstream counterpart. It carries the
// Connect, gRPC, and gRPC-Web protocols on the HTTP path, exactly as
// upstream does. Only the Connect protocol upgrades to a WebSocket.
package draft5

import (
	"context"
	"crypto/tls"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectgzip"
	"connectrpc.com/connect/v2/connectproto"
	"github.com/coder/websocket"
	"golang.org/x/net/http2"
)

// HTTPClient is the HTTP client surface the transport drives. The
// standard library's [*http.Client] satisfies it.
type HTTPClient interface {
	// Do sends request and returns the HTTP response.
	Do(*http.Request) (*http.Response, error)
}

// ClientInfo is the HTTP metadata for a client RPC, carried on
// [connect.CallInfo.TransportInfo].
//
// Request-side accessors are safe to read once the first Send (or Receive)
// returns, response-side accessors once the first Receive returns. Reading
// them earlier on a streaming RPC races the transport's dispatch goroutine.
type ClientInfo struct {
	request  *http.Request
	response *http.Response
}

// ClientInfoForContext returns the [*ClientInfo] carried on the client-side
// [connect.CallInfo]'s TransportInfo, if there is one. It reports false when
// the RPC is not dispatched over HTTP. The transport sets it when it opens
// the stream. To read it after a call returns, attach the CallInfo with
// [connect.NewClientContext] first.
func ClientInfoForContext(ctx context.Context) (*ClientInfo, bool) {
	callInfo, ok := connect.CallInfoForClientContext(ctx)
	if !ok {
		return nil, false
	}
	info, ok := callInfo.TransportInfo.(*ClientInfo)
	return info, ok
}

// HTTPMethod is the HTTP method of the dispatched request, "POST" by
// default, "GET" when [WithHTTPGet] upgrades a side-effect-free unary call.
func (c *ClientInfo) HTTPMethod() string {
	if c == nil || c.request == nil {
		return ""
	}
	return c.request.Method
}

// RequestURL is the URL the transport dispatched to. Returned by
// value, so callers may inspect or modify their copy without
// affecting the live request.
func (c *ClientInfo) RequestURL() url.URL {
	if c == nil || c.request == nil || c.request.URL == nil {
		return url.URL{}
	}
	return *c.request.URL
}

// ResponseStatus is the HTTP status code returned by the server. Zero
// before the response arrives.
func (c *ClientInfo) ResponseStatus() int {
	if c == nil || c.response == nil {
		return 0
	}
	return c.response.StatusCode
}

// ResponseProto is the HTTP protocol version reported on the
// response, e.g. "HTTP/2.0". Empty before the response arrives.
func (c *ClientInfo) ResponseProto() string {
	if c == nil || c.response == nil {
		return ""
	}
	return c.response.Proto
}

// TLS is the TLS connection state for the response, or nil if the
// transport was plain HTTP or the response has not yet arrived.
func (c *ClientInfo) TLS() *tls.ConnectionState {
	if c == nil || c.response == nil {
		return nil
	}
	return c.response.TLS
}

// ServerInfo is the HTTP metadata for a server RPC, carried on
// [connect.CallInfo.TransportInfo].
type ServerInfo struct {
	request *http.Request
}

// ServerInfoForContext returns the [*ServerInfo] carried on the server-side
// [connect.CallInfo]'s TransportInfo, if there is one. It reports false when
// the RPC is not served over HTTP.
func ServerInfoForContext(ctx context.Context) (*ServerInfo, bool) {
	callInfo, ok := connect.CallInfoForServerContext(ctx)
	if !ok {
		return nil, false
	}
	info, ok := callInfo.TransportInfo.(*ServerInfo)
	return info, ok
}

// HTTPMethod is the HTTP method of the incoming request.
func (s *ServerInfo) HTTPMethod() string {
	if s == nil || s.request == nil {
		return ""
	}
	return s.request.Method
}

// RequestURL is the URL of the incoming request as net/http parsed
// it (request-URI form, not absolute). Returned by value.
func (s *ServerInfo) RequestURL() url.URL {
	if s == nil || s.request == nil || s.request.URL == nil {
		return url.URL{}
	}
	return *s.request.URL
}

// RequestProto is the HTTP protocol version reported on the request,
// e.g. "HTTP/1.1" or "HTTP/2.0".
func (s *ServerInfo) RequestProto() string {
	if s == nil || s.request == nil {
		return ""
	}
	return s.request.Proto
}

// TLS is the TLS connection state for the incoming connection, or
// nil for plain HTTP.
func (s *ServerInfo) TLS() *tls.ConnectionState {
	if s == nil || s.request == nil {
		return nil
	}
	return s.request.TLS
}

// HTTPGetQueryParams returns the raw HTTP Get URL parameters.
func (s *ServerInfo) HTTPGetQueryParams() url.Values {
	if s == nil || s.request == nil || s.request.Method != http.MethodGet {
		return nil
	}
	return s.request.URL.Query()
}

// ServeMux is the subset of [http.ServeMux] used by [Mount].
// Any router that satisfies this interface can host Connect routes.
type ServeMux interface {
	// Handle registers handler for pattern.
	Handle(pattern string, handler http.Handler)
}

// encodingOrIdentity normalizes an empty compression name to "identity".
func encodingOrIdentity(name string) string {
	if name == "" {
		return connect.CompressionNameIdentity
	}
	return name
}

// NewTransport returns a [connect.Transport] that dispatches RPCs over
// httpClient against baseURL. Pass the returned transport to
// [connect.NewClient] before constructing generated service clients.
func NewTransport(httpClient HTTPClient, baseURL string, options ...Option) connect.Transport {
	// One endpoint serves both dispatch paths, so the caller configures one
	// URL; a WebSocket spelling is normalized for the HTTP path here and
	// rewritten back for the dial.
	parsed, err := url.Parse(normalizeBaseURL(baseURL))
	if err != nil {
		parsed = nil
	}
	opts := defaultOptions()
	for _, opt := range options {
		opt.apply(&opts)
	}
	return &transport{
		httpClient: httpClient,
		baseURLPtr: parsed,
		baseURLErr: err,
		options:    opts,
	}
}

// defaultOptions returns the options with the defaults every transport and
// server starts from before user options are applied.
func defaultOptions() options {
	return options{
		protocol:        connect.ProtocolNameConnect,
		codecs:          defaultCodecs(),
		sendCodecName:   connect.CodecNameProto,
		compressors:     defaultCompressors(),
		compressorNames: []string{connect.CompressionNameGzip},
		getUseFallback:  true,
	}
}

// defaultCodecs returns the proto and JSON codecs every transport and handler
// registers by default.
func defaultCodecs() map[string]connect.Codec {
	return map[string]connect.Codec{
		connect.CodecNameProto: connectproto.NewBinaryCodec(),
		connect.CodecNameJSON:  connectproto.NewJSONCodec(),
	}
}

// defaultCompressors returns the gzip compressor registered by default.
func defaultCompressors() map[string]connect.Compressor {
	return map[string]connect.Compressor{
		connect.CompressionNameGzip: connectgzip.New(),
	}
}

// Mount registers an [http.Handler] for each of server's procedures on mux,
// plus a per-service catch-all that routes RPC requests for unknown methods
// through [connect.Server.Call]. Non-RPC requests get a plain 404, and
// requests for unknown services fall through to mux.
func Mount(mux ServeMux, server *connect.Server, options ...Option) {
	opts := defaultOptions()
	for _, opt := range options {
		opt.apply(&opts)
	}
	services := make(map[string]struct{})
	for spec := range server.Specs() {
		// Draft 5 has no path of its own: the procedure URL answers POST
		// with ordinary Connect HTTP and GET+Upgrade with the WebSocket
		// protocol.
		procedure := withWebSocket(server, spec, &opts, newProcedureHandler(server, spec, &opts))
		mux.Handle(spec.Procedure, procedure)
		// "/package.Service/Method" -> subtree pattern "/package.Service/".
		if idx := strings.LastIndexByte(spec.Procedure, '/'); idx > 0 {
			services[spec.Procedure[:idx+1]] = struct{}{}
		}
	}
	for service := range services {
		mux.Handle(service, newUnknownMethodHandler(server, &opts))
	}
}

// newUnknownMethodHandler routes RPC-shaped POST requests through
// [connect.Server.Call] and 404s everything else. Connect GET requests are
// not dispatched: an unknown method's idempotency is unknown.
func newUnknownMethodHandler(server *connect.Server, opts *options) http.Handler {
	classifier := &ErrorWriter{
		protobuf:                     newReadOnlyCodecs(opts.codecs).Protobuf(),
		requireConnectProtocolHeader: opts.requireConnectProtocolHeader,
	}
	return http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.NotFound(responseWriter, request)
			return
		}
		streamType := connect.StreamTypeBidi
		switch classifier.classifyRequest(request) {
		case unknownProtocol:
			http.NotFound(responseWriter, request)
			return
		case connectUnaryProtocol:
			// Connect unary framing differs; other protocols use one framing
			// for every stream shape.
			streamType = connect.StreamTypeUnary
		case connectStreamProtocol, grpcProtocol, grpcWebProtocol:
		}
		spec := connect.Spec{
			StreamType: streamType,
			Procedure:  request.URL.Path,
		}
		// Cold path: build the handler per request to reuse the
		// registered-procedure machinery.
		newProcedureHandler(server, spec, opts).ServeHTTP(responseWriter, request)
	})
}

type transport struct {
	httpClient HTTPClient
	options    options

	baseURLPtr *url.URL
	baseURLErr error

	procedureURLs sync.Map // procedure string -> *url.URL
}

// NewClientStream implements [connect.Transport].
func (t *transport) NewClientStream(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
	info, ok := connect.CallInfoForClientContext(ctx)
	if !ok {
		ctx, info = connect.NewClientContext(ctx)
	}
	clientInfo := &ClientInfo{}
	info.TransportInfo = clientInfo
	opts := t.options.forSpec(spec)
	if opts.sendCompressor != "" && opts.sendCompressor != connect.CompressionNameIdentity {
		if _, ok := opts.compressors[opts.sendCompressor]; !ok {
			return nil, connect.Errorf(connect.CodeUnknown, "unknown compression %q", opts.sendCompressor)
		}
	}
	if _, ok := opts.codecs[opts.sendCodecName]; !ok {
		return nil, connect.Errorf(connect.CodeUnknown, "unknown codec %q", opts.sendCodecName)
	}
	if t.baseURLErr != nil {
		return nil, connect.Errorf(connect.CodeUnavailable, "invalid base URL: %v", t.baseURLErr)
	}
	// Streaming RPCs upgrade; unary RPCs stay on ordinary Connect HTTP.
	// Only the Connect protocol has a WebSocket mapping — WithGRPC and
	// WithGRPCWeb keep every RPC on the HTTP path.
	if spec.StreamType != connect.StreamTypeUnary && opts.protocol == connect.ProtocolNameConnect {
		return t.newWebSocketStream(ctx, spec, opts, info)
	}
	protocolClient, err := t.newProtocolClient(spec, opts)
	if err != nil {
		return nil, err
	}
	conn := protocolClient.NewConn(ctx, spec, make(http.Header, 8))
	peer := conn.Peer()
	info.Spec = spec
	info.PeerAddr = peer.Addr
	info.Protocol = peer.Protocol
	info.Codec = opts.sendCodecName
	info.RequestEncoding = encodingOrIdentity(opts.sendCompressor)
	conn.onRequestSend(func(request *http.Request) {
		clientInfo.request = request
	})
	conn.onResponseReceive(func(response *http.Response) {
		clientInfo.response = response
	})
	if spec.StreamType == connect.StreamTypeUnary {
		return &connectUnaryClientStream{conn: conn, info: info, protoClient: protocolClient, streamType: spec.StreamType}, nil
	}
	return &connectStreamingClientStream{conn: conn, info: info, protoClient: protocolClient, streamType: spec.StreamType}, nil
}

// urlForProcedure returns a cached *url.URL for the given procedure path,
// rooted at the transport's baseURL. The returned URL must not be mutated
// because it is shared across concurrent dispatches.
func (t *transport) urlForProcedure(procedure string) *url.URL {
	if v, ok := t.procedureURLs.Load(procedure); ok {
		return v.(*url.URL) //nolint:forcetypeassert // map only stores *url.URL
	}
	u := *t.baseURLPtr
	u.Path = joinURLPath(t.baseURLPtr.Path, procedure)
	actual, _ := t.procedureURLs.LoadOrStore(procedure, &u)
	return actual.(*url.URL) //nolint:forcetypeassert // map only stores *url.URL
}

// options holds the resolved options for a transport ([NewTransport]) or server
// ([Mount]). A single [Option] list builds it up; each entry point reads the
// fields relevant to it and ignores the rest.
type options struct {
	// Shared by clients and servers.
	codecs           map[string]connect.Codec
	compressors      map[string]connect.Compressor
	compressorNames  []string
	compressMinBytes int
	readMaxBytes     int
	sendMaxBytes     int

	// Client-only ([Mount] ignores these).
	protocol       string
	sendCodecName  string
	sendCompressor string
	getEnabled     bool
	getMaxURLBytes int
	getUseFallback bool

	// Server-only ([NewTransport] ignores these).
	requireConnectProtocolHeader bool

	// WebSocket settings, used by whichever side upgrades. The HTTP
	// dispatch path ignores them.
	webSocketDialOptions   *websocket.DialOptions
	webSocketAcceptOptions *websocket.AcceptOptions
	withoutCompression     bool
	h2                     *http2.Transport

	// conditional holds per-procedure option functions registered with
	// WithConditionalOptions. They are evaluated against each spec by forSpec.
	conditional []func(connect.Spec) []Option
}

// forSpec returns the options to apply for spec. When no conditional options are
// registered it returns the receiver unchanged; otherwise it clones the options
// and applies the options each conditional returns for spec.
func (o *options) forSpec(spec connect.Spec) *options {
	if len(o.conditional) == 0 {
		return o
	}
	clone := o.clone()
	for _, conditional := range o.conditional {
		for _, opt := range conditional(spec) {
			opt.apply(clone)
		}
	}
	return clone
}

// clone returns a deep-enough copy of c that applying options to the copy does
// not mutate the receiver's maps and slices. The conditional list is dropped to
// avoid re-evaluating it.
func (o *options) clone() *options {
	opts := *o
	opts.codecs = maps.Clone(o.codecs)
	opts.compressors = maps.Clone(o.compressors)
	opts.compressorNames = slices.Clone(o.compressorNames)
	opts.conditional = nil
	return &opts
}

// joinURLPath joins a base path and a procedure path with exactly one
// '/' between them. Connect procedures always begin with '/'.
func joinURLPath(base, procedure string) string {
	switch {
	case base == "":
		return procedure
	case base[len(base)-1] == '/' && len(procedure) > 0 && procedure[0] == '/':
		return base + procedure[1:]
	default:
		return base + procedure
	}
}
