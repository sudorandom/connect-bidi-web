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

package draft2

import (
	"context"
	"log/slog"
	"net/http"
	"sync"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
)

// Handler serves Connect RPCs over WebSocket connections using draft 2 of
// the wire protocol. Each accepted connection carries any number of
// concurrent RPCs, demultiplexed by the stream ID on every frame.
type Handler struct {
	server *connect.Server
	opts   serverOptions
}

// NewHandler creates a new Handler for serving Connect RPCs over WebSockets
// using draft 2 of the wire protocol.
func NewHandler(server *connect.Server, opts ...Option) *Handler {
	sOpts := serverOptions{protocolOptions: newServerProtocolOptions()}
	for _, opt := range opts {
		opt.applyServer(&sOpts)
	}
	sOpts.finalize()

	return &Handler{
		server: server,
		opts:   sOpts,
	}
}

// ServeHTTP implements http.Handler by turning the request into a
// WebSocket connection and serving RPCs on it until the client
// disconnects. Two bootstraps are supported: an HTTP/1.1 Upgrade
// handshake, and an RFC 8441 extended CONNECT stream on HTTP/2 (which
// reaches handlers only when the server runs with
// GODEBUG=http2xconnect=1). The wire protocol is identical on both. A
// frame with an unknown stream ID and a headers type starts a new RPC; a
// reset frame cancels an in-flight one.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isExtendedConnectWebSocket(r) {
		h.serveH2(w, r)
		return
	}
	acceptOpts := &websocket.AcceptOptions{}
	if h.opts.acceptOptions != nil {
		cloned := *h.opts.acceptOptions
		acceptOpts = &cloned
	}
	// The protocol has no per-message compression, so the WebSocket's own —
	// permessage-deflate — is protocol-owned rather than a caller knob:
	// custom accept options never silently disable it.
	acceptOpts.CompressionMode = websocket.CompressionNoContextTakeover
	if h.opts.withoutCompression {
		acceptOpts.CompressionMode = websocket.CompressionDisabled
	}
	conn, err := websocket.Accept(w, r, acceptOpts)
	if err != nil {
		return
	}
	h.serveConn(r.Context(), newCoderConn(conn), r.RemoteAddr)
}

// serveConn runs the multiplexed read loop on one accepted connection,
// whatever its bootstrap, until the client disconnects.
func (h *Handler) serveConn(ctx context.Context, conn messageConn, remoteAddr string) {
	mc := newMuxConn(ctx, conn)
	var handlers sync.WaitGroup
	for {
		streamID, frameType, payload, err := readFrame(ctx, conn)
		if err != nil {
			slog.DebugContext(ctx, "websocket server: read ended", "error", err)
			break
		}
		stream := mc.lookup(streamID)
		if stream == nil {
			if frameType != frameTypeHeaders {
				// A frame for a stream that already finished; drop it.
				continue
			}
			h.startStream(ctx, mc, streamID, payload, &handlers, remoteAddr)
			continue
		}
		if frameType == frameTypeReset {
			mc.deregister(streamID)
			stream.terminate(context.Canceled)
			continue
		}
		stream.deliver(frameType, payload)
	}
	mc.terminateAll(errConnClosed)
	handlers.Wait()
	_ = conn.Close()
}

// startStream registers a new stream and dispatches its RPC on its own
// goroutine, seeded with the headers frame that opened it.
func (h *Handler) startStream(
	ctx context.Context,
	mc *muxConn,
	streamID uint32,
	headersPayload []byte,
	handlers *sync.WaitGroup,
	remoteAddr string,
) {
	streamCtx, cancel := context.WithCancel(ctx)
	stream := newMuxStream(streamCtx, mc, streamID)
	stream.cancel = cancel
	// The inbox always has room for the headers frame on a fresh stream.
	stream.deliver(frameTypeHeaders, headersPayload)
	if !mc.register(stream) {
		cancel()
		return
	}
	handlers.Add(1)
	go func() {
		defer handlers.Done()
		defer mc.deregister(streamID)
		// Terminating unblocks the read loop if it is delivering a frame to
		// this stream, and cancels streamCtx.
		defer stream.terminate(errStreamClosed)
		handleRPC(streamCtx, stream, h.server, h.opts.protocolOptions, remoteAddr)
	}()
}
