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

package connectwebsocket

import (
	"context"
	"log/slog"
	"net/http"
	"sync"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/internal/bidiprotocol"
)

// Handler serves Connect RPCs over WebSocket connections. Each accepted
// connection carries any number of concurrent RPCs, demultiplexed by the
// stream ID on every frame.
type Handler struct {
	server *connect.Server
	opts   serverOptions
}

// NewHandler creates a new Handler for serving Connect RPCs over WebSockets.
func NewHandler(server *connect.Server, opts ...Option) *Handler {
	sOpts := serverOptions{Options: bidiprotocol.NewServerOptions()}
	for _, opt := range opts {
		opt.applyServer(&sOpts)
	}
	sOpts.Finalize()

	return &Handler{
		server: server,
		opts:   sOpts,
	}
}

// ServeHTTP implements http.Handler by upgrading the request to a WebSocket
// connection and serving RPCs on it until the client disconnects. A frame
// with an unknown stream ID and a headers flag starts a new RPC; a reset
// frame cancels an in-flight one.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, h.opts.acceptOptions)
	if err != nil {
		return
	}
	conn.SetReadLimit(-1)

	ctx := r.Context()
	mc := newMuxConn(ctx, conn)
	var handlers sync.WaitGroup
	for {
		streamID, flag, payload, err := readFrame(ctx, conn)
		if err != nil {
			slog.DebugContext(ctx, "bidi server: websocket read ended", "error", err)
			break
		}
		stream := mc.lookup(streamID)
		if stream == nil {
			if flag != bidiprotocol.FlagEnvelopeHeaders {
				// A frame for a stream that already finished; drop it.
				continue
			}
			h.startStream(ctx, mc, streamID, payload, &handlers, r.RemoteAddr)
			continue
		}
		if flag == bidiprotocol.FlagEnvelopeReset {
			mc.deregister(streamID)
			stream.terminate(context.Canceled)
			continue
		}
		stream.deliver(flag, payload)
	}
	mc.terminateAll(errConnClosed)
	handlers.Wait()
	_ = conn.Close(websocket.StatusNormalClosure, "")
}

// startStream registers a new stream and dispatches its RPC on its own
// goroutine, seeded with the headers envelope that opened it.
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
	// The inbox always has room for the headers envelope on a fresh stream.
	stream.deliver(bidiprotocol.FlagEnvelopeHeaders, headersPayload)
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
		bidiprotocol.HandleRPC(streamCtx, stream, h.server, h.opts.Options, "websocket", remoteAddr)
	}()
}
