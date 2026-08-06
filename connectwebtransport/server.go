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

package connectwebtransport

import (
	"context"
	"net/http"

	"connectrpc.com/connect/v2"
	"github.com/quic-go/webtransport-go"
	"github.com/sudorandom/connect-bidi-web/internal/bidiprotocol"
)

// Handler serves Connect RPCs over WebTransport sessions. Each bidirectional
// stream accepted on a session carries exactly one RPC.
type Handler struct {
	server *connect.Server
	opts   serverOptions
}

// NewHandler returns a new Handler that serves RPCs over WebTransport.
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

// UpgradeHandler returns an http.Handler that upgrades WebTransport CONNECT
// requests via wtServer and serves Connect RPCs on the accepted session.
// Mount it on the path clients dial, on the mux served by wtServer.H3:
//
//	mux.Handle("/webtransport", handler.UpgradeHandler(wtServer))
//
// Unlike a WebSocket upgrade, a WebTransport upgrade cannot be done by an
// arbitrary http.Handler alone: the CONNECT request must be correlated with
// QUIC connection state that only the webtransport.Server owns, which is why
// it is a required argument. On upgrade failure the response has already
// been written by wtServer and the handler simply returns.
//
// Use HandleSession directly instead when sessions are accepted elsewhere.
func (h *Handler) UpgradeHandler(wtServer *webtransport.Server) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session, err := wtServer.Upgrade(w, r)
		if err != nil {
			return
		}
		h.HandleSession(r.Context(), session)
	})
}

// HandleSession accepts bidirectional streams on the session until the
// context is canceled or the session ends, serving one RPC per stream.
func (h *Handler) HandleSession(ctx context.Context, session *webtransport.Session) {
	for {
		stream, err := session.AcceptStream(ctx)
		if err != nil {
			// AcceptStream errors are terminal: the session is closed or the
			// context is done.
			return
		}
		go h.handleStream(ctx, session, stream)
	}
}

func (h *Handler) handleStream(ctx context.Context, session *webtransport.Session, stream *webtransport.Stream) {
	defer func() {
		_ = stream.Close()
		stream.CancelRead(0)
	}()
	bidiprotocol.HandleRPC(
		ctx,
		&streamConn{stream: stream},
		h.server,
		h.opts.Options,
		"webtransport",
		session.RemoteAddr().String(),
	)
}
