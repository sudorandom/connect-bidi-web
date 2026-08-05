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
	"net/http"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/internal/bidiprotocol"
)

// Handler serves Connect RPCs over WebSocket connections. Each accepted
// connection carries exactly one RPC.
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
// connection and serving a single RPC on it.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, h.opts.acceptOptions)
	if err != nil {
		return
	}
	defer func() {
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}()
	conn.SetReadLimit(-1)

	rwc := websocket.NetConn(r.Context(), conn, websocket.MessageBinary)
	bidiprotocol.HandleRPC(
		r.Context(),
		bidiprotocol.NewNetConn(rwc),
		h.server,
		h.opts.Options,
		"websocket",
		r.RemoteAddr,
	)
}
