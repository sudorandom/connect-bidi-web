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
	"sync"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/quic-go/webtransport-go"
)

// dialTimeout bounds the session handshake when the RPC context has no
// sooner deadline. On a network that silently drops UDP — the most common
// way WebTransport is unavailable — a QUIC handshake would otherwise hang
// for the RPC's whole lifetime, and callers arranging fallback (such as
// connectfallback) need a prompt failure to degrade on.
const dialTimeout = 10 * time.Second

// dialTransport lazily dials a WebTransport session on first use and
// re-dials it after it dies, delegating RPCs to a NewTransport bound to
// the live session.
type dialTransport struct {
	url    string
	dialer *webtransport.Transport
	opts   []Option

	mu       sync.Mutex
	inner    connect.Transport
	session  *webtransport.Session
	dialResp *http.Response
}

// NewDialTransport returns a connect.Transport that dials the WebTransport
// session itself: lazily on the first RPC, and again if the session dies.
// This differs from NewTransport, which is bound to one caller-provided
// session for its lifetime.
//
// Session establishment failures — including a handshake that a
// UDP-hostile network leaves hanging, capped at 10 seconds — surface as
// CodeUnavailable, so a fallback arrangement can degrade to a WebSocket
// transport. dialer configures the QUIC and TLS setup; nil is equivalent
// to a zero webtransport.Transport.
//
// The returned Transport also implements io.Closer: Close closes the
// current session, terminating any RPCs still running on it. The
// transport remains usable afterwards; the next RPC dials a new session.
func NewDialTransport(url string, dialer *webtransport.Transport, opts ...Option) connect.Transport {
	if dialer == nil {
		dialer = &webtransport.Transport{}
	}
	return &dialTransport{
		url:    url,
		dialer: dialer,
		opts:   opts,
	}
}

func (t *dialTransport) NewClientStream(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
	inner, err := t.transport(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := inner.NewClientStream(ctx, spec)
	if err != nil {
		// Opening a stream on an established session fails only when the
		// session has died underneath us; the next RPC re-dials.
		return nil, connect.Errorf(connect.CodeUnavailable, "failed to open WebTransport stream: %v", err)
	}
	return stream, nil
}

// Close closes the current session, if one is open, terminating any RPCs
// still running on it. The next RPC dials a new session.
func (t *dialTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closeSessionLocked()
	return nil
}

// transport returns a Transport bound to a live session, dialing one if
// there is none or the previous one died.
func (t *dialTransport) transport(ctx context.Context) (connect.Transport, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.session != nil && t.session.Context().Err() == nil {
		return t.inner, nil
	}
	t.closeSessionLocked()

	// The dial context bounds only the handshake; the session outlives it.
	dialCtx := ctx
	if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > dialTimeout {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, dialTimeout)
		defer cancel()
	}
	resp, session, err := t.dialer.Dial(dialCtx, t.url, nil) //nolint:bodyclose // the response is the session's CONNECT stream; closeSessionLocked closes it
	if err != nil {
		return nil, connect.Errorf(connect.CodeUnavailable, "failed to dial WebTransport session: %v", err)
	}
	t.session = session
	t.dialResp = resp
	t.inner = NewTransport(session, t.opts...)
	return t.inner, nil
}

func (t *dialTransport) closeSessionLocked() {
	if t.session != nil {
		_ = t.session.CloseWithError(0, "")
		t.session = nil
	}
	if t.dialResp != nil && t.dialResp.Body != nil {
		_ = t.dialResp.Body.Close()
		t.dialResp = nil
	}
	t.inner = nil
}
