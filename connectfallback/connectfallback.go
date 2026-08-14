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

// Package connectfallback provides a connect.Transport that degrades
// through a ladder of transports, ordered best-first, remembering the rung
// that works. The canonical ladder puts the most capable transport first
// and the most compatible one last:
//
//	transport := connectfallback.New(
//		connectwebtransport.NewDialTransport(url, nil),   // WebTransport (HTTP/3)
//		draft2.NewH2Transport(url, nil),                  // WebSocket over HTTP/2
//		draft2.NewTransport(url),                         // WebSocket over HTTP/1.1
//	)
//
// A rung is skipped when opening a stream on it fails with
// CodeUnavailable — the code every transport here returns when the
// transport itself cannot be established (an unreachable UDP path, a
// server without GODEBUG=http2xconnect=1, a refused upgrade). Any other
// error is an RPC-level failure and surfaces unchanged, without trying
// further rungs.
package connectfallback

import (
	"context"
	"errors"
	"io"
	"sync"

	"connectrpc.com/connect/v2"
)

// Transport is a connect.Transport that delegates each RPC to the first
// working rung of its ladder.
type Transport struct {
	rungs []connect.Transport

	mu      sync.Mutex
	current int
}

// New returns a Transport that tries rungs in the given order,
// best-first. At least one rung is required.
func New(rungs ...connect.Transport) *Transport {
	if len(rungs) == 0 {
		panic("connectfallback: at least one transport is required")
	}
	return &Transport{rungs: rungs}
}

// NewClientStream implements connect.Transport. It opens the stream on the
// remembered rung, degrading to the rungs below it — and, if the bottom is
// reached, wrapping around to the rungs above, so a transport that
// recovered (or a network that changed) is eventually retried — whenever a
// rung fails with CodeUnavailable. The rung that succeeds is remembered
// for subsequent RPCs.
func (t *Transport) NewClientStream(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
	t.mu.Lock()
	start := t.current
	t.mu.Unlock()

	var lastErr error
	for attempt := range len(t.rungs) {
		index := (start + attempt) % len(t.rungs)
		stream, err := t.rungs[index].NewClientStream(ctx, spec)
		if err == nil {
			t.mu.Lock()
			t.current = index
			t.mu.Unlock()
			return stream, nil
		}
		if connect.CodeOf(err) != connect.CodeUnavailable {
			// An RPC-level failure, not a transport-establishment one:
			// surface it rather than replaying the RPC elsewhere.
			return nil, err
		}
		if ctx.Err() != nil {
			// The caller gave up; transports wrap that as CodeUnavailable,
			// but burning the remaining rungs on a dead context is useless.
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

// Close closes every rung that implements io.Closer, terminating any RPCs
// still running on them. The transport remains usable afterwards.
func (t *Transport) Close() error {
	var errs []error
	for _, rung := range t.rungs {
		if closer, ok := rung.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}
