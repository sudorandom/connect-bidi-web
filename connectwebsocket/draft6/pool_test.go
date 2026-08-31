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
	"sync"
	"testing"

	"connectrpc.com/connect/v2"
)

// The hazard every connection pool has: nobody reads a connection while it
// is idle, so a peer that hangs up in the meantime goes unnoticed until the
// next RPC writes to it. That write has to be retried on a fresh connection
// rather than failing a call that never reached the server.
//
// Driving that through a real server is awkward — there is no handle to the
// pooled socket from the outside — so these tests stub the connection.

// stubConn is a messageConn whose behaviour the test dictates.
type stubConn struct {
	mu sync.Mutex
	// writeErr, when set, fails every write: a connection the peer closed
	// while it sat in the pool.
	writeErr error
	writes   [][]byte
	closed   bool
}

func (c *stubConn) ReadMessage(context.Context) ([]byte, bool, error) {
	return nil, false, errors.New("stubConn: no reads scripted")
}

func (c *stubConn) WriteMessage(_ context.Context, data []byte, _ bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writeErr != nil {
		return c.writeErr
	}
	c.writes = append(c.writes, append([]byte(nil), data...))
	return nil
}

func (c *stubConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *stubConn) CloseNow() error { return c.Close() }

func (c *stubConn) writeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.writes)
}

func (c *stubConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func newTestTransport(t *testing.T, dialled ...*stubConn) (*transport, *int) {
	t.Helper()
	opts := transportOptions{
		protocolOptions: newClientProtocolOptions(),
		maxIdleConns:    DefaultMaxIdleConns,
	}
	opts.finalize()
	dials := 0
	trans := &transport{url: "ws://example.invalid/ws", opts: opts}
	trans.dial = func(context.Context) (messageConn, error) {
		if dials >= len(dialled) {
			return nil, errors.New("no more connections scripted")
		}
		conn := dialled[dials]
		dials++
		return conn, nil
	}
	return trans, &dials
}

// TestStalePooledConnectionIsRetried: the first write on a connection taken
// from the pool fails, and the RPC recovers on a fresh one instead of
// surfacing an error the server never saw.
func TestStalePooledConnectionIsRetried(t *testing.T) {
	t.Parallel()
	stale := &stubConn{writeErr: errors.New("broken pipe")}
	fresh := &stubConn{}
	trans, dials := newTestTransport(t, fresh)
	// A connection the peer closed while it sat idle.
	trans.release(stale)

	spec := connect.Spec{StreamType: connect.StreamTypeBidi, Procedure: "/pkg.Service/Method"}
	stream, err := trans.NewClientStream(context.Background(), spec)
	if err != nil {
		t.Fatalf("NewClientStream: %v", err)
	}
	if err := stream.SendHeaders(); err != nil {
		t.Fatalf("SendHeaders should have retried on a fresh connection, got %v", err)
	}

	if *dials != 1 {
		t.Errorf("dialled %d times, want 1", *dials)
	}
	if !stale.isClosed() {
		t.Error("the dead connection was not torn down")
	}
	if got := fresh.writeCount(); got != 1 {
		t.Errorf("fresh connection received %d messages, want the headers message", got)
	}
}

// TestFreshConnectionFailureIsNotRetried: a write that fails on a
// connection this RPC just dialled is a real failure. Retrying it would
// loop, and there is nothing to recover from.
func TestFreshConnectionFailureIsNotRetried(t *testing.T) {
	t.Parallel()
	broken := &stubConn{writeErr: errors.New("broken pipe")}
	trans, dials := newTestTransport(t, broken)

	spec := connect.Spec{StreamType: connect.StreamTypeBidi, Procedure: "/pkg.Service/Method"}
	stream, err := trans.NewClientStream(context.Background(), spec)
	if err != nil {
		t.Fatalf("NewClientStream: %v", err)
	}
	if err := stream.SendHeaders(); err == nil {
		t.Fatal("a failure on a freshly dialled connection should surface")
	}
	if *dials != 1 {
		t.Errorf("dialled %d times, want 1 — a fresh connection must not be retried", *dials)
	}
}

// TestPoolIsBounded: idle connections beyond the cap are closed rather than
// kept, so an idle client does not sit on sockets forever.
func TestPoolIsBounded(t *testing.T) {
	t.Parallel()
	trans, _ := newTestTransport(t)
	trans.opts.maxIdleConns = 2

	kept := []*stubConn{{}, {}}
	overflow := &stubConn{}
	for _, conn := range kept {
		trans.release(conn)
	}
	trans.release(overflow)

	if !overflow.isClosed() {
		t.Error("a connection past the idle cap should be closed, not pooled")
	}
	for i, conn := range kept {
		if conn.isClosed() {
			t.Errorf("pooled connection %d was closed", i)
		}
	}
	if got := len(trans.idle); got != 2 {
		t.Errorf("pool holds %d connections, want 2", got)
	}
}

// TestCloseDropsIdleConnections: Close is the transport's way of letting a
// process exit without waiting on sockets it is only holding for reuse.
func TestCloseDropsIdleConnections(t *testing.T) {
	t.Parallel()
	trans, _ := newTestTransport(t)
	conns := []*stubConn{{}, {}, {}}
	for _, conn := range conns {
		trans.release(conn)
	}
	if err := trans.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i, conn := range conns {
		if !conn.isClosed() {
			t.Errorf("idle connection %d survived Close", i)
		}
	}
	if got := len(trans.idle); got != 0 {
		t.Errorf("pool holds %d connections after Close, want 0", got)
	}
}
