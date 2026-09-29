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

package draft3

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
)

// Wire framing: every binary WebSocket message carries exactly one frame
// belonging to exactly one stream:
//
//	[4-byte big-endian stream ID][1-byte frame descriptor][payload]
//
// The descriptor is the frame type in bits 0-6 plus the frameCompressed
// flag in bit 7. The stream ID lets several RPCs share one connection:
// receivers use it to match each frame to the appropriate caller. IDs are
// assigned by the client, start at 1, increase by one per stream, and are
// never reused within a connection. The payload is delimited by the
// WebSocket message itself, so frames carry no length of their own.

const (
	streamIDLen  = 4
	frameHeadLen = streamIDLen + 1 // stream ID + frame descriptor
)

var (
	errConnClosed   = errors.New("websocket connection closed")
	errStreamClosed = errors.New("websocket stream closed")
)

// writeFrame writes one WebSocket message carrying one frame for one
// stream. Callers must serialize calls (muxConn.writeFrame does). The
// frame is assembled into one buffer and sent as one message. With
// compress set (subprotocolDeflate negotiated), payloads at or above
// compressMinBytes that actually shrink are deflated and marked with the
// frameCompressed bit; everything else is sent as-is.
func writeFrame(ctx context.Context, conn messageConn, streamID uint32, frameType uint8, payload []byte, compress bool) error {
	descriptor := frameType
	if compress && len(payload) >= compressMinBytes {
		if deflated, ok := deflatePayload(payload); ok {
			payload = deflated
			descriptor |= frameCompressed
		}
	}
	frame := make([]byte, frameHeadLen+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], streamID)
	frame[4] = descriptor
	copy(frame[frameHeadLen:], payload)
	return conn.WriteMessage(ctx, frame)
}

// readFrame reads one WebSocket message and splits it into stream ID, frame
// type, and payload, inflating a compressed payload. A frameCompressed bit
// on a connection that didn't negotiate compression is a protocol error.
func readFrame(ctx context.Context, conn messageConn, compress bool) (streamID uint32, frameType uint8, payload []byte, err error) {
	data, err := conn.ReadMessage(ctx)
	if err != nil {
		return 0, 0, nil, err
	}
	if len(data) < frameHeadLen {
		return 0, 0, nil, fmt.Errorf("frame too short: %d bytes", len(data))
	}
	streamID = binary.BigEndian.Uint32(data[0:4])
	descriptor := data[4]
	frameType = descriptor &^ frameCompressed
	payload = data[frameHeadLen:]
	if descriptor&frameCompressed != 0 {
		if !compress {
			return 0, 0, nil, fmt.Errorf("compressed frame on a connection that negotiated %q", subprotocolIdentity)
		}
		payload, err = inflatePayload(payload)
		if err != nil {
			return 0, 0, nil, err
		}
	}
	return streamID, frameType, payload, nil
}

// muxConn multiplexes streams onto a single WebSocket connection. Both the
// client transport and the server handler use it: a single read loop routes
// each incoming frame to the inbox of the stream it belongs to, and outgoing
// frames from all streams are serialized onto the connection.
type muxConn struct {
	conn messageConn
	// compress records the negotiated subprotocol's compression choice,
	// applied to both directions for the connection's lifetime.
	compress bool
	// writeCtx lives as long as the connection and is used for every write.
	// Stream or RPC contexts must never reach a write: coder/websocket
	// treats a context canceled mid-write as fatal to the connection (the
	// frame may be half-written), which would tear down every other stream.
	// Per-stream cancellation is checked before writing instead.
	writeCtx context.Context //nolint:containedctx // see above; the connection outlives any caller context

	writeMu sync.Mutex

	mu       sync.Mutex
	streams  map[uint32]*muxStream
	nextID   uint32
	closed   bool
	closeErr error
}

func newMuxConn(writeCtx context.Context, conn messageConn, compress bool) *muxConn {
	return &muxConn{
		conn:     conn,
		compress: compress,
		writeCtx: writeCtx,
		streams:  make(map[uint32]*muxStream),
	}
}

// newStream registers the next client-initiated stream. A dedicated stream
// owns the connection outright: closing the stream closes the connection.
func (mc *muxConn) newStream(ctx context.Context, dedicated bool) (*muxStream, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.closed {
		return nil, mc.closeErr
	}
	for i := 0; i < math.MaxUint32; i++ {
		mc.nextID++
		if mc.nextID == 0 {
			mc.nextID = 1
		}
		if _, exists := mc.streams[mc.nextID]; !exists {
			stream := newMuxStream(ctx, mc, mc.nextID)
			stream.dedicated = dedicated
			mc.streams[stream.id] = stream
			return stream, nil
		}
	}
	return nil, errors.New("stream ID space exhausted")
}

// register adds a peer-initiated stream (server side). It reports false if
// the connection is closed or the ID is already in use.
func (mc *muxConn) register(stream *muxStream) bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.closed {
		return false
	}
	if _, exists := mc.streams[stream.id]; exists {
		return false
	}
	mc.streams[stream.id] = stream
	return true
}

func (mc *muxConn) deregister(streamID uint32) {
	mc.mu.Lock()
	delete(mc.streams, streamID)
	mc.mu.Unlock()
}

func (mc *muxConn) lookup(streamID uint32) *muxStream {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.streams[streamID]
}

func (mc *muxConn) writeFrame(streamID uint32, frameType uint8, payload []byte) error {
	mc.writeMu.Lock()
	defer mc.writeMu.Unlock()
	return writeFrame(mc.writeCtx, mc.conn, streamID, frameType, payload, mc.compress)
}

// readLoopClient routes incoming frames to the client streams that opened
// them until the connection fails or is closed. Frames for unknown streams
// (already closed on this side) are dropped.
func (mc *muxConn) readLoopClient(ctx context.Context) {
	for {
		streamID, frameType, payload, err := readFrame(ctx, mc.conn, mc.compress)
		if err != nil {
			mc.terminateAll(connReadError(err))
			_ = mc.conn.CloseNow() //nolint:contextcheck // CloseNow takes no context by design
			return
		}
		stream := mc.lookup(streamID)
		if stream == nil {
			continue
		}
		if frameType == frameTypeReset {
			mc.deregister(streamID)
			stream.terminate(connect.Errorf(connect.CodeCanceled, "stream reset by peer"))
			continue
		}
		stream.deliver(frameType, payload)
	}
}

// terminateAll marks the connection closed and terminates every stream.
func (mc *muxConn) terminateAll(err error) {
	mc.mu.Lock()
	if !mc.closed {
		mc.closed = true
		mc.closeErr = err
	}
	streams := make([]*muxStream, 0, len(mc.streams))
	for _, stream := range mc.streams {
		streams = append(streams, stream)
	}
	clear(mc.streams)
	mc.mu.Unlock()
	for _, stream := range streams {
		stream.terminate(err)
	}
}

// shutdown terminates every stream and closes the connection gracefully.
func (mc *muxConn) shutdown() error {
	mc.terminateAll(errConnClosed)
	return mc.conn.Close()
}

// connReadError converts a read-loop failure into the error surfaced by the
// streams that were cut off by it.
func connReadError(err error) error {
	if errors.Is(err, io.EOF) {
		return io.EOF
	}
	switch status := websocket.CloseStatus(err); status {
	case -1:
		return connect.Errorf(connect.CodeUnavailable, "websocket read: %v", err)
	case websocket.StatusNormalClosure, websocket.StatusGoingAway:
		return io.EOF
	default:
		return connect.Errorf(connect.CodeUnavailable, "websocket closed with status %v", status)
	}
}

// frame is one frame routed to a stream's inbox.
type frame struct {
	frameType uint8
	payload   []byte
}

// muxStream is one logical stream on a muxConn, implementing frameConn. The
// connection's read loop delivers this stream's frames to inbox; writes go
// back through the shared connection with this stream's ID.
type muxStream struct {
	mc  *muxConn
	id  uint32
	ctx context.Context //nolint:containedctx // carries the RPC context into frameConn's context-free interface

	inbox chan frame

	terminateOnce sync.Once
	terminated    chan struct{}
	terminalErr   error

	// cancel aborts the server-side handler for this stream; nil on clients.
	cancel context.CancelFunc

	// dedicated marks a client stream that owns its connection outright.
	dedicated bool

	// rxEnd records that the peer finished this stream with an end-stream
	// frame, making a reset frame on close unnecessary.
	rxEnd atomic.Bool
}

var _ frameConn = (*muxStream)(nil)

func newMuxStream(ctx context.Context, mc *muxConn, id uint32) *muxStream {
	return &muxStream{
		mc:         mc,
		id:         id,
		ctx:        ctx,
		inbox:      make(chan frame, 64),
		terminated: make(chan struct{}),
	}
}

// deliver hands a frame to the stream's consumer. It blocks until the
// consumer accepts it, providing connection-wide backpressure, and drops the
// frame if the stream terminates first.
func (s *muxStream) deliver(frameType uint8, payload []byte) {
	select {
	case s.inbox <- frame{frameType: frameType, payload: payload}:
	case <-s.terminated:
	}
}

// terminate ends the stream with err as the terminal error for both
// directions, and cancels the server-side handler if there is one.
func (s *muxStream) terminate(err error) {
	s.terminateOnce.Do(func() {
		s.terminalErr = err
		close(s.terminated)
		if s.cancel != nil {
			s.cancel()
		}
	})
}

// ReadFrame implements frameConn.
func (s *muxStream) ReadFrame() (uint8, []byte, error) {
	// Prefer a frame delivered before termination, so an end-stream racing a
	// connection failure isn't lost.
	select {
	case f := <-s.inbox:
		return s.acceptFrame(f)
	default:
	}
	select {
	case f := <-s.inbox:
		return s.acceptFrame(f)
	case <-s.terminated:
		return 0, nil, s.terminalErr
	case <-s.ctx.Done():
		return 0, nil, s.ctx.Err()
	}
}

func (s *muxStream) acceptFrame(f frame) (uint8, []byte, error) {
	if f.frameType == frameTypeEndStream {
		s.rxEnd.Store(true)
	}
	return f.frameType, f.payload, nil
}

// WriteFrame implements frameConn. Stream cancellation is checked before
// the write; the write itself runs under the connection's context so a
// cancellation can never poison the shared connection mid-frame.
func (s *muxStream) WriteFrame(frameType uint8, payload []byte) error {
	select {
	case <-s.terminated:
		return s.terminalErr
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}
	return s.mc.writeFrame(s.id, frameType, payload)
}

// CloseSend implements frameConn by writing an explicit end-stream frame: a
// WebSocket has no per-stream half-close of its own.
func (s *muxStream) CloseSend() error {
	return s.WriteFrame(frameTypeEndStream, nil)
}

// Close implements frameConn, releasing the client side of the stream. If
// the peer hasn't finished the stream, a reset frame tells it to stop work;
// a dedicated connection is closed outright instead.
func (s *muxStream) Close() error {
	s.mc.deregister(s.id)
	finished := s.rxEnd.Load()
	s.terminate(errStreamClosed)
	if s.dedicated {
		return s.mc.conn.Close()
	}
	if !finished {
		_ = s.mc.writeFrame(s.id, frameTypeReset, nil)
	}
	return nil
}
