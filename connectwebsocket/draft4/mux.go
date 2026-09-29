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

package draft4

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"
	"sync/atomic"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
)

// Wire framing: every WebSocket message carries exactly one frame belonging
// to exactly one stream, behind an ASCII text head:
//
//	<stream ID>|<flags>|<payload>
//
// Both fields are unpadded decimal ASCII, so a frame reads as
// "7|1|{"metadata":...}" in a packet capture or a browser's network
// inspector. Parsing splits on the first two '|' bytes only, which is why
// the payload needs no escaping: a '|' inside JSON or protobuf bytes is
// just a payload byte. The payload is delimited by the WebSocket message
// itself, so frames carry no length of their own.
//
// The stream ID lets several RPCs share one connection: receivers use it to
// match each frame to the appropriate caller. IDs are assigned by the
// client, start at 1, increase by one per stream, and are never reused
// within a connection.

// fieldSep separates the frame head's fields, and the head from the
// payload.
const fieldSep = '|'

// maxHeadLen bounds how far the parser scans for the two separators: ten
// digits of uint32 stream ID, three digits of uint8 flags, and the two
// separators. A frame whose separators are further out than this is
// malformed, and bounding the scan keeps a hostile peer from making us
// search a whole large message for a separator that isn't there.
//
// The window has to fit the whole uint8 flags range, not just the four
// frame types defined today: a reserved type must parse so the stream code
// can reject it as an unknown frame type rather than as a broken frame.
const maxHeadLen = 10 + 1 + 3 + 1

var (
	errConnClosed   = errors.New("websocket connection closed")
	errStreamClosed = errors.New("websocket stream closed")
)

// appendFrameHead appends "<streamID>|<flags>|" to dst. flags is the whole
// second field: a frame type in its low bits, ORed with any flag bits.
func appendFrameHead(dst []byte, streamID uint32, flags uint8) []byte {
	dst = strconv.AppendUint(dst, uint64(streamID), 10)
	dst = append(dst, fieldSep)
	dst = strconv.AppendUint(dst, uint64(flags), 10)
	return append(dst, fieldSep)
}

// writeFrame writes one WebSocket message carrying one frame for one
// stream. Callers must serialize calls (muxConn.writeFrame does). The frame
// is assembled into one buffer and sent as one message: coder/websocket
// decides whether permessage-deflate applies to a message by the size of
// the first write, so writing the head separately would leave every message
// uncompressed.
//
// text sends the message with a text opcode rather than a binary one, which
// is what makes draft 4 frames legible to tooling. Only a caller that knows
// the payload is valid UTF-8 may set it.
func writeFrame(ctx context.Context, conn messageConn, streamID uint32, flags uint8, payload []byte, text bool) error {
	frame := make([]byte, 0, maxHeadLen+len(payload))
	frame = appendFrameHead(frame, streamID, flags)
	frame = append(frame, payload...)
	return conn.WriteMessage(ctx, frame, text)
}

// parseFrame splits one WebSocket message into stream ID, flags, and
// payload. flags is returned whole, flag bits included; callers apply
// frameTypeOf to get the frame type. The payload aliases data.
func parseFrame(data []byte) (streamID uint32, flags uint8, payload []byte, err error) {
	head := data
	if len(head) > maxHeadLen {
		head = head[:maxHeadLen]
	}
	firstSep := bytes.IndexByte(head, fieldSep)
	if firstSep < 0 {
		return 0, 0, nil, fmt.Errorf("frame head has no %q separator in its first %d bytes", fieldSep, len(head))
	}
	rest := head[firstSep+1:]
	secondSep := bytes.IndexByte(rest, fieldSep)
	if secondSep < 0 {
		return 0, 0, nil, fmt.Errorf("frame head has only one %q separator in its first %d bytes", fieldSep, len(head))
	}

	id, err := strconv.ParseUint(string(head[:firstSep]), 10, 32)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("invalid stream ID %q: %w", head[:firstSep], err)
	}
	parsedFlags, err := strconv.ParseUint(string(rest[:secondSep]), 10, 8)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("invalid flags %q: %w", rest[:secondSep], err)
	}
	return uint32(id), uint8(parsedFlags), data[firstSep+1+secondSep+1:], nil
}

// readFrame reads one WebSocket message and splits it into stream ID,
// flags, and payload.
func readFrame(ctx context.Context, conn messageConn) (streamID uint32, flags uint8, payload []byte, err error) {
	data, err := conn.ReadMessage(ctx)
	if err != nil {
		return 0, 0, nil, err
	}
	return parseFrame(data)
}

// muxConn multiplexes streams onto a single WebSocket connection. Both the
// client transport and the server handler use it: a single read loop routes
// each incoming frame to the inbox of the stream it belongs to, and outgoing
// frames from all streams are serialized onto the connection.
type muxConn struct {
	conn messageConn
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

func newMuxConn(writeCtx context.Context, conn messageConn) *muxConn {
	return &muxConn{
		conn:     conn,
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

func (mc *muxConn) writeFrame(streamID uint32, flags uint8, payload []byte, text bool) error {
	mc.writeMu.Lock()
	defer mc.writeMu.Unlock()
	return writeFrame(mc.writeCtx, mc.conn, streamID, flags, payload, text)
}

// readLoopClient routes incoming frames to the client streams that opened
// them until the connection fails or is closed. Frames for unknown streams
// (already closed on this side) are dropped.
func (mc *muxConn) readLoopClient(ctx context.Context) {
	for {
		streamID, flags, payload, err := readFrame(ctx, mc.conn)
		if err != nil {
			mc.terminateAll(connReadError(err))
			_ = mc.conn.CloseNow() //nolint:contextcheck // CloseNow takes no context by design
			return
		}
		stream := mc.lookup(streamID)
		if stream == nil {
			continue
		}
		if frameTypeOf(flags) == frameTypeReset {
			mc.deregister(streamID)
			stream.terminate(connect.Errorf(connect.CodeCanceled, "stream reset by peer"))
			continue
		}
		stream.deliver(flags, payload)
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

// frame is one frame routed to a stream's inbox. The flags field is kept
// whole rather than pre-masked, so a flag defined by a later revision
// reaches the stream code instead of being dropped here.
type frame struct {
	flags   uint8
	payload []byte
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
func (s *muxStream) deliver(flags uint8, payload []byte) {
	select {
	case s.inbox <- frame{flags: flags, payload: payload}:
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
	if frameTypeOf(f.flags) == frameTypeEndStream {
		s.rxEnd.Store(true)
	}
	return f.flags, f.payload, nil
}

// WriteFrame implements frameConn. Stream cancellation is checked before
// the write; the write itself runs under the connection's context so a
// cancellation can never poison the shared connection mid-frame.
func (s *muxStream) WriteFrame(flags uint8, payload []byte, text bool) error {
	select {
	case <-s.terminated:
		return s.terminalErr
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}
	return s.mc.writeFrame(s.id, flags, payload, text)
}

// CloseSend implements frameConn by writing an explicit end-stream frame: a
// WebSocket has no per-stream half-close of its own.
func (s *muxStream) CloseSend() error {
	return s.WriteFrame(frameTypeEndStream, nil, true)
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
		_ = s.mc.writeFrame(s.id, frameTypeReset, nil, true)
	}
	return nil
}
