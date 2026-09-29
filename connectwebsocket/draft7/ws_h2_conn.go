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

package draft7

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/coder/websocket"
	"github.com/gobwas/ws"
)

// h2Conn carries WebSocket messages over an RFC 8441 extended-CONNECT
// HTTP/2 stream. There is no Upgrade handshake and no Sec-WebSocket-Key,
// but RFC 6455 message framing still applies on the stream: browsers send
// real — masked, possibly fragmented — WebSocket frames inside DATA
// frames. gobwas/ws provides the frame codec; this type adds message
// assembly, control-frame handling, write serialization, and the size
// limit, which is enforced on the whole message across its fragments.
//
// permessage-deflate is implemented here rather than inherited, because
// coder/websocket — which provides it on the HTTP/1.1 path — cannot accept
// over HTTP/2. See ws_compress.go.
type h2Conn struct {
	reader *bufio.Reader
	// closeRead unblocks a pending read: the request body on servers, the
	// response body plus the request cancel on clients.
	closeRead func()
	writer    io.Writer
	// flush pushes buffered response bytes onto the stream; nil when
	// writes need no explicit flush (the client's pipe).
	flush func() error
	// client marks the client side, which must mask every frame it sends.
	client bool
	// deflater is non-nil when permessage-deflate was negotiated. Guarded
	// by writeMu, like every other write-side resource.
	deflater *messageDeflater

	writeMu   sync.Mutex
	closeOnce sync.Once
	// closed records that the stream has been torn down, by either side. It
	// is what keeps a courtesy close frame from racing the peer's own.
	closed atomic.Bool
}

func newH2Conn(reader io.Reader, closeRead func(), writer io.Writer, flush func() error, client, deflate bool) *h2Conn {
	conn := &h2Conn{
		reader:    bufio.NewReader(reader),
		closeRead: closeRead,
		writer:    writer,
		flush:     flush,
		client:    client,
	}
	if deflate {
		conn.deflater = newMessageDeflater()
	}
	return conn
}

// ReadMessage reads frames until one complete message has been assembled,
// transparently answering pings and completing the closing handshake, then
// splits off the marker. The context is not consulted: an HTTP/2 stream
// read is unblocked by the stream ending (peer reset, connection loss) or
// by CloseNow.
//
// The size limit applies to the message, not the frame: a sender cannot
// slip past it by fragmenting. A frame that would carry the message past
// the limit is not read at all, so no more than one frame header past the
// limit is ever consumed.
func (c *h2Conn) ReadMessage(_ context.Context, readMaxBytes int) (message, error) {
	var payload []byte
	assembling := false
	text := false
	// RSV1 on the first frame of a message marks the whole message as one
	// deflated stream (RFC 7692 §6.2).
	compressed := false
	// The marker is not counted against the payload limit.
	limit := 0
	if readMaxBytes > 0 {
		limit = readMaxBytes + 1
	}
	for {
		header, err := ws.ReadHeader(c.reader)
		if err != nil {
			return message{}, err
		}
		rsv1, rsv2, rsv3 := ws.RsvBits(header.Rsv)
		if rsv2 || rsv3 {
			return message{}, fmt.Errorf("frame uses reserved bits 0b%03b, but no such extension was negotiated", header.Rsv)
		}
		if rsv1 && c.deflater == nil {
			return message{}, errors.New("frame is marked compressed, but permessage-deflate was not negotiated")
		}
		isData := header.OpCode == ws.OpText || header.OpCode == ws.OpBinary || header.OpCode == ws.OpContinuation
		if isData && limit > 0 && int64(len(payload))+header.Length > int64(limit) && !compressed && !rsv1 {
			// Only uncompressed messages can be refused before the payload
			// is read; a compressed one is bounded when it is inflated.
			return message{}, fmt.Errorf("%w of %d bytes", errMessageTooBig, readMaxBytes)
		}
		frame := make([]byte, header.Length)
		if _, err := io.ReadFull(c.reader, frame); err != nil {
			return message{}, err
		}
		if header.Masked {
			ws.Cipher(frame, header.Mask, 0)
		}
		switch header.OpCode {
		case ws.OpPing:
			if err := c.writeFrame(ws.NewPongFrame(frame)); err != nil {
				return message{}, err
			}
		case ws.OpPong:
			// Unsolicited pongs are permitted and ignored.
		case ws.OpClose:
			// Complete the closing handshake, then surface it like a
			// normally closed connection.
			_ = c.writeFrame(ws.NewCloseFrame(nil))
			c.shutdown()
			return message{}, io.EOF
		case ws.OpText, ws.OpBinary:
			if assembling {
				return message{}, errors.New("interleaved message frames")
			}
			payload = frame
			assembling = true
			compressed = rsv1
			text = header.OpCode == ws.OpText
			if header.Fin {
				return c.finishMessage(payload, compressed, text, readMaxBytes)
			}
		case ws.OpContinuation:
			if !assembling {
				return message{}, errors.New("continuation frame without a message")
			}
			if rsv1 {
				// RSV1 belongs to the first frame of a message only.
				return message{}, errors.New("continuation frame sets RSV1")
			}
			payload = append(payload, frame...)
			if header.Fin {
				return c.finishMessage(payload, compressed, text, readMaxBytes)
			}
		default:
			return message{}, fmt.Errorf("unknown frame opcode 0x%x", byte(header.OpCode))
		}
	}
}

// finishMessage inflates an assembled message when the sender marked it
// compressed — reading no more than one byte past the limit — and applies
// the rules every receiver shares: a marker must be present, a text frame
// must be UTF-8.
func (c *h2Conn) finishMessage(payload []byte, compressed, text bool, readMaxBytes int) (message, error) {
	if compressed {
		limit := 0
		if readMaxBytes > 0 {
			limit = readMaxBytes + 2
		}
		inflated, err := inflate(payload, limit)
		if err != nil {
			return message{}, err
		}
		payload = inflated
	}
	if len(payload) == 0 {
		return message{}, errEmptyMessage
	}
	if readMaxBytes > 0 && len(payload)-1 > readMaxBytes {
		return message{}, fmt.Errorf("%w of %d bytes", errMessageTooBig, readMaxBytes)
	}
	if text && !utf8.Valid(payload) {
		return message{}, errors.New("text message is not valid UTF-8")
	}
	return message{marker: payload[0], payload: payload[1:], text: text}, nil
}

// WriteMessage sends one message — marker, then payload — as a single
// unfragmented frame, with a text opcode if text is set and a binary one
// otherwise. When permessage-deflate was negotiated the message is
// compressed and RSV1 set, unless compressing it would not shrink it.
func (c *h2Conn) WriteMessage(_ context.Context, marker byte, payload []byte, text bool) error {
	opcode := ws.OpBinary
	if text {
		opcode = ws.OpText
	}
	data := make([]byte, 1+len(payload))
	data[0] = marker
	copy(data[1:], payload)
	// One lock for compress-and-send: the deflater is shared state, and
	// splitting the two would let another writer slip a frame between a
	// message's compression and its write.
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	compressed := false
	if c.deflater != nil {
		if deflated, ok := c.deflater.deflate(data); ok {
			data = deflated
			compressed = true
		}
	}
	frame := ws.NewFrame(opcode, true, data)
	if compressed {
		frame.Header.Rsv = ws.Rsv(true, false, false)
	}
	return c.writeFrameLocked(frame)
}

// writeFrame masks (on clients), writes, and flushes one frame. The lock
// keeps frames atomic on the stream: data frames, pong replies from the
// read side, and close frames all pass through here.
func (c *h2Conn) writeFrame(frame ws.Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeFrameLocked(frame)
}

// writeFrameLocked is writeFrame's body; callers already hold writeMu.
func (c *h2Conn) writeFrameLocked(frame ws.Frame) error {
	if c.client {
		frame = ws.MaskFrameInPlace(frame)
	}
	if err := ws.WriteFrame(c.writer, frame); err != nil {
		return err
	}
	if c.flush != nil {
		return c.flush()
	}
	return nil
}

// Close sends a close frame with the given status, then tears the stream
// down. Waiting for the peer's close reply is unnecessary: the HTTP/2
// stream itself confirms delivery, and the read side answers a
// peer-initiated close on its own.
//
// Closing an already-closed connection is not an error. One WebSocket is
// one RPC, so both sides close as soon as the RPC ends, and whichever
// close frame loses the race finds the stream already gone.
func (c *h2Conn) Close(code websocket.StatusCode, reason string) error {
	if c.closed.Load() {
		return nil
	}
	// websocket.StatusCode is an int holding a 16-bit close status; the
	// conversion cannot overflow for any defined code.
	status := ws.StatusCode(uint16(code)) //nolint:gosec // close statuses fit in 16 bits by definition
	err := c.writeFrame(ws.NewCloseFrame(ws.NewCloseFrameBody(status, reason)))
	c.shutdown()
	if errors.Is(err, io.ErrClosedPipe) {
		// The peer's close raced ours; the connection is closed either way.
		return nil
	}
	return err
}

// CloseNow implements messageConn by closing the read side, which unblocks
// a pending ReadMessage and, on clients, cancels the CONNECT request.
func (c *h2Conn) CloseNow() error {
	c.shutdown()
	return nil
}

// shutdown releases the stream exactly once and records that it is gone.
func (c *h2Conn) shutdown() {
	c.closed.Store(true)
	c.closeOnce.Do(c.closeRead)
}
