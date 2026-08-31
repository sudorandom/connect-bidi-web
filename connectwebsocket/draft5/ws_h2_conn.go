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

package draft5

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/gobwas/ws"
)

// h2Conn carries WebSocket messages over an RFC 8441 extended-CONNECT
// HTTP/2 stream. There is no Upgrade handshake and no Sec-WebSocket-Key,
// but RFC 6455 message framing still applies on the stream: browsers send
// real — masked, possibly fragmented — WebSocket frames inside DATA
// frames. gobwas/ws provides the frame codec; this type adds message
// assembly, control-frame handling, and write serialization.
//
// permessage-deflate is implemented here rather than inherited, because
// coder/websocket — which provides it on the HTTP/1.1 path — cannot accept
// over HTTP/2. See ws_compress.go. The opcode is reported back to the
// caller: in draft 5 it is the protocol's only framing, not the legibility
// hint it is in draft 4.
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
// transparently answering pings and completing the closing handshake. It
// reports whether the message arrived as text. The context is not
// consulted: an HTTP/2 stream read is unblocked by the stream ending (peer
// reset, connection loss) or by CloseNow.
func (c *h2Conn) ReadMessage(_ context.Context) ([]byte, bool, error) {
	var message []byte
	assembling := false
	text := false
	// RSV1 on the first frame of a message marks the whole message as one
	// deflated stream (RFC 7692 §6.2).
	compressed := false
	for {
		header, err := ws.ReadHeader(c.reader)
		if err != nil {
			return nil, false, err
		}
		rsv1, rsv2, rsv3 := ws.RsvBits(header.Rsv)
		if rsv2 || rsv3 {
			return nil, false, fmt.Errorf("frame uses reserved bits 0b%03b, but no such extension was negotiated", header.Rsv)
		}
		if rsv1 && c.deflater == nil {
			return nil, false, errors.New("frame is marked compressed, but permessage-deflate was not negotiated")
		}
		payload := make([]byte, header.Length)
		if _, err := io.ReadFull(c.reader, payload); err != nil {
			return nil, false, err
		}
		if header.Masked {
			ws.Cipher(payload, header.Mask, 0)
		}
		switch header.OpCode {
		case ws.OpPing:
			if err := c.writeFrame(ws.NewPongFrame(payload)); err != nil {
				return nil, false, err
			}
		case ws.OpPong:
			// Unsolicited pongs are permitted and ignored.
		case ws.OpClose:
			// Complete the closing handshake, then surface EOF like a
			// normally closed connection.
			_ = c.writeFrame(ws.NewCloseFrame(nil))
			c.shutdown()
			return nil, false, io.EOF
		case ws.OpText, ws.OpBinary:
			if assembling {
				return nil, false, errors.New("interleaved message frames")
			}
			message = payload
			assembling = true
			compressed = rsv1
			text = header.OpCode == ws.OpText
			if header.Fin {
				return c.finishMessage(message, compressed, text)
			}
		case ws.OpContinuation:
			if !assembling {
				return nil, false, errors.New("continuation frame without a message")
			}
			if rsv1 {
				// RSV1 belongs to the first frame of a message only.
				return nil, false, errors.New("continuation frame sets RSV1")
			}
			message = append(message, payload...)
			if header.Fin {
				return c.finishMessage(message, compressed, text)
			}
		default:
			return nil, false, fmt.Errorf("unknown frame opcode 0x%x", byte(header.OpCode))
		}
	}
}

// finishMessage inflates an assembled message when the sender marked it
// compressed, and carries the opcode back out with it.
func (c *h2Conn) finishMessage(message []byte, compressed, text bool) ([]byte, bool, error) {
	if !compressed {
		return message, text, nil
	}
	inflated, err := inflate(message)
	if err != nil {
		return nil, false, err
	}
	return inflated, text, nil
}

// WriteMessage sends one message as a single unfragmented frame, with a
// text opcode if text is set and a binary one otherwise. When
// permessage-deflate was negotiated the payload is compressed and RSV1 set,
// unless compressing it would not shrink it.
func (c *h2Conn) WriteMessage(_ context.Context, message []byte, text bool) error {
	opcode := ws.OpBinary
	if text {
		opcode = ws.OpText
	}
	// One lock for compress-and-send: the deflater is shared state, and
	// splitting the two would let another writer slip a frame between a
	// message's compression and its write.
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	compressed := false
	if c.deflater != nil {
		if deflated, ok := c.deflater.deflate(message); ok {
			message = deflated
			compressed = true
		}
	}
	frame := ws.NewFrame(opcode, true, message)
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

// Close sends a close frame, then tears the stream down. Waiting for the
// peer's close reply is unnecessary: the HTTP/2 stream itself confirms
// delivery, and the read side answers a peer-initiated close on its own.
//
// Closing an already-closed connection is not an error. In draft 5 that is
// the common case rather than a corner: one WebSocket is one RPC, so both
// sides close as soon as the RPC ends, and whichever close frame loses the
// race finds the stream already gone. The read side tears the stream down
// when it sees the peer's close, so this write can land on a closed pipe
// with nothing wrong having happened.
func (c *h2Conn) Close() error {
	if c.closed.Load() {
		return nil
	}
	err := c.writeFrame(ws.NewCloseFrame(ws.NewCloseFrameBody(ws.StatusNormalClosure, "")))
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
