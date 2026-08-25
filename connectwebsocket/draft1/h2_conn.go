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

package draft1

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/gobwas/ws"
)

// h2Conn carries WebSocket messages over an RFC 8441 extended-CONNECT
// HTTP/2 stream. There is no Upgrade handshake and no Sec-WebSocket-Key,
// but RFC 6455 message framing still applies on the stream: browsers send
// real — masked, possibly fragmented — WebSocket frames inside DATA
// frames. gobwas/ws provides the frame codec; this type adds message
// assembly, control-frame handling, and write serialization.
//
// This bootstrap negotiates no extensions, so unlike the HTTP/1.1 one it
// has no permessage-deflate and messages travel uncompressed. That is a
// gap in this implementation, not a rule: RFC 8441 §5 keeps
// Sec-WebSocket-Extensions in the CONNECT exchange, and draft 4 implements
// the extension over HTTP/2 on exactly that basis (see
// draft4/compress.go). Draft 1's own per-message Connect compression is
// unaffected either way — it sits above the connection.
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

	writeMu   sync.Mutex
	closeOnce sync.Once
}

func newH2Conn(reader io.Reader, closeRead func(), writer io.Writer, flush func() error, client bool) *h2Conn {
	return &h2Conn{
		reader:    bufio.NewReader(reader),
		closeRead: closeRead,
		writer:    writer,
		flush:     flush,
		client:    client,
	}
}

// ReadMessage reads frames until one complete binary message has been
// assembled, transparently answering pings and completing the closing
// handshake. The context is not consulted: an HTTP/2 stream read is
// unblocked by the stream ending (peer reset, connection loss) or by
// CloseNow.
func (c *h2Conn) ReadMessage(_ context.Context) ([]byte, error) {
	var message []byte
	assembling := false
	for {
		header, err := ws.ReadHeader(c.reader)
		if err != nil {
			return nil, err
		}
		if header.Rsv != 0 {
			return nil, fmt.Errorf("frame uses reserved bits 0b%03b, but no extension was negotiated", header.Rsv)
		}
		payload := make([]byte, header.Length)
		if _, err := io.ReadFull(c.reader, payload); err != nil {
			return nil, err
		}
		if header.Masked {
			ws.Cipher(payload, header.Mask, 0)
		}
		switch header.OpCode {
		case ws.OpPing:
			if err := c.writeFrame(ws.NewPongFrame(payload)); err != nil {
				return nil, err
			}
		case ws.OpPong:
			// Unsolicited pongs are permitted and ignored.
		case ws.OpClose:
			// Complete the closing handshake, then surface EOF like a
			// normally closed connection.
			_ = c.writeFrame(ws.NewCloseFrame(nil))
			c.closeOnce.Do(c.closeRead)
			return nil, io.EOF
		case ws.OpBinary:
			if assembling {
				return nil, errors.New("interleaved message frames")
			}
			message = payload
			assembling = true
			if header.Fin {
				return message, nil
			}
		case ws.OpContinuation:
			if !assembling {
				return nil, errors.New("continuation frame without a message")
			}
			message = append(message, payload...)
			if header.Fin {
				return message, nil
			}
		case ws.OpText:
			return nil, errors.New("received non-binary websocket message")
		default:
			return nil, fmt.Errorf("unknown frame opcode 0x%x", byte(header.OpCode))
		}
	}
}

// WriteMessage sends one binary message, assembled from parts, as a
// single unfragmented frame. Server frames stream the parts directly
// after the frame header; client frames must be masked, which ciphers in
// place, so the parts are first copied into one buffer.
func (c *h2Conn) WriteMessage(_ context.Context, parts ...[]byte) error {
	var length int64
	for _, part := range parts {
		length += int64(len(part))
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	header := ws.Header{Fin: true, OpCode: ws.OpBinary, Length: length}
	if c.client {
		header.Masked = true
		header.Mask = ws.NewMask()
		buf := make([]byte, 0, length)
		for _, part := range parts {
			buf = append(buf, part...)
		}
		ws.Cipher(buf, header.Mask, 0)
		if err := ws.WriteHeader(c.writer, header); err != nil {
			return err
		}
		if _, err := c.writer.Write(buf); err != nil {
			return err
		}
	} else {
		if err := ws.WriteHeader(c.writer, header); err != nil {
			return err
		}
		for _, part := range parts {
			if len(part) == 0 {
				continue
			}
			if _, err := c.writer.Write(part); err != nil {
				return err
			}
		}
	}
	if c.flush != nil {
		return c.flush()
	}
	return nil
}

// writeFrame masks (on clients), writes, and flushes one frame. The lock
// keeps frames atomic on the stream: data frames, pong replies from the
// read side, and close frames all pass through here.
func (c *h2Conn) writeFrame(frame ws.Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
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
func (c *h2Conn) Close() error {
	err := c.writeFrame(ws.NewCloseFrame(ws.NewCloseFrameBody(ws.StatusNormalClosure, "")))
	c.closeOnce.Do(c.closeRead)
	return err
}

// CloseNow implements messageConn by closing the read side, which unblocks
// a pending ReadMessage and, on clients, cancels the CONNECT request.
func (c *h2Conn) CloseNow() error {
	c.closeOnce.Do(c.closeRead)
	return nil
}
