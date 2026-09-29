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
	"errors"

	"github.com/coder/websocket"
)

// messageConn is one message-oriented, full-duplex WebSocket connection
// carrying draft 3 frames, abstracting how it was bootstrapped: an HTTP/1.1
// Upgrade handshake, or an RFC 8441 extended-CONNECT stream on HTTP/2. The
// wire protocol above it is identical either way.
type messageConn interface {
	// ReadMessage returns the next binary message. A non-binary message is
	// an error. Whether ctx can interrupt a blocked read is
	// bootstrap-specific; CloseNow always unblocks it.
	ReadMessage(ctx context.Context) ([]byte, error)
	// WriteMessage sends one binary message.
	WriteMessage(ctx context.Context, message []byte) error
	// Close closes the connection gracefully.
	Close() error
	// CloseNow tears the connection down without a closing handshake and
	// unblocks a pending ReadMessage.
	CloseNow() error
}

// coderConn adapts a coder/websocket connection (an HTTP/1.1 upgrade) to
// messageConn.
type coderConn struct {
	conn *websocket.Conn
}

func newCoderConn(conn *websocket.Conn) coderConn {
	// Message sizes are limited by the protocol options (ReadMaxBytes), not
	// by the WebSocket library.
	conn.SetReadLimit(-1)
	return coderConn{conn: conn}
}

func (c coderConn) ReadMessage(ctx context.Context) ([]byte, error) {
	msgType, data, err := c.conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if msgType != websocket.MessageBinary {
		return nil, errors.New("received non-binary websocket message")
	}
	return data, nil
}

func (c coderConn) WriteMessage(ctx context.Context, message []byte) error {
	// One buffer, one Write call: coder/websocket decides whether
	// permessage-deflate applies to a message by the size of the first
	// write, so writing a frame head separately would leave every message
	// uncompressed.
	return c.conn.Write(ctx, websocket.MessageBinary, message)
}

func (c coderConn) Close() error {
	return c.conn.Close(websocket.StatusNormalClosure, "")
}

func (c coderConn) CloseNow() error {
	return c.conn.CloseNow()
}
