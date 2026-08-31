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
	"net/http"
	"strings"
	"unicode/utf8"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
	"github.com/coder/websocket"
)

// The draft 6 wire protocol. It is draft 5's, with two changes: the
// procedure is named by a `:path` pseudo-header in the request headers
// message rather than by the URL, and a connection outlives the RPC that
// opened it.
//
// Each direction still sends:
//
//	headers, zero or more data messages, separator
//
// and the server still appends an end-stream message after its separator.
// The only framing is the WebSocket opcode: an empty text message is the
// separator, everything else is a message of the RPC, and position tells
// metadata from data. Senders use text when the payload is non-empty and
// valid UTF-8, binary otherwise — including whenever it is empty, because
// an empty protobuf message encodes to zero bytes and must never read as a
// half-close.
//
// What changes is what happens after the end-stream message: nothing. Both
// sides return to waiting for a headers message, and the next RPC starts on
// the same connection.
const (
	// protocolName is surfaced through connect.CallInfo so callers can tell
	// which protocol carried an RPC.
	protocolName = "websocket-draft6"

	// subprotocol is the required WebSocket subprotocol.
	subprotocol = "connect.bidi.d6"

	// DefaultPath is where a draft 6 handler is mounted unless the
	// deployment picks somewhere else. Unlike drafts 3 and 4, the path is
	// not part of the protocol — one endpoint dispatches every procedure,
	// so it is a deployment choice.
	DefaultPath = "/ws"

	// contentTypePrefix prefixes the content types draft 6 carries; the
	// suffix is the codec name.
	contentTypePrefix = "application/connect+"

	// pseudoHeaderPath names the procedure in the request headers message.
	// Drafts 1, 3, and 4 spelled it the same way, and so does HTTP/2: a
	// ":"-prefixed key is protocol metadata rather than application
	// metadata, and never reaches a handler as a request header.
	pseudoHeaderPath = ":path"
)

// messageConn is one message-oriented, full-duplex WebSocket connection,
// abstracting how it was bootstrapped: an HTTP/1.1 Upgrade handshake, or an
// RFC 8441 extended-CONNECT stream on HTTP/2.
//
// Unlike draft 5's, a draft 6 connection carries a *sequence* of RPCs, one
// at a time.
type messageConn interface {
	// ReadMessage returns the next message and whether it arrived as text.
	// The flag is the protocol's only framing.
	ReadMessage(ctx context.Context) (data []byte, text bool, err error)
	// WriteMessage sends one message, with a text opcode if text is set and
	// a binary one otherwise.
	WriteMessage(ctx context.Context, data []byte, text bool) error
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
	// Message sizes are limited by the protocol options (WithReadMaxBytes),
	// not by the WebSocket library.
	conn.SetReadLimit(-1)
	return coderConn{conn: conn}
}

func (c coderConn) ReadMessage(ctx context.Context) ([]byte, bool, error) {
	msgType, data, err := c.conn.Read(ctx)
	if err != nil {
		return nil, false, err
	}
	return data, msgType == websocket.MessageText, nil
}

func (c coderConn) WriteMessage(ctx context.Context, data []byte, text bool) error {
	msgType := websocket.MessageBinary
	if text {
		msgType = websocket.MessageText
	}
	// One buffer, one Write call: coder/websocket decides whether
	// permessage-deflate applies to a message by the size of the first
	// write, so writing in parts would leave every message uncompressed.
	return c.conn.Write(ctx, msgType, data)
}

func (c coderConn) Close() error {
	return c.conn.Close(websocket.StatusNormalClosure, "")
}

func (c coderConn) CloseNow() error {
	return c.conn.CloseNow()
}

// dataIsText reports whether a data message may travel as a text WebSocket
// message: it must be valid UTF-8, so tooling can render it, and it must be
// non-empty, because an empty text message is the separator. An empty
// protobuf message encodes to zero bytes, so that second condition is not
// hypothetical.
func dataIsText(payload []byte) bool {
	return len(payload) > 0 && utf8.Valid(payload)
}

// codecNameForContentType returns the codec named by a content type, or ""
// if it is not one this protocol carries.
func codecNameForContentType(contentType string) string {
	name, ok := strings.CutPrefix(strings.TrimSpace(contentType), contentTypePrefix)
	if !ok || name == "" {
		return ""
	}
	return name
}

// protocolOptions holds the codec configuration shared by the client
// transport and the server handler.
type protocolOptions struct {
	Codecs        map[string]connect.Codec
	SendCodecName string
	SendCodec     connect.Codec
	ReadMaxBytes  int
	SendMaxBytes  int
}

// newClientProtocolOptions returns protocolOptions with client defaults:
// the proto codec for sending.
func newClientProtocolOptions() protocolOptions {
	opts := newProtocolOptions()
	opts.SendCodecName = connect.CodecNameProto
	return opts
}

// newServerProtocolOptions returns protocolOptions with server defaults.
func newServerProtocolOptions() protocolOptions {
	return newProtocolOptions()
}

func newProtocolOptions() protocolOptions {
	return protocolOptions{
		Codecs: map[string]connect.Codec{
			connect.CodecNameProto: connectproto.NewBinaryCodec(),
			connect.CodecNameJSON:  connectproto.NewJSONCodec(),
		},
	}
}

// finalize resolves derived fields after all user options were applied.
func (o *protocolOptions) finalize() {
	o.SendCodec = o.Codecs[o.SendCodecName]
	if o.SendCodec == nil {
		o.SendCodec = o.Codecs[connect.CodecNameProto]
	}
}

// addCodecs registers codecs by name.
func (o *protocolOptions) addCodecs(codecs ...connect.Codec) {
	for _, codec := range codecs {
		o.Codecs[codec.Name()] = codec
	}
}

// isWebSocketUpgrade reports whether r is an HTTP/1.1 Upgrade handshake
// opening a WebSocket.
func isWebSocketUpgrade(request *http.Request) bool {
	if request.Method != http.MethodGet {
		return false
	}
	return headerListContains(request.Header, "Connection", "upgrade") &&
		headerListContains(request.Header, "Upgrade", "websocket")
}

// headerListContains reports whether a comma-separated header lists value,
// case-insensitively.
func headerListContains(header http.Header, key, value string) bool {
	for _, entry := range header.Values(key) {
		for field := range strings.SplitSeq(entry, ",") {
			if strings.EqualFold(strings.TrimSpace(field), value) {
				return true
			}
		}
	}
	return false
}

// offersSubprotocol reports whether the handshake offers draft 6's
// subprotocol.
func offersSubprotocol(header http.Header) bool {
	return headerListContains(header, "Sec-WebSocket-Protocol", subprotocol)
}
