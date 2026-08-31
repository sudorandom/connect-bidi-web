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
	"context"
	"net/http"
	"strings"

	"github.com/coder/websocket"
)

// The draft 5 wire protocol. See README.md for the design and its
// rationale; what follows is the whole of it.
//
// One WebSocket carries one RPC, so there is no stream ID, no frame type,
// no envelope, and no length field. Each direction sends:
//
//	headers, zero or more data messages, separator
//
// and the server appends an end-stream message after its separator. The
// only framing is the WebSocket opcode:
//
//	binary  one RPC message, encoded with the negotiated codec
//	text    a JSON metadata object: headers, or end-stream
//	text    empty: the separator ending this direction's data phase
//
// The opcode has to carry that distinction, because an empty protobuf
// message encodes to zero bytes: "any empty message" would make a
// legitimate empty data message indistinguishable from the separator, and
// a service whose messages are sometimes empty would half-close at random.
const (
	// protocolName is surfaced through connect.CallInfo so callers can tell
	// which protocol carried an RPC.
	protocolName = "websocket-draft5"

	// subprotocol is the required WebSocket subprotocol. The server must
	// select it, and rejects a handshake that does not offer it. It is what
	// distinguishes an RPC upgrade from any other WebSocket a deployment
	// serves, and — being one of the two things a browser may put on a
	// handshake — the only identification available from every client.
	subprotocol = "connect.bidi.d5"

	// contentTypePrefix prefixes the content types draft 5 carries. The
	// suffix is the codec name, as in the Connect protocol's own streaming
	// content types.
	contentTypePrefix = "application/connect+"
)

// messageConn is one message-oriented, full-duplex WebSocket connection
// carrying a single RPC, abstracting how it was bootstrapped: an HTTP/1.1
// Upgrade handshake, or an RFC 8441 extended-CONNECT stream on HTTP/2. The
// protocol above it is identical either way.
type messageConn interface {
	// ReadMessage returns the next message and whether it arrived as text.
	// Unlike in draft 4, the caller needs that flag: it is the protocol's
	// only framing. Whether ctx can interrupt a blocked read is
	// bootstrap-specific; CloseNow always unblocks it.
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
	// Message sizes are limited by the transport's own options
	// (WithReadMaxBytes), not by the WebSocket library.
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

// isWebSocketUpgrade reports whether r is an HTTP/1.1 Upgrade handshake
// opening a WebSocket. Both header values are comma-separated lists in
// principle, so neither is compared whole.
func isWebSocketUpgrade(request *http.Request) bool {
	if request.Method != http.MethodGet {
		return false
	}
	return headerListContains(request.Header, "Connection", "upgrade") &&
		headerListContains(request.Header, "Upgrade", "websocket")
}

// isExtendedConnectWebSocket reports whether r is an RFC 8441 extended
// CONNECT request opening a WebSocket. Such requests reach handlers only
// over HTTP/2 with extended CONNECT enabled (GODEBUG=http2xconnect=1);
// net/http surfaces the :protocol pseudo-header through r.Header.
func isExtendedConnectWebSocket(request *http.Request) bool {
	return request.Method == http.MethodConnect &&
		request.ProtoMajor == 2 &&
		request.Header.Get(":protocol") == "websocket"
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

// offersSubprotocol reports whether the handshake offers draft 5's
// subprotocol.
func offersSubprotocol(header http.Header) bool {
	return headerListContains(header, "Sec-WebSocket-Protocol", subprotocol)
}

// codecNameForContentType returns the codec named by a draft 5 content
// type, or "" if the content type is not one draft 5 carries.
func codecNameForContentType(contentType string) string {
	name, ok := strings.CutPrefix(canonicalizeContentType(contentType), contentTypePrefix)
	if !ok || name == "" {
		return ""
	}
	return name
}

// handshakeHeaders are the headers that belong to the WebSocket handshake
// rather than to the RPC. They are stripped before the upgrade request's
// headers are used as the base of the request metadata, along with every
// Sec-WebSocket-* header.
var handshakeHeaders = map[string]struct{}{
	"Connection":          {},
	"Upgrade":             {},
	"Keep-Alive":          {},
	"Proxy-Authenticate":  {},
	"Proxy-Authorization": {},
	"Te":                  {},
	"Trailer":             {},
	"Transfer-Encoding":   {},
	":protocol":           {},
}

// requestMetadata returns the RPC request metadata carried by the upgrade
// request itself: every header that is not part of the handshake. The
// headers message is merged over the result, so a client that can set real
// headers and one that cannot end up with the same metadata.
func requestMetadata(header http.Header) http.Header {
	metadata := make(http.Header, len(header))
	for key, values := range header {
		if _, isHandshake := handshakeHeaders[key]; isHandshake {
			continue
		}
		if strings.HasPrefix(strings.ToLower(key), "sec-websocket-") {
			continue
		}
		metadata[key] = append([]string(nil), values...)
	}
	return metadata
}
