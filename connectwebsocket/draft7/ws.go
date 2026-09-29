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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
)

// The draft 7 wire protocol. See README.md for the design and its
// rationale; what follows is the whole of it.
//
// One WebSocket carries one RPC, so there is no stream ID and no length
// field. Every message is one marker byte followed by its payload:
//
//	M  leading metadata     JSON object, text frame
//	B  body                 one RPC message; text under JSON, binary under proto
//	C  client end-of-stream a final body (framed as B would be), or bare, text
//	S  server end-of-stream Connect EndStreamResponse JSON, text frame
//
// Each direction opens with M. The client ends with C; the server ends
// with S. The frame type names the encoding — text is JSON, binary is
// Protobuf — so a receiver knows how to parse a payload before it reads
// anything but the marker.
const (
	// protocolName is surfaced through connect.CallInfo so callers can tell
	// which protocol carried an RPC.
	protocolName = "websocket-draft7"

	// The subprotocol tokens. The codec is selected by which one the
	// server echoes; the base token means JSON.
	subprotocolBase  = "connectrpc.1"
	subprotocolProto = "connectrpc.1+proto"
	subprotocolJSON  = "connectrpc.1+json"

	// The message markers.
	markerBody            byte = 'B'
	markerLeadingMetadata byte = 'M'
	markerServerEndStream byte = 'S'
	markerClientEndStream byte = 'C'

	// timeoutQueryParameter carries the client's deadline on the handshake
	// URI, in milliseconds.
	timeoutQueryParameter = "connect-timeout-ms"

	// maxTimeoutDigits bounds the deadline's decimal representation, as in
	// Connect over HTTP.
	maxTimeoutDigits = 10
)

// subprotocolCodecs maps each token to the codec it selects.
var subprotocolCodecs = map[string]string{ //nolint:gochecknoglobals
	subprotocolBase:  connect.CodecNameJSON,
	subprotocolProto: connect.CodecNameProto,
	subprotocolJSON:  connect.CodecNameJSON,
}

// subprotocolForCodec returns the token a client offers for a codec, and
// "" when the codec has no token — a custom codec has no WebSocket
// mapping here, because the subprotocol vocabulary is fixed.
func subprotocolForCodec(name string) string {
	switch name {
	case connect.CodecNameProto:
		return subprotocolProto
	case connect.CodecNameJSON:
		return subprotocolJSON
	default:
		return ""
	}
}

// codecIsBinary reports whether bodies in the named codec travel as
// binary frames. Only Protobuf binary does; the frame type names the
// encoding, and everything else this protocol carries is JSON.
func codecIsBinary(name string) bool {
	return name == connect.CodecNameProto
}

// message is one WebSocket message after its marker has been split off.
type message struct {
	marker  byte
	payload []byte
	text    bool
}

// messageConn is one message-oriented, full-duplex WebSocket connection
// carrying a single RPC.
type messageConn interface {
	// ReadMessage returns the next message, enforcing readMaxBytes on the
	// payload. A message that exceeds the limit is abandoned unread past
	// the limit, and the error unwraps to errMessageTooBig.
	ReadMessage(ctx context.Context, readMaxBytes int) (message, error)
	// WriteMessage sends one message, with a text frame if text is set and
	// a binary one otherwise.
	WriteMessage(ctx context.Context, marker byte, payload []byte, text bool) error
	// Close closes the connection with a closing handshake.
	Close(code websocket.StatusCode, reason string) error
	// CloseNow tears the connection down without a closing handshake and
	// unblocks a pending ReadMessage.
	CloseNow() error
}

// errMessageTooBig is the cause of a read that stopped at the size limit.
var errMessageTooBig = errors.New("message exceeds the read limit")

// errEmptyMessage is the cause of a read that found no marker.
var errEmptyMessage = errors.New("empty message: no marker")

// coderConn adapts a coder/websocket connection to messageConn.
type coderConn struct {
	conn *websocket.Conn
}

func newCoderConn(conn *websocket.Conn) coderConn {
	// Message sizes are limited by the protocol's own options
	// (WithReadMaxBytes) and enforced in ReadMessage, so that the
	// oversized message can be reported on the socket before the
	// connection is dropped; the library's limit would close it first.
	conn.SetReadLimit(-1)
	return coderConn{conn: conn}
}

func (c coderConn) ReadMessage(ctx context.Context, readMaxBytes int) (message, error) {
	msgType, reader, err := c.conn.Reader(ctx)
	if err != nil {
		return message{}, err
	}
	// Read the marker plus at most one byte past the payload limit: enough
	// to know the message overran, without consuming the rest of it.
	limited := reader
	if readMaxBytes > 0 {
		limited = io.LimitReader(reader, int64(readMaxBytes)+2)
	}
	data, err := io.ReadAll(limited)
	if err != nil {
		return message{}, err
	}
	if len(data) == 0 {
		return message{}, errEmptyMessage
	}
	if readMaxBytes > 0 && len(data)-1 > readMaxBytes {
		return message{}, fmt.Errorf("%w of %d bytes", errMessageTooBig, readMaxBytes)
	}
	text := msgType == websocket.MessageText
	if text && !utf8.Valid(data) {
		return message{}, errors.New("text message is not valid UTF-8")
	}
	return message{marker: data[0], payload: data[1:], text: text}, nil
}

func (c coderConn) WriteMessage(ctx context.Context, marker byte, payload []byte, text bool) error {
	msgType := websocket.MessageBinary
	if text {
		msgType = websocket.MessageText
	}
	// One buffer, one Write call: coder/websocket decides whether
	// permessage-deflate applies to a message by the size of the first
	// write, so writing the marker separately would leave every message
	// uncompressed.
	data := make([]byte, 1+len(payload))
	data[0] = marker
	copy(data[1:], payload)
	return c.conn.Write(ctx, msgType, data)
}

func (c coderConn) Close(code websocket.StatusCode, reason string) error {
	return c.conn.Close(code, reason)
}

func (c coderConn) CloseNow() error {
	return c.conn.CloseNow()
}

// isWebSocketUpgrade reports whether r is an HTTP/1.1 Upgrade handshake
// opening a WebSocket. Both header values are comma-separated lists in
// principle, so neither is compared whole.
func isWebSocketUpgrade(request *http.Request) bool {
	return headerListContains(request.Header, "Connection", "upgrade") &&
		headerListContains(request.Header, "Upgrade", "websocket")
}

// isExtendedConnectWebSocket reports whether r is an RFC 8441 extended
// CONNECT request opening a WebSocket over HTTP/2. Such requests reach
// handlers only with extended CONNECT enabled (GODEBUG=http2xconnect=1);
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

// offeredSubprotocols returns the tokens a handshake offers, in order.
func offeredSubprotocols(header http.Header) []string {
	var offered []string
	for _, entry := range header.Values("Sec-WebSocket-Protocol") {
		for field := range strings.SplitSeq(entry, ",") {
			if token := strings.TrimSpace(field); token != "" {
				offered = append(offered, token)
			}
		}
	}
	return offered
}

// selectSubprotocol picks the first offered token the server both
// recognizes and can serve. It reports whether any token was recognized at
// all, which decides between a 400 and a 415 when nothing is selected.
func selectSubprotocol(offered []string, codecs map[string]connect.Codec) (token string, recognized bool) {
	for _, candidate := range offered {
		codecName, known := subprotocolCodecs[candidate]
		if !known {
			continue
		}
		recognized = true
		if _, ok := codecs[codecName]; ok {
			return candidate, true
		}
	}
	return "", recognized
}

// supportedSubprotocols lists the tokens whose codec the server has, for
// the body of a 415.
func supportedSubprotocols(codecs map[string]connect.Codec) []string {
	var supported []string
	for _, token := range []string{subprotocolBase, subprotocolProto, subprotocolJSON} {
		if _, ok := codecs[subprotocolCodecs[token]]; ok {
			supported = append(supported, token)
		}
	}
	return supported
}

// requestMetadata returns the RPC request metadata carried by the upgrade
// request itself. The specification makes every request header available
// to the business logic, so nothing is filtered; the client's
// leading-metadata message then replaces values key by key.
func requestMetadata(header http.Header) http.Header {
	metadata := make(http.Header, len(header))
	for key, values := range header {
		metadata[key] = append([]string(nil), values...)
	}
	return metadata
}

// isCleanClose reports whether err is the ordinary end of a connection the
// peer closed politely.
func isCleanClose(err error) bool {
	if errors.Is(err, io.EOF) {
		return true
	}
	switch websocket.CloseStatus(err) {
	case websocket.StatusNormalClosure, websocket.StatusGoingAway:
		return true
	default:
		return false
	}
}
