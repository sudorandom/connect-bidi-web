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
	"context"
	"encoding/binary"
	"errors"
	"math"
	"net/http"
	"strings"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
	"github.com/coder/websocket"
)

// The draft 1 wire protocol.
//
// One WebSocket carries one streaming RPC. Each direction sends:
//
//	headers, zero or more data envelopes, end-stream
//
// Every WebSocket message is exactly one Connect envelope: a flag byte, a
// four-byte big-endian payload length, and the payload. Messages are
// always binary, because an envelope is a binary structure however its
// payload is encoded.
//
// The envelope's length field restates what the WebSocket message boundary
// already establishes. That redundancy is deliberate: the envelope is the
// Connect protocol's, byte for byte, so a draft 1 payload can be handed to
// Connect's own envelope reader without translation.
const (
	// protocolName is surfaced through connect.CallInfo so callers can tell
	// which protocol carried an RPC.
	protocolName = "websocket-draft1"

	// subprotocol is the required WebSocket subprotocol. Browsers can set
	// this on the handshake even though they cannot set headers, which is
	// why it is where the protocol version lives.
	subprotocol = "connect.bidi.d1"

	// contentTypePrefix prefixes the content types draft 1 carries; the
	// suffix is the codec name.
	contentTypePrefix = "application/connect+"
)

// Envelope flags. These are complete flag-byte values, not bitmasks.
//
// Draft 1 defines three of them. Connect's compressed-data flag (0x01) is
// absent because compression is permessage-deflate's job here, and the
// reset flag drafts 3 and 4 need (0x07) is absent because a connection
// carrying one RPC cancels by closing.
const (
	// flagData carries one RPC message encoded with the selected codec.
	flagData uint8 = 0x00
	// flagEndStream ends a direction. Empty on requests; the Connect
	// EndStreamResponse JSON on responses.
	flagEndStream uint8 = 0x02
	// flagHeaders carries the JSON metadata object that opens a direction,
	// standing in for the per-RPC HTTP headers the upgrade cannot carry.
	flagHeaders uint8 = 0x06
)

// envelopeHeadLen is the size of the envelope head: one flag byte and a
// four-byte big-endian payload length.
const envelopeHeadLen = 5

// errShortEnvelope reports a message too small to hold an envelope head.
var errShortEnvelope = errors.New("websocket message is shorter than an envelope head")

// encodeEnvelope returns one complete envelope: head then payload.
func encodeEnvelope(flag uint8, payload []byte) ([]byte, error) {
	if uint64(len(payload)) > math.MaxUint32 {
		return nil, errors.New("envelope payload is too large")
	}
	out := make([]byte, envelopeHeadLen+len(payload))
	out[0] = flag
	//nolint:gosec // the payload length is bounded to MaxUint32 above
	binary.BigEndian.PutUint32(out[1:envelopeHeadLen], uint32(len(payload)))
	copy(out[envelopeHeadLen:], payload)
	return out, nil
}

// decodeEnvelope splits one WebSocket message into its flag and payload.
// The declared length must match the message exactly: a message carries one
// whole envelope and nothing else, so a mismatch means the peer framed
// something this protocol cannot represent.
func decodeEnvelope(message []byte) (uint8, []byte, error) {
	if len(message) < envelopeHeadLen {
		return 0, nil, errShortEnvelope
	}
	flag := message[0]
	declared := binary.BigEndian.Uint32(message[1:envelopeHeadLen])
	payload := message[envelopeHeadLen:]
	if uint64(declared) != uint64(len(payload)) {
		return 0, nil, errors.New("envelope length does not match the websocket message")
	}
	return flag, payload, nil
}

// messageConn is one message-oriented, full-duplex WebSocket connection,
// abstracting how it was bootstrapped: an HTTP/1.1 Upgrade handshake, or an
// RFC 8441 extended-CONNECT stream on HTTP/2.
//
// Unlike the multiplexing drafts, a draft 1 connection carries exactly one
// RPC, so there is no reader to share and no writes to interleave.
type messageConn interface {
	// ReadMessage returns the next message, which is one whole envelope.
	ReadMessage(ctx context.Context) ([]byte, error)
	// WriteMessage sends one message, which must be one whole envelope.
	WriteMessage(ctx context.Context, data []byte) error
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

func (c coderConn) ReadMessage(ctx context.Context) ([]byte, error) {
	msgType, data, err := c.conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if msgType != websocket.MessageBinary {
		return nil, errors.New("expected a binary websocket message carrying an envelope")
	}
	return data, nil
}

func (c coderConn) WriteMessage(ctx context.Context, data []byte) error {
	// One buffer, one Write call: coder/websocket decides whether
	// permessage-deflate applies to a message by the size of the first
	// write, so writing the head and payload separately would leave every
	// message uncompressed.
	return c.conn.Write(ctx, websocket.MessageBinary, data)
}

func (c coderConn) Close() error {
	return c.conn.Close(websocket.StatusNormalClosure, "")
}

func (c coderConn) CloseNow() error {
	return c.conn.CloseNow()
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

// procedureFromPath returns the Connect procedure a request URL addresses:
// its last two path segments, "/package.Service/Method".
//
// Taking them from the end rather than from the whole path is what lets one
// handler serve whether it is mounted on the procedure URLs themselves — as
// a deployment following this draft would — or under a prefix, as the demo
// server does to keep several drafts on one origin.
func procedureFromPath(path string) string {
	method := strings.LastIndexByte(path, '/')
	if method <= 0 {
		return ""
	}
	service := strings.LastIndexByte(path[:method], '/')
	if service < 0 {
		return ""
	}
	if method == len(path)-1 || method-service == 1 {
		// An empty method or service name is not a procedure.
		return ""
	}
	return path[service:]
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

// offersSubprotocol reports whether the handshake offers draft 1's
// subprotocol.
func offersSubprotocol(header http.Header) bool {
	return headerListContains(header, "Sec-WebSocket-Protocol", subprotocol)
}
