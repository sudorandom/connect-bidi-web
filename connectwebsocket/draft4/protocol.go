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
	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
)

// protocolName is surfaced through connect.CallInfo so callers can tell
// which WebSocket protocol draft carried an RPC.
const protocolName = "websocket-draft4"

// The frame head's second field is an 8-bit "flags" value in decimal ASCII,
// partitioned so it can be read either as a small enum or with bit math:
//
//	 7   6   5   4   3   2   1   0
//	+---+---+---+---+---+---+---+---+
//	|      flags        |   type    |
//	+---+---+---+---+---+---+---+---+
//
// The low 3 bits are the frame type; the high 5 bits are independent flags,
// none of which is defined yet. So today's frames read as the plain numbers
// 0-3, and a future revision can OR a flag in without moving anything or
// breaking the wire: `type | flagSomething`.
//
// Unlike draft 3's descriptor byte, nothing here is a compression bit:
// draft 4 has no compression of its own, leaving it to the WebSocket's
// permessage-deflate extension.
const (
	// frameTypeMask selects the frame type from the flags field.
	frameTypeMask uint8 = 0x07
	// frameFlagsMask selects the flag bits from the flags field. Every bit
	// in it is currently reserved; see frameTypeOf on why receivers ignore
	// rather than reject them.
	frameFlagsMask uint8 = 0xF8
)

// Frame types, in the low 3 bits of the flags field. Values 4-7 are
// reserved for future frame types; a receiver rejects them as unknown.
const (
	// frameTypeData carries one RPC message encoded with the selected codec.
	frameTypeData uint8 = 0
	// frameTypeHeaders marks the leading metadata frame of a request or
	// response, standing in for the HTTP headers a raw socket doesn't have.
	// Its payload is always JSON.
	frameTypeHeaders uint8 = 1
	// frameTypeEndStream half-closes a stream: empty on requests, the
	// Connect EndStreamResponse JSON on responses.
	frameTypeEndStream uint8 = 2
	// frameTypeReset aborts a single stream, with an empty payload, leaving
	// the other streams multiplexed onto the connection running.
	frameTypeReset uint8 = 3
)

// frameTypeOf extracts the frame type from a flags field, discarding the
// flag bits.
//
// Discarding is deliberate: unknown flags are *ignored*, not rejected, so a
// later revision can define one and still be understood by peers built
// against this one — the same forward-compatibility rule HTTP/2 uses for
// its own frame flags. An unknown frame *type* is a different matter and is
// rejected, because its payload semantics would be anyone's guess.
func frameTypeOf(flags uint8) uint8 {
	return flags & frameTypeMask
}

// frameConn is a duplex channel carrying the frames of a single RPC stream,
// adapting a stream multiplexed onto a WebSocket connection to the stream
// code.
type frameConn interface {
	// ReadFrame reads the next frame of this stream, returning the flags
	// field whole; callers apply frameTypeOf to get the frame type.
	ReadFrame() (flags uint8, payload []byte, err error)
	// WriteFrame writes a single frame. flags is the whole second field, so
	// a caller may OR flag bits onto the frame type. text reports whether
	// the payload is UTF-8 text, which decides the WebSocket message's
	// opcode: a text message renders as readable text in browser devtools
	// and packet captures, which is the whole point of an ASCII frame head.
	WriteFrame(flags uint8, payload []byte, text bool) error
	// CloseSend signals that no further frames will be written, with an
	// explicit end-stream frame: neither the stream nor the WebSocket has a
	// send-direction close of its own.
	CloseSend() error
	// Close releases the underlying stream.
	Close() error
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

// codecIsText reports whether a codec's output is UTF-8 text, and so
// whether data frames using it can travel in text WebSocket messages.
func codecIsText(codec connect.Codec) bool {
	return codec != nil && codec.Name() == connect.CodecNameJSON
}
