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

package draft2

import (
	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
)

// protocolName is surfaced through connect.CallInfo so callers can tell
// which WebSocket protocol draft carried an RPC.
const protocolName = "websocket-draft2"

// Frame type constants. These are complete byte values, not bitmasks; 0x04
// and up are reserved for future frame types. The protocol has no
// compressed-data type: compression is the WebSocket's job
// (permessage-deflate), below this protocol.
const (
	// frameTypeData carries one RPC message encoded with the selected codec.
	frameTypeData uint8 = 0x00
	// frameTypeHeaders marks the leading metadata frame of a request or
	// response, standing in for the HTTP headers a raw socket doesn't have.
	frameTypeHeaders uint8 = 0x01
	// frameTypeEndStream half-closes a stream: empty on requests, the
	// Connect EndStreamResponse JSON on responses.
	frameTypeEndStream uint8 = 0x02
	// frameTypeReset aborts a single stream, with an empty payload, leaving
	// the other streams multiplexed onto the connection running.
	frameTypeReset uint8 = 0x03
)

// frameConn is a duplex channel carrying the frames of a single RPC stream,
// adapting a stream multiplexed onto a WebSocket connection to the stream
// code.
type frameConn interface {
	// ReadFrame reads the next frame of this stream.
	ReadFrame() (frameType uint8, payload []byte, err error)
	// WriteFrame writes a single frame.
	WriteFrame(frameType uint8, payload []byte) error
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
