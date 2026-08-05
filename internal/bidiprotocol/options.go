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

package bidiprotocol

import (
	"strings"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
	"github.com/sudorandom/connect-bidi-web/internal/connectprotocol"
)

// Options holds the codec and compression configuration shared by clients and
// servers of both transports.
type Options struct {
	Codecs            map[string]connect.Codec
	SendCodecName     string
	SendCodec         connect.Codec
	SendCompressor    string
	Compressors       map[string]connect.Compressor
	CompressorNames   []string
	AcceptCompression string
	ReadMaxBytes      int
	SendMaxBytes      int
}

// NewClientOptions returns Options with client defaults: the proto codec for
// sending, no outgoing compression, and gzip accepted.
func NewClientOptions() Options {
	opts := newOptions()
	opts.SendCodecName = connect.CodecNameProto
	opts.SendCompressor = connect.CompressionNameIdentity
	return opts
}

// NewServerOptions returns Options with server defaults.
func NewServerOptions() Options {
	return newOptions()
}

func newOptions() Options {
	opts := Options{
		Codecs: map[string]connect.Codec{
			connect.CodecNameProto: connectproto.NewBinaryCodec(),
			connect.CodecNameJSON:  connectproto.NewJSONCodec(),
		},
	}
	opts.Compressors, opts.CompressorNames = connectprotocol.DefaultCompressors()
	return opts
}

// Finalize resolves derived fields after all user options were applied.
func (o *Options) Finalize() {
	o.SendCodec = o.Codecs[o.SendCodecName]
	if o.SendCodec == nil {
		o.SendCodec = o.Codecs[connect.CodecNameProto]
	}
	o.AcceptCompression = strings.Join(o.CompressorNames, ",")
}

// AddCodecs registers codecs by name.
func (o *Options) AddCodecs(codecs ...connect.Codec) {
	for _, codec := range codecs {
		o.Codecs[codec.Name()] = codec
	}
}

// AddCompressors registers compressors by name.
func (o *Options) AddCompressors(compressors ...connect.Compressor) {
	for _, compressor := range compressors {
		o.Compressors[compressor.Name()] = compressor
		o.CompressorNames = append(o.CompressorNames, compressor.Name())
	}
}
