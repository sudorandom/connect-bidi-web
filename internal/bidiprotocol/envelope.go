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

// Package bidiprotocol implements the shared enveloping protocol used by the
// WebSocket and WebTransport transports. Every frame is a Connect-style
// envelope: a one-byte flag, a four-byte big-endian payload length, and the
// payload itself.
package bidiprotocol

import (
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/sudorandom/connect-bidi-web/internal/connectprotocol"
)

// Envelope flag constants. These are complete flag-byte values, not
// bitmasks; 0x08 and up are reserved for extended flags.
const (
	FlagEnvelopeData       uint8 = 0x00
	FlagEnvelopeCompressed uint8 = connectprotocol.FlagEnvelopeCompressed
	FlagEnvelopeEndStream  uint8 = connectprotocol.FlagEnvelopeEndStream
	// FlagEnvelopeHeaders marks the leading metadata frame of a request or
	// response, standing in for the HTTP headers a raw socket doesn't have.
	FlagEnvelopeHeaders uint8 = 0x06
	// FlagEnvelopeReset aborts a single stream, with an empty payload. It is
	// used only by transports that multiplex several streams onto one
	// connection (WebSocket); transports with one stream per connection
	// simply close the connection instead.
	FlagEnvelopeReset uint8 = 0x07
)

const envelopeLen = 5

// WriteEnvelopeChunked writes the envelope head and payload as separate Write
// calls, avoiding a copy of the payload. Suitable for byte-stream transports
// (such as a WebTransport stream) where message boundaries don't exist.
func WriteEnvelopeChunked(writer io.Writer, flag uint8, payload []byte) error {
	if uint64(len(payload)) > math.MaxUint32 {
		return errors.New("payload too large")
	}
	var head [envelopeLen]byte
	head[0] = flag
	//nolint:gosec // the payload length is bounded to MaxUint32 above
	binary.BigEndian.PutUint32(head[1:5], uint32(len(payload)))
	if _, err := writer.Write(head[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := writer.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// DefaultMaxReadFrameSize is the default upper limit (128 MiB) on envelope payloads
// read from raw socket readers to prevent unbounded memory allocation.
const DefaultMaxReadFrameSize = 128 * 1024 * 1024

// ReadEnvelope reads a single envelope from the reader, returning its flag
// and payload.
func ReadEnvelope(reader io.Reader) (uint8, []byte, error) {
	return ReadEnvelopeWithSizeLimit(reader, DefaultMaxReadFrameSize)
}

// ReadEnvelopeWithSizeLimit reads a single envelope from the reader up to maxReadSize bytes.
func ReadEnvelopeWithSizeLimit(reader io.Reader, maxReadSize int) (uint8, []byte, error) {
	var head [envelopeLen]byte
	if _, err := io.ReadFull(reader, head[:]); err != nil {
		return 0, nil, err
	}
	flag := head[0]
	length := binary.BigEndian.Uint32(head[1:5])
	if length == 0 {
		return flag, nil, nil
	}
	if maxReadSize > 0 && int64(length) > int64(maxReadSize) {
		return 0, nil, errors.New("envelope payload exceeds maximum allowed size")
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, nil, err
	}
	return flag, payload, nil
}
