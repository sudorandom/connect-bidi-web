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

// Conn is a duplex channel carrying the envelopes of a single RPC stream.
// Implementations adapt a transport-specific connection (a stream
// multiplexed onto a WebSocket connection, a WebTransport stream) to the
// shared protocol code.
type Conn interface {
	// ReadEnvelope reads the next envelope.
	ReadEnvelope() (flag uint8, payload []byte, err error)
	// WriteEnvelope writes a single envelope.
	WriteEnvelope(flag uint8, payload []byte) error
	// CloseSend signals that no further envelopes will be written. How the
	// half-close is expressed on the wire is transport-specific: an explicit
	// end-stream envelope, or a stream FIN.
	CloseSend() error
	// Close releases the underlying stream.
	Close() error
}
