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
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"strings"
)

// permessage-deflate (RFC 7692) for the HTTP/2 bootstrap.
//
// On the HTTP/1.1 path coder/websocket implements this for us. It cannot
// accept over HTTP/2, so that path runs its own RFC 6455 framing (h2Conn)
// and needs its own copy of the extension. RFC 8441 §5 keeps
// Sec-WebSocket-Extensions ("used in the CONNECT request and
// response-header fields as defined in [RFC6455]"), so the negotiation is
// the ordinary one; only the framing underneath is ours.
//
// Both sides are negotiated with no context takeover — which the protocol
// requires anyway, imposing it on a client that did not offer it — and
// that is what makes this implementable in a few dozen lines: every
// message is an independent DEFLATE stream, so there is no window to carry
// between messages and no ordering coupling between them.

const (
	// extensionName is the token offered and accepted in the handshake.
	extensionName = "permessage-deflate"
	// extensionOffer is what a compressing client offers. Both
	// no-context-takeover parameters are requested so neither side has to
	// keep a window.
	extensionOffer = extensionName +
		"; client_no_context_takeover; server_no_context_takeover"
	// extensionHeader is the handshake header carrying the offer and the
	// acceptance.
	extensionHeader = "Sec-WebSocket-Extensions"
)

// deflateTail is the empty stored block that flate.Writer emits on Flush.
// RFC 7692 §7.2.1 requires senders to drop it, so a message's payload is
// the DEFLATE stream without its terminator.
var deflateTail = []byte{0x00, 0x00, 0xff, 0xff}

// inflateTail is what a receiver appends instead: the sync marker the
// sender removed, plus a final empty block (BFINAL=1). The marker alone
// only *flushes* the stream — flate would then read past it and report
// io.ErrUnexpectedEOF — so the final block is what lets the reader stop
// cleanly.
var inflateTail = []byte{0x00, 0x00, 0xff, 0xff, 0x01, 0x00, 0x00, 0xff, 0xff}

// acceptsDeflate reports whether a Sec-WebSocket-Extensions value carries a
// permessage-deflate offer (or acceptance) this implementation can use.
//
// A server may impose no_context_takeover in both directions whatever the
// client offered (RFC 7692 §7.1.1), so those two parameters are simply
// noted. A bare client_max_window_bits, or one with a value, only limits
// the window the *peer* compresses with, which any inflater can decode.
// server_max_window_bits below 15 is declined rather than misinterpreted:
// agreeing to a window we cannot set would corrupt the stream.
func acceptsDeflate(header string) bool {
	for extension := range strings.SplitSeq(header, ",") {
		fields := strings.Split(extension, ";")
		if strings.TrimSpace(fields[0]) != extensionName {
			continue
		}
		usable := true
		for _, param := range fields[1:] {
			param = strings.TrimSpace(param)
			switch {
			case param == "client_no_context_takeover", param == "server_no_context_takeover":
			case param == "client_max_window_bits", strings.HasPrefix(param, "client_max_window_bits="):
			case param == "server_max_window_bits=15":
			default:
				usable = false
			}
		}
		if usable {
			return true
		}
	}
	return false
}

// messageDeflater compresses one message at a time. It is not safe for
// concurrent use; h2Conn serializes writes.
type messageDeflater struct {
	buf    bytes.Buffer
	writer *flate.Writer
}

func newMessageDeflater() *messageDeflater {
	deflater := &messageDeflater{}
	// flate.NewWriter only fails on an invalid level.
	writer, _ := flate.NewWriter(&deflater.buf, flate.DefaultCompression)
	deflater.writer = writer
	return deflater
}

// deflate compresses payload into one self-contained DEFLATE stream with
// the RFC 7692 tail removed. It reports false when compression would not
// shrink the payload, in which case the caller sends it uncompressed and
// leaves RSV1 clear.
func (d *messageDeflater) deflate(payload []byte) ([]byte, bool) {
	d.buf.Reset()
	// Reset per message: no context takeover means no window carried over,
	// so each message decompresses on its own.
	d.writer.Reset(&d.buf)
	if _, err := d.writer.Write(payload); err != nil {
		return nil, false
	}
	if err := d.writer.Flush(); err != nil {
		return nil, false
	}
	out := d.buf.Bytes()
	// Drop the terminating empty block; the receiver appends it back.
	out = bytes.TrimSuffix(out, deflateTail)
	if len(out) >= len(payload) {
		return nil, false
	}
	// The buffer is reused on the next message, so hand back a copy.
	return append([]byte(nil), out...), true
}

// inflate decompresses one permessage-deflate message payload, restoring
// the tail the sender dropped. It reads at most limit bytes of output when
// limit is positive, so an oversized message is recognized without being
// decompressed in full; the caller compares the length to its limit.
func inflate(payload []byte, limit int) ([]byte, error) {
	var source io.Reader = flate.NewReader(io.MultiReader(
		bytes.NewReader(payload),
		bytes.NewReader(inflateTail),
	))
	if limit > 0 {
		source = io.LimitReader(source, int64(limit))
	}
	out, err := io.ReadAll(source)
	if err != nil {
		return nil, fmt.Errorf("inflate permessage-deflate payload: %w", err)
	}
	return out, nil
}
