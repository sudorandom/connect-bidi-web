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
	"bytes"
	"strings"
	"testing"
)

func TestDeflateRoundTrip(t *testing.T) {
	t.Parallel()
	deflater := newMessageDeflater()
	payload := []byte(strings.Repeat("all work and no play. ", 500))

	compressed, ok := deflater.deflate(payload)
	if !ok {
		t.Fatal("deflate reported no gain on a highly compressible payload")
	}
	if len(compressed) >= len(payload) {
		t.Errorf("compressed to %d bytes, want fewer than %d", len(compressed), len(payload))
	}
	// The sender must strip the sync marker; a receiver that appends it
	// back would otherwise see it twice.
	if bytes.HasSuffix(compressed, deflateTail) {
		t.Error("compressed payload still carries the RFC 7692 tail")
	}

	back, err := inflate(compressed)
	if err != nil {
		t.Fatalf("inflate: %v", err)
	}
	if !bytes.Equal(back, payload) {
		t.Errorf("round trip returned %d bytes, want %d", len(back), len(payload))
	}
}

// TestDeflateNoContextTakeover is the property the negotiation promises:
// every message decompresses on its own, in any order, with no window
// carried between them.
func TestDeflateNoContextTakeover(t *testing.T) {
	t.Parallel()
	deflater := newMessageDeflater()
	payloads := [][]byte{
		[]byte(strings.Repeat("first message. ", 200)),
		[]byte(strings.Repeat("second message. ", 200)),
		[]byte(strings.Repeat("third message. ", 200)),
	}
	compressed := make([][]byte, len(payloads))
	for i, payload := range payloads {
		out, ok := deflater.deflate(payload)
		if !ok {
			t.Fatalf("payload %d did not compress", i)
		}
		compressed[i] = out
	}
	// Decompress out of order: with a shared window this would fail.
	for i := len(compressed) - 1; i >= 0; i-- {
		back, err := inflate(compressed[i])
		if err != nil {
			t.Fatalf("inflate payload %d: %v", i, err)
		}
		if !bytes.Equal(back, payloads[i]) {
			t.Errorf("payload %d did not round-trip", i)
		}
	}
}

// TestDeflateSkipsIncompressible keeps the sender from paying bytes for
// nothing: when DEFLATE cannot shrink a payload it goes out as-is, with
// RSV1 clear.
func TestDeflateSkipsIncompressible(t *testing.T) {
	t.Parallel()
	deflater := newMessageDeflater()
	// Deterministic pseudo-random bytes: essentially incompressible.
	payload := make([]byte, 4096)
	state := uint32(1)
	for i := range payload {
		state = state*1664525 + 1013904223
		payload[i] = byte(state >> 24)
	}
	if _, ok := deflater.deflate(payload); ok {
		t.Error("deflate claimed a gain on incompressible data")
	}
}

func TestAcceptsDeflate(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		offer string
		want  bool
	}{
		{name: "empty", offer: "", want: false},
		{name: "bare token", offer: "permessage-deflate", want: true},
		{
			name:  "both no-context-takeover params",
			offer: "permessage-deflate; client_no_context_takeover; server_no_context_takeover",
			want:  true,
		},
		{name: "spacing tolerated", offer: "  permessage-deflate ;client_no_context_takeover ", want: true},
		{name: "other extension", offer: "x-webkit-deflate-frame", want: false},
		{
			// Agreeing to a window size we do not implement would corrupt
			// the stream, so an offer carrying one is declined.
			name:  "window bits declined",
			offer: "permessage-deflate; client_max_window_bits=10",
			want:  false,
		},
		{
			// Browsers offer this alongside a bare fallback; take the one
			// we can serve.
			name:  "falls back to an acceptable alternative",
			offer: "permessage-deflate; client_max_window_bits=10, permessage-deflate",
			want:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := acceptsDeflate(test.offer); got != test.want {
				t.Errorf("acceptsDeflate(%q) = %v, want %v", test.offer, got, test.want)
			}
		})
	}
}

// The offer this implementation sends must be one it would itself accept,
// or two of these peers could never agree.
func TestOfferIsSelfAccepting(t *testing.T) {
	t.Parallel()
	if !acceptsDeflate(extensionOffer) {
		t.Errorf("acceptsDeflate rejects our own offer %q", extensionOffer)
	}
}
