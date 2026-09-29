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
	"strings"
	"testing"

	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	"google.golang.org/protobuf/proto"
)

// TestDataIsText pins the opcode rule at the boundaries where it changes,
// because they are not where intuition puts them.
//
// "protobuf is the binary one" is not true, and not false either. A string
// field's contents are UTF-8 by protobuf's own rules, so whether the
// *encoding* is valid UTF-8 comes down to the framing bytes in front of
// it: the tag, and the length varint. Both stay in ASCII range only for a
// low field number and a payload under 128 bytes. One byte more and the
// length varint becomes 0x80 0x01, which is not valid UTF-8, and the same
// method's messages start going out as binary.
//
// Nothing depends on which opcode a data message used — receivers treat
// them identically — but a test that says so out loud is cheaper than
// rediscovering it.
func TestDataIsText(t *testing.T) {
	t.Parallel()

	marshal := func(t *testing.T, msg proto.Message) []byte {
		t.Helper()
		data, err := proto.Marshal(msg)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return data
	}

	tests := []struct {
		name    string
		payload []byte
		want    bool
	}{
		{
			// Field 2, 127 bytes: tag 0x12, length 0x7f, then ASCII.
			name:    "short string is text",
			payload: marshal(t, &pingv1.PingRequest{Text: strings.Repeat("a", 127)}),
			want:    true,
		},
		{
			// One byte longer: the length varint becomes 0x80 0x01, and
			// 0x80 is a continuation byte with nothing to continue.
			name:    "string of 128 bytes is binary",
			payload: marshal(t, &pingv1.PingRequest{Text: strings.Repeat("a", 128)}),
			want:    false,
		},
		{
			// A varint number large enough to set the high bit.
			name:    "large number is binary",
			payload: marshal(t, &pingv1.PingRequest{Number: 300}),
			want:    false,
		},
		{
			// Multi-byte UTF-8 in the string is still UTF-8.
			name:    "emoji is text",
			payload: marshal(t, &pingv1.PingRequest{Text: "hello 🎉"}),
			want:    true,
		},
		{
			// The invariant the whole rule exists to protect: an empty
			// message is binary, so it can never read as the separator.
			name:    "empty message is binary",
			payload: marshal(t, &pingv1.PingRequest{}),
			want:    false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := dataIsText(test.payload); got != test.want {
				t.Errorf("dataIsText(% x) = %v, want %v", test.payload, got, test.want)
			}
		})
	}
}

// TestEmptyProtoMessageEncodesToNothing is the premise the separator rule
// rests on. If this ever stopped being true, "an empty text message is the
// separator" would stop needing its carve-out — and if it silently became
// true of some other encoding, the carve-out would stop being enough.
func TestEmptyProtoMessageEncodesToNothing(t *testing.T) {
	t.Parallel()
	data, err := proto.Marshal(&pingv1.PingRequest{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("an empty PingRequest encoded to %d bytes, want 0", len(data))
	}
}
