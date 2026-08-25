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
	"math"
	"strings"
	"testing"
)

func TestAppendFrameHead(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		streamID  uint32
		frameType uint8
		want      string
	}{
		{streamID: 1, frameType: frameTypeData, want: "1|0|"},
		{streamID: 7, frameType: frameTypeHeaders, want: "7|1|"},
		{streamID: 12, frameType: frameTypeEndStream, want: "12|2|"},
		{streamID: 12, frameType: frameTypeReset, want: "12|3|"},
		{streamID: math.MaxUint32, frameType: frameTypeReset, want: "4294967295|3|"},
	} {
		if got := string(appendFrameHead(nil, test.streamID, test.frameType)); got != test.want {
			t.Errorf("appendFrameHead(%d, %d) = %q, want %q", test.streamID, test.frameType, got, test.want)
		}
	}
	// The longest legal head must still fit the parser's scan window, or
	// parseFrame would reject frames appendFrameHead produced.
	if got := len(appendFrameHead(nil, math.MaxUint32, math.MaxUint8)); got > maxHeadLen {
		t.Errorf("longest head is %d bytes, exceeding the %d-byte scan window", got, maxHeadLen)
	}
}

// TestFrameTypeOf pins the flags field's bit layout: the low 3 bits are the
// frame type, the high 5 are flags that a receiver must ignore rather than
// reject, so a later revision can define one without breaking this one.
func TestFrameTypeOf(t *testing.T) {
	t.Parallel()
	// The masks must partition the byte exactly: no overlap, no gaps.
	if frameTypeMask&frameFlagsMask != 0 {
		t.Errorf("type mask %#02x and flags mask %#02x overlap", frameTypeMask, frameFlagsMask)
	}
	if frameTypeMask|frameFlagsMask != 0xFF {
		t.Errorf("masks %#02x|%#02x leave gaps, want full coverage", frameTypeMask, frameFlagsMask)
	}
	// Every defined type must fit the type mask, or ORing a flag in would
	// corrupt it.
	for _, frameType := range []uint8{frameTypeData, frameTypeHeaders, frameTypeEndStream, frameTypeReset} {
		if frameType&frameFlagsMask != 0 {
			t.Errorf("frame type %d overflows into the flag bits", frameType)
		}
	}
	// Bit math: every flag bit ORed onto every type must leave the type
	// recoverable.
	for _, frameType := range []uint8{frameTypeData, frameTypeHeaders, frameTypeEndStream, frameTypeReset} {
		for _, flag := range []uint8{0x08, 0x10, 0x20, 0x40, 0x80} {
			if got := frameTypeOf(frameType | flag); got != frameType {
				t.Errorf("frameTypeOf(%d|%#02x) = %d, want %d", frameType, flag, got, frameType)
			}
		}
		// ...and all of them at once.
		if got := frameTypeOf(frameType | frameFlagsMask); got != frameType {
			t.Errorf("frameTypeOf(%d|all flags) = %d, want %d", frameType, got, frameType)
		}
	}
}

// TestFrameHeadWithFlags shows the wire form a future flag would take: the
// head stays three fields, and the flags value is just a bigger number.
func TestFrameHeadWithFlags(t *testing.T) {
	t.Parallel()
	const flagFuture uint8 = 0x08 // the first free bit
	head := string(appendFrameHead(nil, 7, frameTypeData|flagFuture))
	if want := "7|8|"; head != want {
		t.Errorf("head = %q, want %q", head, want)
	}
	_, flags, _, err := parseFrame([]byte(head))
	if err != nil {
		t.Fatalf("parseFrame(%q) failed: %v", head, err)
	}
	if got := frameTypeOf(flags); got != frameTypeData {
		t.Errorf("frame type = %d, want %d (data)", got, frameTypeData)
	}
	if flags&flagFuture == 0 {
		t.Error("the flag bit did not survive the round trip")
	}
}

func TestParseFrame(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		frame       string
		wantID      uint32
		wantType    uint8
		wantPayload string
	}{
		{
			name:        "headers",
			frame:       `7|1|{"metadata":{":path":["/x.Y/Z"]}}`,
			wantID:      7,
			wantType:    frameTypeHeaders,
			wantPayload: `{"metadata":{":path":["/x.Y/Z"]}}`,
		},
		{
			name:     "empty payload",
			frame:    "3|2|",
			wantID:   3,
			wantType: frameTypeEndStream,
		},
		{
			// The parser splits on the first two separators only, so a
			// payload is free to contain the separator byte -- which JSON
			// strings and protobuf bytes both do, unescaped.
			name:        "payload containing separators",
			frame:       `4|0|a|b||c|`,
			wantID:      4,
			wantType:    frameTypeData,
			wantPayload: `a|b||c|`,
		},
		{
			name:        "max stream id",
			frame:       "4294967295|0|x",
			wantID:      math.MaxUint32,
			wantType:    frameTypeData,
			wantPayload: "x",
		},
		{
			// Senders emit no leading zeros, but accepting them costs
			// nothing and keeps hand-written frames working.
			name:     "leading zeros",
			frame:    "007|01|",
			wantID:   7,
			wantType: frameTypeHeaders,
		},
		{
			// The flags field parses whole; masking to a type happens
			// later, so a set flag bit reaches the stream code.
			name:     "all flag bits set",
			frame:    "1|255|",
			wantID:   1,
			wantType: 255,
		},
		{
			// A reserved frame type (4-7) must parse, so the stream code can
			// reject it as an unknown type rather than as a broken frame.
			name:     "reserved frame type",
			frame:    "1|7|",
			wantID:   1,
			wantType: 7,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			gotID, gotType, gotPayload, err := parseFrame([]byte(test.frame))
			if err != nil {
				t.Fatalf("parseFrame(%q) failed: %v", test.frame, err)
			}
			if gotID != test.wantID {
				t.Errorf("stream ID = %d, want %d", gotID, test.wantID)
			}
			if gotType != test.wantType {
				t.Errorf("frame type = %d, want %d", gotType, test.wantType)
			}
			if string(gotPayload) != test.wantPayload {
				t.Errorf("payload = %q, want %q", gotPayload, test.wantPayload)
			}
		})
	}
}

func TestParseFrameErrors(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		frame string
	}{
		{name: "empty", frame: ""},
		{name: "no separator", frame: "1"},
		{name: "one separator", frame: "1|0"},
		{name: "missing stream id", frame: "|0|x"},
		{name: "missing flags", frame: "1||x"},
		{name: "non-numeric stream id", frame: "abc|0|x"},
		{name: "non-numeric flags", frame: "1|data|x"},
		{name: "negative stream id", frame: "-1|0|x"},
		{name: "stream id overflows uint32", frame: "4294967296|0|x"},
		{name: "flags overflow uint8", frame: "1|256|x"},
		// A hostile peer must not be able to make the parser scan a whole
		// large message looking for separators.
		{name: "separators beyond the scan window", frame: strings.Repeat("9", 64) + "|0|x"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, _, _, err := parseFrame([]byte(test.frame)); err == nil {
				t.Errorf("parseFrame(%q) succeeded, want an error", test.frame)
			}
		})
	}
}

// TestParseFrameAliasesPayload documents that the payload is a subslice of
// the message, not a copy: the read loop hands it straight to a stream.
func TestParseFrameAliasesPayload(t *testing.T) {
	t.Parallel()
	message := []byte("1|0|payload")
	_, _, payload, err := parseFrame(message)
	if err != nil {
		t.Fatalf("parseFrame failed: %v", err)
	}
	if len(payload) == 0 || &payload[0] != &message[len("1|0|")] {
		t.Error("payload does not alias the message")
	}
}

func TestFrameRoundTrip(t *testing.T) {
	t.Parallel()
	payload := []byte(`{"pipe":"a|b"}`)
	frame := appendFrameHead(nil, 9, frameTypeData)
	frame = append(frame, payload...)

	gotID, gotType, gotPayload, err := parseFrame(frame)
	if err != nil {
		t.Fatalf("parseFrame failed: %v", err)
	}
	if gotID != 9 || gotType != frameTypeData {
		t.Errorf("head round-trip = (%d, %d), want (9, %d)", gotID, gotType, frameTypeData)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Errorf("payload round-trip = %q, want %q", gotPayload, payload)
	}
}
