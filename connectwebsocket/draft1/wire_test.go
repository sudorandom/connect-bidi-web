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
	"bytes"
	"testing"
)

// TestEnvelopeLayout pins the five-byte head: a flag byte, then the payload
// length as a big-endian uint32.
func TestEnvelopeLayout(t *testing.T) {
	t.Parallel()
	got, err := encodeEnvelope(flagData, []byte("hello"))
	if err != nil {
		t.Fatalf("encodeEnvelope: %v", err)
	}
	want := []byte{0x00, 0x00, 0x00, 0x00, 0x05, 'h', 'e', 'l', 'l', 'o'}
	if !bytes.Equal(got, want) {
		t.Errorf("encodeEnvelope = % x, want % x", got, want)
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		flag    uint8
		payload []byte
	}{
		{"empty data", flagData, nil},
		{"data", flagData, []byte{0x01, 0x02, 0x03}},
		{"headers", flagHeaders, []byte(`{"metadata":{}}`)},
		{"empty end-stream", flagEndStream, nil},
		{"end-stream", flagEndStream, []byte(`{"metadata":{}}`)},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			message, err := encodeEnvelope(test.flag, test.payload)
			if err != nil {
				t.Fatalf("encodeEnvelope: %v", err)
			}
			flag, payload, err := decodeEnvelope(message)
			if err != nil {
				t.Fatalf("decodeEnvelope: %v", err)
			}
			if flag != test.flag {
				t.Errorf("flag = 0x%02x, want 0x%02x", flag, test.flag)
			}
			if !bytes.Equal(payload, test.payload) && len(payload)+len(test.payload) > 0 {
				t.Errorf("payload = % x, want % x", payload, test.payload)
			}
		})
	}
}

// TestDecodeEnvelopeRejects: a message carries one whole envelope and
// nothing else, so anything else is the peer framing something this
// protocol cannot represent.
func TestDecodeEnvelopeRejects(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		message []byte
	}{
		{"empty message", nil},
		{"truncated head", []byte{0x00, 0x00, 0x00}},
		{"length longer than the message", []byte{0x00, 0x00, 0x00, 0x00, 0x09, 'a'}},
		{"length shorter than the message", []byte{0x00, 0x00, 0x00, 0x00, 0x01, 'a', 'b'}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, _, err := decodeEnvelope(test.message); err == nil {
				t.Errorf("decodeEnvelope(% x) succeeded, want an error", test.message)
			}
		})
	}
}

// TestProcedureFromPath: the procedure is the URL's last two segments, so
// one handler serves whether it is mounted on the Connect procedure URLs or
// under a prefix.
func TestProcedureFromPath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		path string
		want string
	}{
		{"/connectbidi.ping.v1.PingService/CumSum", "/connectbidi.ping.v1.PingService/CumSum"},
		{"/websocket-draft1/connectbidi.ping.v1.PingService/CumSum", "/connectbidi.ping.v1.PingService/CumSum"},
		{"/a/b/connectbidi.ping.v1.PingService/CumSum", "/connectbidi.ping.v1.PingService/CumSum"},
		{"", ""},
		{"/", ""},
		{"/OnlyOneSegment", ""},
		{"/Service/", ""},
		{"//Method", ""},
	} {
		t.Run(test.path, func(t *testing.T) {
			t.Parallel()
			if got := procedureFromPath(test.path); got != test.want {
				t.Errorf("procedureFromPath(%q) = %q, want %q", test.path, got, test.want)
			}
		})
	}
}

// TestCodecNameForContentType: draft 1 carries the Connect content types
// and nothing else.
func TestCodecNameForContentType(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		contentType string
		want        string
	}{
		{"application/connect+proto", "proto"},
		{"application/connect+json", "json"},
		{" application/connect+json ", "json"},
		{"application/connect+", ""},
		{"application/proto", ""},
		{"", ""},
	} {
		t.Run(test.contentType, func(t *testing.T) {
			t.Parallel()
			if got := codecNameForContentType(test.contentType); got != test.want {
				t.Errorf("codecNameForContentType(%q) = %q, want %q", test.contentType, got, test.want)
			}
		})
	}
}
