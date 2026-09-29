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
	"net/http"
	"strings"
	"testing"
)

// TestMarshalMetadata pins the payload shape of an M message: a flat JSON
// object, lower-case keys in sorted order, array values, and unpadded
// base64 for -bin keys.
func TestMarshalMetadata(t *testing.T) {
	t.Parallel()
	got, err := marshalMetadata(http.Header{
		"X-Zebra":     {"z"},
		"Acme-Tenant": {"tenant-42", "tenant-43"},
		"X-Data-Bin":  {"/wAB"}, // unpadded: kept
		"X-More-Bin":  {"/wA="}, // padded: normalized
		"X-Empty":     {},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"acme-tenant":["tenant-42","tenant-43"],"x-data-bin":["/wAB"],"x-more-bin":["/wA"],"x-zebra":["z"]}`
	if string(got) != want {
		t.Errorf("marshalMetadata =\n %s\nwant\n %s", got, want)
	}

	empty, err := marshalMetadata(nil)
	if err != nil || string(empty) != "{}" {
		t.Errorf("marshalMetadata(nil) = %s, %v; want {}", empty, err)
	}

	if _, err := marshalMetadata(http.Header{"X-Bad": {"line\nbreak"}}); err == nil {
		t.Error("a value with a line break was marshaled")
	}
	if _, err := marshalMetadata(http.Header{"X-Bad-Bin": {"not base64!"}}); err == nil {
		t.Error("a -bin value that is not base64 was marshaled")
	}
	if _, err := marshalMetadata(http.Header{"bad key": {"v"}}); err == nil {
		t.Error("a key that is not a token was marshaled")
	}
}

// TestUnmarshalMetadata pins what a receiver accepts and refuses.
func TestUnmarshalMetadata(t *testing.T) {
	t.Parallel()
	header, err := unmarshalMetadata([]byte(`{"Acme-Tenant":["a","b"],"x-data-bin":["/wAB"]}`))
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := header.Values("Acme-Tenant"); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("Acme-Tenant = %v, want [a b]", got)
	}
	if got := header.Get("X-Data-Bin"); got != "/wAB" {
		t.Errorf("X-Data-Bin = %q, want /wAB", got)
	}

	rejected := []struct {
		name string
		data string
		want string
	}{
		{"empty", ``, "not a JSON object"},
		{"array", `[]`, "not a JSON object"},
		{"bare string value", `{"k":"v"}`, "array of strings"},
		{"non-string element", `{"k":[1]}`, "array of strings"},
		{"duplicate key", `{"k":["a"],"k":["b"]}`, "more than once"},
		{"duplicate by case", `{"K":["a"],"k":["b"]}`, "more than once"},
		{"invalid name", `{"bad key":["a"]}`, "not a valid HTTP field name"},
		{"CR in value", "{\"k\":[\"a\\rb\"]}", "CR, LF, or NUL"},
		{"bad base64", `{"k-bin":["***"]}`, "not base64"},
		{"trailing data", `{} {}`, "trailing data"},
	}
	for _, test := range rejected {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := unmarshalMetadata([]byte(test.data))
			if err == nil {
				t.Fatalf("unmarshalMetadata(%q) succeeded", test.data)
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Errorf("error = %v, want it to mention %q", err, test.want)
			}
		})
	}
}

// TestReservedHeaderReason pins the three sources of reserved names.
func TestReservedHeaderReason(t *testing.T) {
	t.Parallel()
	infra := defaultInfrastructureHeaders()
	tests := map[string]bool{
		"Cookie":                   true,
		"Host":                     true,
		"Sec-Websocket-Key":        true,
		"Proxy-Authorization":      true,
		"X-Http-Method-Override":   true,
		"Forwarded":                true,
		"X-Forwarded-For":          true,
		"X-Real-Ip":                true,
		"Connect-Protocol-Version": true,
		"Connect-Timeout-Ms":       true,
		"Content-Type":             true,
		"Connect-Accept-Encoding":  true,
		"Authorization":            false,
		"Acme-Tenant":              false,
		"User-Agent":               false,
		"Accept":                   false,
	}
	for name, reserved := range tests {
		if got := reservedHeaderReason(name, infra) != ""; got != reserved {
			t.Errorf("reservedHeaderReason(%q) reserved = %v, want %v", name, got, reserved)
		}
	}
}
