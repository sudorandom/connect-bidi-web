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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"strings"

	"connectrpc.com/connect/v2"
)

// The leading-metadata message (M). Its payload is a JSON object mapping
// header names to arrays of strings — not wrapped in a "metadata" key as
// drafts 1 through 5 did:
//
//	{"acme-tenant":["tenant-42"],"authorization":["Bearer ..."]}
//
// Keys are case-insensitive and emitted lower-case; two keys that fold to
// the same name are a protocol error, since a receiver would otherwise
// silently pick one. A key ending in -bin carries bytes, base64-encoded
// with the standard alphabet and no padding, as Connect's binary headers
// already are.

// marshalMetadata encodes header as the payload of an M message. Values
// are validated as they go, so a client cannot emit a message the server
// is required to reject: a -bin value is normalized to unpadded base64,
// and any other value must be a valid field value.
func marshalMetadata(header http.Header) ([]byte, error) {
	keys := make([]string, 0, len(header))
	for key := range header {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	buf.WriteByte('{')
	first := true
	for _, key := range keys {
		values := header[key]
		if len(values) == 0 {
			continue
		}
		name := strings.ToLower(key)
		if !validFieldName(name) {
			return nil, fmt.Errorf("metadata key %q is not a valid HTTP field name", key)
		}
		if !first {
			buf.WriteByte(',')
		}
		first = false
		nameJSON, err := json.Marshal(name)
		if err != nil {
			return nil, err
		}
		buf.Write(nameJSON)
		buf.WriteString(":[")
		for i, value := range values {
			if isBinaryName(name) {
				decoded, err := connect.DecodeBinaryHeader(value)
				if err != nil {
					return nil, fmt.Errorf("metadata %s: value is not base64: %w", key, err)
				}
				value = base64.RawStdEncoding.EncodeToString(decoded)
			} else if !validFieldValue(value) {
				return nil, fmt.Errorf("metadata %s: value contains CR, LF, or NUL", key)
			}
			if i > 0 {
				buf.WriteByte(',')
			}
			valueJSON, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}
			buf.Write(valueJSON)
		}
		buf.WriteByte(']')
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// unmarshalMetadata decodes the payload of an M message into canonical
// http.Header form. It walks the JSON token by token rather than through a
// map so that duplicate keys — including ones that differ only in case —
// are caught rather than silently collapsed.
func unmarshalMetadata(data []byte) (http.Header, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := expectDelim(decoder, '{'); err != nil {
		return nil, err
	}
	header := make(http.Header)
	seen := make(map[string]struct{})
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, fmt.Errorf("metadata key %v is not a string", keyToken)
		}
		name := strings.ToLower(key)
		if !validFieldName(name) {
			return nil, fmt.Errorf("metadata key %q is not a valid HTTP field name", key)
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("metadata key %q appears more than once", key)
		}
		seen[name] = struct{}{}

		var values []string
		if err := decoder.Decode(&values); err != nil {
			return nil, fmt.Errorf("metadata %s: value must be an array of strings: %w", key, err)
		}
		for _, value := range values {
			if isBinaryName(name) {
				if _, err := connect.DecodeBinaryHeader(value); err != nil {
					return nil, fmt.Errorf("metadata %s: value is not base64: %w", key, err)
				}
			} else if !validFieldValue(value) {
				return nil, fmt.Errorf("metadata %s: value contains CR, LF, or NUL", key)
			}
		}
		header[http.CanonicalHeaderKey(name)] = values
	}
	if err := expectDelim(decoder, '}'); err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("metadata message has trailing data")
	}
	return header, nil
}

// expectDelim consumes one JSON delimiter.
func expectDelim(decoder *json.Decoder, want json.Delim) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("metadata message is not a JSON object: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != want {
		return fmt.Errorf("metadata message is not a JSON object: got %v, want %c", token, want)
	}
	return nil
}

// isBinaryName reports whether a lower-case header name carries base64
// bytes.
func isBinaryName(name string) bool {
	return strings.HasSuffix(name, "-bin")
}

// validFieldName reports whether name is an RFC 9110 token.
func validFieldName(name string) bool {
	if name == "" {
		return false
	}
	for i := range len(name) {
		if !isTokenChar(name[i]) {
			return false
		}
	}
	return true
}

func isTokenChar(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}
	return strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0
}

// validFieldValue reports whether value may travel as an HTTP field value:
// no CR, LF, or NUL.
func validFieldValue(value string) bool {
	return !strings.ContainsAny(value, "\r\n\x00")
}

// The names a client's leading-metadata message must not carry. A server
// ends the RPC with an error rather than ignoring the key, because
// ignoring it would leave the two ends disagreeing about the effective
// headers with nothing on the wire to show it.

// fetchForbiddenHeaders is the Fetch standard's forbidden request-header
// list, in canonical form. Names beginning Proxy- or Sec- are forbidden by
// prefix, below.
var fetchForbiddenHeaders = map[string]struct{}{ //nolint:gochecknoglobals
	"Accept-Charset":                 {},
	"Accept-Encoding":                {},
	"Access-Control-Request-Headers": {},
	"Access-Control-Request-Method":  {},
	"Connection":                     {},
	"Content-Length":                 {},
	"Cookie":                         {},
	"Cookie2":                        {},
	"Date":                           {},
	"Dnt":                            {},
	"Expect":                         {},
	"Host":                           {},
	"Keep-Alive":                     {},
	"Origin":                         {},
	"Referer":                        {},
	"Set-Cookie":                     {},
	"Te":                             {},
	"Trailer":                        {},
	"Transfer-Encoding":              {},
	"Upgrade":                        {},
	"Via":                            {},
	"X-Http-Method":                  {},
	"X-Http-Method-Override":         {},
	"X-Method-Override":              {},
}

// protocolControlledHeaders are the names this protocol owns: the
// subprotocol implies the protocol version and the codec, the query string
// carries the deadline, and compression has no per-message form here.
var protocolControlledHeaders = map[string]struct{}{ //nolint:gochecknoglobals
	"Connect-Protocol-Version": {},
	"Connect-Timeout-Ms":       {},
	"Content-Type":             {},
	"Content-Encoding":         {},
	"Connect-Content-Encoding": {},
	"Connect-Accept-Encoding":  {},
}

// defaultInfrastructureHeaders is the default deny list of names the
// server's own infrastructure sets, which a client must not be able to
// forge. Entries ending in "-" are prefixes.
func defaultInfrastructureHeaders() []string {
	return []string{"Forwarded", "X-Forwarded-", "X-Real-Ip"}
}

// reservedHeaderReason reports why a canonical header name may not appear
// in a client's leading-metadata message, or "" if it may.
func reservedHeaderReason(canonicalName string, infrastructure []string) string {
	if _, ok := fetchForbiddenHeaders[canonicalName]; ok {
		return "a forbidden request header"
	}
	if strings.HasPrefix(canonicalName, "Proxy-") || strings.HasPrefix(canonicalName, "Sec-") {
		return "a forbidden request header"
	}
	if _, ok := protocolControlledHeaders[canonicalName]; ok {
		return "controlled by the protocol"
	}
	if slices.ContainsFunc(infrastructure, func(entry string) bool {
		if strings.HasSuffix(entry, "-") {
			return strings.HasPrefix(canonicalName, entry)
		}
		return entry == canonicalName
	}) {
		return "set by the server's infrastructure"
	}
	return ""
}

// clientMetadataHeaders are the headers a client strips from the request
// metadata it sends in M, because the protocol carries them elsewhere or
// not at all. Everything else the caller set goes out as-is, and a
// reserved name is the server's to reject.
func clientMetadataHeaders(header *connect.Header) http.Header {
	out := make(http.Header)
	if header == nil {
		return out
	}
	for key, values := range header.All() {
		if _, isProtocol := protocolHeaders[key]; isProtocol {
			continue
		}
		out[key] = append([]string(nil), values...)
	}
	return out
}
