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

package connectprotocol

import (
	"encoding/json"
	"net/http"
	"strings"
)

// HeaderMessage is the JSON payload of a headers envelope (flag 0x06). Like
// EndStreamMessage, it is defined directly in code rather than as a protobuf
// message. On the wire it looks like:
//
//	{"metadata": {":path": ["/pkg.Service/Method"], "content-type": ["application/connect+proto"]}}
//
// Metadata keys are HTTP-header-shaped; values are string lists. Keys
// beginning with ":" are pseudo-headers (only ":path" is currently used) and
// are not surfaced to applications. The TypeScript packages define the
// equivalent HeadersMessage type.
type HeaderMessage struct {
	Metadata http.Header `json:"metadata,omitempty"`
}

// MarshalHeaders serializes an HTTP header map into JSON.
func MarshalHeaders(header http.Header) ([]byte, error) {
	msg := HeaderMessage{Metadata: header}
	return json.Marshal(&msg)
}

// UnmarshalHeaders parses a JSON header control frame payload.
func UnmarshalHeaders(data []byte) (http.Header, error) {
	var msg HeaderMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	res := make(http.Header)
	if msg.Metadata != nil {
		for k, vs := range msg.Metadata {
			if strings.HasPrefix(k, ":") {
				res[k] = vs
			} else {
				res[http.CanonicalHeaderKey(k)] = vs
			}
		}
	}
	return res, nil
}
