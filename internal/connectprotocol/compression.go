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
	"strings"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectgzip"
)

// DefaultCompressors returns the compressors available to transports by
// default, in registration order.
func DefaultCompressors() (map[string]connect.Compressor, []string) {
	gzipCompressor := connectgzip.New()
	return map[string]connect.Compressor{
		gzipCompressor.Name(): gzipCompressor,
	}, []string{gzipCompressor.Name()}
}

// NegotiateCompression selects the first accepted compressor supported by the
// server. AcceptEncoding is a comma-separated list in preference order.
func NegotiateCompression(
	acceptEncoding string,
	compressors map[string]connect.Compressor,
) (string, connect.Compressor) {
	for part := range strings.SplitSeq(acceptEncoding, ",") {
		name := strings.TrimSpace(part)
		if compressor := compressors[name]; compressor != nil {
			return name, compressor
		}
	}
	return "", nil
}
