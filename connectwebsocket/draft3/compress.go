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

package draft3

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"sync"
)

// Per-frame raw DEFLATE (RFC 1951), with no context takeover: every frame
// is one complete, independently decodable DEFLATE stream. The independence
// is deliberate — it keeps the scheme implementable with the web platform's
// CompressionStream("deflate-raw"), which cannot flush at message
// boundaries and therefore cannot share a window between frames.

var flateWriterPool = sync.Pool{
	New: func() any {
		writer, err := flate.NewWriter(io.Discard, flate.DefaultCompression)
		if err != nil {
			// flate.NewWriter fails only on an invalid level constant.
			panic(err)
		}
		return writer
	},
}

var flateReaderPool = sync.Pool{
	New: func() any {
		return flate.NewReader(bytes.NewReader(nil))
	},
}

// deflatePayload compresses payload, reporting false when compression
// would not shrink it (incompressible data), in which case the caller
// sends the original uncompressed.
func deflatePayload(payload []byte) ([]byte, bool) {
	writer := flateWriterPool.Get().(*flate.Writer) //nolint:forcetypeassert // the pool only holds *flate.Writer
	defer flateWriterPool.Put(writer)
	var buf bytes.Buffer
	writer.Reset(&buf)
	if _, err := writer.Write(payload); err != nil {
		return nil, false
	}
	if err := writer.Close(); err != nil {
		return nil, false
	}
	if buf.Len() >= len(payload) {
		return nil, false
	}
	return buf.Bytes(), true
}

// inflatePayload decompresses one complete raw DEFLATE stream.
func inflatePayload(payload []byte) ([]byte, error) {
	reader := flateReaderPool.Get().(io.ReadCloser) //nolint:forcetypeassert // the pool only holds flate readers
	defer flateReaderPool.Put(reader)
	resetter := reader.(flate.Resetter) //nolint:forcetypeassert // flate readers always implement Resetter
	if err := resetter.Reset(bytes.NewReader(payload), nil); err != nil {
		return nil, fmt.Errorf("reset inflate: %w", err)
	}
	inflated, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("inflate frame payload: %w", err)
	}
	return inflated, nil
}
