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

package bidiprotocol

import (
	"bytes"
	"io"

	"connectrpc.com/connect/v2"
)

func compress(compressor connect.Compressor, payload []byte) ([]byte, error) {
	var buf bytes.Buffer
	compWriter, err := compressor.Compress(&buf)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to compress message: %v", err)
	}
	if _, err := compWriter.Write(payload); err != nil {
		_ = compWriter.Close()
		return nil, connect.Errorf(connect.CodeInternal, "failed to write compressed message: %v", err)
	}
	if err := compWriter.Close(); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to flush compressed message: %v", err)
	}
	return buf.Bytes(), nil
}

// decompress inflates payload. With a positive readMaxBytes it stops at that
// many bytes and reports CodeResourceExhausted, so a small compressed message
// cannot inflate to an arbitrary size in memory.
func decompress(compressor connect.Compressor, payload []byte, readMaxBytes int) ([]byte, error) {
	reader, err := compressor.Decompress(bytes.NewReader(payload))
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to decompress message: %v", err)
	}
	var limited io.Reader = reader
	if readMaxBytes > 0 {
		limited = io.LimitReader(reader, int64(readMaxBytes)+1)
	}
	decompressed, err := io.ReadAll(limited)
	_ = reader.Close()
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to read decompressed message: %v", err)
	}
	if readMaxBytes > 0 && len(decompressed) > readMaxBytes {
		return nil, connect.Errorf(connect.CodeResourceExhausted, "decompressed message exceeds read limit %d", readMaxBytes)
	}
	return decompressed, nil
}
