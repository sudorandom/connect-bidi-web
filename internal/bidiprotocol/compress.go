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

func decompress(compressor connect.Compressor, payload []byte) ([]byte, error) {
	reader, err := compressor.Decompress(bytes.NewReader(payload))
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to decompress message: %v", err)
	}
	decompressed, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to read decompressed message: %v", err)
	}
	return decompressed, nil
}
