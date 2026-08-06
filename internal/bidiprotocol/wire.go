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
	"context"
	"errors"
	"net/http"

	"connectrpc.com/connect/v2"
	"github.com/sudorandom/connect-bidi-web/internal/connectprotocol"
)

// ErrorForWire converts an arbitrary error into a *connect.Error suitable for
// sending in an end-stream envelope. Errors that arrived from a remote peer
// are wrapped so they aren't forwarded verbatim.
func ErrorForWire(err error) *connect.Error {
	var cerr *connect.Error
	if errors.As(err, &cerr) {
		if cerr.IsRemote() {
			return connect.Errorf(cerr.Code(), "").WithCause(err)
		}
		return cerr
	}
	switch {
	case errors.Is(err, context.Canceled):
		return connect.Errorf(connect.CodeCanceled, "").WithCause(err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.Errorf(connect.CodeDeadlineExceeded, "").WithCause(err)
	}
	return connect.Errorf(connect.CodeUnknown, "").WithCause(err)
}

// MarshalHeaders encodes HTTP-style headers for a headers envelope.
func MarshalHeaders(header http.Header) ([]byte, error) {
	return connectprotocol.MarshalHeaders(header)
}

// UnmarshalHeaders decodes the payload of a headers envelope.
func UnmarshalHeaders(data []byte) (http.Header, error) {
	return connectprotocol.UnmarshalHeaders(data)
}

// MarshalEndStream encodes the payload of an end-stream envelope. A nil error
// indicates success.
func MarshalEndStream(err *connect.Error, trailers http.Header) ([]byte, error) {
	var errVal error
	if err != nil {
		errVal = err
	}
	return connectprotocol.MarshalEndStream(errVal, trailers)
}

// UnmarshalEndStream decodes the payload of an end-stream envelope.
func UnmarshalEndStream(data []byte) (*connect.Error, http.Header, error) {
	return connectprotocol.UnmarshalEndStream(data)
}
