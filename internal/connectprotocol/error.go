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
	"context"
	"errors"

	"connectrpc.com/connect/v2"
)

// ErrorForWire converts an arbitrary error into a *connect.Error suitable
// for sending in an end-stream payload. Errors that arrived from a remote
// peer are wrapped so they aren't forwarded verbatim.
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
