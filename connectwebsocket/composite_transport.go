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

package connectwebsocket

import (
	"context"

	"connectrpc.com/connect/v2"
)

// CompositeTransport is a [connect.Transport] that routes Unary RPCs to one
// transport, and Streaming RPCs (Client, Server, Bidi) to another.
// This is typically used to route Unary RPCs over standard HTTP/1.1 or HTTP/2
// for better observability, caching, and proxy support, while using WebTransport
// or WebSockets for streaming RPCs.
type CompositeTransport struct {
	Unary     connect.Transport
	Streaming connect.Transport
}

// NewCompositeTransport returns a new CompositeTransport.
func NewCompositeTransport(unary connect.Transport, streaming connect.Transport) *CompositeTransport {
	return &CompositeTransport{
		Unary:     unary,
		Streaming: streaming,
	}
}

// NewClientStream implements [connect.Transport].
func (c *CompositeTransport) NewClientStream(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
	if spec.StreamType == connect.StreamTypeUnary {
		return c.Unary.NewClientStream(ctx, spec)
	}
	return c.Streaming.NewClientStream(ctx, spec)
}
