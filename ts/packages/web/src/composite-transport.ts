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

import type { Transport } from "@connectrpc/connect";

/**
 * createCompositeTransport creates a Transport that delegates Unary RPCs to one
 * transport, and Streaming RPCs (Client, Server, Bidi) to another.
 *
 * This is typically used to route Unary RPCs over standard HTTP/1.1 or HTTP/2
 * (using createConnectTransport) for better observability, caching, and proxy
 * support, while using WebTransport or WebSockets for streaming RPCs.
 */
export function createCompositeTransport(
  unaryTransport: Transport,
  streamingTransport: Transport,
): Transport {
  return {
    unary(method, signal, timeoutMs, header, message, contextValues) {
      return unaryTransport.unary(
        method,
        signal,
        timeoutMs,
        header,
        message,
        contextValues,
      );
    },
    stream(method, signal, timeoutMs, header, input, contextValues) {
      return streamingTransport.stream(
        method,
        signal,
        timeoutMs,
        header,
        input,
        contextValues,
      );
    },
  };
}
