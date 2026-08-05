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

import type {
  DescMessage,
  DescMethodStreaming,
  DescMethodUnary,
  MessageInitShape,
} from "@bufbuild/protobuf";
import type {
  ContextValues,
  StreamResponse,
  Transport,
  UnaryResponse,
} from "@connectrpc/connect";

/**
 * A `Transport` whose underlying transport can be swapped out at runtime.
 *
 * `createClient` binds to a `Transport` instance once. The live demo lets
 * visitors switch between WebTransport and WebSocket from a dropdown, so we
 * hand `createClient` this stable wrapper and swap what it delegates to
 * underneath, instead of recreating the client on every change.
 */
export class SwappableTransport implements Transport {
  private current: Transport;

  constructor(initial: Transport) {
    this.current = initial;
  }

  /** Replaces the transport that subsequent calls will delegate to. */
  swap(next: Transport): void {
    this.current = next;
  }

  unary<I extends DescMessage, O extends DescMessage>(
    method: DescMethodUnary<I, O>,
    signal: AbortSignal | undefined,
    timeoutMs: number | undefined,
    header: HeadersInit | undefined,
    message: MessageInitShape<I>,
    contextValues?: ContextValues,
  ): Promise<UnaryResponse<I, O>> {
    return this.current.unary(
      method,
      signal,
      timeoutMs,
      header,
      message,
      contextValues,
    );
  }

  stream<I extends DescMessage, O extends DescMessage>(
    method: DescMethodStreaming<I, O>,
    signal: AbortSignal | undefined,
    timeoutMs: number | undefined,
    header: HeadersInit | undefined,
    input: AsyncIterable<MessageInitShape<I>>,
    contextValues?: ContextValues,
  ): Promise<StreamResponse<I, O>> {
    return this.current.stream(
      method,
      signal,
      timeoutMs,
      header,
      input,
      contextValues,
    );
  }
}
