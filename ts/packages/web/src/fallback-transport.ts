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
import { Code, ConnectError } from "@connectrpc/connect";

/**
 * A Transport that degrades through a ladder of transports plus control
 * over the rungs that hold live connections.
 */
export interface FallbackTransport extends Transport {
  /**
   * Closes every rung that has a `close()` (the WebSocket transports'
   * shared multiplexed connections). The transport remains usable: the
   * next RPC reconnects.
   */
  close(): void;
}

/**
 * Wraps a request-message iterable so each ladder attempt gets an
 * independent view over one shared source. This matters twice over: a
 * failed attempt's call pipeline closes the iterable it was handed
 * (connect-es aborts the request flow, which would silently empty the
 * retry's input), and any messages a failed attempt already pulled must be
 * replayed to the next one. Views never close the source, and messages
 * are recorded until an attempt succeeds — after that no further attempt
 * can happen for this call, so recording stops and the buffer is dropped.
 */
function replayableIterable<T>(source: AsyncIterable<T>): {
  view(): AsyncIterable<T>;
  commit(): void;
} {
  const iterator = source[Symbol.asyncIterator]();
  let buffer: T[] = [];
  let recording = true;
  return {
    commit() {
      recording = false;
      buffer = [];
    },
    view(): AsyncIterable<T> {
      // Copy, not alias: this view records into `buffer` as it pulls, and
      // must not replay its own recordings back to itself.
      const replay = [...buffer];
      let replayIndex = 0;
      return {
        [Symbol.asyncIterator]() {
          return {
            next: async (): Promise<IteratorResult<T>> => {
              if (replayIndex < replay.length) {
                return { done: false, value: replay[replayIndex++] };
              }
              const result = await iterator.next();
              if (result.done !== true && recording) {
                buffer.push(result.value);
              }
              return result;
            },
            // No `return`: closing a view must not close the shared
            // source, which the next attempt still needs.
          };
        },
      };
    },
  };
}

/**
 * createFallbackTransport creates a Transport that tries a ladder of
 * transports in order, best-first, and remembers the rung that works. The
 * canonical browser ladder is WebTransport, then WebSocket:
 *
 * ```ts
 * const transport = createFallbackTransport(
 *   createConnectWebTransportTransport({ baseUrl, session }),
 *   createConnectWebSocketDraft2Transport({ baseUrl }),
 * );
 * ```
 *
 * (The WebSocket rung internally spans both HTTP/2 and HTTP/1.1: the
 * browser bootstraps `new WebSocket(...)` over an existing HTTP/2
 * connection when the server advertises RFC 8441 extended CONNECT
 * support, falling back to an HTTP/1.1 upgrade otherwise. JavaScript can
 * neither force nor observe that choice, so it cannot be a separate
 * rung.)
 *
 * A rung is skipped when it fails with Code.Unavailable — what these
 * transports throw when the connection itself cannot be established —
 * and, if the bottom of the ladder is reached, the search wraps around to
 * the top, so a transport that recovered is eventually retried. Any other
 * error surfaces unchanged, without trying further rungs.
 */
export function createFallbackTransport(
  ...transports: Transport[]
): FallbackTransport {
  if (transports.length === 0) {
    throw new Error("createFallbackTransport requires at least one transport");
  }
  let current = 0;

  async function withLadder<T>(
    run: (transport: Transport) => Promise<T>,
  ): Promise<T> {
    const start = current;
    let lastError: unknown;
    for (let attempt = 0; attempt < transports.length; attempt++) {
      const index = (start + attempt) % transports.length;
      try {
        const result = await run(transports[index]);
        current = index;
        return result;
      } catch (error) {
        if (ConnectError.from(error).code !== Code.Unavailable) {
          // An RPC-level failure, not a transport-establishment one:
          // surface it rather than replaying the RPC elsewhere.
          throw error;
        }
        lastError = error;
      }
    }
    throw lastError;
  }

  return {
    unary(method, signal, timeoutMs, header, message, contextValues) {
      return withLadder((transport) =>
        transport.unary(
          method,
          signal,
          timeoutMs,
          header,
          message,
          contextValues,
        ),
      );
    },
    stream(method, signal, timeoutMs, header, input, contextValues) {
      const source = replayableIterable(input);
      return withLadder(async (transport) => {
        const response = await transport.stream(
          method,
          signal,
          timeoutMs,
          header,
          source.view(),
          contextValues,
        );
        source.commit();
        return response;
      });
    },
    close() {
      for (const transport of transports) {
        if ("close" in transport && typeof transport.close === "function") {
          transport.close();
        }
      }
    },
  };
}
