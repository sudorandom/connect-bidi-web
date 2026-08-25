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

import { handleMuxedBidiSocketDraft1 } from "@sudorandom/connect-bidi-core";
import type { UniversalHandler } from "@connectrpc/connect/protocol";
import type { CreateBidiWebSocketHandlerOptions } from "./bidi-websocket-handler-common.js";
import { isWebSocketUpgrade } from "./bidi-websocket-handler-common.js";
import { wrapWebSocket } from "./websocket-like.js";

/**
 * Creates a fetch-handler helper that upgrades `Upgrade: websocket`
 * requests into a multiplexed draft 1 bidi connection, bridged to
 * `handlers` via `@sudorandom/connect-bidi-core`'s
 * `handleMuxedBidiSocketDraft1`. Get `handlers` from
 * `createConnectRouter(...).handlers` (`@connectrpc/connect`).
 *
 * The returned function returns the 101 upgrade `Response` for WebSocket
 * upgrade requests, or `null` for everything else, so callers can fall
 * through to their own Connect-over-fetch handling:
 *
 * ```ts
 * const bidiWebSocket = createBidiWebSocketDraft1Handler(handlers);
 *
 * export default {
 *   fetch(request: Request): Response | Promise<Response> {
 *     return bidiWebSocket(request) ?? handleConnectFetch(request);
 *   },
 * };
 * ```
 *
 * One WebSocket connection carries any number of concurrent RPCs,
 * demultiplexed by the stream ID on every frame, matching the Go and Node
 * adapters.
 */
export function createBidiWebSocketDraft1Handler(
  handlers: readonly UniversalHandler[],
  options?: CreateBidiWebSocketHandlerOptions,
): (request: Request) => Response | null {
  return function handleUpgrade(request: Request): Response | null {
    if (!isWebSocketUpgrade(request)) {
      return null;
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();

    handleMuxedBidiSocketDraft1(wrapWebSocket(server), handlers, options).catch(
      (error: unknown) => {
        options?.onError?.(error);
      },
    );

    return new Response(null, { status: 101, webSocket: client });
  };
}
