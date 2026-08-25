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

import { handleMuxedBidiSocketDraft4 } from "@sudorandom/connect-bidi-core";
import type { UniversalHandler } from "@connectrpc/connect/protocol";
import type { CreateBidiWebSocketHandlerOptions } from "./bidi-websocket-handler-common.js";
import { isWebSocketUpgrade } from "./bidi-websocket-handler-common.js";
import { wrapDraft4WebSocket } from "./websocket-like.js";

/**
 * Creates a fetch-handler helper that upgrades `Upgrade: websocket`
 * requests into a multiplexed draft 4 bidi connection, bridged to
 * `handlers` via `@sudorandom/connect-bidi-core`'s
 * `handleMuxedBidiSocketDraft4`. Get `handlers` from
 * `createConnectRouter(...).handlers` (`@connectrpc/connect`).
 *
 * The returned function returns the 101 upgrade `Response` for WebSocket
 * upgrade requests, or `null` for everything else, so callers can fall
 * through to their own Connect-over-fetch handling:
 *
 * ```ts
 * const bidiWebSocket = createBidiWebSocketDraft4Handler(handlers);
 *
 * export default {
 *   fetch(request: Request): Response | Promise<Response> {
 *     return bidiWebSocket(request) ?? handleConnectFetch(request);
 *   },
 * };
 * ```
 *
 * Draft 4 has no compression of its own; on Workers, permessage-deflate is
 * governed by the `web_socket_compression` compatibility flag (default-on
 * for compatibility dates of 2023-08-15 and later).
 */
export function createBidiWebSocketDraft4Handler(
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

    handleMuxedBidiSocketDraft4(
      wrapDraft4WebSocket(server),
      handlers,
      options,
    ).catch((error: unknown) => {
      options?.onError?.(error);
    });

    return new Response(null, { status: 101, webSocket: client });
  };
}
