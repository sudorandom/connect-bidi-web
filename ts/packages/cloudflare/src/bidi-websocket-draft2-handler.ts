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

import { handleMuxedBidiSocketDraft2 } from "@sudorandom/connect-bidi-core";
import type { UniversalHandler } from "@connectrpc/connect/protocol";
import type { CreateBidiWebSocketHandlerOptions } from "./bidi-websocket-handler.js";
import { isWebSocketUpgrade } from "./bidi-websocket-handler.js";
import { wrapWebSocket } from "./websocket-like.js";

/**
 * Creates a fetch-handler helper that upgrades `Upgrade: websocket`
 * requests into a multiplexed bidi connection speaking draft 2 of the wire
 * protocol, bridged to `handlers` via `@sudorandom/connect-bidi-core`'s
 * `handleMuxedBidiSocketDraft2`. The draft 1 equivalent is
 * `createBidiWebSocketHandler`; the two wire protocols are incompatible,
 * so route each path to the handler that speaks its draft:
 *
 * ```ts
 * const draft1 = createBidiWebSocketHandler(handlers);
 * const draft2 = createBidiWebSocketDraft2Handler(handlers);
 *
 * export default {
 *   fetch(request: Request): Response | Promise<Response> {
 *     const url = new URL(request.url);
 *     if (url.pathname === "/websocket-draft2") {
 *       return draft2(request) ?? new Response(null, { status: 426 });
 *     }
 *     return draft1(request) ?? handleConnectFetch(request);
 *   },
 * };
 * ```
 *
 * Draft 2 has no per-message compression; on Workers, permessage-deflate is
 * governed by the `web_socket_compression` compatibility flag (default-on
 * for compatibility dates of 2023-08-15 and later).
 */
export function createBidiWebSocketDraft2Handler(
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

    handleMuxedBidiSocketDraft2(wrapWebSocket(server), handlers, options).catch(
      (error: unknown) => {
        options?.onError?.(error);
      },
    );

    return new Response(null, { status: 101, webSocket: client });
  };
}
