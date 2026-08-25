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

import type { ConnectRouter } from "@connectrpc/connect";
import type { UniversalHandler } from "@connectrpc/connect/protocol";
import { handleMuxedBidiSocketDraft4 } from "@sudorandom/connect-bidi-core";
import type { WebSocket } from "ws";
import { WebSocketServer } from "ws";
import { getPathname, isConnectRouter } from "./bidi-websocket-handler.js";
import type {
  BidiWebSocketHandler,
  BidiWebSocketHandlerOptions,
} from "./bidi-websocket-handler.js";
import { websocketToDraft4DuplexMessageStream } from "./websocket-duplex-draft4.js";

/**
 * The path draft 4 WebSocket upgrades are accepted on by default, when
 * using the handler's `upgrade()`. Matches the path used by
 * `@sudorandom/connect-bidi-web`'s draft 4 client transport and the Go
 * connectwebsocket/draft4 server. Distinct from the other drafts' paths:
 * the wire protocols are incompatible, so each connection must reach the
 * handler that speaks its draft.
 */
export const defaultBidiWebSocketDraft4Path = "/websocket-draft4";

/**
 * Creates a handler that bridges `ws` WebSocket connections speaking
 * draft 4 of the wire protocol to Connect RPCs, using
 * `@sudorandom/connect-bidi-core`'s `handleMuxedBidiSocketDraft4`. Accepts
 * either a `ConnectRouter` (as returned by `createConnectRouter()`) or a
 * plain `UniversalHandler[]` array (`router.handlers`).
 *
 * Draft 4 has no compression of its own, so `upgrade()` enables `ws`'s
 * permessage-deflate by default, as draft 2 does; set
 * `webSocketServerOptions.perMessageDeflate` to configure or disable it.
 * Disabling it is worth considering here: draft 4's whole point is a wire
 * you can read, and a compressed text frame is no more readable than a
 * binary one.
 */
export function createBidiWebSocketDraft4Handler(
  routerOrHandlers: ConnectRouter | readonly UniversalHandler[],
  options?: BidiWebSocketHandlerOptions,
): BidiWebSocketHandler {
  const handlers = isConnectRouter(routerOrHandlers)
    ? routerOrHandlers.handlers
    : routerOrHandlers;

  async function handleConnection(ws: WebSocket): Promise<void> {
    const socket = websocketToDraft4DuplexMessageStream(ws);
    await handleMuxedBidiSocketDraft4(socket, handlers, {
      contextValues: options?.contextValues,
      idleTimeoutMs: options?.idleTimeoutMs,
    });
  }

  return {
    handleConnection,
    upgrade(server, path = options?.path ?? defaultBidiWebSocketDraft4Path) {
      const wss = new WebSocketServer({
        perMessageDeflate: true,
        ...options?.webSocketServerOptions,
        noServer: true,
      });
      server.on("upgrade", (request, socket, head) => {
        const requestPath = getPathname(request.url);
        if (requestPath !== path) {
          // Not ours: leave the upgrade request for another listener.
          return;
        }
        wss.handleUpgrade(request, socket, head, (ws) => {
          // handleMuxedBidiSocketDraft4 already reports RPC-level failures
          // to the client via an error end-stream frame; a rejection here
          // means the connection itself failed in some more fundamental
          // way, and there is nothing left to do but let it close.
          handleConnection(ws).catch(() => {
            // Intentionally ignored; see above.
          });
        });
      });
    },
  };
}
