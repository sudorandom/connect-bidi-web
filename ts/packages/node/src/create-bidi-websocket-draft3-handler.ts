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
import {
  draft3SubprotocolDeflate,
  draft3SubprotocolIdentity,
  handleMuxedBidiSocketDraft3,
} from "@sudorandom/connect-bidi-core";
import type { WebSocket } from "ws";
import { WebSocketServer } from "ws";
import {
  getPathname,
  isConnectRouter,
} from "./create-bidi-websocket-handler.js";
import type {
  BidiWebSocketHandler,
  BidiWebSocketHandlerOptions,
} from "./create-bidi-websocket-handler.js";
import { websocketToDuplexMessageStream } from "./websocket-duplex.js";

/**
 * The path draft 3 WebSocket upgrades are accepted on by default, when
 * using the handler's `upgrade()`. Matches the path used by
 * `@sudorandom/connect-bidi-web`'s draft 3 client transport and the Go
 * connectwebsocket/draft3 server.
 */
export const defaultBidiWebSocketDraft3Path = "/websocket-draft3";

/**
 * Creates a handler that bridges `ws` WebSocket connections speaking
 * draft 3 of the wire protocol to Connect RPCs, using
 * `@sudorandom/connect-bidi-core`'s `handleMuxedBidiSocketDraft3`.
 *
 * Draft 3 negotiates its compression through the WebSocket subprotocols
 * (`connect.bidi.d3.deflate` / `connect.bidi.d3`); connections that offer
 * neither are rejected. The permessage-deflate extension is never enabled:
 * the deflate subprotocol replaces it.
 */
export function createBidiWebSocketDraft3Handler(
  routerOrHandlers: ConnectRouter | readonly UniversalHandler[],
  options?: BidiWebSocketHandlerOptions,
): BidiWebSocketHandler {
  const handlers = isConnectRouter(routerOrHandlers)
    ? routerOrHandlers.handlers
    : routerOrHandlers;

  async function handleConnection(ws: WebSocket): Promise<void> {
    // ws.protocol carries the subprotocol the handshake settled on.
    if (
      ws.protocol !== draft3SubprotocolDeflate &&
      ws.protocol !== draft3SubprotocolIdentity
    ) {
      ws.close(1008, "missing draft 3 subprotocol");
      return;
    }
    const socket = websocketToDuplexMessageStream(ws);
    await handleMuxedBidiSocketDraft3(socket, handlers, {
      compression: ws.protocol === draft3SubprotocolDeflate,
      contextValues: options?.contextValues,
      idleTimeoutMs: options?.idleTimeoutMs,
    });
  }

  return {
    handleConnection,
    upgrade(server, path = options?.path ?? defaultBidiWebSocketDraft3Path) {
      const wss = new WebSocketServer({
        ...options?.webSocketServerOptions,
        // Selection order is the client's preference order, matching the
        // Go server. perMessageDeflate stays off: draft 3's own
        // compression replaces it.
        handleProtocols: (protocols) => {
          for (const protocol of protocols) {
            if (
              protocol === draft3SubprotocolDeflate ||
              protocol === draft3SubprotocolIdentity
            ) {
              return protocol;
            }
          }
          return false;
        },
        perMessageDeflate: false,
        noServer: true,
      });
      server.on("upgrade", (request, socket, head) => {
        const requestPath = getPathname(request.url);
        if (requestPath !== path) {
          // Not ours: leave the upgrade request for another listener.
          return;
        }
        wss.handleUpgrade(request, socket, head, (ws) => {
          handleConnection(ws).catch(() => {
            // handleMuxedBidiSocketDraft3 reports RPC-level failures to the
            // client itself; a rejection means the connection failed in a
            // more fundamental way, and there is nothing left to do.
          });
        });
      });
    },
  };
}
