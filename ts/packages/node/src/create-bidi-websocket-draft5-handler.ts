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

import type * as http from "node:http";
import type * as https from "node:https";
import type * as stream from "node:stream";
import type { ConnectRouter, ContextValues } from "@connectrpc/connect";
import type { UniversalHandler } from "@connectrpc/connect/protocol";
import {
  draft5Subprotocol,
  handleBidiSocketDraft5,
} from "@sudorandom/connect-bidi-core";
import type { ServerOptions, WebSocket } from "ws";
import { WebSocketServer } from "ws";
import { getPathname, isConnectRouter } from "./bidi-websocket-handler.js";
import { websocketToDraft5MessageStream } from "./websocket-duplex-draft5.js";

export interface BidiWebSocketDraft5HandlerOptions {
  /** Context values made available to handler implementations. */
  contextValues?: ContextValues;
  /**
   * Options forwarded to the underlying `ws.WebSocketServer` used by
   * `upgrade()`. `noServer` and `handleProtocols` are always set by
   * `upgrade()` and cannot be overridden: the subprotocol is the protocol's,
   * not the caller's.
   */
  webSocketServerOptions?: Omit<
    ServerOptions,
    "noServer" | "server" | "port" | "host" | "path" | "handleProtocols"
  >;
}

export interface BidiWebSocketDraft5Handler {
  /**
   * Subscribes to `server`'s `'upgrade'` event and accepts WebSocket
   * upgrades addressed to any streaming procedure in the router.
   *
   * There is no path to configure, because draft 5 has none: the upgrade
   * request is the RPC request, so its URL path is the procedure. That also
   * means this handler leaves ordinary HTTP requests entirely alone — the
   * same procedure URL should keep serving Connect over POST, which is how
   * unary RPCs are dispatched.
   *
   * Upgrades for paths that are not registered procedures are left for
   * other `'upgrade'` listeners, so this can share an `http.Server`.
   */
  upgrade(server: http.Server | https.Server): void;

  /**
   * Serves one RPC on an already-accepted WebSocket connection. Use this if
   * you manage the `'upgrade'` event yourself. `path` is the procedure the
   * upgrade addressed, and `requestHeaders` the headers it carried.
   */
  handleConnection(
    ws: WebSocket,
    path: string,
    requestHeaders?: Headers,
  ): Promise<void>;
}

/**
 * Creates a handler that bridges `ws` WebSocket connections speaking draft
 * 5 of the wire protocol to Connect RPCs, using
 * `@sudorandom/connect-bidi-core`'s `handleBidiSocketDraft5`. Accepts
 * either a `ConnectRouter` (as returned by `createConnectRouter()`) or a
 * plain `UniversalHandler[]` array (`router.handlers`).
 *
 * One connection carries one RPC, so there is no multiplexing and no idle
 * timeout to configure: the socket lives exactly as long as its RPC.
 *
 * Draft 5 has no compression of its own — with no envelope, a message has
 * nowhere to carry a "this one is compressed" bit — so `upgrade()` enables
 * `ws`'s permessage-deflate by default. Set
 * `webSocketServerOptions.perMessageDeflate` to configure or disable it.
 */
export function createBidiWebSocketDraft5Handler(
  routerOrHandlers: ConnectRouter | readonly UniversalHandler[],
  options?: BidiWebSocketDraft5HandlerOptions,
): BidiWebSocketDraft5Handler {
  const handlers = isConnectRouter(routerOrHandlers)
    ? routerOrHandlers.handlers
    : routerOrHandlers;
  const byPath = new Map<string, UniversalHandler>();
  for (const handler of handlers) {
    byPath.set(handler.requestPath, handler);
  }

  async function handleConnection(
    ws: WebSocket,
    path: string,
    requestHeaders?: Headers,
  ): Promise<void> {
    const socket = websocketToDraft5MessageStream(ws);
    await handleBidiSocketDraft5(socket, handlers, {
      path,
      requestHeaders,
      contextValues: options?.contextValues,
    });
  }

  return {
    handleConnection,
    upgrade(server) {
      const wss = new WebSocketServer({
        perMessageDeflate: true,
        ...options?.webSocketServerOptions,
        noServer: true,
        // The subprotocol is required and identifies the protocol; refusing
        // the handshake without it is what keeps an RPC upgrade distinct
        // from any other WebSocket served on the same origin.
        handleProtocols: (protocols) =>
          protocols.has(draft5Subprotocol) ? draft5Subprotocol : false,
      });
      server.on("upgrade", (request, socket, head) => {
        const path = getPathname(request.url);
        const handler = byPath.get(path);
        if (handler === undefined) {
          // Not a procedure of ours: leave it for another listener.
          return;
        }
        if (handler.method.methodKind === "unary") {
          // Unary RPCs never upgrade; the POST this URL already answers is
          // the better trade for one request and one response.
          abortHandshake(socket, 405, "Method Not Allowed");
          return;
        }
        wss.handleUpgrade(request, socket, head, (ws) => {
          // handleBidiSocketDraft5 reports RPC-level failures to the client
          // in the end-stream message; a rejection here means the
          // connection itself failed, and there is nothing left to do.
          handleConnection(ws, path, toHeaders(request.headers)).catch(() => {
            // Intentionally ignored; see above.
          });
        });
      });
    },
  };
}

/** Refuse an upgrade with an ordinary HTTP response. */
function abortHandshake(
  socket: stream.Duplex,
  status: number,
  message: string,
): void {
  socket.write(
    `HTTP/1.1 ${status} ${message}\r\n` +
      "Connection: close\r\n" +
      "Content-Length: 0\r\n" +
      "Allow: POST\r\n" +
      "\r\n",
  );
  socket.destroy();
}

/** Convert Node's incoming headers to the WHATWG Headers the bridge uses. */
function toHeaders(incoming: http.IncomingHttpHeaders): Headers {
  const headers = new Headers();
  for (const [key, value] of Object.entries(incoming)) {
    if (value === undefined) {
      continue;
    }
    for (const entry of Array.isArray(value) ? value : [value]) {
      headers.append(key, entry);
    }
  }
  return headers;
}
