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
  draft1ProcedureFromPath,
  draft1Subprotocol,
  handleBidiSocketDraft1,
} from "@sudorandom/connect-bidi-core";
import type { ServerOptions, WebSocket } from "ws";
import { WebSocketServer } from "ws";
import { getPathname, isConnectRouter } from "./bidi-websocket-handler.js";
import { websocketToDuplexMessageStream } from "./websocket-duplex.js";

export interface BidiWebSocketDraft1HandlerOptions {
  /** Context values made available to handler implementations. */
  contextValues?: ContextValues;
  /**
   * Only claim upgrades whose path starts with this prefix, such as
   * `/websocket-draft1`. Defaults to none, which is what a deployment
   * following this draft uses: the procedure URLs themselves.
   *
   * The prefix is not part of the protocol — the procedure is the path's
   * last two segments either way — so this only decides which upgrades this
   * handler claims and which it leaves for other `'upgrade'` listeners. Set
   * it when something else already serves the procedure URLs, such as
   * another draft on the same origin.
   */
  pathPrefix?: string;
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

export interface BidiWebSocketDraft1Handler {
  /**
   * Subscribes to `server`'s `'upgrade'` event and accepts WebSocket
   * upgrades addressed to any streaming procedure in the router.
   *
   * Draft 1 takes the procedure from the URL, so there is no path of its
   * own to configure beyond an optional prefix. Ordinary HTTP requests are
   * left entirely alone — the same procedure URL should keep serving
   * Connect over POST, which is how unary RPCs are dispatched.
   *
   * Upgrades for paths that are not registered streaming procedures are
   * left for other `'upgrade'` listeners, so this can share an
   * `http.Server`.
   */
  upgrade(server: http.Server | https.Server): void;

  /**
   * Serves one RPC on an already-accepted WebSocket connection. Use this if
   * you manage the `'upgrade'` event yourself. `path` is the URL the
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
 * 1 of the wire protocol to Connect RPCs, using
 * `@sudorandom/connect-bidi-core`'s `handleBidiSocketDraft1`. Accepts
 * either a `ConnectRouter` (as returned by `createConnectRouter()`) or a
 * plain `UniversalHandler[]` array (`router.handlers`).
 *
 * One connection carries one RPC, so there is no multiplexing and no idle
 * timeout to configure: the socket lives exactly as long as its RPC.
 *
 * Draft 1's only compression is the WebSocket's own permessage-deflate — the
 * envelope has a compressed-data flag, but this draft leaves it unused — so
 * `upgrade()` enables `ws`'s permessage-deflate by default. Set
 * `webSocketServerOptions.perMessageDeflate` to configure or disable it.
 */
export function createBidiWebSocketDraft1Handler(
  routerOrHandlers: ConnectRouter | readonly UniversalHandler[],
  options?: BidiWebSocketDraft1HandlerOptions,
): BidiWebSocketDraft1Handler {
  const handlers = isConnectRouter(routerOrHandlers)
    ? routerOrHandlers.handlers
    : routerOrHandlers;
  const byProcedure = new Map<string, UniversalHandler>();
  for (const handler of handlers) {
    byProcedure.set(handler.requestPath, handler);
  }

  async function handleConnection(
    ws: WebSocket,
    path: string,
    requestHeaders?: Headers,
  ): Promise<void> {
    const socket = websocketToDuplexMessageStream(ws);
    await handleBidiSocketDraft1(socket, handlers, {
      path,
      requestHeaders,
      contextValues: options?.contextValues,
    });
  }

  const pathPrefix = options?.pathPrefix?.replace(/\/$/, "") ?? "";

  return {
    handleConnection,
    upgrade(server) {
      const wss = new WebSocketServer({
        perMessageDeflate: true,
        ...options?.webSocketServerOptions,
        noServer: true,
        handleProtocols: (protocols) =>
          protocols.has(draft1Subprotocol) ? draft1Subprotocol : false,
      });
      server.on("upgrade", (request, socket, head) => {
        const path = getPathname(request.url);
        if (!path.startsWith(pathPrefix)) {
          return;
        }
        const handler = byProcedure.get(
          draft1ProcedureFromPath(path.slice(pathPrefix.length)),
        );
        if (handler === undefined) {
          // Not a procedure of ours: leave it for another listener.
          return;
        }
        if (handler.method.methodKind === "unary") {
          // Unary RPCs never upgrade; the POST this URL already answers is
          // the better trade for one request and one response.
          abortHandshake(socket, 405, "Method Not Allowed", "POST");
          return;
        }
        if (!offersSubprotocol(request.headers["sec-websocket-protocol"])) {
          // The subprotocol is required and identifies the protocol;
          // refusing the handshake without it is what keeps an RPC upgrade
          // distinct from any other WebSocket served on the same origin.
          // `handleProtocols` returning false only declines to *select* a
          // subprotocol -- it still completes the handshake -- so the
          // refusal has to happen here.
          abortHandshake(socket, 400, "Bad Request");
          return;
        }
        wss.handleUpgrade(request, socket, head, (ws) => {
          // handleBidiSocketDraft1 reports RPC-level failures to the client
          // in the end-stream envelope; a rejection here means the
          // connection itself failed, and there is nothing left to do.
          handleConnection(ws, path, toHeaders(request.headers)).catch(() => {
            // Intentionally ignored; see above.
          });
        });
      });
    },
  };
}

/** Whether a handshake's Sec-WebSocket-Protocol header offers draft 1's. */
function offersSubprotocol(offered: string | string[] | undefined): boolean {
  if (offered === undefined) {
    return false;
  }
  const entries = Array.isArray(offered) ? offered : [offered];
  return entries.some((entry) =>
    entry.split(",").some((protocol) => protocol.trim() === draft1Subprotocol),
  );
}

/** Refuse an upgrade with an ordinary HTTP response. */
function abortHandshake(
  socket: stream.Duplex,
  status: number,
  message: string,
  allow?: string,
): void {
  socket.write(
    `HTTP/1.1 ${status} ${message}\r\n` +
      "Connection: close\r\n" +
      "Content-Length: 0\r\n" +
      (allow !== undefined ? `Allow: ${allow}\r\n` : "") +
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
