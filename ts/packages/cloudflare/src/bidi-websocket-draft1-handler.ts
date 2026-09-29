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

import type { ContextValues } from "@connectrpc/connect";
import type { UniversalHandler } from "@connectrpc/connect/protocol";
import {
  draft1ProcedureFromPath,
  draft1Subprotocol,
  handleBidiSocketDraft1,
} from "@sudorandom/connect-bidi-core";
import { isWebSocketUpgrade } from "./bidi-websocket-handler-common.js";
import { wrapWebSocket } from "./websocket-like.js";

export interface CreateBidiWebSocketDraft1HandlerOptions {
  /** Context values made available to handler implementations. */
  contextValues?: ContextValues;
  /**
   * Only claim upgrades whose path starts with this prefix, such as
   * `/websocket-draft1`. Defaults to none, which is what a deployment
   * following this draft uses: the procedure URLs themselves.
   *
   * The prefix is not part of the protocol — the procedure is the path's
   * last two segments either way — so this only decides which upgrades this
   * handler claims. Set it when something else already serves the procedure
   * URLs, such as another draft on the same origin.
   */
  pathPrefix?: string;
  /**
   * Called if the connection's handler rejects (a bug in a handler
   * implementation; protocol errors are reported to the client instead of
   * throwing).
   */
  onError?: (error: unknown) => void;
}

/**
 * Creates a fetch-handler helper that upgrades WebSocket requests addressed
 * to a streaming procedure into a draft 1 connection, bridged to `handlers`
 * via `@sudorandom/connect-bidi-core`'s `handleBidiSocketDraft1`. Get
 * `handlers` from `createConnectRouter(...).handlers`
 * (`@connectrpc/connect`).
 *
 * Draft 1 takes the procedure from the URL — the path's last two segments,
 * so a prefix in front of them changes nothing. The returned function
 * returns `null` for anything that is not an upgrade addressed to a
 * streaming procedure, including unary procedures, which never upgrade — so
 * callers fall through to their ordinary Connect-over-fetch handling on the
 * very same URL:
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
 * Draft 1's only compression is the WebSocket's own permessage-deflate; on
 * Workers that is governed by the `web_socket_compression` compatibility
 * flag (default-on for compatibility dates of 2023-08-15 and later).
 */
export function createBidiWebSocketDraft1Handler(
  handlers: readonly UniversalHandler[],
  options?: CreateBidiWebSocketDraft1HandlerOptions,
): (request: Request) => Response | null {
  const byProcedure = new Map<string, UniversalHandler>();
  for (const handler of handlers) {
    byProcedure.set(handler.requestPath, handler);
  }
  const pathPrefix = options?.pathPrefix?.replace(/\/$/, "") ?? "";

  return function handleUpgrade(request: Request): Response | null {
    if (!isWebSocketUpgrade(request)) {
      return null;
    }
    const path = new URL(request.url).pathname;
    if (!path.startsWith(pathPrefix)) {
      return null;
    }
    const handler = byProcedure.get(
      draft1ProcedureFromPath(path.slice(pathPrefix.length)),
    );
    if (handler === undefined || handler.method.methodKind === "unary") {
      // Not a streaming procedure of ours. Unary RPCs never upgrade: the
      // POST this URL already answers is the better trade.
      return null;
    }
    if (!offersSubprotocol(request)) {
      return new Response(
        `expected the ${draft1Subprotocol} websocket subprotocol`,
        { status: 400 },
      );
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();

    handleBidiSocketDraft1(wrapWebSocket(server), handlers, {
      path,
      requestHeaders: request.headers,
      contextValues: options?.contextValues,
    }).catch((error: unknown) => {
      options?.onError?.(error);
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
      // The server must select the subprotocol; a client that does not see
      // it echoed back refuses the connection.
      headers: { "Sec-WebSocket-Protocol": draft1Subprotocol },
    });
  };
}

/** Whether the handshake offers draft 1's required subprotocol. */
function offersSubprotocol(request: Request): boolean {
  const offered = request.headers.get("sec-websocket-protocol");
  if (offered === null) {
    return false;
  }
  return offered
    .split(",")
    .some((protocol) => protocol.trim() === draft1Subprotocol);
}
