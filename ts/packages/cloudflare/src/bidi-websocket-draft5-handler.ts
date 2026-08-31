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
  draft5Subprotocol,
  handleBidiSocketDraft5,
} from "@sudorandom/connect-bidi-core";
import { isWebSocketUpgrade } from "./bidi-websocket-handler-common.js";
import { wrapDraft5WebSocket } from "./websocket-like.js";

export interface CreateBidiWebSocketDraft5HandlerOptions {
  /** Context values made available to handler implementations. */
  contextValues?: ContextValues;
  /**
   * Called if the connection's handler rejects (a bug in a handler
   * implementation; protocol errors are reported to the client instead of
   * throwing).
   */
  onError?: (error: unknown) => void;
}

/**
 * Creates a fetch-handler helper that upgrades WebSocket requests addressed
 * to a streaming procedure into a draft 5 connection, bridged to `handlers`
 * via `@sudorandom/connect-bidi-core`'s `handleBidiSocketDraft5`. Get
 * `handlers` from `createConnectRouter(...).handlers`
 * (`@connectrpc/connect`).
 *
 * There is no path to configure, because draft 5 has none: the upgrade
 * request is the RPC request, so its URL path is the procedure. The
 * returned function therefore returns `null` for anything that is not an
 * upgrade addressed to a streaming procedure — including unary procedures,
 * which never upgrade — so callers fall through to their ordinary
 * Connect-over-fetch handling on the very same URL:
 *
 * ```ts
 * const bidiWebSocket = createBidiWebSocketDraft5Handler(handlers);
 *
 * export default {
 *   fetch(request: Request): Response | Promise<Response> {
 *     return bidiWebSocket(request) ?? handleConnectFetch(request);
 *   },
 * };
 * ```
 *
 * Draft 5 has no compression of its own; on Workers, permessage-deflate is
 * governed by the `web_socket_compression` compatibility flag (default-on
 * for compatibility dates of 2023-08-15 and later).
 */
export function createBidiWebSocketDraft5Handler(
  handlers: readonly UniversalHandler[],
  options?: CreateBidiWebSocketDraft5HandlerOptions,
): (request: Request) => Response | null {
  const byPath = new Map<string, UniversalHandler>();
  for (const handler of handlers) {
    byPath.set(handler.requestPath, handler);
  }

  return function handleUpgrade(request: Request): Response | null {
    if (!isWebSocketUpgrade(request)) {
      return null;
    }
    const path = new URL(request.url).pathname;
    const handler = byPath.get(path);
    if (handler === undefined || handler.method.methodKind === "unary") {
      // Not a streaming procedure of ours. Unary RPCs never upgrade: the
      // POST this URL already answers is the better trade.
      return null;
    }
    if (!offersSubprotocol(request)) {
      return new Response(
        `expected the ${draft5Subprotocol} websocket subprotocol`,
        { status: 400 },
      );
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();

    handleBidiSocketDraft5(wrapDraft5WebSocket(server), handlers, {
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
      headers: { "Sec-WebSocket-Protocol": draft5Subprotocol },
    });
  };
}

/** Whether the handshake offers draft 5's required subprotocol. */
function offersSubprotocol(request: Request): boolean {
  const offered = request.headers.get("sec-websocket-protocol");
  if (offered === null) {
    return false;
  }
  return offered
    .split(",")
    .some((protocol) => protocol.trim() === draft5Subprotocol);
}
