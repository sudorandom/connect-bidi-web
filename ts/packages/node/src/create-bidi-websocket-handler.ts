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
import type { ConnectRouter, ContextValues } from "@connectrpc/connect";
import type { UniversalHandler } from "@connectrpc/connect/protocol";
import { handleMuxedBidiSocket } from "@sudorandom/connect-bidi-core";
import type { ServerOptions, WebSocket } from "ws";
import { WebSocketServer } from "ws";
import { websocketToDuplexMessageStream } from "./websocket-duplex.js";

/**
 * The path WebSocket upgrades are accepted on by default, when using
 * `BidiWebSocketHandler.upgrade()`. Matches the path used by
 * `@sudorandom/connect-bidi-web`'s client transports and the Go
 * connectwebsocket server.
 */
export const defaultBidiWebSocketPath = "/websocket";

export interface BidiWebSocketHandlerOptions {
  /**
   * The path to accept WebSocket upgrades on when using `upgrade()`.
   * Defaults to "/websocket". Has no effect on `handleConnection()`.
   */
  path?: string;

  /**
   * Context values made available to handler implementations.
   */
  contextValues?: ContextValues;

  /**
   * Options forwarded to the underlying `ws.WebSocketServer` used by
   * `upgrade()`. `noServer` is always set by `upgrade()` and cannot be
   * overridden here.
   */
  webSocketServerOptions?: Omit<
    ServerOptions,
    "noServer" | "server" | "port" | "host" | "path"
  >;
}

export interface BidiWebSocketHandler {
  /**
   * Subscribes to `server`'s `'upgrade'` event and accepts WebSocket
   * upgrade requests whose path matches `path` (or the handler's
   * configured `path`, default "/websocket"), serving any number of
   * concurrent RPCs per accepted connection, demultiplexed by the stream
   * ID on every frame. Upgrade requests for other paths are left
   * untouched, so multiple `BidiWebSocketHandler`s -- or other `'upgrade'`
   * listeners -- can share the same `http.Server`. Ordinary HTTP requests
   * (the `'request'` event) are entirely unaffected.
   */
  upgrade(server: http.Server | https.Server, path?: string): void;

  /**
   * Serves RPCs on an already-accepted WebSocket connection until it
   * closes. Use this if you manage the `'upgrade'` event yourself, for
   * example to integrate with a framework that already runs its own
   * `ws.WebSocketServer`.
   */
  handleConnection(ws: WebSocket): Promise<void>;
}

/**
 * Creates a handler that bridges `ws` WebSocket connections to Connect
 * RPCs, using `@sudorandom/connect-bidi-core`'s `handleMuxedBidiSocket`.
 * Accepts either a `ConnectRouter` (as returned by `createConnectRouter()`)
 * or a plain `UniversalHandler[]` array (`router.handlers`).
 */
export function createBidiWebSocketHandler(
  routerOrHandlers: ConnectRouter | readonly UniversalHandler[],
  options?: BidiWebSocketHandlerOptions,
): BidiWebSocketHandler {
  const handlers = isConnectRouter(routerOrHandlers)
    ? routerOrHandlers.handlers
    : routerOrHandlers;

  async function handleConnection(ws: WebSocket): Promise<void> {
    const socket = websocketToDuplexMessageStream(ws);
    await handleMuxedBidiSocket(socket, handlers, {
      contextValues: options?.contextValues,
    });
  }

  return {
    handleConnection,
    upgrade(server, path = options?.path ?? defaultBidiWebSocketPath) {
      const wss = new WebSocketServer({
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
          // handleMuxedBidiSocket already reports RPC-level failures to the
          // client via an error end-stream envelope; a rejection here means
          // the connection itself failed in some more fundamental way, and
          // there is nothing left to do but let it close.
          handleConnection(ws).catch(() => {
            // Intentionally ignored; see above.
          });
        });
      });
    },
  };
}

function isConnectRouter(
  value: ConnectRouter | readonly UniversalHandler[],
): value is ConnectRouter {
  return !Array.isArray(value);
}

function getPathname(url: string | undefined): string {
  // `request.url` is a path-and-query, not an absolute URL; the base is
  // only needed to satisfy the URL constructor and is never used.
  return new URL(url ?? "/", "http://bidi.invalid").pathname;
}
