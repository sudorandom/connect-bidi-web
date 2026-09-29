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
import type { Draft7Codec } from "@sudorandom/connect-bidi-core";
import {
  draft7OriginIsSameHost,
  draft7Subprotocols,
  handleBidiSocketDraft7,
  parseDraft7Timeout,
  parseSubprotocolHeader,
  selectDraft7Subprotocol,
  supportedDraft7Subprotocols,
} from "@sudorandom/connect-bidi-core";
import type { ServerOptions, WebSocket } from "ws";
import { WebSocketServer } from "ws";
import { getPathname, isConnectRouter } from "./bidi-websocket-handler.js";
import { websocketToDraft7MessageStream } from "./websocket-duplex-draft7.js";

export interface BidiWebSocketDraft7HandlerOptions {
  /** Context values made available to handler implementations. */
  contextValues?: ContextValues;
  /**
   * Confine WebSocket handshakes to this path prefix: an upgrade is
   * accepted only at prefix + procedure, a non-upgrade request there is
   * answered 426 by `handleRequest()`, and an upgrade at a bare procedure
   * path is refused with 400. Both peers must agree on the value. Without
   * it, every procedure URL accepts an upgrade.
   */
  pathPrefix?: string;
  /**
   * Cross-origin handshakes to permit. By default a handshake whose
   * `Origin` host is not the request's own `Host` is refused with 403
   * (scheme deliberately not compared), and one with no `Origin` is never
   * refused on origin grounds. Give a list of hosts (`host` or
   * `host:port`), or a predicate over the Origin's host and the request's.
   */
  allowedOrigins?:
    | readonly string[]
    | ((originHost: string, requestHost: string) => boolean);
  /**
   * The codecs this server serves, which decides which subprotocol tokens
   * it selects. Defaults to both.
   */
  codecs?: readonly Draft7Codec[];
  /**
   * The server's own deadline for every RPC, in milliseconds, from the
   * moment the handshake is accepted. The effective deadline is the
   * shorter of this and the client's `connect-timeout-ms`. Defaults to
   * `draft7DefaultServerTimeoutMs` (an hour); zero disables it.
   */
  serverTimeoutMs?: number;
  /**
   * Header names the server's infrastructure sets, which a client's
   * leading-metadata message must not carry. Replaces the default
   * (`forwarded`, `x-forwarded-*`, `x-real-ip`); an entry ending in "-"
   * is a prefix.
   */
  infrastructureHeaders?: readonly string[];
  /**
   * Options forwarded to the underlying `ws.WebSocketServer` used by
   * `upgrade()`. `noServer` and `handleProtocols` are always set by
   * `upgrade()`: the subprotocol is the protocol's, not the caller's.
   * `perMessageDeflate` may be set to `false` to disable compression; any
   * other value is used with `serverNoContextTakeover` and
   * `clientNoContextTakeover` forced on, as the protocol requires.
   */
  webSocketServerOptions?: Omit<
    ServerOptions,
    "noServer" | "server" | "port" | "host" | "path" | "handleProtocols"
  >;
}

export interface BidiWebSocketDraft7Handler {
  /**
   * Subscribes to `server`'s `'upgrade'` event and accepts WebSocket
   * upgrades addressed to any procedure in the router — unary ones
   * included, as the protocol requires. Ordinary HTTP requests are left
   * entirely alone, so the same URLs keep serving Connect over POST.
   *
   * Upgrades for paths that are not registered procedures are left for
   * other `'upgrade'` listeners, so this can share an `http.Server`.
   */
  upgrade(server: http.Server | https.Server): void;

  /**
   * Answers the one ordinary HTTP request this protocol defines an answer
   * for: with a `pathPrefix` configured, a non-upgrade request under the
   * prefix gets `426 Upgrade Required`. Call it from the `'request'`
   * handler; it returns true when it wrote a response.
   */
  handleRequest(
    request: http.IncomingMessage,
    response: http.ServerResponse,
  ): boolean;

  /**
   * Serves one RPC on an already-accepted WebSocket connection. Use this
   * if you manage the `'upgrade'` event yourself. `path` is the procedure
   * (with any prefix removed), `codec` the one the subprotocol you selected
   * names, and `timeoutMs` the `connect-timeout-ms` query parameter.
   */
  handleConnection(
    ws: WebSocket,
    path: string,
    codec: Draft7Codec,
    requestHeaders?: Headers,
    timeoutMs?: number,
  ): Promise<void>;
}

/**
 * Creates a handler that bridges `ws` WebSocket connections speaking draft
 * 7 of the wire protocol — the Connect-over-WebSocket specification — to
 * Connect RPCs, using `@sudorandom/connect-bidi-core`'s
 * `handleBidiSocketDraft7`. Accepts either a `ConnectRouter` (as returned
 * by `createConnectRouter()`) or a plain `UniversalHandler[]` array
 * (`router.handlers`).
 *
 * One connection carries one RPC. The handshake selects the codec by
 * subprotocol, refuses a cross-origin handshake unless permitted, and
 * negotiates permessage-deflate with no context takeover in both
 * directions, imposing that on a client that did not offer it.
 */
export function createBidiWebSocketDraft7Handler(
  routerOrHandlers: ConnectRouter | readonly UniversalHandler[],
  options?: BidiWebSocketDraft7HandlerOptions,
): BidiWebSocketDraft7Handler {
  const handlers = isConnectRouter(routerOrHandlers)
    ? routerOrHandlers.handlers
    : routerOrHandlers;
  const byPath = new Map<string, UniversalHandler>();
  for (const handler of handlers) {
    byPath.set(handler.requestPath, handler);
  }
  const prefix = normalizePrefix(options?.pathPrefix);
  const codecs = options?.codecs ?? ["proto", "json"];

  async function handleConnection(
    ws: WebSocket,
    path: string,
    codec: Draft7Codec,
    requestHeaders?: Headers,
    timeoutMs?: number,
  ): Promise<void> {
    const socket = websocketToDraft7MessageStream(ws);
    await handleBidiSocketDraft7(socket, handlers, {
      path,
      codec,
      requestHeaders,
      timeoutMs,
      serverTimeoutMs: options?.serverTimeoutMs,
      infrastructureHeaders: options?.infrastructureHeaders,
      contextValues: options?.contextValues,
    });
  }

  /** Whether a handshake's origin is permitted. */
  function originPermitted(request: http.IncomingMessage): boolean {
    const origin = request.headers.origin ?? null;
    const host = request.headers.host ?? null;
    if (draft7OriginIsSameHost(origin, host)) {
      return true;
    }
    const allowed = options?.allowedOrigins;
    if (allowed === undefined || origin === null) {
      return false;
    }
    let originHost: string;
    try {
      originHost = new URL(origin).host;
    } catch {
      return false;
    }
    if (typeof allowed === "function") {
      return allowed(originHost, host ?? "");
    }
    return allowed.some(
      (entry) => entry.toLowerCase() === originHost.toLowerCase(),
    );
  }

  return {
    handleConnection,

    handleRequest(request, response) {
      if (prefix === "" || !getPathname(request.url).startsWith(`${prefix}/`)) {
        return false;
      }
      response.writeHead(426, {
        Connection: "Upgrade",
        Upgrade: "websocket",
        "Content-Type": "text/plain",
      });
      response.end("this path serves WebSocket upgrades only\n");
      return true;
    },

    upgrade(server) {
      const deflate = options?.webSocketServerOptions?.perMessageDeflate;
      const wss = new WebSocketServer({
        ...options?.webSocketServerOptions,
        // No context takeover in both directions, imposed on the client if
        // it did not offer it: a compression context shared across
        // messages leaks plaintext across trust boundaries.
        perMessageDeflate:
          deflate === false
            ? false
            : {
                ...(typeof deflate === "object" ? deflate : {}),
                serverNoContextTakeover: true,
                clientNoContextTakeover: true,
              },
        noServer: true,
        // Selected per handshake below; the server's choice is stashed on
        // the request so the callback here can echo exactly that token.
        handleProtocols: (_protocols, request) =>
          (request as SelectedRequest).draft7Subprotocol ?? false,
      });
      server.on("upgrade", (request, socket, head) => {
        const pathname = getPathname(request.url);
        let path = pathname;
        if (prefix !== "") {
          if (pathname.startsWith(`${prefix}/`)) {
            path = pathname.slice(prefix.length);
          } else if (byPath.has(pathname)) {
            // Both peers agreed on the prefix; an upgrade at the bare
            // procedure path is an error.
            abortHandshake(
              socket,
              400,
              "Bad Request",
              "WebSocket upgrades are served under the configured path prefix",
            );
            return;
          }
        }
        if (!byPath.has(path)) {
          // Not a procedure of ours: leave it for another listener.
          return;
        }
        if (!originPermitted(request)) {
          abortHandshake(socket, 403, "Forbidden", "origin not permitted");
          return;
        }
        const offered = parseSubprotocolHeader(
          headerValue(request.headers["sec-websocket-protocol"]),
        );
        const { token, recognized } = selectDraft7Subprotocol(offered, codecs);
        if (token === undefined) {
          if (!recognized) {
            abortHandshake(
              socket,
              400,
              "Bad Request",
              `expected one of the ${Object.keys(draft7Subprotocols).join(", ")} websocket subprotocols`,
            );
          } else {
            abortHandshake(
              socket,
              415,
              "Unsupported Media Type",
              `none of the offered codecs is supported; this server serves ${supportedDraft7Subprotocols(codecs).join(", ")}`,
            );
          }
          return;
        }
        (request as SelectedRequest).draft7Subprotocol = token;
        const codec = draft7Subprotocols[token];

        // A malformed deadline is reported on the socket, not by refusing
        // the handshake: JavaScript cannot read a failed handshake's status.
        let timeoutMs: number | undefined;
        let timeoutError: unknown;
        try {
          timeoutMs = parseDraft7Timeout(
            new URL(request.url ?? "/", "http://bidi.invalid").search,
          );
        } catch (err) {
          timeoutError = err;
        }

        wss.handleUpgrade(request, socket, head, (ws) => {
          const run =
            timeoutError !== undefined
              ? failOnSocket(ws, path, codec, timeoutError)
              : handleConnection(
                  ws,
                  path,
                  codec,
                  toHeaders(request.headers),
                  timeoutMs,
                );
          run.catch(() => {
            // handleBidiSocketDraft7 reports RPC-level failures to the
            // client in S; a rejection here means the connection itself
            // failed, and there is nothing left to do.
          });
        });
      });
    },
  };

  /**
   * Answers an RPC that failed before it could be dispatched — an invalid
   * deadline on the URI — with M and S on the socket, by handing the
   * bridge a request whose first read fails with that error.
   */
  async function failOnSocket(
    ws: WebSocket,
    path: string,
    codec: Draft7Codec,
    error: unknown,
  ): Promise<void> {
    const socket = websocketToDraft7MessageStream(ws);
    const failing = new ReadableStream<never>({
      start(controller) {
        controller.error(error);
      },
    });
    await handleBidiSocketDraft7(
      { readable: failing, writable: socket.writable, close: socket.close },
      handlers,
      { path, codec, contextValues: options?.contextValues },
    );
  }
}

/** The handshake request, with the token the server selected stashed on it. */
interface SelectedRequest extends http.IncomingMessage {
  draft7Subprotocol?: string;
}

/** A prefix beginning with "/" and not ending with one, or "" for none. */
function normalizePrefix(prefix: string | undefined): string {
  if (prefix === undefined || prefix === "" || prefix === "/") {
    return "";
  }
  const trimmed = prefix.replace(/\/+$/, "");
  return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
}

/** Refuse an upgrade with an ordinary HTTP response. */
function abortHandshake(
  socket: stream.Duplex,
  status: number,
  statusText: string,
  message: string,
): void {
  const body = `${message}\n`;
  socket.write(
    `HTTP/1.1 ${status} ${statusText}\r\n` +
      "Connection: close\r\n" +
      "Content-Type: text/plain\r\n" +
      `Content-Length: ${Buffer.byteLength(body)}\r\n` +
      "\r\n" +
      body,
  );
  socket.destroy();
}

function headerValue(value: string | string[] | undefined): string | null {
  if (value === undefined) {
    return null;
  }
  return Array.isArray(value) ? value.join(",") : value;
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
