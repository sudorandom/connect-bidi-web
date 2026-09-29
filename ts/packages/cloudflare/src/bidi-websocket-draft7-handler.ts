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
import { isWebSocketUpgrade } from "./bidi-websocket-handler-common.js";
import { wrapDraft7WebSocket } from "./websocket-like.js";

export interface CreateBidiWebSocketDraft7HandlerOptions {
  /** Context values made available to handler implementations. */
  contextValues?: ContextValues;
  /**
   * Confine WebSocket handshakes to this path prefix: an upgrade is
   * accepted only at prefix + procedure, a non-upgrade request there is
   * answered 426, and an upgrade at a bare procedure path is refused with
   * 400. Both peers must agree on the value.
   */
  pathPrefix?: string;
  /**
   * Cross-origin handshakes to permit. By default a handshake whose
   * `Origin` host is not the request's own `Host` is refused with 403,
   * and one with no `Origin` is never refused on origin grounds. Give a
   * list of hosts, or a predicate over the Origin's host and the request's.
   */
  allowedOrigins?:
    | readonly string[]
    | ((originHost: string, requestHost: string) => boolean);
  /** The codecs this server serves. Defaults to both. */
  codecs?: readonly Draft7Codec[];
  /**
   * The server's own deadline for every RPC, in milliseconds. Defaults to
   * `draft7DefaultServerTimeoutMs` (an hour); zero disables it.
   */
  serverTimeoutMs?: number;
  /**
   * Header names the server's infrastructure sets, which a client's
   * leading-metadata message must not carry. Replaces the default
   * (`forwarded`, `x-forwarded-*`, `x-real-ip`).
   */
  infrastructureHeaders?: readonly string[];
  /**
   * Called if the connection's handler rejects (a bug in a handler
   * implementation; protocol errors are reported to the client instead of
   * throwing).
   */
  onError?: (error: unknown) => void;
}

/**
 * Creates a fetch-handler helper that upgrades WebSocket requests addressed
 * to a procedure into a draft 7 connection — the Connect-over-WebSocket
 * specification — bridged to `handlers` via
 * `@sudorandom/connect-bidi-core`'s `handleBidiSocketDraft7`. Get
 * `handlers` from `createConnectRouter(...).handlers`.
 *
 * The returned function returns `null` for anything that is not this
 * protocol's business, so callers fall through to their ordinary
 * Connect-over-fetch handling on the very same URL:
 *
 * ```ts
 * const bidiWebSocket = createBidiWebSocketDraft7Handler(handlers);
 *
 * export default {
 *   fetch(request: Request): Response | Promise<Response> {
 *     return bidiWebSocket(request) ?? handleConnectFetch(request);
 *   },
 * };
 * ```
 *
 * Compression is not this code's to negotiate on Workers: permessage-deflate
 * is governed by the `web_socket_compression` compatibility flag, and the
 * runtime does not expose the context-takeover parameters.
 */
export function createBidiWebSocketDraft7Handler(
  handlers: readonly UniversalHandler[],
  options?: CreateBidiWebSocketDraft7HandlerOptions,
): (request: Request) => Response | null {
  const byPath = new Map<string, UniversalHandler>();
  for (const handler of handlers) {
    byPath.set(handler.requestPath, handler);
  }
  const prefix = normalizePrefix(options?.pathPrefix);
  const codecs = options?.codecs ?? ["proto", "json"];

  function originPermitted(request: Request): boolean {
    const origin = request.headers.get("origin");
    const host = request.headers.get("host") ?? new URL(request.url).host;
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
      return allowed(originHost, host);
    }
    return allowed.some(
      (entry) => entry.toLowerCase() === originHost.toLowerCase(),
    );
  }

  return function handleUpgrade(request: Request): Response | null {
    const url = new URL(request.url);
    const upgrade = isWebSocketUpgrade(request);
    let path = url.pathname;
    if (prefix !== "") {
      if (url.pathname.startsWith(`${prefix}/`)) {
        path = url.pathname.slice(prefix.length);
        if (!upgrade) {
          return byPath.has(path)
            ? new Response("this path serves WebSocket upgrades only\n", {
                status: 426,
                headers: { Connection: "Upgrade", Upgrade: "websocket" },
              })
            : null;
        }
      } else if (upgrade && byPath.has(url.pathname)) {
        return new Response(
          "WebSocket upgrades are served under the configured path prefix\n",
          { status: 400 },
        );
      }
    }
    if (!upgrade || !byPath.has(path)) {
      return null;
    }
    if (!originPermitted(request)) {
      return new Response("origin not permitted\n", { status: 403 });
    }
    const offered = parseSubprotocolHeader(
      request.headers.get("sec-websocket-protocol"),
    );
    const { token, recognized } = selectDraft7Subprotocol(offered, codecs);
    if (token === undefined) {
      return recognized
        ? new Response(
            `none of the offered codecs is supported; this server serves ${supportedDraft7Subprotocols(codecs).join(", ")}\n`,
            { status: 415 },
          )
        : new Response(
            `expected one of the ${Object.keys(draft7Subprotocols).join(", ")} websocket subprotocols\n`,
            { status: 400 },
          );
    }
    const codec = draft7Subprotocols[token];

    // A malformed deadline is reported on the socket, not by refusing the
    // handshake: JavaScript cannot read a failed handshake's status.
    let timeoutMs: number | undefined;
    let timeoutError: unknown;
    try {
      timeoutMs = parseDraft7Timeout(url.search);
    } catch (err) {
      timeoutError = err;
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();

    const socket = wrapDraft7WebSocket(server);
    const run =
      timeoutError !== undefined
        ? handleBidiSocketDraft7(
            {
              readable: new ReadableStream<never>({
                start(controller) {
                  controller.error(timeoutError);
                },
              }),
              writable: socket.writable,
              close: socket.close,
            },
            handlers,
            { path, codec, contextValues: options?.contextValues },
          )
        : handleBidiSocketDraft7(socket, handlers, {
            path,
            codec,
            requestHeaders: request.headers,
            timeoutMs,
            serverTimeoutMs: options?.serverTimeoutMs,
            infrastructureHeaders: options?.infrastructureHeaders,
            contextValues: options?.contextValues,
          });
    run.catch((error: unknown) => {
      options?.onError?.(error);
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
      // Echo exactly the selected token; a client verifies it.
      headers: { "Sec-WebSocket-Protocol": token },
    });
  };
}

/** A prefix beginning with "/" and not ending with one, or "" for none. */
function normalizePrefix(prefix: string | undefined): string {
  if (prefix === undefined || prefix === "" || prefix === "/") {
    return "";
  }
  const trimmed = prefix.replace(/\/+$/, "");
  return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
}
