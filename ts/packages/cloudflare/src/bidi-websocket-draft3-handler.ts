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

import {
  draft3SubprotocolDeflate,
  draft3SubprotocolIdentity,
  handleMuxedBidiSocketDraft3,
} from "@sudorandom/connect-bidi-core";
import type { UniversalHandler } from "@connectrpc/connect/protocol";
import type { CreateBidiWebSocketHandlerOptions } from "./bidi-websocket-handler-common.js";
import { isWebSocketUpgrade } from "./bidi-websocket-handler-common.js";
import { wrapWebSocket } from "./websocket-like.js";

export interface CreateBidiWebSocketDraft3HandlerOptions
  extends CreateBidiWebSocketHandlerOptions {
  /**
   * Restrict the server to the identity subprotocol (connect.bidi.d3), so
   * no frame is ever compressed in either direction; clients that offer
   * only the deflate token are then refused. By default the server
   * selects connect.bidi.d3.deflate when the client offers it.
   */
  withoutCompression?: boolean;
}

/**
 * Selects the first supported token from a comma-separated
 * Sec-WebSocket-Protocol offer, in the client's preference order.
 */
function selectSubprotocol(
  offer: string | null,
  supported: string[],
): string | undefined {
  for (const token of (offer ?? "").split(",")) {
    const trimmed = token.trim();
    if (supported.includes(trimmed)) {
      return trimmed;
    }
  }
  return undefined;
}

/**
 * Creates a fetch-handler helper that upgrades `Upgrade: websocket`
 * requests into a multiplexed bidi connection speaking draft 3 of the wire
 * protocol, bridged to `handlers` via `@sudorandom/connect-bidi-core`'s
 * `handleMuxedBidiSocketDraft3`.
 *
 * Draft 3 negotiates its compression through the WebSocket subprotocols
 * (`connect.bidi.d3.deflate` / `connect.bidi.d3`), echoed on the 101
 * response; upgrades that offer neither are rejected with a 400. Unlike
 * draft 4, compression does not depend on the platform's
 * permessage-deflate support.
 */
export function createBidiWebSocketDraft3Handler(
  handlers: readonly UniversalHandler[],
  options?: CreateBidiWebSocketDraft3HandlerOptions,
): (request: Request) => Response | null {
  const supported =
    options?.withoutCompression === true
      ? [draft3SubprotocolIdentity]
      : [draft3SubprotocolDeflate, draft3SubprotocolIdentity];
  return function handleUpgrade(request: Request): Response | null {
    if (!isWebSocketUpgrade(request)) {
      return null;
    }
    const subprotocol = selectSubprotocol(
      request.headers.get("sec-websocket-protocol"),
      supported,
    );
    if (subprotocol === undefined) {
      return new Response("missing draft 3 subprotocol", { status: 400 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();

    handleMuxedBidiSocketDraft3(wrapWebSocket(server), handlers, {
      compression: subprotocol === draft3SubprotocolDeflate,
      ...options,
    }).catch((error: unknown) => {
      options?.onError?.(error);
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
      headers: { "sec-websocket-protocol": subprotocol },
    });
  };
}
