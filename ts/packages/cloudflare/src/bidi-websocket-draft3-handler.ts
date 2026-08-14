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
import type { CreateBidiWebSocketHandlerOptions } from "./bidi-websocket-handler.js";
import { isWebSocketUpgrade } from "./bidi-websocket-handler.js";
import { wrapWebSocket } from "./websocket-like.js";

/**
 * Selects the first draft 3 token from a comma-separated
 * Sec-WebSocket-Protocol offer, in the client's preference order.
 */
function selectSubprotocol(offer: string | null): string | undefined {
  for (const token of (offer ?? "").split(",")) {
    const trimmed = token.trim();
    if (
      trimmed === draft3SubprotocolDeflate ||
      trimmed === draft3SubprotocolIdentity
    ) {
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
 * draft 2, compression does not depend on the platform's
 * permessage-deflate support.
 */
export function createBidiWebSocketDraft3Handler(
  handlers: readonly UniversalHandler[],
  options?: CreateBidiWebSocketHandlerOptions,
): (request: Request) => Response | null {
  return function handleUpgrade(request: Request): Response | null {
    if (!isWebSocketUpgrade(request)) {
      return null;
    }
    const subprotocol = selectSubprotocol(
      request.headers.get("sec-websocket-protocol"),
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
