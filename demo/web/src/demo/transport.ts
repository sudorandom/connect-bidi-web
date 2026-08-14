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

import type { Transport } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import {
  createCompositeTransport,
  createConnectWebSocketDraft2Transport,
  createConnectWebSocketDraft3Transport,
  createConnectWebSocketTransport,
  createConnectWebTransportTransport,
  createFallbackTransport,
} from "@sudorandom/connect-bidi-web";
import { isWebTransportSupported } from "./webtransport-support.js";

/** Which bidi-capable transport carries streaming RPCs. */
export type StreamingTransportChoice =
  | "auto"
  | "webtransport"
  | "websocket"
  | "websocket-draft2"
  | "websocket-draft3";

export interface DemoTransportOptions {
  /**
   * WebSocket only: dial a dedicated connection per streaming RPC (the
   * transport's `connectionPerStream` option) instead of multiplexing all
   * RPCs onto one shared connection. Ignored for WebTransport, where QUIC
   * streams make the question moot.
   */
  connectionPerStream?: boolean;
}

export interface DemoTransport {
  transport: Transport;
  /** A short, transport-specific description shown next to the demo. */
  description: string;
}

/**
 * Builds the composite transport used by the live demo: unary RPCs always
 * go over plain Connect-over-HTTP (for caching, observability, and proxy
 * support), while streaming RPCs go over whichever bidi transport the
 * visitor picked from the dropdown.
 */
export function createDemoTransport(
  choice: StreamingTransportChoice,
  serverUrl: string,
  options: DemoTransportOptions = {},
): DemoTransport {
  const unary = createConnectTransport({ baseUrl: serverUrl });

  if (choice === "webtransport") {
    if (!isWebTransportSupported()) {
      throw new Error(
        "WebTransport is not supported in this browser or host",
      );
    }
    // The WebTransport constructor throws a DOMException for non-https
    // URLs; fail with a clearer message instead.
    if (new URL(serverUrl).protocol !== "https:") {
      throw new Error(
        `WebTransport requires an https:// server URL, got ${serverUrl}`,
      );
    }
    return {
      transport: createCompositeTransport(
        unary,
        createWebTransportStreaming(serverUrl),
      ),
      description:
        "Each lane below opens an independent bidirectional QUIC stream, " +
        "multiplexed within one WebTransport session over HTTP/3.",
    };
  }

  if (choice === "auto") {
    // The degrading ladder: WebTransport first, WebSocket (draft 2) as the
    // fallback. The WebTransport rung is included whenever the API and URL
    // scheme allow a dial attempt at all — whether it actually works
    // (HTTP/3 reachability, certificates) is exactly what the ladder finds
    // out, remembers, and degrades past.
    const rungs: Transport[] = [];
    const webTransportPossible =
      isWebTransportSupported() &&
      new URL(serverUrl).protocol === "https:";
    if (webTransportPossible) {
      rungs.push(createWebTransportStreaming(serverUrl));
    }
    rungs.push(createConnectWebSocketDraft2Transport({ baseUrl: serverUrl }));
    return {
      transport: createCompositeTransport(
        unary,
        createFallbackTransport(...rungs),
      ),
      description: webTransportPossible
        ? "Degrading transport: WebTransport is tried first, and the " +
          "connection falls back to WebSocket (draft 2) if it can't be " +
          "established. The rung that works is remembered."
        : "Degrading transport: WebTransport is not available in this " +
          "browser or host, so the ladder starts at WebSocket (draft 2).",
    };
  }

  const connectionPerStream = options.connectionPerStream === true;

  // With connectionPerStream, each lane below dials its own WebSocket
  // connection: fully isolated, no head-of-line blocking, one handshake
  // per stream. Without it, lanes are independent streams multiplexed
  // onto one shared connection.
  const lanes = connectionPerStream
    ? "Each lane below dials its own WebSocket connection."
    : "Each lane below is an independent stream, multiplexed onto one " +
      "shared connection.";

  if (choice === "websocket-draft2") {
    const streaming = createConnectWebSocketDraft2Transport({
      baseUrl: serverUrl,
      connectionPerStream,
    });
    return {
      transport: createCompositeTransport(unary, streaming),
      description:
        "Draft 2 wire protocol: inside each WebSocket message, a frame is " +
        "just a stream ID and a type byte — payload length and compression " +
        "are left to the WebSocket layer itself. " +
        lanes,
    };
  }

  if (choice === "websocket-draft3") {
    const streaming = createConnectWebSocketDraft3Transport({
      baseUrl: serverUrl,
      connectionPerStream,
    });
    return {
      transport: createCompositeTransport(unary, streaming),
      description:
        "Draft 3 wire protocol: draft 2's framing inside each WebSocket " +
        "message (stream ID, descriptor byte, payload), with compression " +
        "as a protocol option — a subprotocol negotiates per-frame raw " +
        "DEFLATE, signaled by one bit in that descriptor, independent of " +
        "the WebSocket layer. " +
        lanes,
    };
  }

  const streaming = createConnectWebSocketTransport({
    baseUrl: serverUrl,
    connectionPerStream,
  });
  return {
    transport: createCompositeTransport(unary, streaming),
    description:
      "Draft 1 wire protocol: inside each WebSocket message, a frame is a " +
      "stream ID followed by a standard 5-byte Connect envelope. " +
      lanes,
  };
}

/**
 * Builds the WebTransport streaming transport with a lazily dialed,
 * self-replacing session. Dialing happens on the first streaming RPC:
 * merely loading the page (or having the option preselected in the
 * dropdown) must not open a session — no demo-server traffic happens
 * until the visitor actually interacts with the demo.
 */
function createWebTransportStreaming(serverUrl: string): Transport {
  let session: WebTransport | undefined;
  function dialSession(): WebTransport {
    if (session === undefined) {
      const dialed = new WebTransport(new URL("/webtransport", serverUrl));
      // A rejected connection rejects `ready` and `closed` too. RPC
      // attempts surface the failure as ConnectErrors through the
      // transport, so handle the bare promises here to keep the rejection
      // from also reaching the console as an uncaught error.
      dialed.ready.catch(() => {});
      // Once the session dies — a rejected handshake (e.g. the browser
      // refusing a locally-trusted certificate), a network drop, a server
      // restart — forget it, so the next RPC dials a fresh session
      // instead of failing forever on the corpse.
      const forget = (): void => {
        if (session === dialed) {
          session = undefined;
        }
      };
      dialed.closed.then(forget, forget);
      session = dialed;
    }
    return session;
  }
  return createConnectWebTransportTransport({
    baseUrl: serverUrl,
    // The session option accepts a factory, resolved per RPC.
    session: dialSession,
  });
}
