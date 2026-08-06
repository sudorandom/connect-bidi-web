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
  createConnectWebSocketTransport,
  createConnectWebTransportTransport,
} from "@sudorandom/connect-bidi-web";
import { isWebTransportSupported } from "./webtransport-support.js";

/** Which bidi-capable transport carries streaming RPCs. */
export type StreamingTransportChoice = "webtransport" | "websocket";

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
    // Dial lazily, on the first streaming RPC: merely loading the page (or
    // having WebTransport preselected in the dropdown) must not open a
    // session — no demo-server traffic happens until the visitor actually
    // interacts with the demo.
    let session: WebTransport | undefined;
    function dialSession(): WebTransport {
      if (session === undefined) {
        session = new WebTransport(new URL("/webtransport", serverUrl));
        // A rejected connection rejects `ready` and `closed` too. RPC
        // attempts surface the failure as ConnectErrors through the
        // transport, so handle the bare promises here to keep the rejection
        // from also reaching the console as an uncaught error.
        session.ready.catch(() => {});
        session.closed.catch(() => {});
      }
      return session;
    }
    const streaming = createConnectWebTransportTransport({
      baseUrl: serverUrl,
      // The session option accepts a factory, resolved per RPC.
      session: dialSession,
    });
    return {
      transport: createCompositeTransport(unary, streaming),
      description:
        "Each lane below opens an independent bidirectional QUIC stream, " +
        "multiplexed within one WebTransport session over HTTP/3.",
    };
  }

  const connectionPerStream = options.connectionPerStream === true;
  const streaming = createConnectWebSocketTransport({
    baseUrl: serverUrl,
    connectionPerStream,
  });
  return {
    transport: createCompositeTransport(unary, streaming),
    description: connectionPerStream
      ? "Each lane below dials its own WebSocket connection " +
        "(connectionPerStream) — fully isolated, no head-of-line " +
        "blocking, one handshake per stream."
      : "Each lane below is an independent stream, multiplexed onto one " +
        "shared WebSocket connection by the stream ID on every frame.",
  };
}
