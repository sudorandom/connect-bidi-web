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
  createConnectWebSocketDraft1Transport,
  createConnectWebSocketDraft3Transport,
  createConnectWebSocketDraft4Transport,
  createConnectWebSocketDraft5Transport,
  createConnectWebSocketDraft7Transport,
  createConnectWebTransportTransport,
  createFallbackTransport,
} from "@sudorandom/connect-bidi-web";
import { isWebTransportSupported } from "./webtransport-support.js";

/** Which bidi-capable transport carries streaming RPCs. */
export type StreamingTransportChoice =
  | "auto"
  | "webtransport"
  | "websocket-draft1"
  | "websocket-draft3"
  | "websocket-draft4"
  | "websocket-draft5"
  | "websocket-draft7";

export interface DemoTransportOptions {
  /**
   * WebSocket only: dial a dedicated connection per streaming RPC (the
   * transport's `connectionPerStream` option) instead of multiplexing all
   * RPCs onto one shared connection. Ignored for WebTransport, where QUIC
   * streams make the question moot.
   */
  connectionPerStream?: boolean;
  /**
   * Auto only: the /capabilities.json probe's answer to whether the server
   * terminates WebTransport. `false` drops the WebTransport rung from the
   * ladder outright (a Workers deployment, say, would otherwise cost every
   * fresh visitor a 10-second handshake timeout before degrading);
   * undefined (probe not yet answered) keeps the rung and lets the ladder
   * find out.
   */
  serverWebTransport?: boolean;
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
    // The degrading ladder: WebTransport first, WebSocket (draft 7, the
    // specification) as the fallback. The WebTransport rung is included whenever the API and URL
    // scheme allow a dial attempt and the server hasn't already said no —
    // whether it actually works beyond that (HTTP/3 reachability,
    // certificates) is exactly what the ladder finds out, remembers, and
    // degrades past.
    const rungs: Transport[] = [];
    const webTransportPossible =
      isWebTransportSupported() &&
      new URL(serverUrl).protocol === "https:" &&
      options.serverWebTransport !== false;
    if (webTransportPossible) {
      rungs.push(createWebTransportStreaming(serverUrl));
    }
    rungs.push(
      createConnectWebSocketDraft7Transport({
        baseUrl: serverUrl,
        pathPrefix: "/websocket-draft7",
        useBinaryFormat: false,
        unaryTransport: unary,
      }),
    );
    return {
      transport: createCompositeTransport(
        unary,
        createFallbackTransport(...rungs),
      ),
      description: webTransportPossible
        ? "Degrading transport: WebTransport is tried first, and the " +
          "connection falls back to WebSocket (draft 7) if it can't be " +
          "established. The rung that works is remembered."
        : "Degrading transport: WebTransport is not available in this " +
          "browser or on this server, so the ladder starts at WebSocket " +
          "(draft 7).",
    };
  }

  if (choice === "websocket-draft1") {
    // Draft 1 carries streaming RPCs only, so it is composed with the plain
    // Connect transport for unary ones. It ignores the connection-per-RPC
    // toggle, because that is all it does — one WebSocket per streaming
    // RPC is the design.
    return {
      transport: createCompositeTransport(
        unary,
        createConnectWebSocketDraft1Transport({
          // The demo server mounts draft 1 under a prefix, because draft 5
          // already has the bare procedure URLs. A deployment serving draft
          // 1 alone would point this straight at serverUrl.
          baseUrl: `${serverUrl.replace(/\/$/, "")}/websocket-draft1`,
          // application/connect+json, so the payload after each envelope
          // head reads as the JSON it is rather than as protobuf bytes.
          useBinaryFormat: false,
          unaryTransport: unary,
        }),
      ),
      description:
        "Draft 1 wire protocol: each lane below opens its own WebSocket on " +
        "the RPC's own URL, and every message on it is one standard 5-byte " +
        "Connect envelope — a flag byte, a length, and the payload. " +
        "Each direction opens with a headers envelope, because a browser " +
        "can neither set headers on the upgrade nor read them off the " +
        "response. In the Network tab the messages are binary: the five " +
        "bytes of head are the price of keeping Connect's envelope intact, " +
        "and the price draft 5 refused to pay.",
    };
  }

  if (choice === "websocket-draft5") {
    // Draft 5 needs no composite transport: it routes unary RPCs to the
    // plain Connect transport itself, and upgrades only streaming ones. It
    // also ignores the connection-per-RPC toggle, because that is all it
    // does — one WebSocket per streaming RPC is the design.
    return {
      transport: createConnectWebSocketDraft5Transport({
        baseUrl: serverUrl,
        // application/connect+json, so the messages in the Network tab read
        // as the JSON they are rather than as protobuf bytes.
        useBinaryFormat: false,
        unaryTransport: unary,
      }),
      description:
        "Draft 5 wire protocol: no framing at all. Each lane below opens " +
        "its own WebSocket on the RPC's own URL \u2014 the handshake is " +
        "the request \u2014 and the messages that follow are the codec's " +
        "output, unadorned. This demo negotiates " +
        "application/connect+json, so in the Network tab every message is " +
        "readable: a JSON metadata message opens and closes each " +
        "connection, the RPC messages are JSON between them, and an empty " +
        "text message marks the end of each direction's data.",
    };
  }

  if (choice === "websocket-draft7") {
    // Draft 7 is the Connect-over-WebSocket specification. Like draft 5
    // it routes unary RPCs to the plain Connect transport itself and dials
    // one WebSocket per streaming RPC, so the connection-per-RPC toggle
    // does not apply. The demo server mounts it under a prefix, which the
    // specification provides for, because draft 5 has the bare URLs.
    return {
      transport: createConnectWebSocketDraft7Transport({
        baseUrl: serverUrl,
        pathPrefix: "/websocket-draft7",
        // JSON, so the messages in the Network tab read as the JSON they
        // are: an M, B, C, or S marker, then the payload.
        useBinaryFormat: false,
        unaryTransport: unary,
      }),
      description:
        "Draft 7 wire protocol \u2014 the Connect-over-WebSocket " +
        "specification. Each lane below opens its own WebSocket under " +
        "/websocket-draft7 on the RPC's own URL, and every message is one " +
        "marker byte and a payload: M for the leading metadata that opens " +
        "each direction, B for a body, C when the client is done, S for " +
        "the server's EndStreamResponse. The codec is selected by the " +
        "subprotocol (connectrpc.1+json here, so every message is a " +
        "readable text frame), and the deadline rides on the handshake URI.",
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

  if (choice === "websocket-draft3") {
    const streaming = createConnectWebSocketDraft3Transport({
      baseUrl: serverUrl,
      connectionPerStream,
    });
    return {
      transport: createCompositeTransport(unary, streaming),
      description:
        "Draft 3 wire protocol: a packed head inside each WebSocket " +
        "message (stream ID, descriptor byte, payload), with compression " +
        "as a protocol option — a subprotocol negotiates per-frame raw " +
        "DEFLATE, signaled by one bit in that descriptor, independent of " +
        "the WebSocket layer. " +
        lanes,
    };
  }

  if (choice === "websocket-draft4") {
    const streaming = createConnectWebSocketDraft4Transport({
      baseUrl: serverUrl,
      connectionPerStream,
    });
    return {
      transport: createCompositeTransport(unary, streaming),
      description:
        "Draft 4 wire protocol: the frame head is ASCII text rather than " +
        "packed bytes \u2014 \"7|1|{...}\", a stream ID and a flags field " +
        "separated by pipes \u2014 and every frame here is a text WebSocket " +
        "message. Open the Network tab, pick this connection, and read the " +
        "conversation as it happens. " +
        lanes,
    };
  }

  // Any remaining choice is draft 3 (currentChoice() maps unknown values
  // to draft 7 before reaching here).
  const streaming = createConnectWebSocketDraft3Transport({
    baseUrl: serverUrl,
    connectionPerStream,
  });
  return {
    transport: createCompositeTransport(unary, streaming),
    description:
      "Draft 3 wire protocol: a packed head inside each WebSocket message " +
      "(stream ID, descriptor byte, payload), with compression as a " +
      "protocol option \u2014 a subprotocol negotiates per-frame raw " +
      "DEFLATE, signaled by one bit in that descriptor, independent of the " +
      "WebSocket layer. " +
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
