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

// These tests exercise `wrapWebSocket()` bridged to `handleBidiSocket()`
// end-to-end against a mock WebSocket-like object, proving the
// DuplexByteStream adapter itself is correct. `createBidiWebSocketHandler()`
// additionally relies on the real Workers `WebSocketPair`/`Response.webSocket`
// globals, which don't exist under plain Node -- that integration is
// verified against `wrangler dev` instead (see the worker demo).

import * as assert from "node:assert";
import { describe, it } from "node:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import type { ServiceImpl } from "@connectrpc/connect";
import { Code, ConnectError, createConnectRouter } from "@connectrpc/connect";
import type {
  EnvelopedMessage,
  UniversalHandler,
} from "@connectrpc/connect/protocol";
import { encodeEnvelope } from "@connectrpc/connect/protocol";
import {
  contentTypeStreamProto,
  contentTypeUnaryProto,
  endStreamFlag,
  endStreamFromJson,
  type EndStreamResponse,
} from "@connectrpc/connect/protocol-connect";
import { handleBidiSocket } from "@sudorandom/connect-bidi-core";
import {
  CountUpRequestSchema,
  CountUpResponseSchema,
  FailRequestSchema,
  PingRequestSchema,
  PingResponseSchema,
  PingService,
} from "./gen/connectbidi/ping/v1/ping_pb.js";
import { wrapWebSocket } from "./websocket-like.js";

// Wire-level constants for the bidi-web envelope protocol's leading
// metadata frame. These must match @sudorandom/connect-bidi-core's
// wire.ts (and @sudorandom/connect-bidi-web's client transports) -- they
// are not part of connect-es's own protocol-connect flags, so they are
// redefined here rather than imported, the same way each implementation of
// the wire protocol keeps its own copy in sync by hand.
const flagEnvelopeHeaders = 0x04;
const flagEnvelopeData = 0x00;

// -- Test service implementation ---------------------------------------------

const pingImpl: ServiceImpl<typeof PingService> = {
  ping: (req) => ({ number: req.number, text: req.text }),
  fail: (req) => {
    throw new ConnectError(`failed with code ${req.code}`, req.code as Code);
  },
  sum: async (reqs) => {
    let sum = BigInt(0);
    for await (const req of reqs) {
      sum += req.number;
    }
    return { sum };
  },
  countUp: async function* (req) {
    for (let i = BigInt(1); i <= req.number; i++) {
      yield { number: i };
    }
  },
  cumSum: async function* (reqs) {
    let sum = BigInt(0);
    for await (const req of reqs) {
      sum += req.number;
      yield { sum };
    }
  },
};

function createTestHandlers(): UniversalHandler[] {
  const router = createConnectRouter();
  router.service(PingService, pingImpl);
  return router.handlers;
}

function findHandler(
  handlers: UniversalHandler[],
  methodName: string,
): UniversalHandler {
  const handler = handlers.find((h) => h.method.name === methodName);
  assert.ok(handler, `no handler registered for ${methodName}`);
  return handler;
}

// -- Mock WebSocket -----------------------------------------------------------

/**
 * A minimal mock of the Workers `WebSocket` surface `wrapWebSocket` needs.
 * Structurally satisfies `BidiWebSocketLike` without polyfilling any
 * Workers globals.
 */
class MockWebSocket {
  private readonly listeners: {
    message: ((event: { data: unknown }) => void)[];
    close: (() => void)[];
    error: (() => void)[];
  } = { message: [], close: [], error: [] };
  readonly sentFrames: Uint8Array[] = [];
  closed = false;
  // Workers WebSockets follow the spec default of "blob"; wrapWebSocket
  // must flip this to "arraybuffer".
  binaryType = "blob";
  // Workers WebSockets throw from send() once the socket is closed; opt in
  // to that behavior to model a peer that disconnects mid-RPC.
  private readonly failSendAfterClose: boolean;

  constructor(opts: { failSendAfterClose?: boolean } = {}) {
    this.failSendAfterClose = opts.failSendAfterClose === true;
  }

  addEventListener(
    type: "message",
    listener: (event: { data: unknown }) => void,
  ): void;
  addEventListener(type: "close", listener: () => void): void;
  addEventListener(type: "error", listener: () => void): void;
  addEventListener(
    type: "message" | "close" | "error",
    listener: ((event: { data: unknown }) => void) | (() => void),
  ): void {
    // TS can't correlate `type` with the matching arm of `listener`'s union
    // across two independently-typed overload parameters, hence the casts.
    switch (type) {
      case "message":
        this.listeners.message.push(
          listener as (event: { data: unknown }) => void,
        );
        break;
      case "close":
        this.listeners.close.push(listener as () => void);
        break;
      case "error":
        this.listeners.error.push(listener as () => void);
        break;
    }
  }

  send(message: ArrayBuffer | ArrayBufferView | string): void {
    if (this.closed && this.failSendAfterClose) {
      throw new TypeError("Can't call WebSocket send() after close().");
    }
    this.sentFrames.push(toBytes(message));
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    for (const listener of this.listeners.close) {
      listener();
    }
  }

  /** Test helper: simulates the peer sending one raw frame. */
  emit(data: Uint8Array): void {
    for (const listener of this.listeners.message) {
      listener({ data: data.slice() });
    }
  }
}

function toBytes(message: ArrayBuffer | ArrayBufferView | string): Uint8Array {
  if (typeof message === "string") {
    return new TextEncoder().encode(message);
  }
  if (message instanceof Uint8Array) {
    return message;
  }
  if (message instanceof ArrayBuffer) {
    return new Uint8Array(message);
  }
  return new Uint8Array(message.buffer, message.byteOffset, message.byteLength);
}

// -- Wire-level test helpers --------------------------------------------------

function encodeHeadersEnvelope(path: string, contentType: string): Uint8Array {
  const metadata = { ":path": [path], "content-type": [contentType] };
  const payload = new TextEncoder().encode(JSON.stringify({ metadata }));
  return encodeEnvelope(flagEnvelopeHeaders, payload);
}

function encodeEndStream(): Uint8Array {
  return encodeEnvelope(endStreamFlag, new Uint8Array());
}

/**
 * Splits a `MockWebSocket`'s captured, already envelope-framed `send()`
 * calls back into individual envelopes. Each `send()` call here always
 * carries exactly one complete envelope -- `wrapWebSocket`'s writable
 * forwards `handleBidiSocket`'s writes verbatim, and `handleBidiSocket`
 * itself only ever writes one complete envelope per `write()` call.
 */
function parseSentEnvelopes(frames: readonly Uint8Array[]): EnvelopedMessage[] {
  return frames.map((frame) => {
    const flags = frame[0];
    const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
    const length = view.getUint32(1, false);
    return { flags, data: frame.subarray(5, 5 + length) };
  });
}

function parseResponse(frames: readonly Uint8Array[]): {
  headers: EnvelopedMessage;
  dataEnvelopes: EnvelopedMessage[];
  end: EndStreamResponse;
} {
  const envelopes = parseSentEnvelopes(frames);
  assert.ok(envelopes.length >= 2, "expected at least headers + end-stream");
  const [headers, ...rest] = envelopes;
  assert.strictEqual(headers.flags, flagEnvelopeHeaders);
  const last = rest[rest.length - 1];
  assert.strictEqual(last.flags, endStreamFlag, "expected a trailing end-stream envelope");
  return {
    headers,
    dataEnvelopes: rest.slice(0, -1),
    end: endStreamFromJson(last.data),
  };
}

// -- Tests ---------------------------------------------------------------------

describe("wrapWebSocket() + handleBidiSocket()", () => {
  it("unary success", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "Ping");
    const socket = new MockWebSocket();
    const duplex = wrapWebSocket(socket);

    socket.emit(encodeHeadersEnvelope(handler.requestPath, contentTypeUnaryProto));
    socket.emit(
      encodeEnvelope(
        flagEnvelopeData,
        toBinary(
          PingRequestSchema,
          create(PingRequestSchema, { number: BigInt(42), text: "hi" }),
        ),
      ),
    );
    socket.emit(encodeEndStream());

    await handleBidiSocket(duplex, handlers);

    const response = parseResponse(socket.sentFrames);
    assert.strictEqual(response.dataEnvelopes.length, 1);
    const body = fromBinary(PingResponseSchema, response.dataEnvelopes[0].data);
    assert.strictEqual(body.number, BigInt(42));
    assert.strictEqual(body.text, "hi");
    assert.strictEqual(response.end.error, undefined);
    assert.ok(socket.closed, "expected the mock socket to be closed");
  });

  it("unary error", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "Fail");
    const socket = new MockWebSocket();
    const duplex = wrapWebSocket(socket);

    socket.emit(encodeHeadersEnvelope(handler.requestPath, contentTypeUnaryProto));
    socket.emit(
      encodeEnvelope(
        flagEnvelopeData,
        toBinary(
          FailRequestSchema,
          create(FailRequestSchema, { code: Code.InvalidArgument }),
        ),
      ),
    );
    socket.emit(encodeEndStream());

    await handleBidiSocket(duplex, handlers);

    const response = parseResponse(socket.sentFrames);
    assert.strictEqual(response.dataEnvelopes.length, 0);
    assert.ok(response.end.error, "expected an error in the end-stream envelope");
    assert.strictEqual(response.end.error?.code, Code.InvalidArgument);
  });

  it("server-streaming", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "CountUp");
    const socket = new MockWebSocket();
    const duplex = wrapWebSocket(socket);

    socket.emit(encodeHeadersEnvelope(handler.requestPath, contentTypeStreamProto));
    socket.emit(
      encodeEnvelope(
        flagEnvelopeData,
        toBinary(
          CountUpRequestSchema,
          create(CountUpRequestSchema, { number: BigInt(3) }),
        ),
      ),
    );
    socket.emit(encodeEndStream());

    await handleBidiSocket(duplex, handlers);

    const response = parseResponse(socket.sentFrames);
    const numbers = response.dataEnvelopes.map(
      (env) => fromBinary(CountUpResponseSchema, env.data).number,
    );
    assert.deepStrictEqual(numbers, [BigInt(1), BigInt(2), BigInt(3)]);
    assert.strictEqual(response.end.error, undefined);
  });

  it("a WebSocket close event ends the request stream, like a half-close", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "CountUp");
    const socket = new MockWebSocket();
    const duplex = wrapWebSocket(socket);

    // No explicit end-stream envelope this time -- only the WebSocket
    // "close" event marks the end of the request, exercising the
    // controller.close() wiring in wrapWebSocket's readable source.
    socket.emit(encodeHeadersEnvelope(handler.requestPath, contentTypeStreamProto));
    socket.emit(
      encodeEnvelope(
        flagEnvelopeData,
        toBinary(
          CountUpRequestSchema,
          create(CountUpRequestSchema, { number: BigInt(2) }),
        ),
      ),
    );
    socket.close();

    await handleBidiSocket(duplex, handlers);

    const response = parseResponse(socket.sentFrames);
    const numbers = response.dataEnvelopes.map(
      (env) => fromBinary(CountUpResponseSchema, env.data).number,
    );
    assert.deepStrictEqual(numbers, [BigInt(1), BigInt(2)]);
    assert.strictEqual(response.end.error, undefined);
  });

  it("unknown :path yields an unimplemented error", async () => {
    const handlers = createTestHandlers();
    const socket = new MockWebSocket();
    const duplex = wrapWebSocket(socket);

    socket.emit(
      encodeHeadersEnvelope(
        "/connectbidi.ping.v1.PingService/DoesNotExist",
        contentTypeUnaryProto,
      ),
    );
    socket.emit(encodeEndStream());

    await handleBidiSocket(duplex, handlers);

    const response = parseResponse(socket.sentFrames);
    assert.strictEqual(response.dataEnvelopes.length, 0);
    assert.ok(response.end.error, "expected an error in the end-stream envelope");
    assert.strictEqual(response.end.error?.code, Code.Unimplemented);
  });

  it("sets binaryType to arraybuffer", () => {
    // Regression: workerd honors the WebSocket spec default of
    // binaryType = "blob", and a Blob cannot be read synchronously by the
    // envelope parser -- every message was rejected as unsupported.
    const socket = new MockWebSocket();
    wrapWebSocket(socket);
    assert.strictEqual(socket.binaryType, "arraybuffer");
  });

  it("resolves when the peer closes before the response is written", async () => {
    // Regression: Workers throw from send() after the socket closes. A peer
    // disconnecting mid-response is a normal way for an RPC to end, so
    // handleBidiSocket must resolve rather than reject (rejections are
    // surfaced through onError and logged as server errors).
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "Ping");
    const socket = new MockWebSocket({ failSendAfterClose: true });
    const duplex = wrapWebSocket(socket);

    socket.emit(
      encodeHeadersEnvelope(handler.requestPath, contentTypeUnaryProto),
    );
    socket.emit(
      encodeEnvelope(
        flagEnvelopeData,
        toBinary(
          PingRequestSchema,
          create(PingRequestSchema, { number: BigInt(1), text: "bye" }),
        ),
      ),
    );
    socket.emit(encodeEndStream());
    // The peer disconnects before the handler has produced its response;
    // every subsequent send() throws, like on real workerd.
    socket.close();

    await handleBidiSocket(duplex, handlers);

    assert.strictEqual(
      socket.sentFrames.length,
      0,
      "no frames can be delivered after the peer closed",
    );
  });

  it("ignores socket events after the stream is canceled", async () => {
    // Regression: canceling the readable closes the socket, which fires a
    // "close" event; calling controller.close() on the already-canceled
    // stream threw, and on workerd the exception escaping the event
    // listener tore down the connection before buffered response frames
    // were flushed to the peer.
    const socket = new MockWebSocket();
    const duplex = wrapWebSocket(socket);

    await duplex.readable.cancel();
    assert.ok(socket.closed, "expected cancel to close the socket");

    // Late events (a frame already in flight, a duplicate close) must be
    // no-ops rather than throwing at the dead stream's controller.
    socket.emit(encodeEndStream());
    socket.close();
  });
});
