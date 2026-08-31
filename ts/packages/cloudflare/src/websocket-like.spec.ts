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

// These tests exercise `wrapWebSocket()` bridged to `handleMuxedBidiSocketDraft3()`
// end-to-end against a mock WebSocket-like object, proving the
// DuplexMessageStream adapter itself is correct. `createBidiWebSocketDraft3Handler()`
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
import {
  contentTypeStreamProto,
  contentTypeUnaryProto,
  endStreamFromJson,
  type EndStreamResponse,
} from "@connectrpc/connect/protocol-connect";
import { handleMuxedBidiSocketDraft3 } from "@sudorandom/connect-bidi-core";
import {
  CountUpRequestSchema,
  CountUpResponseSchema,
  CumSumRequestSchema,
  FailRequestSchema,
  PingRequestSchema,
  PingResponseSchema,
  PingService,
} from "./gen/connectbidi/ping/v1/ping_pb.js";
import { wrapWebSocket } from "./websocket-like.js";

// Wire-level constants for draft 3 of the WebSocket protocol. They are
// redefined here rather than imported, the same way each implementation of
// the wire protocol keeps its own copy in sync by hand. Every WebSocket
// message is a 4-byte big-endian stream ID, a descriptor byte carrying the
// frame type in bits 0-6 and a compressed flag in bit 7, and the payload --
// which the message boundary delimits, so there is no length field.
const draft3TypeData = 0x00;
const draft3TypeHeaders = 0x01;
const draft3TypeEndStream = 0x02;
const draft3TypeReset = 0x03;
const draft3Compressed = 0x80;
const streamIdLength = 4;

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
  closeReason: string | undefined;
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

  close(_code?: number, reason?: string): void {
    if (this.closed) {
      // Workers throw when close() is called twice; the adapter must have
      // guarded against that.
      throw new TypeError("Can't call WebSocket close() after close().");
    }
    this.closed = true;
    this.closeReason = reason;
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

  /** Test helper: fires a (possibly duplicate) close event. */
  emitClose(): void {
    for (const listener of this.listeners.close) {
      listener();
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

/** One draft 3 frame: stream ID, descriptor byte, payload. */
function encodeFrame(
  streamId: number,
  type: number,
  payload: Uint8Array,
): Uint8Array {
  const frame = new Uint8Array(streamIdLength + 1 + payload.byteLength);
  new DataView(frame.buffer).setUint32(0, streamId);
  frame[streamIdLength] = type;
  frame.set(payload, streamIdLength + 1);
  return frame;
}

function encodeHeadersFrame(
  streamId: number,
  path: string,
  contentType: string,
): Uint8Array {
  const metadata = { ":path": [path], "content-type": [contentType] };
  const payload = new TextEncoder().encode(JSON.stringify({ metadata }));
  return encodeFrame(streamId, draft3TypeHeaders, payload);
}

function encodeDataFrame(streamId: number, payload: Uint8Array): Uint8Array {
  return encodeFrame(streamId, draft3TypeData, payload);
}

function encodeEndStreamFrame(streamId: number): Uint8Array {
  return encodeFrame(streamId, draft3TypeEndStream, new Uint8Array());
}

function encodeResetFrame(streamId: number): Uint8Array {
  return encodeFrame(streamId, draft3TypeReset, new Uint8Array());
}

/**
 * Splits a `MockWebSocket`'s captured `send()` calls back into the frames
 * belonging to one stream, reported in the `EnvelopedMessage` shape the
 * assertions below use: `flags` is the frame type. Each `send()` call here
 * always carries exactly one frame -- `wrapWebSocket`'s writable forwards
 * `handleMuxedBidiSocketDraft3`'s writes verbatim, and the muxed handler
 * only ever writes one complete frame per `write()` call.
 */
function parseSentEnvelopes(
  frames: readonly Uint8Array[],
  streamId: number,
): EnvelopedMessage[] {
  const envelopes: EnvelopedMessage[] = [];
  for (const frame of frames) {
    assert.ok(
      frame.byteLength >= streamIdLength + 1,
      `frame too short: ${frame.byteLength} bytes`,
    );
    const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
    if (view.getUint32(0) !== streamId) {
      continue;
    }
    const descriptor = view.getUint8(streamIdLength);
    assert.strictEqual(
      descriptor & draft3Compressed,
      0,
      "these tests negotiate no compression, so no frame may set the deflate bit",
    );
    envelopes.push({
      flags: descriptor & ~draft3Compressed,
      // Draft 3 has no length field: the message boundary delimits the
      // payload.
      data: frame.subarray(streamIdLength + 1),
    });
  }
  return envelopes;
}

/**
 * Waits until a stream's response is complete (its end-stream envelope has
 * been sent). Closing the mock socket terminates the connection and aborts
 * in-flight RPCs -- like the Go server -- so tests must wait for the
 * response before closing.
 */
async function waitForEndStream(
  socket: MockWebSocket,
  streamId: number,
): Promise<void> {
  const hasEndStream = (): boolean =>
    parseSentEnvelopes(socket.sentFrames, streamId).some(
      (env) => env.flags === draft3TypeEndStream,
    );
  for (let i = 0; i < 1000 && !hasEndStream(); i++) {
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  assert.ok(hasEndStream(), `stream ${streamId} never finished`);
}

function parseResponse(
  frames: readonly Uint8Array[],
  streamId: number,
): {
  headers: EnvelopedMessage;
  dataEnvelopes: EnvelopedMessage[];
  end: EndStreamResponse;
} {
  const envelopes = parseSentEnvelopes(frames, streamId);
  assert.ok(envelopes.length >= 2, "expected at least headers + end-stream");
  const [headers, ...rest] = envelopes;
  assert.strictEqual(headers.flags, draft3TypeHeaders);
  const last = rest[rest.length - 1];
  assert.strictEqual(
    last.flags,
    draft3TypeEndStream,
    "expected a trailing end-stream frame",
  );
  return {
    headers,
    dataEnvelopes: rest.slice(0, -1),
    end: endStreamFromJson(last.data),
  };
}

// -- Tests ---------------------------------------------------------------------

describe("wrapWebSocket() + handleMuxedBidiSocketDraft3()", () => {
  it("unary success", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "Ping");
    const socket = new MockWebSocket();
    const duplex = wrapWebSocket(socket);

    socket.emit(
      encodeHeadersFrame(1, handler.requestPath, contentTypeUnaryProto),
    );
    socket.emit(
      encodeDataFrame(
        1,
        toBinary(
          PingRequestSchema,
          create(PingRequestSchema, { number: BigInt(42), text: "hi" }),
        ),
      ),
    );
    socket.emit(encodeEndStreamFrame(1));

    const done = handleMuxedBidiSocketDraft3(duplex, handlers, {
      compression: false,
    });
    await waitForEndStream(socket, 1);
    socket.close();
    await done;

    const response = parseResponse(socket.sentFrames, 1);
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

    socket.emit(
      encodeHeadersFrame(1, handler.requestPath, contentTypeUnaryProto),
    );
    socket.emit(
      encodeDataFrame(
        1,
        toBinary(
          FailRequestSchema,
          create(FailRequestSchema, { code: Code.InvalidArgument }),
        ),
      ),
    );
    socket.emit(encodeEndStreamFrame(1));

    const done = handleMuxedBidiSocketDraft3(duplex, handlers, {
      compression: false,
    });
    await waitForEndStream(socket, 1);
    socket.close();
    await done;

    const response = parseResponse(socket.sentFrames, 1);
    assert.strictEqual(response.dataEnvelopes.length, 0);
    assert.ok(
      response.end.error,
      "expected an error in the end-stream envelope",
    );
    assert.strictEqual(response.end.error?.code, Code.InvalidArgument);
  });

  it("server-streaming", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "CountUp");
    const socket = new MockWebSocket();
    const duplex = wrapWebSocket(socket);

    socket.emit(
      encodeHeadersFrame(1, handler.requestPath, contentTypeStreamProto),
    );
    socket.emit(
      encodeDataFrame(
        1,
        toBinary(
          CountUpRequestSchema,
          create(CountUpRequestSchema, { number: BigInt(3) }),
        ),
      ),
    );
    socket.emit(encodeEndStreamFrame(1));

    const done = handleMuxedBidiSocketDraft3(duplex, handlers, {
      compression: false,
    });
    await waitForEndStream(socket, 1);
    socket.close();
    await done;

    const response = parseResponse(socket.sentFrames, 1);
    const numbers = response.dataEnvelopes.map(
      (env) => fromBinary(CountUpResponseSchema, env.data).number,
    );
    assert.deepStrictEqual(numbers, [BigInt(1), BigInt(2), BigInt(3)]);
    assert.strictEqual(response.end.error, undefined);
  });

  it("multiplexes concurrent RPCs on one connection", async () => {
    const handlers = createTestHandlers();
    const countUp = findHandler(handlers, "CountUp");
    const ping = findHandler(handlers, "Ping");
    const socket = new MockWebSocket();
    const duplex = wrapWebSocket(socket);

    // Interleave two streams' frames on the same connection.
    socket.emit(
      encodeHeadersFrame(1, countUp.requestPath, contentTypeStreamProto),
    );
    socket.emit(encodeHeadersFrame(2, ping.requestPath, contentTypeUnaryProto));
    socket.emit(
      encodeDataFrame(
        2,
        toBinary(
          PingRequestSchema,
          create(PingRequestSchema, { number: BigInt(9), text: "hi" }),
        ),
      ),
    );
    socket.emit(
      encodeDataFrame(
        1,
        toBinary(
          CountUpRequestSchema,
          create(CountUpRequestSchema, { number: BigInt(2) }),
        ),
      ),
    );
    socket.emit(encodeEndStreamFrame(2));
    socket.emit(encodeEndStreamFrame(1));

    const done = handleMuxedBidiSocketDraft3(duplex, handlers, {
      compression: false,
    });
    await waitForEndStream(socket, 1);
    await waitForEndStream(socket, 2);
    socket.close();
    await done;

    const countUpResponse = parseResponse(socket.sentFrames, 1);
    const numbers = countUpResponse.dataEnvelopes.map(
      (env) => fromBinary(CountUpResponseSchema, env.data).number,
    );
    assert.deepStrictEqual(numbers, [BigInt(1), BigInt(2)]);
    assert.strictEqual(countUpResponse.end.error, undefined);

    const pingResponse = parseResponse(socket.sentFrames, 2);
    assert.strictEqual(pingResponse.dataEnvelopes.length, 1);
    assert.strictEqual(
      fromBinary(PingResponseSchema, pingResponse.dataEnvelopes[0].data).number,
      BigInt(9),
    );
  });

  it("a reset frame cancels an in-flight RPC without ending the connection", async () => {
    const handlers = createTestHandlers();
    const cumSum = findHandler(handlers, "CumSum");
    const ping = findHandler(handlers, "Ping");
    const socket = new MockWebSocket();
    const duplex = wrapWebSocket(socket);

    // Stream 1 never half-closes; the client abandons it with a reset
    // frame instead. Stream 2 then runs to completion on the same
    // connection, proving the reset didn't take the connection down.
    socket.emit(
      encodeHeadersFrame(1, cumSum.requestPath, contentTypeStreamProto),
    );
    socket.emit(
      encodeDataFrame(
        1,
        toBinary(
          CumSumRequestSchema,
          create(CumSumRequestSchema, { number: BigInt(5) }),
        ),
      ),
    );
    socket.emit(encodeResetFrame(1));
    socket.emit(encodeHeadersFrame(2, ping.requestPath, contentTypeUnaryProto));
    socket.emit(
      encodeDataFrame(
        2,
        toBinary(
          PingRequestSchema,
          create(PingRequestSchema, { number: BigInt(3), text: "after" }),
        ),
      ),
    );
    socket.emit(encodeEndStreamFrame(2));

    const done = handleMuxedBidiSocketDraft3(duplex, handlers, {
      compression: false,
    });
    await waitForEndStream(socket, 2);
    socket.close();
    await done;

    const pingResponse = parseResponse(socket.sentFrames, 2);
    assert.strictEqual(pingResponse.dataEnvelopes.length, 1);
    assert.strictEqual(
      fromBinary(PingResponseSchema, pingResponse.dataEnvelopes[0].data).number,
      BigInt(3),
    );
    assert.strictEqual(pingResponse.end.error, undefined);
  });

  it("a WebSocket close event aborts in-flight streams", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "CumSum");
    const socket = new MockWebSocket();
    const duplex = wrapWebSocket(socket);

    // No explicit end-stream envelope and no reset: only the WebSocket
    // "close" event. On a multiplexed connection that is not a half-close
    // of the stream -- it terminates the connection, aborting the RPC.
    socket.emit(
      encodeHeadersFrame(1, handler.requestPath, contentTypeStreamProto),
    );
    socket.close();

    // Must resolve promptly (the aborted handler ends) rather than waiting
    // forever for request messages that can never arrive.
    await handleMuxedBidiSocketDraft3(duplex, handlers, { compression: false });
  });

  it("unknown :path yields an unimplemented error", async () => {
    const handlers = createTestHandlers();
    const socket = new MockWebSocket();
    const duplex = wrapWebSocket(socket);

    socket.emit(
      encodeHeadersFrame(
        1,
        "/connectbidi.ping.v1.PingService/DoesNotExist",
        contentTypeUnaryProto,
      ),
    );
    socket.emit(encodeEndStreamFrame(1));

    const done = handleMuxedBidiSocketDraft3(duplex, handlers, {
      compression: false,
    });
    await waitForEndStream(socket, 1);
    socket.close();
    await done;

    const response = parseResponse(socket.sentFrames, 1);
    assert.strictEqual(response.dataEnvelopes.length, 0);
    assert.ok(
      response.end.error,
      "expected an error in the end-stream envelope",
    );
    assert.strictEqual(response.end.error?.code, Code.Unimplemented);
  });

  it("idle timeout tears down a quiet connection", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "CumSum");
    const socket = new MockWebSocket();
    const duplex = wrapWebSocket(socket);

    // A stream opens, then the peer goes silent — never half-closing,
    // never resetting, never disconnecting. Without the idle timeout this
    // would park the handler (and on Workers, pin the invocation) forever.
    socket.emit(
      encodeHeadersFrame(1, handler.requestPath, contentTypeStreamProto),
    );

    await handleMuxedBidiSocketDraft3(duplex, handlers, {
      compression: false,
      idleTimeoutMs: 20,
    });

    assert.ok(socket.closed, "expected the idle connection to be closed");
    assert.strictEqual(
      socket.closeReason,
      "idle timeout",
      "the close reason tells the peer this was an expected disconnect",
    );
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
    // the handler must resolve rather than reject (rejections are surfaced
    // through onError and logged as server errors).
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "Ping");
    const socket = new MockWebSocket({ failSendAfterClose: true });
    const duplex = wrapWebSocket(socket);

    socket.emit(
      encodeHeadersFrame(1, handler.requestPath, contentTypeUnaryProto),
    );
    socket.emit(
      encodeDataFrame(
        1,
        toBinary(
          PingRequestSchema,
          create(PingRequestSchema, { number: BigInt(1), text: "bye" }),
        ),
      ),
    );
    socket.emit(encodeEndStreamFrame(1));
    // The peer disconnects before the handler has produced its response;
    // every subsequent send() throws, like on real workerd.
    socket.close();

    await handleMuxedBidiSocketDraft3(duplex, handlers, { compression: false });

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
    socket.emit(encodeEndStreamFrame(1));
    socket.emitClose();
  });
});
