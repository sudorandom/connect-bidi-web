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

import * as http from "node:http";
import * as assert from "node:assert";
import { describe, it } from "node:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import type { ServiceImpl } from "@connectrpc/connect";
import { Code, ConnectError, createConnectRouter } from "@connectrpc/connect";
import type { UniversalHandler } from "@connectrpc/connect/protocol";
import {
  contentTypeStreamProto,
  contentTypeUnaryJson,
  contentTypeUnaryProto,
  endStreamFromJson,
  type EndStreamResponse,
} from "@connectrpc/connect/protocol-connect";
import type { Draft4OutgoingFrame } from "@sudorandom/connect-bidi-core";
import type { RawData } from "ws";
import { WebSocket } from "ws";
import {
  CumSumRequestSchema,
  CumSumResponseSchema,
  FailRequestSchema,
  PingRequestSchema,
  PingResponseSchema,
  PingService,
} from "./gen/connectbidi/ping/v1/ping_pb.js";
import {
  createBidiWebSocketDraft4Handler,
  defaultBidiWebSocketDraft4Path,
} from "./create-bidi-websocket-draft4-handler.js";
import { websocketToDraft4DuplexMessageStream } from "./websocket-duplex-draft4.js";

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

// -- Server + client test helpers ---------------------------------------------

interface RunningServer {
  server: http.Server;
  port: number;
  clients: WebSocket[];
  close(): Promise<void>;
}

async function startServer(
  handlers: UniversalHandler[],
): Promise<RunningServer> {
  const server = http.createServer();
  createBidiWebSocketDraft4Handler(handlers).upgrade(server);
  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  const clients: WebSocket[] = [];
  return {
    server,
    port,
    clients,
    close: () => {
      // A muxed connection stays open across RPCs, so the clients must be
      // closed for server.close() to complete.
      for (const ws of clients) {
        ws.close();
      }
      return new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      });
    },
  };
}

async function connectClient(
  running: RunningServer,
  path: string = defaultBidiWebSocketDraft4Path,
): Promise<WebSocket> {
  const ws = new WebSocket(`ws://127.0.0.1:${running.port}${path}`);
  await new Promise<void>((resolve, reject) => {
    ws.once("open", () => resolve());
    ws.once("error", reject);
  });
  running.clients.push(ws);
  return ws;
}

// -- Wire-level helpers (mirrors draft 4 of the wire protocol used by
// @sudorandom/connect-bidi-core and @sudorandom/connect-bidi-web; see those
// packages for the canonical definition). Every WebSocket message is an
// ASCII head, "<stream ID>|<flags>|", followed by the payload -- no payload
// length, no compression flag. -------------------------------------------

const frameTypeData = 0;
const frameTypeHeaders = 1;
const frameTypeEndStream = 2;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * Build one draft 4 frame. `text` decides the WebSocket opcode; control
 * frames are always JSON, and a proto data payload must go out as binary.
 */
function frame(
  streamId: number,
  type: number,
  payload: Uint8Array,
  text: boolean,
): Draft4OutgoingFrame {
  const head = encoder.encode(`${streamId}|${type}|`);
  const data = new Uint8Array(head.byteLength + payload.byteLength);
  data.set(head, 0);
  data.set(payload, head.byteLength);
  return { data, text };
}

function encodeHeadersPayload(
  path: string,
  contentType: string,
  extraHeaders: Record<string, string> = {},
): Uint8Array {
  const metadata: Record<string, string[]> = {
    ":path": [path],
    "content-type": [contentType],
    ...Object.fromEntries(
      Object.entries(extraHeaders).map(([k, v]) => [k, [v]]),
    ),
  };
  return encoder.encode(JSON.stringify({ metadata }));
}

function decodeHeadersPayload(payload: Uint8Array): Headers {
  const parsed = JSON.parse(decoder.decode(payload)) as {
    metadata?: Record<string, string[]>;
  };
  const headers = new Headers();
  for (const [key, values] of Object.entries(parsed.metadata ?? {})) {
    if (key.startsWith(":")) {
      continue;
    }
    for (const value of values) {
      headers.append(key, value);
    }
  }
  return headers;
}

async function writeEndStream(
  writer: WritableStreamDefaultWriter<Draft4OutgoingFrame>,
  streamId: number,
): Promise<void> {
  await writer.write(
    frame(streamId, frameTypeEndStream, new Uint8Array(), true),
  );
}

interface StreamFrame {
  type: number;
  payload: Uint8Array;
}

interface ParsedResponse {
  headers: Headers;
  dataFrames: StreamFrame[];
  end: EndStreamResponse;
}

/** Split one draft 4 message into stream ID, flags, and payload. */
function decodeFrame(message: Uint8Array): {
  streamId: number;
  type: number;
  payload: Uint8Array;
} {
  const text = decoder.decode(message);
  const firstSep = text.indexOf("|");
  assert.ok(firstSep > 0, `frame has no separator: ${JSON.stringify(text)}`);
  const secondSep = text.indexOf("|", firstSep + 1);
  assert.ok(
    secondSep > firstSep,
    `frame has one separator: ${JSON.stringify(text)}`,
  );
  const streamId = Number(text.slice(0, firstSep));
  const type = Number(text.slice(firstSep + 1, secondSep));
  assert.ok(Number.isInteger(streamId), `bad stream ID in ${text}`);
  assert.ok(Number.isInteger(type), `bad flags in ${text}`);
  // Slice the *bytes*, not the decoded string: a proto payload is not
  // UTF-8, so decoding it would be lossy. The head is ASCII, so its byte
  // length equals its character length.
  return {
    streamId,
    type,
    payload: message.subarray(secondSep + 1),
  };
}

/**
 * Splits a message stream into per-stream frame streams by stream ID, so
 * tests can run several RPCs on one connection and read each response
 * independently.
 */
function demuxResponses(
  readable: ReadableStream<Uint8Array>,
): (streamId: number) => Promise<ParsedResponse> {
  interface PendingStream {
    frames: StreamFrame[];
    resolve?: () => void;
  }
  const byStream = new Map<number, PendingStream>();
  const pending = (streamId: number): PendingStream => {
    let entry = byStream.get(streamId);
    if (entry === undefined) {
      entry = { frames: [] };
      byStream.set(streamId, entry);
    }
    return entry;
  };
  const readAll = (async () => {
    const reader = readable.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) {
        return;
      }
      const { streamId, type, payload } = decodeFrame(value);
      const entry = pending(streamId);
      entry.frames.push({ type, payload });
      entry.resolve?.();
    }
  })();
  return async (streamId: number): Promise<ParsedResponse> => {
    const entry = pending(streamId);
    // Wait until this stream's end-stream frame has arrived.
    while (!entry.frames.some((f) => f.type === frameTypeEndStream)) {
      await Promise.race([
        new Promise<void>((resolve) => {
          entry.resolve = resolve;
        }),
        readAll,
      ]);
    }
    const [first, ...rest] = entry.frames;
    assert.strictEqual(first.type, frameTypeHeaders);
    const headers = decodeHeadersPayload(first.payload);
    const dataFrames: StreamFrame[] = [];
    let end: EndStreamResponse | undefined;
    for (const f of rest) {
      if (f.type === frameTypeEndStream) {
        end = endStreamFromJson(f.payload);
        break;
      }
      dataFrames.push(f);
    }
    assert.ok(end, "expected an end-stream frame");
    return { headers, dataFrames, end };
  };
}

// -- Tests ---------------------------------------------------------------------

describe("createBidiWebSocketDraft4Handler()", () => {
  it("unary over a real WebSocket connection", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "Ping");
    const running = await startServer(handlers);
    try {
      const ws = await connectClient(running);
      const socket = websocketToDraft4DuplexMessageStream(ws);
      const readStream = demuxResponses(socket.readable);

      const streamId = 1;
      const writer = socket.writable.getWriter();
      await writer.write(
        frame(
          streamId,
          frameTypeHeaders,
          encodeHeadersPayload(handler.requestPath, contentTypeUnaryProto),
          true,
        ),
      );
      await writer.write(
        frame(
          streamId,
          frameTypeData,
          toBinary(
            PingRequestSchema,
            create(PingRequestSchema, { number: BigInt(9), text: "hi" }),
          ),
          false,
        ),
      );
      await writeEndStream(writer, streamId);

      const response = await readStream(streamId);
      assert.strictEqual(response.dataFrames.length, 1);
      const body = fromBinary(
        PingResponseSchema,
        response.dataFrames[0].payload,
      );
      assert.strictEqual(body.number, BigInt(9));
      assert.strictEqual(body.text, "hi");
      assert.strictEqual(response.end.error, undefined);
    } finally {
      await running.close();
    }
  });

  it("unary error over a real WebSocket connection", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "Fail");
    const running = await startServer(handlers);
    try {
      const ws = await connectClient(running);
      const socket = websocketToDraft4DuplexMessageStream(ws);
      const readStream = demuxResponses(socket.readable);

      const streamId = 1;
      const writer = socket.writable.getWriter();
      await writer.write(
        frame(
          streamId,
          frameTypeHeaders,
          encodeHeadersPayload(handler.requestPath, contentTypeUnaryProto),
          true,
        ),
      );
      await writer.write(
        frame(
          streamId,
          frameTypeData,
          toBinary(
            FailRequestSchema,
            create(FailRequestSchema, { code: Code.NotFound }),
          ),
          false,
        ),
      );
      await writeEndStream(writer, streamId);

      const response = await readStream(streamId);
      assert.strictEqual(response.dataFrames.length, 0);
      assert.ok(response.end.error);
      assert.strictEqual(response.end.error?.code, Code.NotFound);
    } finally {
      await running.close();
    }
  });

  it("bidi echo with client half-close over a real WebSocket connection", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "CumSum");
    const running = await startServer(handlers);
    try {
      const ws = await connectClient(running);
      const socket = websocketToDraft4DuplexMessageStream(ws);
      const readStream = demuxResponses(socket.readable);

      const streamId = 1;
      const writer = socket.writable.getWriter();
      await writer.write(
        frame(
          streamId,
          frameTypeHeaders,
          encodeHeadersPayload(handler.requestPath, contentTypeStreamProto),
          true,
        ),
      );
      for (const n of [1, 2, 3]) {
        await writer.write(
          frame(
            streamId,
            frameTypeData,
            toBinary(
              CumSumRequestSchema,
              create(CumSumRequestSchema, { number: BigInt(n) }),
            ),
            false,
          ),
        );
      }
      // Explicit half-close marker, matching the WebSocket client's own
      // behavior (a real WebSocket can't half-close the underlying
      // connection the way a WebTransport stream can).
      await writeEndStream(writer, streamId);

      const response = await readStream(streamId);
      const sums = response.dataFrames.map(
        (f) => fromBinary(CumSumResponseSchema, f.payload).sum,
      );
      assert.deepStrictEqual(sums, [BigInt(1), BigInt(3), BigInt(6)]);
      assert.strictEqual(response.end.error, undefined);
    } finally {
      await running.close();
    }
  });

  it("multiplexes concurrent RPCs on one WebSocket connection", async () => {
    const handlers = createTestHandlers();
    const cumSum = findHandler(handlers, "CumSum");
    const ping = findHandler(handlers, "Ping");
    const running = await startServer(handlers);
    try {
      const ws = await connectClient(running);
      const socket = websocketToDraft4DuplexMessageStream(ws);
      const readStream = demuxResponses(socket.readable);
      const writer = socket.writable.getWriter();

      // Open two streams, interleaving their frames: a bidi stream (ID 1)
      // that stays open while a unary RPC (ID 2) starts and finishes.
      await writer.write(
        frame(
          1,
          frameTypeHeaders,
          encodeHeadersPayload(cumSum.requestPath, contentTypeStreamProto),
          true,
        ),
      );
      await writer.write(
        frame(
          1,
          frameTypeData,
          toBinary(
            CumSumRequestSchema,
            create(CumSumRequestSchema, { number: BigInt(4) }),
          ),
          false,
        ),
      );
      await writer.write(
        frame(
          2,
          frameTypeHeaders,
          encodeHeadersPayload(ping.requestPath, contentTypeUnaryProto),
          true,
        ),
      );
      await writer.write(
        frame(
          2,
          frameTypeData,
          toBinary(
            PingRequestSchema,
            create(PingRequestSchema, { number: BigInt(9), text: "hi" }),
          ),
          false,
        ),
      );
      await writeEndStream(writer, 2);

      // The unary RPC completes while stream 1 is still open.
      const pingResponse = await readStream(2);
      assert.strictEqual(pingResponse.dataFrames.length, 1);
      assert.strictEqual(
        fromBinary(PingResponseSchema, pingResponse.dataFrames[0].payload)
          .number,
        BigInt(9),
      );

      await writeEndStream(writer, 1);
      const cumSumResponse = await readStream(1);
      const sums = cumSumResponse.dataFrames.map(
        (f) => fromBinary(CumSumResponseSchema, f.payload).sum,
      );
      assert.deepStrictEqual(sums, [BigInt(4)]);
      assert.strictEqual(cumSumResponse.end.error, undefined);
    } finally {
      await running.close();
    }
  });

  // Draft 4's reason for existing: the frames a reader sees are text.
  it("sends control frames as text and proto data frames as binary", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "Ping");
    const running = await startServer(handlers);
    try {
      const ws = await connectClient(running);
      // Observe the raw messages, which the duplex adapter's readable
      // deliberately hides: it yields bytes whatever the opcode was.
      const seen: { text: boolean; frame: string }[] = [];
      ws.on("message", (data: RawData, isBinary: boolean) => {
        seen.push({
          text: !isBinary,
          frame: decoder.decode(data as Uint8Array).slice(0, 4),
        });
      });

      ws.send(
        `1|${frameTypeHeaders}|` +
          decoder.decode(
            encodeHeadersPayload(handler.requestPath, contentTypeUnaryProto),
          ),
      );
      ws.send(
        Buffer.concat([
          Buffer.from(`1|${frameTypeData}|`),
          Buffer.from(
            toBinary(
              PingRequestSchema,
              create(PingRequestSchema, { number: BigInt(1), text: "x" }),
            ),
          ),
        ]),
        { binary: true },
      );
      ws.send(`1|${frameTypeEndStream}|`);

      // headers, data, end-stream
      while (seen.length < 3) {
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
      assert.deepStrictEqual(seen.slice(0, 3), [
        { text: true, frame: `1|${frameTypeHeaders}|` },
        { text: false, frame: `1|${frameTypeData}|` },
        { text: true, frame: `1|${frameTypeEndStream}|` },
      ]);
    } finally {
      await running.close();
    }
  });

  // With the JSON codec nothing on the connection is binary, which is the
  // configuration draft 4 is designed around.
  it("sends every frame as text when the codec is JSON", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "Ping");
    const running = await startServer(handlers);
    try {
      const ws = await connectClient(running);
      const seen: { text: boolean; frame: string }[] = [];
      ws.on("message", (data: RawData, isBinary: boolean) => {
        seen.push({
          text: !isBinary,
          frame: decoder.decode(data as Uint8Array),
        });
      });

      ws.send(
        `1|${frameTypeHeaders}|` +
          decoder.decode(
            encodeHeadersPayload(handler.requestPath, contentTypeUnaryJson),
          ),
      );
      ws.send(`1|${frameTypeData}|{"text":"json"}`);
      ws.send(`1|${frameTypeEndStream}|`);

      while (seen.length < 3) {
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
      for (const message of seen.slice(0, 3)) {
        assert.ok(
          message.text,
          `frame ${JSON.stringify(message.frame)} was sent as binary`,
        );
      }
      const data = seen.find((m) => m.frame.startsWith(`1|${frameTypeData}|`));
      assert.ok(data, "no data frame");
      assert.strictEqual(data.frame, `1|${frameTypeData}|{"text":"json"}`);
    } finally {
      await running.close();
    }
  });

  // A payload is never escaped, so separators inside it must survive.
  it("round-trips a payload full of separators", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "Ping");
    const running = await startServer(handlers);
    try {
      const ws = await connectClient(running);
      const socket = websocketToDraft4DuplexMessageStream(ws);
      const readStream = demuxResponses(socket.readable);
      const writer = socket.writable.getWriter();

      const text = "a|b||c|||";
      await writer.write(
        frame(
          1,
          frameTypeHeaders,
          encodeHeadersPayload(handler.requestPath, contentTypeUnaryProto),
          true,
        ),
      );
      await writer.write(
        frame(
          1,
          frameTypeData,
          toBinary(PingRequestSchema, create(PingRequestSchema, { text })),
          false,
        ),
      );
      await writeEndStream(writer, 1);

      const response = await readStream(1);
      assert.strictEqual(
        fromBinary(PingResponseSchema, response.dataFrames[0].payload).text,
        text,
      );
    } finally {
      await running.close();
    }
  });
});
