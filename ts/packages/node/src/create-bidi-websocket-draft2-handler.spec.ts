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
import { encodeEnvelope } from "@connectrpc/connect/protocol";
import {
  contentTypeStreamProto,
  contentTypeUnaryProto,
  endStreamFromJson,
  type EndStreamResponse,
} from "@connectrpc/connect/protocol-connect";
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
  createBidiWebSocketHandler,
  defaultBidiWebSocketPath,
} from "./create-bidi-websocket-handler.js";
import {
  createBidiWebSocketDraft2Handler,
  defaultBidiWebSocketDraft2Path,
} from "./create-bidi-websocket-draft2-handler.js";
import { websocketToDuplexMessageStream } from "./websocket-duplex.js";

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
  // Both drafts share the server, each on its default path, matching the
  // intended side-by-side deployment.
  createBidiWebSocketHandler(handlers).upgrade(server);
  createBidiWebSocketDraft2Handler(handlers).upgrade(server);
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
  path: string = defaultBidiWebSocketDraft2Path,
): Promise<WebSocket> {
  const ws = new WebSocket(`ws://127.0.0.1:${running.port}${path}`);
  await new Promise<void>((resolve, reject) => {
    ws.once("open", () => resolve());
    ws.once("error", reject);
  });
  running.clients.push(ws);
  return ws;
}

// -- Wire-level helpers (mirrors draft 2 of the wire protocol used by
// @sudorandom/connect-bidi-core and @sudorandom/connect-bidi-web; see those
// packages for the canonical definition). Every WebSocket message is a
// 4-byte big-endian stream ID, a 1-byte frame type, and the payload — no
// payload length and no per-message compression. -------------------------

const frameTypeData = 0x00;
const frameTypeHeaders = 0x01;
const frameTypeEndStream = 0x02;
const streamIdLength = 4;
const frameHeadLength = streamIdLength + 1;

function encodeFrame(
  streamId: number,
  type: number,
  payload: Uint8Array,
): Uint8Array {
  const frame = new Uint8Array(frameHeadLength + payload.byteLength);
  new DataView(frame.buffer).setUint32(0, streamId);
  frame[streamIdLength] = type;
  frame.set(payload, frameHeadLength);
  return frame;
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
  return new TextEncoder().encode(JSON.stringify({ metadata }));
}

function decodeHeadersPayload(payload: Uint8Array): Headers {
  const parsed = JSON.parse(new TextDecoder().decode(payload)) as {
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
  writer: WritableStreamDefaultWriter<Uint8Array>,
  streamId: number,
): Promise<void> {
  await writer.write(
    encodeFrame(streamId, frameTypeEndStream, new Uint8Array()),
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
      assert.ok(
        value.byteLength >= frameHeadLength,
        `frame too short: ${value.byteLength} bytes`,
      );
      const view = new DataView(
        value.buffer,
        value.byteOffset,
        value.byteLength,
      );
      const streamId = view.getUint32(0);
      const type = view.getUint8(streamIdLength);
      const payload = value.subarray(frameHeadLength);
      const entry = pending(streamId);
      entry.frames.push({ type, payload });
      entry.resolve?.();
    }
  })();
  return async (streamId: number): Promise<ParsedResponse> => {
    const entry = pending(streamId);
    // Wait until this stream's end-stream frame has arrived.
    while (!entry.frames.some((frame) => frame.type === frameTypeEndStream)) {
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
    for (const frame of rest) {
      if (frame.type === frameTypeEndStream) {
        end = endStreamFromJson(frame.payload);
        break;
      }
      dataFrames.push(frame);
    }
    assert.ok(end, "expected an end-stream frame");
    return { headers, dataFrames, end };
  };
}

// -- Tests ---------------------------------------------------------------------

describe("createBidiWebSocketDraft2Handler()", () => {
  it("unary over a real WebSocket connection", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "Ping");
    const running = await startServer(handlers);
    try {
      const ws = await connectClient(running);
      const socket = websocketToDuplexMessageStream(ws);
      const readStream = demuxResponses(socket.readable);

      const streamId = 1;
      const writer = socket.writable.getWriter();
      await writer.write(
        encodeFrame(
          streamId,
          frameTypeHeaders,
          encodeHeadersPayload(handler.requestPath, contentTypeUnaryProto),
        ),
      );
      await writer.write(
        encodeFrame(
          streamId,
          frameTypeData,
          toBinary(
            PingRequestSchema,
            create(PingRequestSchema, { number: BigInt(9), text: "hi" }),
          ),
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
      const socket = websocketToDuplexMessageStream(ws);
      const readStream = demuxResponses(socket.readable);

      const streamId = 1;
      const writer = socket.writable.getWriter();
      await writer.write(
        encodeFrame(
          streamId,
          frameTypeHeaders,
          encodeHeadersPayload(handler.requestPath, contentTypeUnaryProto),
        ),
      );
      await writer.write(
        encodeFrame(
          streamId,
          frameTypeData,
          toBinary(
            FailRequestSchema,
            create(FailRequestSchema, { code: Code.NotFound }),
          ),
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
      const socket = websocketToDuplexMessageStream(ws);
      const readStream = demuxResponses(socket.readable);

      const streamId = 1;
      const writer = socket.writable.getWriter();
      await writer.write(
        encodeFrame(
          streamId,
          frameTypeHeaders,
          encodeHeadersPayload(handler.requestPath, contentTypeStreamProto),
        ),
      );
      for (const n of [1, 2, 3]) {
        await writer.write(
          encodeFrame(
            streamId,
            frameTypeData,
            toBinary(
              CumSumRequestSchema,
              create(CumSumRequestSchema, { number: BigInt(n) }),
            ),
          ),
        );
      }
      // Explicit half-close marker, matching the WebSocket client's own
      // behavior (a real WebSocket can't half-close the underlying
      // connection the way a WebTransport stream can).
      await writeEndStream(writer, streamId);

      const response = await readStream(streamId);
      const sums = response.dataFrames.map(
        (frame) => fromBinary(CumSumResponseSchema, frame.payload).sum,
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
      const socket = websocketToDuplexMessageStream(ws);
      const readStream = demuxResponses(socket.readable);
      const writer = socket.writable.getWriter();

      // Open two streams, interleaving their frames: a bidi stream (ID 1)
      // that stays open while a unary RPC (ID 2) starts and finishes.
      await writer.write(
        encodeFrame(
          1,
          frameTypeHeaders,
          encodeHeadersPayload(cumSum.requestPath, contentTypeStreamProto),
        ),
      );
      await writer.write(
        encodeFrame(
          1,
          frameTypeData,
          toBinary(
            CumSumRequestSchema,
            create(CumSumRequestSchema, { number: BigInt(4) }),
          ),
        ),
      );
      await writer.write(
        encodeFrame(
          2,
          frameTypeHeaders,
          encodeHeadersPayload(ping.requestPath, contentTypeUnaryProto),
        ),
      );
      await writer.write(
        encodeFrame(
          2,
          frameTypeData,
          toBinary(
            PingRequestSchema,
            create(PingRequestSchema, { number: BigInt(9), text: "hi" }),
          ),
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
        (frame) => fromBinary(CumSumResponseSchema, frame.payload).sum,
      );
      assert.deepStrictEqual(sums, [BigInt(4)]);
      assert.strictEqual(cumSumResponse.end.error, undefined);
    } finally {
      await running.close();
    }
  });

  it("serves draft 1 and draft 2 side by side on one server", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "Ping");
    const running = await startServer(handlers);
    try {
      // Draft 2 RPC on the draft 2 path.
      const draft2Socket = websocketToDuplexMessageStream(
        await connectClient(running, defaultBidiWebSocketDraft2Path),
      );
      const readDraft2 = demuxResponses(draft2Socket.readable);
      const draft2Writer = draft2Socket.writable.getWriter();
      await draft2Writer.write(
        encodeFrame(
          1,
          frameTypeHeaders,
          encodeHeadersPayload(handler.requestPath, contentTypeUnaryProto),
        ),
      );
      await draft2Writer.write(
        encodeFrame(
          1,
          frameTypeData,
          toBinary(
            PingRequestSchema,
            create(PingRequestSchema, { number: BigInt(2), text: "d2" }),
          ),
        ),
      );
      await writeEndStream(draft2Writer, 1);
      const draft2Response = await readDraft2(1);
      assert.strictEqual(
        fromBinary(PingResponseSchema, draft2Response.dataFrames[0].payload)
          .text,
        "d2",
      );

      // Draft 1 RPC on the draft 1 path: stream ID followed by a standard
      // 5-byte Connect envelope, headers flag 0x06.
      const draft1Socket = websocketToDuplexMessageStream(
        await connectClient(running, defaultBidiWebSocketPath),
      );
      const draft1Writer = draft1Socket.writable.getWriter();
      const prefixStreamId = (streamId: number, envelope: Uint8Array) => {
        const frame = new Uint8Array(streamIdLength + envelope.byteLength);
        new DataView(frame.buffer).setUint32(0, streamId);
        frame.set(envelope, streamIdLength);
        return frame;
      };
      await draft1Writer.write(
        prefixStreamId(
          1,
          encodeEnvelope(
            0x06,
            encodeHeadersPayload(handler.requestPath, contentTypeUnaryProto),
          ),
        ),
      );
      await draft1Writer.write(
        prefixStreamId(
          1,
          encodeEnvelope(
            0x00,
            toBinary(
              PingRequestSchema,
              create(PingRequestSchema, { number: BigInt(1), text: "d1" }),
            ),
          ),
        ),
      );
      await draft1Writer.write(
        prefixStreamId(1, encodeEnvelope(0x02, new Uint8Array())),
      );
      const draft1Frames: Uint8Array[] = [];
      const draft1Reader = draft1Socket.readable.getReader();
      // headers, data, end-stream
      while (draft1Frames.length < 3) {
        const { done, value } = await draft1Reader.read();
        assert.ok(!done, "draft 1 connection ended early");
        draft1Frames.push(value);
      }
      const dataEnvelope = draft1Frames[1];
      assert.strictEqual(dataEnvelope[streamIdLength], 0x00);
      const draft1Body = fromBinary(
        PingResponseSchema,
        dataEnvelope.subarray(streamIdLength + 5),
      );
      assert.strictEqual(draft1Body.text, "d1");
    } finally {
      await running.close();
    }
  });
});
