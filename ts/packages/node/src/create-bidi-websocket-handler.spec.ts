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
  const server = http.createServer((_req, res) => {
    // Coexistence check: ordinary HTTP requests on this same server must
    // keep working; the 'upgrade' listener added below is a separate event
    // entirely and never intercepts these.
    res.writeHead(200, { "content-type": "text/plain" });
    res.end("ok");
  });
  createBidiWebSocketHandler(handlers).upgrade(server);
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

async function connectClient(running: RunningServer): Promise<WebSocket> {
  const ws = new WebSocket(
    `ws://127.0.0.1:${running.port}${defaultBidiWebSocketPath}`,
  );
  await new Promise<void>((resolve, reject) => {
    ws.once("open", () => resolve());
    ws.once("error", reject);
  });
  running.clients.push(ws);
  return ws;
}

// -- Wire-level helpers (mirrors the wire protocol used by
// @sudorandom/connect-bidi-core and @sudorandom/connect-bidi-web; see
// those packages for the canonical definition). Every WebSocket message is
// a 4-byte big-endian stream ID followed by one Connect envelope. ---------

const flagEnvelopeHeaders = 0x06;
const flagEnvelopeData = 0x00;
const streamIdLength = 4;

function prefixStreamId(streamId: number, envelope: Uint8Array): Uint8Array {
  const frame = new Uint8Array(streamIdLength + envelope.byteLength);
  new DataView(frame.buffer).setUint32(0, streamId);
  frame.set(envelope, streamIdLength);
  return frame;
}

function encodeHeadersEnvelope(
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
  const payload = new TextEncoder().encode(JSON.stringify({ metadata }));
  return encodeEnvelope(flagEnvelopeHeaders, payload);
}

function decodeHeadersEnvelope(payload: Uint8Array): Headers {
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
    prefixStreamId(streamId, encodeEnvelope(endStreamFlag, new Uint8Array())),
  );
}

interface ParsedResponse {
  headers: Headers;
  dataEnvelopes: EnvelopedMessage[];
  end: EndStreamResponse;
}

/**
 * Splits a message stream into per-stream envelope streams by stream ID,
 * so tests can run several RPCs on one connection and read each response
 * independently.
 */
function demuxResponses(
  readable: ReadableStream<Uint8Array>,
): (streamId: number) => Promise<ParsedResponse> {
  interface PendingStream {
    envelopes: EnvelopedMessage[];
    resolve?: () => void;
  }
  const byStream = new Map<number, PendingStream>();
  const pending = (streamId: number): PendingStream => {
    let entry = byStream.get(streamId);
    if (entry === undefined) {
      entry = { envelopes: [] };
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
        value.byteLength >= streamIdLength + 5,
        `frame too short: ${value.byteLength} bytes`,
      );
      const view = new DataView(
        value.buffer,
        value.byteOffset,
        value.byteLength,
      );
      const streamId = view.getUint32(0);
      const flags = view.getUint8(streamIdLength);
      const declared = view.getUint32(streamIdLength + 1);
      const data = value.subarray(streamIdLength + 5);
      assert.strictEqual(
        declared,
        data.byteLength,
        "frame must carry exactly one complete envelope",
      );
      const entry = pending(streamId);
      entry.envelopes.push({ flags, data });
      entry.resolve?.();
    }
  })();
  return async (streamId: number): Promise<ParsedResponse> => {
    const entry = pending(streamId);
    // Wait until this stream's end-stream envelope has arrived.
    while (!entry.envelopes.some((env) => env.flags === endStreamFlag)) {
      await Promise.race([
        new Promise<void>((resolve) => {
          entry.resolve = resolve;
        }),
        readAll,
      ]);
    }
    const [first, ...rest] = entry.envelopes;
    assert.strictEqual(first.flags, flagEnvelopeHeaders);
    const headers = decodeHeadersEnvelope(first.data);
    const dataEnvelopes: EnvelopedMessage[] = [];
    let end: EndStreamResponse | undefined;
    for (const env of rest) {
      if (env.flags === endStreamFlag) {
        end = endStreamFromJson(env.data);
        break;
      }
      dataEnvelopes.push(env);
    }
    assert.ok(end, "expected an end-stream envelope");
    return { headers, dataEnvelopes, end };
  };
}

// -- Tests ---------------------------------------------------------------------

describe("createBidiWebSocketHandler()", () => {
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
        prefixStreamId(
          streamId,
          encodeHeadersEnvelope(handler.requestPath, contentTypeUnaryProto),
        ),
      );
      await writer.write(
        prefixStreamId(
          streamId,
          encodeEnvelope(
            flagEnvelopeData,
            toBinary(
              PingRequestSchema,
              create(PingRequestSchema, { number: BigInt(9), text: "hi" }),
            ),
          ),
        ),
      );
      await writeEndStream(writer, streamId);

      const response = await readStream(streamId);
      assert.strictEqual(response.dataEnvelopes.length, 1);
      const body = fromBinary(
        PingResponseSchema,
        response.dataEnvelopes[0].data,
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
        prefixStreamId(
          streamId,
          encodeHeadersEnvelope(handler.requestPath, contentTypeUnaryProto),
        ),
      );
      await writer.write(
        prefixStreamId(
          streamId,
          encodeEnvelope(
            flagEnvelopeData,
            toBinary(
              FailRequestSchema,
              create(FailRequestSchema, { code: Code.NotFound }),
            ),
          ),
        ),
      );
      await writeEndStream(writer, streamId);

      const response = await readStream(streamId);
      assert.strictEqual(response.dataEnvelopes.length, 0);
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
        prefixStreamId(
          streamId,
          encodeHeadersEnvelope(handler.requestPath, contentTypeStreamProto),
        ),
      );
      for (const n of [1, 2, 3]) {
        await writer.write(
          prefixStreamId(
            streamId,
            encodeEnvelope(
              flagEnvelopeData,
              toBinary(
                CumSumRequestSchema,
                create(CumSumRequestSchema, { number: BigInt(n) }),
              ),
            ),
          ),
        );
      }
      // Explicit half-close marker, matching the WebSocket client's own
      // behavior (a real WebSocket can't half-close the underlying
      // connection the way a WebTransport stream can).
      await writeEndStream(writer, streamId);

      const response = await readStream(streamId);
      const sums = response.dataEnvelopes.map(
        (env) => fromBinary(CumSumResponseSchema, env.data).sum,
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
        prefixStreamId(
          1,
          encodeHeadersEnvelope(cumSum.requestPath, contentTypeStreamProto),
        ),
      );
      await writer.write(
        prefixStreamId(
          1,
          encodeEnvelope(
            flagEnvelopeData,
            toBinary(
              CumSumRequestSchema,
              create(CumSumRequestSchema, { number: BigInt(4) }),
            ),
          ),
        ),
      );
      await writer.write(
        prefixStreamId(
          2,
          encodeHeadersEnvelope(ping.requestPath, contentTypeUnaryProto),
        ),
      );
      await writer.write(
        prefixStreamId(
          2,
          encodeEnvelope(
            flagEnvelopeData,
            toBinary(
              PingRequestSchema,
              create(PingRequestSchema, { number: BigInt(9), text: "hi" }),
            ),
          ),
        ),
      );
      await writeEndStream(writer, 2);

      // The unary RPC completes while stream 1 is still open.
      const pingResponse = await readStream(2);
      assert.strictEqual(pingResponse.dataEnvelopes.length, 1);
      assert.strictEqual(
        fromBinary(PingResponseSchema, pingResponse.dataEnvelopes[0].data)
          .number,
        BigInt(9),
      );

      await writeEndStream(writer, 1);
      const cumSumResponse = await readStream(1);
      const sums = cumSumResponse.dataEnvelopes.map(
        (env) => fromBinary(CumSumResponseSchema, env.data).sum,
      );
      assert.deepStrictEqual(sums, [BigInt(4)]);
      assert.strictEqual(cumSumResponse.end.error, undefined);
    } finally {
      await running.close();
    }
  });

  it("coexists with a normal HTTP request handler on the same server", async () => {
    const handlers = createTestHandlers();
    const running = await startServer(handlers);
    try {
      const response = await fetch(`http://127.0.0.1:${running.port}/`);
      assert.strictEqual(response.status, 200);
      assert.strictEqual(await response.text(), "ok");
    } finally {
      await running.close();
    }
  });

  it("leaves upgrade requests for other paths alone", async () => {
    const handlers = createTestHandlers();
    const running = await startServer(handlers);
    try {
      // A second listener on the same server, for a different path, proves
      // the handler's own listener does not swallow upgrades meant for
      // someone else.
      let otherPathHandled = false;
      running.server.on("upgrade", (request, socket) => {
        if (
          new URL(request.url ?? "/", "http://bidi.invalid").pathname ===
          "/other"
        ) {
          otherPathHandled = true;
          socket.destroy();
        }
      });
      const ws = new WebSocket(`ws://127.0.0.1:${running.port}/other`);
      await new Promise<void>((resolve) => {
        ws.once("close", () => resolve());
        ws.once("error", () => resolve());
      });
      assert.strictEqual(otherPathHandled, true);
    } finally {
      await running.close();
    }
  });
});
