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
import {
  createEnvelopeReadableStream,
  encodeEnvelope,
} from "@connectrpc/connect/protocol";
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
import { websocketToDuplexByteStream } from "./websocket-duplex.js";

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
  return {
    server,
    port,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      }),
  };
}

async function connectClient(port: number): Promise<WebSocket> {
  const ws = new WebSocket(`ws://127.0.0.1:${port}${defaultBidiWebSocketPath}`);
  await new Promise<void>((resolve, reject) => {
    ws.once("open", () => resolve());
    ws.once("error", reject);
  });
  return ws;
}

// -- Wire-level helpers (mirrors the wire protocol used by
// @sudorandom/connect-bidi-core and @sudorandom/connect-bidi-web; see
// those packages for the canonical definition) --------------------------

const flagEnvelopeHeaders = 0x04;
const flagEnvelopeData = 0x00;

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
): Promise<void> {
  await writer.write(encodeEnvelope(endStreamFlag, new Uint8Array()));
}

interface ParsedResponse {
  headers: Headers;
  dataEnvelopes: EnvelopedMessage[];
  end: EndStreamResponse;
}

async function readResponse(
  readable: ReadableStream<Uint8Array>,
): Promise<ParsedResponse> {
  const reader = createEnvelopeReadableStream(readable).getReader();
  const first = await reader.read();
  assert.ok(!first.done, "expected a headers envelope, got end of stream");
  assert.strictEqual(first.value.flags, flagEnvelopeHeaders);
  const headers = decodeHeadersEnvelope(first.value.data);

  const dataEnvelopes: EnvelopedMessage[] = [];
  let end: EndStreamResponse | undefined;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    if (value.flags === endStreamFlag) {
      end = endStreamFromJson(value.data);
      break;
    }
    dataEnvelopes.push(value);
  }
  assert.ok(end, "expected an end-stream envelope");
  return { headers, dataEnvelopes, end };
}

// -- Tests ---------------------------------------------------------------------

describe("createBidiWebSocketHandler()", () => {
  it("unary over a real WebSocket connection", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "Ping");
    const running = await startServer(handlers);
    try {
      const ws = await connectClient(running.port);
      const socket = websocketToDuplexByteStream(ws);
      const responsePromise = readResponse(socket.readable);

      const writer = socket.writable.getWriter();
      await writer.write(
        encodeHeadersEnvelope(handler.requestPath, contentTypeUnaryProto),
      );
      await writer.write(
        encodeEnvelope(
          flagEnvelopeData,
          toBinary(
            PingRequestSchema,
            create(PingRequestSchema, { number: BigInt(9), text: "hi" }),
          ),
        ),
      );
      await writeEndStream(writer);

      const response = await responsePromise;
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
      const ws = await connectClient(running.port);
      const socket = websocketToDuplexByteStream(ws);
      const responsePromise = readResponse(socket.readable);

      const writer = socket.writable.getWriter();
      await writer.write(
        encodeHeadersEnvelope(handler.requestPath, contentTypeUnaryProto),
      );
      await writer.write(
        encodeEnvelope(
          flagEnvelopeData,
          toBinary(
            FailRequestSchema,
            create(FailRequestSchema, { code: Code.NotFound }),
          ),
        ),
      );
      await writeEndStream(writer);

      const response = await responsePromise;
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
      const ws = await connectClient(running.port);
      const socket = websocketToDuplexByteStream(ws);
      const responsePromise = readResponse(socket.readable);

      const writer = socket.writable.getWriter();
      await writer.write(
        encodeHeadersEnvelope(handler.requestPath, contentTypeStreamProto),
      );
      for (const n of [1, 2, 3]) {
        await writer.write(
          encodeEnvelope(
            flagEnvelopeData,
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
      await writeEndStream(writer);

      const response = await responsePromise;
      const sums = response.dataEnvelopes.map(
        (env) => fromBinary(CumSumResponseSchema, env.data).sum,
      );
      assert.deepStrictEqual(sums, [BigInt(1), BigInt(3), BigInt(6)]);
      assert.strictEqual(response.end.error, undefined);
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
