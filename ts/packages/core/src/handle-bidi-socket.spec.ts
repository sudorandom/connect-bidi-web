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

import * as assert from "node:assert";
import { describe, it } from "node:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import type { ServiceImpl } from "@connectrpc/connect";
import { Code, ConnectError, createConnectRouter } from "@connectrpc/connect";
import type {
  Compression,
  EnvelopedMessage,
  UniversalHandler,
  UniversalHandlerOptions,
} from "@connectrpc/connect/protocol";
import {
  compressedFlag,
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
import {
  CountUpRequestSchema,
  CountUpResponseSchema,
  CumSumRequestSchema,
  CumSumResponseSchema,
  FailRequestSchema,
  PingRequestSchema,
  PingResponseSchema,
  PingService,
  SumRequestSchema,
  SumResponseSchema,
} from "./gen/connectbidi/ping/v1/ping_pb.js";
import { decodeHeadersFrame, encodeHeadersFrame } from "./headers-frame.js";
import type { DuplexByteStream } from "./handle-bidi-socket.js";
import { handleBidiSocket } from "./handle-bidi-socket.js";
import { flagEnvelopeData, flagEnvelopeHeaders } from "./wire.js";

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

const gzipCompression: Compression = {
  name: "gzip",
  async compress(bytes) {
    const stream = new Blob([new Uint8Array(bytes)])
      .stream()
      .pipeThrough(new CompressionStream("gzip"));
    return new Uint8Array(await new Response(stream).arrayBuffer());
  },
  async decompress(bytes, readMaxBytes) {
    const stream = new Blob([new Uint8Array(bytes)])
      .stream()
      .pipeThrough(new DecompressionStream("gzip"));
    const out = new Uint8Array(await new Response(stream).arrayBuffer());
    if (out.byteLength > readMaxBytes) {
      throw new ConnectError(
        "decompressed message too large",
        Code.ResourceExhausted,
      );
    }
    return out;
  },
};

function createTestHandlers(
  opt?: Partial<UniversalHandlerOptions>,
): UniversalHandler[] {
  const router = createConnectRouter(opt);
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

// -- Wire-level test helpers --------------------------------------------------

function createSocketPair(): [DuplexByteStream, DuplexByteStream] {
  const clientToServer = new TransformStream<Uint8Array, Uint8Array>();
  const serverToClient = new TransformStream<Uint8Array, Uint8Array>();
  const client: DuplexByteStream = {
    readable: serverToClient.readable,
    writable: clientToServer.writable,
  };
  const server: DuplexByteStream = {
    readable: clientToServer.readable,
    writable: serverToClient.writable,
  };
  return [client, server];
}

function encodeHeadersEnvelope(
  path: string,
  contentType: string,
  extraHeaders: Record<string, string> = {},
): Uint8Array {
  const headers = new Headers({ "content-type": contentType, ...extraHeaders });
  return encodeEnvelope(
    flagEnvelopeHeaders,
    encodeHeadersFrame(headers, { ":path": path }),
  );
}

async function writeRequestHeaders(
  writer: WritableStreamDefaultWriter<Uint8Array>,
  path: string,
  contentType: string,
  extraHeaders: Record<string, string> = {},
): Promise<void> {
  await writer.write(encodeHeadersEnvelope(path, contentType, extraHeaders));
}

async function writeEndStream(
  writer: WritableStreamDefaultWriter<Uint8Array>,
): Promise<void> {
  await writer.write(encodeEnvelope(endStreamFlag, new Uint8Array()));
}

function concatBytes(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((n, c) => n + c.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
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
  const { headers } = decodeHeadersFrame(first.value.data);

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

describe("handleBidiSocket()", () => {
  it("unary success", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "Ping");
    const [client, server] = createSocketPair();
    const serverDone = handleBidiSocket(server, handlers);
    // Start draining the response concurrently with writing the request:
    // with only a small transform-stream buffer between the two ends, a
    // truly bidirectional RPC can deadlock if nothing reads the response
    // until every request message has been written (see the bidi-streaming
    // tests below, where the server must interleave reads and writes).
    const responsePromise = readResponse(client.readable);

    const writer = client.writable.getWriter();
    await writeRequestHeaders(
      writer,
      handler.requestPath,
      contentTypeUnaryProto,
    );
    await writer.write(
      encodeEnvelope(
        flagEnvelopeData,
        toBinary(
          PingRequestSchema,
          create(PingRequestSchema, { number: BigInt(42), text: "hi" }),
        ),
      ),
    );
    await writeEndStream(writer);

    const response = await responsePromise;
    assert.strictEqual(response.dataEnvelopes.length, 1);
    assert.strictEqual(response.dataEnvelopes[0].flags, flagEnvelopeData);
    const body = fromBinary(PingResponseSchema, response.dataEnvelopes[0].data);
    assert.strictEqual(body.number, BigInt(42));
    assert.strictEqual(body.text, "hi");
    assert.strictEqual(response.end.error, undefined);

    await serverDone;
  });

  it("unary request with streaming-shaped headers succeeds (Go client interop)", async () => {
    // The Go bidiprotocol client (internal/bidiprotocol/client_stream.go
    // sendHeaders()) sends Content-Type: application/connect+<codec> and the
    // Connect-Content-Encoding/Connect-Accept-Encoding compression headers
    // unconditionally, even for unary calls -- unlike the TS client, which
    // sends the bare application/proto|json and the unary-named compression
    // headers. The bridge must normalize these before dispatch, or
    // connect-es's unary handler rejects the request with 415.
    const handlers = createTestHandlers({
      acceptCompression: [gzipCompression],
    });
    const handler = findHandler(handlers, "Ping");
    const [client, server] = createSocketPair();
    const serverDone = handleBidiSocket(server, handlers);
    const responsePromise = readResponse(client.readable);

    const writer = client.writable.getWriter();
    await writeRequestHeaders(
      writer,
      handler.requestPath,
      contentTypeStreamProto,
      {
        "connect-accept-encoding": "gzip",
      },
    );
    await writer.write(
      encodeEnvelope(
        flagEnvelopeData,
        toBinary(
          PingRequestSchema,
          create(PingRequestSchema, { number: BigInt(7), text: "go" }),
        ),
      ),
    );
    await writeEndStream(writer);

    const response = await responsePromise;
    assert.strictEqual(response.dataEnvelopes.length, 1);
    const body = fromBinary(PingResponseSchema, response.dataEnvelopes[0].data);
    assert.strictEqual(body.number, BigInt(7));
    assert.strictEqual(body.text, "go");
    assert.strictEqual(response.end.error, undefined);
    // The response should carry a bare unary content-type, not a
    // streaming-shaped one, regardless of what the request used.
    assert.strictEqual(
      response.headers.get("content-type"),
      contentTypeUnaryProto,
    );

    await serverDone;
  });

  it("unary error", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "Fail");
    const [client, server] = createSocketPair();
    const serverDone = handleBidiSocket(server, handlers);
    // Start draining the response concurrently with writing the request:
    // with only a small transform-stream buffer between the two ends, a
    // truly bidirectional RPC can deadlock if nothing reads the response
    // until every request message has been written (see the bidi-streaming
    // tests below, where the server must interleave reads and writes).
    const responsePromise = readResponse(client.readable);

    const writer = client.writable.getWriter();
    await writeRequestHeaders(
      writer,
      handler.requestPath,
      contentTypeUnaryProto,
    );
    await writer.write(
      encodeEnvelope(
        flagEnvelopeData,
        toBinary(
          FailRequestSchema,
          create(FailRequestSchema, { code: Code.InvalidArgument }),
        ),
      ),
    );
    await writeEndStream(writer);

    const response = await responsePromise;
    // No data envelope on error -- only headers + end-stream.
    assert.strictEqual(response.dataEnvelopes.length, 0);
    assert.ok(
      response.end.error,
      "expected an error in the end-stream envelope",
    );
    assert.strictEqual(response.end.error?.code, Code.InvalidArgument);

    await serverDone;
  });

  it("server-streaming", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "CountUp");
    const [client, server] = createSocketPair();
    const serverDone = handleBidiSocket(server, handlers);
    // Start draining the response concurrently with writing the request:
    // with only a small transform-stream buffer between the two ends, a
    // truly bidirectional RPC can deadlock if nothing reads the response
    // until every request message has been written (see the bidi-streaming
    // tests below, where the server must interleave reads and writes).
    const responsePromise = readResponse(client.readable);

    const writer = client.writable.getWriter();
    await writeRequestHeaders(
      writer,
      handler.requestPath,
      contentTypeStreamProto,
    );
    await writer.write(
      encodeEnvelope(
        flagEnvelopeData,
        toBinary(
          CountUpRequestSchema,
          create(CountUpRequestSchema, { number: BigInt(3) }),
        ),
      ),
    );
    await writeEndStream(writer);

    const response = await responsePromise;
    assert.strictEqual(response.dataEnvelopes.length, 3);
    const numbers = response.dataEnvelopes.map(
      (env) => fromBinary(CountUpResponseSchema, env.data).number,
    );
    assert.deepStrictEqual(numbers, [BigInt(1), BigInt(2), BigInt(3)]);
    assert.strictEqual(response.end.error, undefined);

    await serverDone;
  });

  it("bidi-streaming (Gotcha B regression: httpVersion must be advertised as 2)", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "CumSum");
    const [client, server] = createSocketPair();
    const serverDone = handleBidiSocket(server, handlers);
    // Start draining the response concurrently with writing the request:
    // with only a small transform-stream buffer between the two ends, a
    // truly bidirectional RPC can deadlock if nothing reads the response
    // until every request message has been written (see the bidi-streaming
    // tests below, where the server must interleave reads and writes).
    const responsePromise = readResponse(client.readable);

    const writer = client.writable.getWriter();
    await writeRequestHeaders(
      writer,
      handler.requestPath,
      contentTypeStreamProto,
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
    // WebTransport-style half-close: end the write side without an explicit
    // end-stream marker.
    await writer.close();

    const response = await responsePromise;
    const sums = response.dataEnvelopes.map(
      (env) => fromBinary(CumSumResponseSchema, env.data).sum,
    );
    assert.deepStrictEqual(sums, [BigInt(1), BigInt(3), BigInt(6)]);
    assert.strictEqual(response.end.error, undefined);

    await serverDone;
  });

  it("client half-close mid-response-stream: responses keep arriving afterwards", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "CumSum");
    const [client, server] = createSocketPair();
    const serverDone = handleBidiSocket(server, handlers);
    // Start draining the response concurrently with writing the request:
    // with only a small transform-stream buffer between the two ends, a
    // truly bidirectional RPC can deadlock if nothing reads the response
    // until every request message has been written (see the bidi-streaming
    // tests below, where the server must interleave reads and writes).
    const responsePromise = readResponse(client.readable);

    const writer = client.writable.getWriter();
    await writeRequestHeaders(
      writer,
      handler.requestPath,
      contentTypeStreamProto,
    );
    // Send every request message and the half-close marker immediately,
    // without reading any response in between -- the server must still be
    // able to finish streaming its responses afterwards.
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
    await writeEndStream(writer);

    const response = await responsePromise;
    const sums = response.dataEnvelopes.map(
      (env) => fromBinary(CumSumResponseSchema, env.data).sum,
    );
    assert.deepStrictEqual(sums, [BigInt(1), BigInt(3), BigInt(6)]);
    assert.strictEqual(response.end.error, undefined);

    await serverDone;
  });

  it("frame re-chunking: identical result whether bytes arrive whole or one at a time", async () => {
    const handlers = createTestHandlers();
    const handler = findHandler(handlers, "CountUp");

    // Build the raw request bytes up front: headers envelope, one data
    // envelope, and an end-stream half-close marker.
    const allBytes = concatBytes([
      encodeHeadersEnvelope(handler.requestPath, contentTypeStreamProto),
      encodeEnvelope(
        flagEnvelopeData,
        toBinary(
          CountUpRequestSchema,
          create(CountUpRequestSchema, { number: BigInt(3) }),
        ),
      ),
      encodeEnvelope(endStreamFlag, new Uint8Array()),
    ]);

    async function runWithChunking(chunkSize: number | undefined) {
      const readable = new ReadableStream<Uint8Array>({
        start(controller) {
          if (chunkSize === undefined) {
            controller.enqueue(allBytes);
          } else {
            for (let i = 0; i < allBytes.byteLength; i += chunkSize) {
              controller.enqueue(allBytes.subarray(i, i + chunkSize));
            }
          }
          controller.close();
        },
      });
      const responseTransform = new TransformStream<Uint8Array, Uint8Array>();
      const server: DuplexByteStream = {
        readable,
        writable: responseTransform.writable,
      };
      const serverDone = handleBidiSocket(server, handlers);
      const response = await readResponse(responseTransform.readable);
      await serverDone;
      return response.dataEnvelopes.map(
        (env) => fromBinary(CountUpResponseSchema, env.data).number,
      );
    }

    const wholeChunk = await runWithChunking(undefined);
    const oneByteAtATime = await runWithChunking(1);
    assert.deepStrictEqual(wholeChunk, [BigInt(1), BigInt(2), BigInt(3)]);
    assert.deepStrictEqual(oneByteAtATime, [BigInt(1), BigInt(2), BigInt(3)]);
  });

  it("compressed request envelope on a streaming RPC", async () => {
    const handlers = createTestHandlers({
      acceptCompression: [gzipCompression],
    });
    const handler = findHandler(handlers, "Sum");
    const [client, server] = createSocketPair();
    const serverDone = handleBidiSocket(server, handlers);
    // Start draining the response concurrently with writing the request:
    // with only a small transform-stream buffer between the two ends, a
    // truly bidirectional RPC can deadlock if nothing reads the response
    // until every request message has been written (see the bidi-streaming
    // tests below, where the server must interleave reads and writes).
    const responsePromise = readResponse(client.readable);

    const writer = client.writable.getWriter();
    await writeRequestHeaders(
      writer,
      handler.requestPath,
      contentTypeStreamProto,
      {
        "connect-content-encoding": "gzip",
      },
    );
    // One plain envelope, one gzip-compressed envelope.
    await writer.write(
      encodeEnvelope(
        flagEnvelopeData,
        toBinary(
          SumRequestSchema,
          create(SumRequestSchema, { number: BigInt(10) }),
        ),
      ),
    );
    const compressed = await gzipCompression.compress(
      toBinary(
        SumRequestSchema,
        create(SumRequestSchema, { number: BigInt(32) }),
      ),
    );
    await writer.write(encodeEnvelope(compressedFlag, compressed));
    await writeEndStream(writer);

    const response = await responsePromise;
    assert.strictEqual(response.dataEnvelopes.length, 1);
    const body = fromBinary(SumResponseSchema, response.dataEnvelopes[0].data);
    assert.strictEqual(body.sum, BigInt(42));
    assert.strictEqual(response.end.error, undefined);

    await serverDone;
  });

  it("unknown :path yields an unimplemented error", async () => {
    const handlers = createTestHandlers();
    const [client, server] = createSocketPair();
    const serverDone = handleBidiSocket(server, handlers);
    // Start draining the response concurrently with writing the request:
    // with only a small transform-stream buffer between the two ends, a
    // truly bidirectional RPC can deadlock if nothing reads the response
    // until every request message has been written (see the bidi-streaming
    // tests below, where the server must interleave reads and writes).
    const responsePromise = readResponse(client.readable);

    const writer = client.writable.getWriter();
    await writeRequestHeaders(
      writer,
      "/connectbidi.ping.v1.PingService/DoesNotExist",
      contentTypeUnaryProto,
    );
    await writeEndStream(writer);

    const response = await responsePromise;
    assert.strictEqual(response.dataEnvelopes.length, 0);
    assert.ok(
      response.end.error,
      "expected an error in the end-stream envelope",
    );
    assert.strictEqual(response.end.error?.code, Code.Unimplemented);

    await serverDone;
  });
});
