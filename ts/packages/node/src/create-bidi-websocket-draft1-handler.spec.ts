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
import * as http from "node:http";
import { describe, it } from "node:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import type { ServiceImpl } from "@connectrpc/connect";
import type { Code } from "@connectrpc/connect";
import { ConnectError, createConnectRouter } from "@connectrpc/connect";
import type { UniversalHandler } from "@connectrpc/connect/protocol";
import { endStreamFromJson } from "@connectrpc/connect/protocol-connect";
import {
  decodeDraft1Envelope,
  draft1Subprotocol,
  encodeDraft1Envelope,
} from "@sudorandom/connect-bidi-core";
import type { RawData } from "ws";
import { WebSocket } from "ws";
import { createBidiWebSocketDraft1Handler } from "./create-bidi-websocket-draft1-handler.js";
import {
  CumSumRequestSchema,
  CumSumResponseSchema,
  PingService,
} from "./gen/connectbidi/ping/v1/ping_pb.js";

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

// -- Wire helpers -------------------------------------------------------------

const flagData = 0x00;
const flagEndStream = 0x02;
const flagHeaders = 0x06;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

const cumSumPath = "/connectbidi.ping.v1.PingService/CumSum";
const pingPath = "/connectbidi.ping.v1.PingService/Ping";

interface RunningServer {
  port: number;
  clients: WebSocket[];
  close(): Promise<void>;
}

async function startServer(): Promise<RunningServer> {
  const server = http.createServer();
  createBidiWebSocketDraft1Handler(createTestHandlers()).upgrade(server);
  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  const clients: WebSocket[] = [];
  return {
    port,
    clients,
    close: () => {
      // An upgraded socket keeps the connection alive, so the clients must
      // be gone for server.close() to complete.
      for (const ws of clients) {
        ws.terminate();
      }
      return new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      });
    },
  };
}

/** Open a draft 1 socket to one procedure and collect its messages. */
async function connect(
  server: RunningServer,
  path: string,
): Promise<{
  socket: WebSocket;
  next: () => Promise<{ flag: number; payload: Uint8Array }>;
}> {
  const socket = new WebSocket(`ws://127.0.0.1:${server.port}${path}`, [
    draft1Subprotocol,
  ]);
  server.clients.push(socket);
  const queue: Uint8Array[] = [];
  let waiting: ((message: Uint8Array | undefined) => void) | undefined;
  const push = (message: Uint8Array | undefined) => {
    if (waiting !== undefined) {
      const resolve = waiting;
      waiting = undefined;
      resolve(message);
      return;
    }
    if (message !== undefined) {
      queue.push(message);
    }
  };
  socket.on("message", (data: RawData, isBinary: boolean) => {
    assert.ok(isBinary, "draft 1 messages must be binary");
    push(new Uint8Array(data as Buffer));
  });
  socket.on("close", () => push(undefined));
  await new Promise<void>((resolve, reject) => {
    socket.on("open", resolve);
    socket.on("error", reject);
  });
  assert.strictEqual(socket.protocol, draft1Subprotocol);
  return {
    socket,
    next: async () => {
      const message =
        queue.shift() ??
        (await new Promise<Uint8Array | undefined>((resolve) => {
          waiting = resolve;
        }));
      assert.ok(message !== undefined, "socket closed before the next message");
      return decodeDraft1Envelope(message);
    },
  };
}

function send(socket: WebSocket, flag: number, payload: Uint8Array): void {
  socket.send(encodeDraft1Envelope(flag, payload));
}

function headersPayload(contentType: string): Uint8Array {
  return encoder.encode(
    JSON.stringify({ metadata: { "content-type": [contentType] } }),
  );
}

// -- Tests --------------------------------------------------------------------

describe("createBidiWebSocketDraft1Handler()", () => {
  it("round-trips a bidi stream, one envelope per message", async () => {
    const server = await startServer();
    try {
      const { socket, next } = await connect(server, cumSumPath);
      send(socket, flagHeaders, headersPayload("application/connect+proto"));
      for (const number of [BigInt(1), BigInt(2), BigInt(3)]) {
        send(
          socket,
          flagData,
          toBinary(
            CumSumRequestSchema,
            create(CumSumRequestSchema, { number }),
          ),
        );
      }
      send(socket, flagEndStream, new Uint8Array(0));

      const first = await next();
      assert.strictEqual(first.flag, flagHeaders);

      const sums: bigint[] = [];
      for (;;) {
        const envelope = await next();
        if (envelope.flag === flagEndStream) {
          const { error } = endStreamFromJson(envelope.payload);
          assert.strictEqual(error, undefined);
          break;
        }
        assert.strictEqual(envelope.flag, flagData);
        sums.push(fromBinary(CumSumResponseSchema, envelope.payload).sum);
      }
      assert.deepStrictEqual(sums, [BigInt(1), BigInt(3), BigInt(6)]);
      socket.close();
    } finally {
      await server.close();
    }
  });

  it("reports an RPC error in the end-stream envelope", async () => {
    const server = await startServer();
    try {
      const { socket, next } = await connect(server, cumSumPath);
      send(socket, flagHeaders, headersPayload("application/connect+proto"));
      // A data envelope that is not a valid CumSumRequest.
      send(socket, flagData, encoder.encode("not a protobuf message"));
      send(socket, flagEndStream, new Uint8Array(0));

      assert.strictEqual((await next()).flag, flagHeaders);
      for (;;) {
        const envelope = await next();
        if (envelope.flag !== flagEndStream) {
          continue;
        }
        const { error } = endStreamFromJson(envelope.payload);
        assert.ok(error, "expected an error in the end-stream envelope");
        break;
      }
      socket.close();
    } finally {
      await server.close();
    }
  });

  it("serves the same procedure under a path prefix", async () => {
    // The procedure is the path's last two segments, so a prefix in front
    // of them changes nothing about which handler runs.
    const server = await startServer();
    try {
      const { socket, next } = await connect(
        server,
        `/websocket-draft1${cumSumPath}`,
      );
      send(socket, flagHeaders, headersPayload("application/connect+json"));
      send(socket, flagData, encoder.encode('{"number":"5"}'));
      send(socket, flagEndStream, new Uint8Array(0));

      assert.strictEqual((await next()).flag, flagHeaders);
      const data = await next();
      assert.strictEqual(data.flag, flagData);
      assert.match(decoder.decode(data.payload), /"5"/);
      socket.close();
    } finally {
      await server.close();
    }
  });

  it("refuses to upgrade a unary procedure", async () => {
    const server = await startServer();
    try {
      const socket = new WebSocket(`ws://127.0.0.1:${server.port}${pingPath}`, [
        draft1Subprotocol,
      ]);
      await new Promise<void>((resolve, reject) => {
        socket.on("open", () =>
          reject(new Error("unary procedure accepted an upgrade")),
        );
        socket.on("error", () => resolve());
      });
    } finally {
      await server.close();
    }
  });

  it("refuses a handshake without the subprotocol", async () => {
    const server = await startServer();
    try {
      const socket = new WebSocket(
        `ws://127.0.0.1:${server.port}${cumSumPath}`,
      );
      await new Promise<void>((resolve, reject) => {
        socket.on("open", () =>
          reject(new Error("handshake without the subprotocol was accepted")),
        );
        socket.on("error", () => resolve());
      });
    } finally {
      await server.close();
    }
  });
});
