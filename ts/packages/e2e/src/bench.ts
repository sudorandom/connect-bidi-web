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

//
// Benchmarks the TypeScript WebSocket transports — draft 1 vs draft 2, with
// and without compression — on speed and wire-size efficiency. The client is
// @sudorandom/connect-bidi-web (over Node's global WebSocket), the server is
// @sudorandom/connect-bidi-node. Wire bytes are measured on the server's raw
// TCP sockets (net.Socket bytesRead/bytesWritten), so they include WebSocket
// framing and permessage-deflate effects.
//
// Node has no WebTransport client, so WebTransport is benchmarked on the Go
// side only (internal/bench).
//
// Run with: npm run bench -w packages/e2e
//

import { writeFile } from "node:fs/promises";
import * as http from "node:http";
import type { Socket } from "node:net";
import type { ServiceImpl } from "@connectrpc/connect";
import { createClient, createConnectRouter } from "@connectrpc/connect";
import type { Transport } from "@connectrpc/connect";
import {
  createBidiWebSocketDraft2Handler,
  createBidiWebSocketDraft3Handler,
  createBidiWebSocketDraft4Handler,
  createBidiWebSocketDraft1Handler,
} from "@sudorandom/connect-bidi-node";
import {
  createConnectWebSocketDraft2Transport,
  createConnectWebSocketDraft3Transport,
  createConnectWebSocketDraft4Transport,
  createConnectWebSocketDraft1Transport,
} from "@sudorandom/connect-bidi-web";
import { ElizaService } from "./gen/connectbidi/eliza/v1/eliza_pb.js";

// -- Payloads -----------------------------------------------------------------

const LARGE_SIZE = 16 * 1024;

const sentence = "all work and no play makes jack a dull boy. ";
const repetitive = sentence
  .repeat(Math.ceil(LARGE_SIZE / sentence.length))
  .slice(0, LARGE_SIZE);

// Deterministic pseudo-random base64-ish text: barely compressible.
function makeRandomText(size: number): string {
  const alphabet =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  let state = 1;
  const chars = new Array<string>(size);
  for (let i = 0; i < size; i++) {
    // Park-Miller PRNG.
    state = (state * 48271) % 2147483647;
    chars[i] = alphabet[state % alphabet.length];
  }
  return chars.join("");
}
const random = makeRandomText(LARGE_SIZE);

// -- Server -------------------------------------------------------------------

const elizaImpl: ServiceImpl<typeof ElizaService> = {
  say: (req) => ({ sentence: req.sentence }),
  converse: async function* (reqs) {
    for await (const req of reqs) {
      yield { sentence: req.sentence };
    }
  },
  introduce: async function* (req) {
    yield { sentence: req.name };
  },
};

interface BenchServer {
  baseUrl: string;
  bytes(): { rx: number; tx: number };
  close(): Promise<void>;
}

type Draft = "draft1" | "draft2" | "draft3" | "draft4";

function startServer(
  draft: Draft,
  perMessageDeflate: boolean,
): Promise<BenchServer> {
  const router = createConnectRouter();
  router.service(ElizaService, elizaImpl);
  const server = http.createServer((_req, res) => {
    res.writeHead(404);
    res.end();
  });

  // Track every upgraded socket so wire bytes can be read at measure points.
  // Sockets stay tracked after close: bytesRead/bytesWritten remain valid.
  const sockets: Socket[] = [];
  server.on("upgrade", (_req, socket) => {
    sockets.push(socket as Socket);
  });

  const options = {
    webSocketServerOptions: { perMessageDeflate },
  };
  if (draft === "draft1") {
    createBidiWebSocketDraft1Handler(router, options).upgrade(server);
  } else if (draft === "draft2") {
    createBidiWebSocketDraft2Handler(router, options).upgrade(server);
  } else if (draft === "draft3") {
    // Draft 3 negotiates its own compression; perMessageDeflate is unused.
    createBidiWebSocketDraft3Handler(router, options).upgrade(server);
  } else {
    createBidiWebSocketDraft4Handler(router, options).upgrade(server);
  }

  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port =
        typeof address === "object" && address !== null ? address.port : 0;
      resolve({
        baseUrl: `http://127.0.0.1:${port}`,
        bytes: () => {
          let rx = 0;
          let tx = 0;
          for (const socket of sockets) {
            rx += socket.bytesRead;
            tx += socket.bytesWritten;
          }
          return { rx, tx };
        },
        close: () =>
          new Promise<void>((done, fail) => {
            server.close((err) => (err ? fail(err) : done()));
          }),
      });
    });
  });
}

// -- Workloads ------------------------------------------------------------------

type ElizaClient = ReturnType<typeof createClient<typeof ElizaService>>;

/**
 * One bidi Converse stream that echoes `text` back `roundTrips` times, in
 * lockstep (each request waits for its response).
 */
async function bidiEcho(
  client: ElizaClient,
  text: string,
  roundTrips: number,
): Promise<void> {
  let push: ((s: string | undefined) => void) | undefined;
  const queue: (string | undefined)[] = [];
  const input = (async function* () {
    for (;;) {
      const item =
        queue.length > 0
          ? queue.shift()
          : await new Promise<string | undefined>((resolve) => {
              push = resolve;
            });
      if (item === undefined) {
        return;
      }
      yield { sentence: item };
    }
  })();
  const send = (item: string | undefined) => {
    if (push !== undefined) {
      const resolve = push;
      push = undefined;
      resolve(item);
    } else {
      queue.push(item);
    }
  };

  const responses = client.converse(input)[Symbol.asyncIterator]();
  for (let i = 0; i < roundTrips; i++) {
    send(text);
    const res = await responses.next();
    if (res.done !== false || res.value.sentence.length !== text.length) {
      throw new Error("echo mismatch");
    }
  }
  send(undefined);
  const end = await responses.next();
  if (end.done !== true) {
    throw new Error("expected end of stream");
  }
}

interface WorkloadResult {
  case: string;
  workload: string;
  "ms/op": number;
  "roundtrips/s": number;
  "rx B/rt": number;
  "tx B/rt": number;
}

async function runWorkload(
  caseName: string,
  workload: string,
  server: BenchServer,
  ops: number,
  roundTripsPerOp: number,
  run: () => Promise<void>,
): Promise<WorkloadResult> {
  // Warmup (also dials the shared connection).
  await run();
  const before = server.bytes();
  const start = process.hrtime.bigint();
  for (let i = 0; i < ops; i++) {
    await run();
  }
  const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
  const after = server.bytes();
  const roundTrips = ops * roundTripsPerOp;
  return {
    case: caseName,
    workload,
    "ms/op": round(elapsedMs / ops),
    "roundtrips/s": round(roundTrips / (elapsedMs / 1000)),
    "rx B/rt": round((after.rx - before.rx) / roundTrips),
    "tx B/rt": round((after.tx - before.tx) / roundTrips),
  };
}

function round(n: number): number {
  return Math.round(n * 10) / 10;
}

// -- Main -----------------------------------------------------------------------

interface BenchCase {
  name: string;
  draft: Draft;
  perMessageDeflate: boolean;
  makeTransport(baseUrl: string): Transport & { close(): void };
}

const cases: BenchCase[] = [
  {
    name: "ws-draft1/identity",
    draft: "draft1",
    perMessageDeflate: false,
    makeTransport: (baseUrl) =>
      createConnectWebSocketDraft1Transport({ baseUrl }),
  },
  {
    // Draft 1 with the server offering permessage-deflate. The TS client
    // has no per-message (Connect gzip) compression, so this is the only
    // compressed draft 1 variant available in TypeScript.
    name: "ws-draft1/deflate",
    draft: "draft1",
    perMessageDeflate: true,
    makeTransport: (baseUrl) =>
      createConnectWebSocketDraft1Transport({ baseUrl }),
  },
  {
    name: "ws-draft2/identity",
    draft: "draft2",
    perMessageDeflate: false,
    makeTransport: (baseUrl) =>
      createConnectWebSocketDraft2Transport({ baseUrl }),
  },
  {
    name: "ws-draft2/deflate",
    draft: "draft2",
    perMessageDeflate: true,
    makeTransport: (baseUrl) =>
      createConnectWebSocketDraft2Transport({ baseUrl }),
  },
  {
    name: "ws-draft3/identity",
    draft: "draft3",
    perMessageDeflate: false,
    makeTransport: (baseUrl) =>
      createConnectWebSocketDraft3Transport({
        baseUrl,
        withoutCompression: true,
      }),
  },
  {
    // Draft 3's protocol-level compression: per-frame raw DEFLATE above
    // 512 bytes, negotiated by subprotocol — and unlike permessage-deflate,
    // the client compresses its own frames too.
    name: "ws-draft3/deflate",
    draft: "draft3",
    perMessageDeflate: false,
    makeTransport: (baseUrl) =>
      createConnectWebSocketDraft3Transport({ baseUrl }),
  },
  {
    // Draft 4 with JSON, the all-text configuration it is designed around:
    // every frame on the connection is a readable text message. This case
    // measures what that legibility costs against draft 2's binary framing.
    name: "ws-draft4/json",
    draft: "draft4",
    perMessageDeflate: false,
    makeTransport: (baseUrl) =>
      createConnectWebSocketDraft4Transport({ baseUrl }),
  },
  {
    // The same protocol with protobuf payloads. NOTE: every other case in
    // this suite uses the JSON codec (the web transports' default), so this
    // row differs from them in *codec as well as draft* -- it is not a
    // framing comparison. Read it against ws-draft4/json to see what the
    // codec costs; read ws-draft4/json against ws-draft2/identity to see
    // what the framing costs.
    name: "ws-draft4/proto",
    draft: "draft4",
    perMessageDeflate: false,
    makeTransport: (baseUrl) =>
      createConnectWebSocketDraft4Transport({
        baseUrl,
        useBinaryFormat: true,
      }),
  },
  {
    name: "ws-draft4/deflate",
    draft: "draft4",
    perMessageDeflate: true,
    makeTransport: (baseUrl) =>
      createConnectWebSocketDraft4Transport({ baseUrl }),
  },
];

async function main(): Promise<void> {
  const results: WorkloadResult[] = [];
  for (const benchCase of cases) {
    const server = await startServer(
      benchCase.draft,
      benchCase.perMessageDeflate,
    );
    const transport = benchCase.makeTransport(server.baseUrl);
    const client = createClient(ElizaService, transport);
    try {
      results.push(
        await runWorkload(
          benchCase.name,
          "bidi_small x1000",
          server,
          5,
          1000,
          () => bidiEcho(client, "hello there", 1000),
        ),
      );
      results.push(
        await runWorkload(
          benchCase.name,
          "bidi_16KiB_repetitive x50",
          server,
          5,
          50,
          () => bidiEcho(client, repetitive, 50),
        ),
      );
      results.push(
        await runWorkload(
          benchCase.name,
          "bidi_16KiB_random x50",
          server,
          5,
          50,
          () => bidiEcho(client, random, 50),
        ),
      );
    } finally {
      transport.close();
      await server.close();
    }
  }
  // With --out <path>, write JSON for the demo site instead of a table.
  const outIndex = process.argv.indexOf("--out");
  if (outIndex !== -1 && process.argv[outIndex + 1] !== undefined) {
    const payload = {
      generatedAt: new Date().toISOString().slice(0, 10),
      runtime: `Node ${process.versions.node}`,
      rows: results,
    };
    await writeFile(
      process.argv[outIndex + 1],
      `${JSON.stringify(payload, null, 2)}\n`,
    );
    return;
  }
  console.log(
    "\nTS WebSocket transport benchmarks (client: connect-bidi-web over " +
      "Node WebSocket; server: connect-bidi-node)\n" +
      "rx = client->server bytes, tx = server->client bytes, per roundtrip\n",
  );
  console.table(results);
}

main().catch((err: unknown) => {
  console.error(err);
  process.exitCode = 1;
});
