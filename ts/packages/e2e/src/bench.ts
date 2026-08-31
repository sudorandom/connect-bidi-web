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
// Benchmarks the TypeScript WebSocket transports — drafts 1, 3, and 4,
// with and without compression — on speed and wire-size efficiency. The client is
// @sudorandom/connect-bidi-web (over Node's global WebSocket), the server is
// @sudorandom/connect-bidi-node. Wire bytes are measured on the server's raw
// TCP sockets (net.Socket bytesRead/bytesWritten), so they include WebSocket
// framing and permessage-deflate effects.
//
// Read the two compression models as different things. Draft 3 compresses
// in the protocol, so both peers do it. Drafts 1 and 4 rely on the
// permessage-deflate extension, where compressing an outgoing message is
// each endpoint's own choice. Node's global WebSocket negotiates the
// extension, inflates what it receives, and then sends everything
// uncompressed — so in THIS suite those rows compress server->client only,
// and their client->server column is the uncompressed size. That is a Node
// limitation, not the protocol's: Chrome and Firefox both compress their
// uploads, so a browser client would show those rows compressing in both
// directions.
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
import { createConnectTransport } from "@connectrpc/connect-web";
import {
  createBidiWebSocketDraft1Handler,
  createBidiWebSocketDraft3Handler,
  createBidiWebSocketDraft4Handler,
  createBidiWebSocketDraft5Handler,
} from "@sudorandom/connect-bidi-node";
import {
  createConnectWebSocketDraft1Transport,
  createConnectWebSocketDraft3Transport,
  createConnectWebSocketDraft4Transport,
  createConnectWebSocketDraft5Transport,
} from "@sudorandom/connect-bidi-web";
import { ElizaService } from "./gen/connectbidi/eliza/v1/eliza_pb.js";

// -- Payloads -----------------------------------------------------------------

const LARGE_SIZE = 16 * 1024;

const OPS = 5;
const SMALL_ROUND_TRIPS = 1000;
const LARGE_ROUND_TRIPS = 50;

/**
 * Builds the text for one round trip. The nonce makes every round trip's
 * payload distinct while keeping its size and compressibility fixed.
 *
 * Case names are "ws-draft<N>/<codec>/<compression>", so a row says on its
 * face which encoding produced its byte counts. That matters across suites:
 * these TypeScript transports default to the JSON codec, while the Go
 * suite's equivalents default to protobuf, so two identically shaped rows
 * in the two tables are not the same measurement. Every draft covers the
 * full codec x compression grid, because JSON compresses far better than
 * protobuf and measuring only one of the four corners misleads.
 *
 * Distinctness is the point. A workload reuses one connection for all of
 * its round trips, so sending the same bytes twice lets a DEFLATE
 * implementation with context takeover emit a back-reference into the
 * previous copy instead of actually compressing it: 16 KiB of
 * *incompressible* text came out at ~135 B that way, which measures the
 * workload rather than the protocol. Varying the payload removes the
 * shortcut whatever the extension is configured to do.
 */
type Payload = (nonce: number) => string;

// Fixed width, so varying it does not move the byte counts around. Eleven
// characters, the length this workload has always used.
const small: Payload = (nonce) =>
  `hello ${String(nonce % 100000).padStart(5, "0")}`;

// Highly compressible on its own — one short unit repeated — but the nonce
// sits inside the unit, so the payload does not share a long tail with its
// predecessor.
const repetitive: Payload = (nonce) => {
  const unit = `all work and no play makes jack ${nonce} a dull boy. `;
  return unit.repeat(Math.ceil(LARGE_SIZE / unit.length)).slice(0, LARGE_SIZE);
};

// Deterministic pseudo-random base64-ish text: barely compressible.
const random: Payload = (nonce) => makeRandomText(LARGE_SIZE, nonce);

function makeRandomText(size: number, seed: number): string {
  const alphabet =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  // Park-Miller PRNG; its state must stay in [1, 2147483646].
  let state = (seed % 2147483646) + 1;
  const chars = new Array<string>(size);
  for (let i = 0; i < size; i++) {
    state = (state * 48271) % 2147483647;
    chars[i] = alphabet[state % alphabet.length];
  }
  return chars.join("");
}

/**
 * Materializes every payload a workload will send, before it is timed —
 * generating 16 KiB of text costs more than the round trip that carries it.
 * One extra op's worth covers the warmup run.
 */
function precompute(payload: Payload, roundTrips: number): string[] {
  return Array.from({ length: roundTrips * (OPS + 1) }, (_, i) => payload(i));
}

/**
 * A cursor over precomputed payloads. One cursor serves a whole workload —
 * warmup and every op — so no payload is sent twice on the connection.
 */
function payloadCursor(payloads: string[]): () => string {
  let index = 0;
  return () => payloads[index++ % payloads.length];
}

const smallPayloads = precompute(small, SMALL_ROUND_TRIPS);
const repetitivePayloads = precompute(repetitive, LARGE_ROUND_TRIPS);
const randomPayloads = precompute(random, LARGE_ROUND_TRIPS);

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

type Draft = "draft1" | "draft3" | "draft4" | "draft5";

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

  // No context takeover, matching draft 3's model: every message is an
  // independent DEFLATE stream. `ws`'s default keeps a window across
  // messages, which on an echo workload compresses against the previous
  // message instead of the current one and makes the numbers unreadable.
  const options = {
    webSocketServerOptions: {
      perMessageDeflate: perMessageDeflate
        ? { serverNoContextTakeover: true, clientNoContextTakeover: true }
        : false,
    },
  };
  if (draft === "draft1") {
    // Draft 1 takes no path either: it upgrades the procedure URLs.
    createBidiWebSocketDraft1Handler(router, options).upgrade(server);
  } else if (draft === "draft3") {
    // Draft 3 negotiates its own compression; perMessageDeflate is unused.
    createBidiWebSocketDraft3Handler(router, options).upgrade(server);
  } else if (draft === "draft4") {
    createBidiWebSocketDraft4Handler(router, options).upgrade(server);
  } else {
    // Draft 5 takes no path: it upgrades the procedure URLs themselves.
    createBidiWebSocketDraft5Handler(router, options).upgrade(server);
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
 * One bidi Converse stream that echoes a payload back `roundTrips` times,
 * in lockstep (each request waits for its response). `next` supplies a
 * fresh payload per round trip; see the Payload docs for why they differ.
 */
async function bidiEcho(
  client: ElizaClient,
  next: () => string,
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
    const text = next();
    send(text);
    const res = await responses.next();
    // Full comparison, not just length: with a distinct payload per round
    // trip an off-by-one in the cursor would otherwise pass unnoticed.
    if (res.done !== false || res.value.sentence !== text) {
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
    name: "ws-draft3/json/identity",
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
    name: "ws-draft3/json/deflate",
    draft: "draft3",
    perMessageDeflate: false,
    makeTransport: (baseUrl) =>
      createConnectWebSocketDraft3Transport({ baseUrl }),
  },
  {
    name: "ws-draft3/proto/identity",
    draft: "draft3",
    perMessageDeflate: false,
    makeTransport: (baseUrl) =>
      createConnectWebSocketDraft3Transport({
        baseUrl,
        withoutCompression: true,
        useBinaryFormat: true,
      }),
  },
  {
    name: "ws-draft3/proto/deflate",
    draft: "draft3",
    perMessageDeflate: false,
    makeTransport: (baseUrl) =>
      createConnectWebSocketDraft3Transport({
        baseUrl,
        useBinaryFormat: true,
      }),
  },
  {
    // Draft 4 with JSON, the all-text configuration it is designed around:
    // every frame on the connection is a readable text message. This case
    // measures what that legibility costs against draft 3's binary framing.
    name: "ws-draft4/json/identity",
    draft: "draft4",
    perMessageDeflate: false,
    makeTransport: (baseUrl) =>
      createConnectWebSocketDraft4Transport({ baseUrl }),
  },
  {
    // The same protocol with protobuf payloads. Read it against
    // ws-draft4/json/identity to see what the codec costs, and
    // ws-draft4/json/identity against ws-draft3/json/identity to see what
    // the framing costs -- each case name states its encoding, so the two
    // comparisons stay separable.
    name: "ws-draft4/proto/identity",
    draft: "draft4",
    perMessageDeflate: false,
    makeTransport: (baseUrl) =>
      createConnectWebSocketDraft4Transport({
        baseUrl,
        useBinaryFormat: true,
      }),
  },
  {
    name: "ws-draft4/json/deflate",
    draft: "draft4",
    perMessageDeflate: true,
    makeTransport: (baseUrl) =>
      createConnectWebSocketDraft4Transport({ baseUrl }),
  },
  {
    name: "ws-draft4/proto/deflate",
    draft: "draft4",
    perMessageDeflate: true,
    makeTransport: (baseUrl) =>
      createConnectWebSocketDraft4Transport({
        baseUrl,
        useBinaryFormat: true,
      }),
  },
  {
    // Draft 1 pays five bytes of Connect envelope on every message, and
    // pays for its connection model the same way draft 5 does: one
    // WebSocket handshake per streaming RPC, where every draft 3 and 4 case
    // above reuses one connection for the whole workload. Read these rows
    // against the draft 5 rows below to price the envelope alone.
    name: "ws-draft1/json/identity",
    draft: "draft1",
    perMessageDeflate: false,
    makeTransport: (baseUrl) => draft1Transport(baseUrl, false),
  },
  {
    name: "ws-draft1/proto/identity",
    draft: "draft1",
    perMessageDeflate: false,
    makeTransport: (baseUrl) => draft1Transport(baseUrl, true),
  },
  {
    name: "ws-draft1/json/deflate",
    draft: "draft1",
    perMessageDeflate: true,
    makeTransport: (baseUrl) => draft1Transport(baseUrl, false),
  },
  {
    name: "ws-draft1/proto/deflate",
    draft: "draft1",
    perMessageDeflate: true,
    makeTransport: (baseUrl) => draft1Transport(baseUrl, true),
  },
  {
    // Draft 5 has no framing to measure: a data message is the codec's
    // output and nothing else. What these rows show instead is the cost it
    // moved from the message to the connection -- one WebSocket handshake
    // per streaming RPC, where every case above reuses one connection for
    // the whole workload.
    name: "ws-draft5/json/identity",
    draft: "draft5",
    perMessageDeflate: false,
    makeTransport: (baseUrl) => withNoopClose(baseUrl, false),
  },
  {
    name: "ws-draft5/proto/identity",
    draft: "draft5",
    perMessageDeflate: false,
    makeTransport: (baseUrl) => withNoopClose(baseUrl, true),
  },
  {
    name: "ws-draft5/json/deflate",
    draft: "draft5",
    perMessageDeflate: true,
    makeTransport: (baseUrl) => withNoopClose(baseUrl, false),
  },
  {
    name: "ws-draft5/proto/deflate",
    draft: "draft5",
    perMessageDeflate: true,
    makeTransport: (baseUrl) => withNoopClose(baseUrl, true),
  },
];

/**
 * Draft 1's transport has no `close()`, for the same reason draft 5's does
 * not: it holds no connection to close, because each RPC opened and closed
 * its own. The bench calls close() between cases, so give it a no-op.
 */
function draft1Transport(
  baseUrl: string,
  useBinaryFormat: boolean,
): Transport & { close(): void } {
  const transport = createConnectWebSocketDraft1Transport({
    baseUrl,
    useBinaryFormat,
    // Unary RPCs are not part of this suite; draft 1 would dispatch them
    // here as ordinary Connect HTTP requests.
    unaryTransport: createConnectTransport({ baseUrl }),
  });
  return { ...transport, close: () => {} };
}

/**
 * Draft 5's transport has no `close()`: it holds no connection to close,
 * because each RPC opened and closed its own. The bench calls close()
 * between cases, so give it a no-op.
 */
function withNoopClose(
  baseUrl: string,
  useBinaryFormat: boolean,
): Transport & { close(): void } {
  const transport = createConnectWebSocketDraft5Transport({
    baseUrl,
    useBinaryFormat,
    // Unary RPCs are not part of this suite; draft 5 would dispatch them
    // here as ordinary Connect HTTP requests.
    unaryTransport: createConnectTransport({ baseUrl }),
  });
  return { ...transport, close: () => {} };
}

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
      // One cursor per workload, created outside runWorkload so the warmup
      // and all OPS runs draw from the same non-repeating sequence.
      const smallCursor = payloadCursor(smallPayloads);
      results.push(
        await runWorkload(
          benchCase.name,
          "bidi_small x1000",
          server,
          OPS,
          SMALL_ROUND_TRIPS,
          () => bidiEcho(client, smallCursor, SMALL_ROUND_TRIPS),
        ),
      );
      const repetitiveCursor = payloadCursor(repetitivePayloads);
      results.push(
        await runWorkload(
          benchCase.name,
          "bidi_16KiB_repetitive x50",
          server,
          OPS,
          LARGE_ROUND_TRIPS,
          () => bidiEcho(client, repetitiveCursor, LARGE_ROUND_TRIPS),
        ),
      );
      const randomCursor = payloadCursor(randomPayloads);
      results.push(
        await runWorkload(
          benchCase.name,
          "bidi_16KiB_random x50",
          server,
          OPS,
          LARGE_ROUND_TRIPS,
          () => bidiEcho(client, randomCursor, LARGE_ROUND_TRIPS),
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
