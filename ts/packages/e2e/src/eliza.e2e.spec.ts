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
// End-to-end tests for the TypeScript client transports:
//
// - TS WebSocket client <-> TS server (@sudorandom/connect-bidi-node),
//   in-process over a real WebSocket connection.
// - TS WebSocket + composite client <-> Go server (connectwebsocket), spawned
//   via `go run ./internal/e2e/cmd/elizaserver`. Skipped when the Go
//   toolchain is unavailable, unless E2E_REQUIRE_INTEROP is set.
//
// Run with: npm run e2e
//
// Note: Node.js has no WebTransport client API, so the WebTransport transport
// is e2e-covered on the Go side (internal/e2e) and by the browser demo.
//

import * as assert from "node:assert";
import { spawn, spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as http from "node:http";
import * as path from "node:path";
import { after, before, describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import type { MessageInitShape } from "@bufbuild/protobuf";
import type { Client, ServiceImpl } from "@connectrpc/connect";
import { createClient, createConnectRouter } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { createConnectTransport } from "@connectrpc/connect-web";
import {
  createBidiWebSocketDraft1Handler,
  createBidiWebSocketDraft3Handler,
  createBidiWebSocketDraft4Handler,
  createBidiWebSocketDraft5Handler,
  createBidiWebSocketDraft7Handler,
} from "@sudorandom/connect-bidi-node";
import type {
  ConnectWebSocketDraft3Transport,
  ConnectWebSocketDraft4Transport,
} from "@sudorandom/connect-bidi-web";
import {
  createCompositeTransport,
  createConnectWebSocketDraft1Transport,
  createConnectWebSocketDraft3Transport,
  createConnectWebSocketDraft4Transport,
  createConnectWebSocketDraft5Transport,
  createConnectWebSocketDraft7Transport,
} from "@sudorandom/connect-bidi-web";
import type { ConverseRequestSchema } from "./gen/connectbidi/eliza/v1/eliza_pb.js";
import { ElizaService } from "./gen/connectbidi/eliza/v1/eliza_pb.js";

type ElizaClient = Client<typeof ElizaService>;

// -- Bidi helper --------------------------------------------------------------

/**
 * An AsyncIterable that the test can push to imperatively, so requests and
 * responses can be interleaved in lockstep (true full-duplex, not just
 * batch-send-then-read).
 */
function createPushIterable<T>(): AsyncIterable<T> & {
  push(item: T): void;
  end(): void;
} {
  const items: T[] = [];
  let done = false;
  let notify: (() => void) | undefined;
  const wake = () => {
    const current = notify;
    notify = undefined;
    current?.();
  };
  return {
    push(item: T) {
      items.push(item);
      wake();
    },
    end() {
      done = true;
      wake();
    },
    async *[Symbol.asyncIterator]() {
      for (;;) {
        const item = items.shift();
        if (item !== undefined) {
          yield item;
          continue;
        }
        if (done) {
          return;
        }
        await new Promise<void>((resolve) => {
          notify = resolve;
        });
      }
    },
  };
}

// -- Shared RPC exercises ------------------------------------------------------

function exerciseStreams(getClient: () => ElizaClient) {
  it("server-streaming: introduce", async () => {
    const sentences: string[] = [];
    for await (const res of getClient().introduce({ name: "e2e" })) {
      sentences.push(res.sentence);
    }
    assert.ok(sentences.length > 0, "expected at least one sentence");
    assert.ok(
      sentences[0]?.includes("e2e"),
      `first sentence ${JSON.stringify(sentences[0])} does not echo the name`,
    );
  });

  it("bidi-streaming: converse in lockstep", async () => {
    const input =
      createPushIterable<MessageInitShape<typeof ConverseRequestSchema>>();
    const responses = getClient().converse(input)[Symbol.asyncIterator]();
    for (let i = 1; i <= 3; i++) {
      const sentence = `bidi msg ${i}`;
      input.push({ sentence });
      const res = await responses.next();
      assert.strictEqual(res.done, false, "response stream ended early");
      assert.ok(
        res.done === false && res.value.sentence.includes(sentence),
        `response does not echo ${JSON.stringify(sentence)}`,
      );
    }
    input.end();
    const end = await responses.next();
    assert.strictEqual(end.done, true, "expected end of response stream");
  });
}

// -- TS client <-> TS server ---------------------------------------------------

const elizaImpl: ServiceImpl<typeof ElizaService> = {
  say: (req) => ({ sentence: `TS Eliza says: ${req.sentence}` }),
  converse: async function* (reqs) {
    for await (const req of reqs) {
      yield { sentence: `TS Eliza hears: ${req.sentence}` };
    }
  },
  introduce: async function* (req) {
    yield { sentence: `Hello, ${req.name}. I am TS Eliza.` };
    yield { sentence: "How are you feeling today?" };
  },
};

describe("TS Draft3 WebSocket client <-> TS Node server", () => {
  let server: http.Server;
  let transport: ConnectWebSocketDraft3Transport;
  let client: ElizaClient;
  let upgrades = 0;

  before(async () => {
    const router = createConnectRouter();
    router.service(ElizaService, elizaImpl);
    server = http.createServer((_req, res) => {
      res.writeHead(404);
      res.end();
    });
    server.on("upgrade", () => {
      upgrades++;
    });
    createBidiWebSocketDraft3Handler(router).upgrade(server);
    await new Promise<void>((resolve) => {
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    const port =
      typeof address === "object" && address !== null ? address.port : 0;
    transport = createConnectWebSocketDraft3Transport({
      baseUrl: `http://127.0.0.1:${port}`,
    });
    client = createClient(ElizaService, transport);
  });

  after(
    () =>
      new Promise<void>((resolve, reject) => {
        // Every RPC in this suite — a server-streaming Introduce and a bidi
        // Converse — must have been multiplexed onto one shared WebSocket
        // connection; a second upgrade means connection reuse broke.
        assert.strictEqual(
          upgrades,
          1,
          `expected all RPCs to share one connection, saw ${upgrades} upgrades`,
        );
        // The shared multiplexed connection outlives individual RPCs and
        // would otherwise keep server.close() (and the process) waiting.
        transport.close();
        server.close((err) => (err ? reject(err) : resolve()));
      }),
  );

  exerciseStreams(() => client);
});

describe("TS Draft3 WebSocket client <-> TS Node server", () => {
  let server: http.Server;
  let transport: ConnectWebSocketDraft3Transport;
  let client: ElizaClient;
  let upgrades = 0;

  before(async () => {
    const router = createConnectRouter();
    router.service(ElizaService, elizaImpl);
    server = http.createServer((_req, res) => {
      res.writeHead(404);
      res.end();
    });
    server.on("upgrade", () => {
      upgrades++;
    });
    createBidiWebSocketDraft3Handler(router).upgrade(server);
    await new Promise<void>((resolve) => {
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    const port =
      typeof address === "object" && address !== null ? address.port : 0;
    transport = createConnectWebSocketDraft3Transport({
      baseUrl: `http://127.0.0.1:${port}`,
    });
    client = createClient(ElizaService, transport);
  });

  after(
    () =>
      new Promise<void>((resolve, reject) => {
        assert.strictEqual(
          upgrades,
          1,
          `expected all RPCs to share one connection, saw ${upgrades} upgrades`,
        );
        transport.close();
        server.close((err) => (err ? reject(err) : resolve()));
      }),
  );

  exerciseStreams(() => client);

  it("compresses large payloads through the negotiated subprotocol", async () => {
    // 16 KiB of repetitive text: far above the 512-byte threshold, so it
    // crosses the wire deflated in both directions and must round-trip.
    const sentence = "all work and no play makes jack a dull boy. ".repeat(400);
    const input =
      createPushIterable<MessageInitShape<typeof ConverseRequestSchema>>();
    const responses = client.converse(input)[Symbol.asyncIterator]();
    input.push({ sentence });
    const res = await responses.next();
    assert.strictEqual(res.done, false);
    assert.ok(
      res.done === false && res.value.sentence.includes(sentence),
      "large compressed payload did not round-trip",
    );
    input.end();
    const end = await responses.next();
    assert.strictEqual(end.done, true);
  });
});

// -- TS client <-> Go server ---------------------------------------------------

const repoRoot = fileURLToPath(new URL("../../../../", import.meta.url));
const goAvailable = spawnSync("go", ["version"]).status === 0;
if (!goAvailable && process.env.E2E_REQUIRE_INTEROP !== undefined) {
  throw new Error("go not found in PATH (E2E_REQUIRE_INTEROP is set)");
}

interface GoServer {
  baseUrl: string;
  stop(): Promise<void>;
}

function startGoServer(): Promise<GoServer> {
  // Build first and spawn the binary directly: `go run` does not reliably
  // forward SIGTERM to its child, which would leave the server orphaned and
  // its stdout pipe holding the Node event loop open.
  const binPath = path.join(repoRoot, ".tmp", "bin", "e2e-elizaserver");
  fs.mkdirSync(path.dirname(binPath), { recursive: true });
  const build = spawnSync(
    "go",
    ["build", "-o", binPath, "./internal/e2e/cmd/elizaserver"],
    { cwd: repoRoot, stdio: ["ignore", "inherit", "inherit"] },
  );
  if (build.status !== 0) {
    return Promise.reject(
      new Error(`go build failed with status ${build.status}`),
    );
  }
  return new Promise((resolve, reject) => {
    const proc = spawn(binPath, { stdio: ["ignore", "pipe", "inherit"] });
    let output = "";
    proc.stdout.setEncoding("utf8");
    proc.stdout.on("data", (chunk: string) => {
      output += chunk;
      const match = output.match(/READY ws:\/\/(\S+)\/websocket-draft3/);
      if (match !== null) {
        resolve({
          baseUrl: `http://${match[1]}`,
          stop: () =>
            new Promise<void>((stopped) => {
              proc.once("exit", () => stopped());
              proc.kill("SIGTERM");
            }),
        });
      }
    });
    proc.on("error", reject);
    proc.on("exit", (code) => {
      reject(new Error(`go server exited early with code ${code}`));
    });
  });
}

// Draft 4 client against the TS Node draft 4 server, both in-process: the
// all-text default, where every frame on the wire is readable.
describe("TS Draft4 WebSocket client <-> TS Node server", () => {
  let server: http.Server;
  let transport: ConnectWebSocketDraft4Transport;
  let client: ElizaClient;
  let upgrades = 0;

  before(async () => {
    const router = createConnectRouter();
    router.service(ElizaService, elizaImpl);
    server = http.createServer((_req, res) => {
      res.writeHead(404);
      res.end();
    });
    server.on("upgrade", () => {
      upgrades++;
    });
    createBidiWebSocketDraft4Handler(router).upgrade(server);
    await new Promise<void>((resolve) => {
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    const port =
      typeof address === "object" && address !== null ? address.port : 0;
    transport = createConnectWebSocketDraft4Transport({
      baseUrl: `http://127.0.0.1:${port}`,
    });
    client = createClient(ElizaService, transport);
  });

  after(
    () =>
      new Promise<void>((resolve, reject) => {
        assert.strictEqual(
          upgrades,
          1,
          `expected all RPCs to share one connection, saw ${upgrades} upgrades`,
        );
        transport.close();
        server.close((err) => (err ? reject(err) : resolve()));
      }),
  );

  exerciseStreams(() => client);
});

describe("TS composite client <-> Go server", {
  skip: goAvailable ? false : "go not found in PATH",
}, () => {
  let goServer: GoServer;
  let wsTransport: ConnectWebSocketDraft3Transport;
  let client: ElizaClient;

  before(async () => {
    goServer = await startGoServer();
    // Unary over plain Connect HTTP, streams over WebSocket — the
    // recommended production setup from the README.
    wsTransport = createConnectWebSocketDraft3Transport({
      baseUrl: goServer.baseUrl,
    });
    client = createClient(
      ElizaService,
      createCompositeTransport(
        createConnectTransport({ baseUrl: goServer.baseUrl }),
        wsTransport,
      ),
    );
  });

  after(async () => {
    wsTransport.close();
    await goServer.stop();
  });

  it("unary: say over plain Connect HTTP", async () => {
    const res = await client.say({ sentence: "unary hello" });
    assert.ok(
      res.sentence.includes("unary hello"),
      `response ${JSON.stringify(res.sentence)} does not echo the request`,
    );
  });

  exerciseStreams(() => client);
});

describe("TS Draft3 WebSocket client <-> Go server", {
  skip: goAvailable ? false : "go not found in PATH",
}, () => {
  let goServer: GoServer;
  let wsTransport: ConnectWebSocketDraft3Transport;
  let client: ElizaClient;

  before(async () => {
    goServer = await startGoServer();
    wsTransport = createConnectWebSocketDraft3Transport({
      baseUrl: goServer.baseUrl,
    });
    client = createClient(ElizaService, wsTransport);
  });

  after(async () => {
    wsTransport.close();
    await goServer.stop();
  });

  exerciseStreams(() => client);
});

// Draft 4 across languages: the ASCII frame head has to be parsed
// identically by both sides, and each side has to accept whichever
// WebSocket message type the other chose to send.
describe("TS Draft4 WebSocket client <-> Go server", {
  skip: goAvailable ? false : "go not found in PATH",
}, () => {
  let goServer: GoServer;
  let wsTransport: ConnectWebSocketDraft4Transport;
  let client: ElizaClient;

  before(async () => {
    goServer = await startGoServer();
    wsTransport = createConnectWebSocketDraft4Transport({
      baseUrl: goServer.baseUrl,
    });
    client = createClient(ElizaService, wsTransport);
  });

  after(async () => {
    wsTransport.close();
    await goServer.stop();
  });

  exerciseStreams(() => client);
});

// The same, with protobuf payloads: the client then sends binary data
// frames and text control frames on one connection, so both sides have to
// handle a mixed-opcode stream.
describe("TS Draft4 proto WebSocket client <-> Go server", {
  skip: goAvailable ? false : "go not found in PATH",
}, () => {
  let goServer: GoServer;
  let wsTransport: ConnectWebSocketDraft4Transport;
  let client: ElizaClient;

  before(async () => {
    goServer = await startGoServer();
    wsTransport = createConnectWebSocketDraft4Transport({
      baseUrl: goServer.baseUrl,
      useBinaryFormat: true,
    });
    client = createClient(ElizaService, wsTransport);
  });

  after(async () => {
    wsTransport.close();
    await goServer.stop();
  });

  exerciseStreams(() => client);
});

// -- Draft 5 -------------------------------------------------------------------
//
// Draft 5 has no path of its own: the Connect procedure URL answers POST
// with ordinary Connect and GET+Upgrade with the WebSocket protocol. So
// these suites check two things the other drafts cannot get wrong — that
// unary RPCs never upgrade, and that streaming RPCs open one socket each.

// Draft 1 against the TS Node server, both in-process. Like draft 5 it
// carries one RPC per socket and leaves unary on plain Connect HTTP; unlike
// draft 5 every message is a Connect envelope, which is what these tests
// exercise across the two implementations.
describe("TS Draft1 WebSocket client <-> TS Node server", () => {
  let server: http.Server;
  let client: ElizaClient;
  let upgrades = 0;

  before(async () => {
    const router = createConnectRouter();
    router.service(ElizaService, elizaImpl);
    // The very same server answers unary RPCs over plain Connect HTTP...
    server = http.createServer(
      connectNodeAdapter({
        routes: (routes) => {
          routes.service(ElizaService, elizaImpl);
        },
      }),
    );
    server.on("upgrade", () => {
      upgrades++;
    });
    // ...and streaming RPCs by upgrading the same URLs.
    createBidiWebSocketDraft1Handler(router).upgrade(server);
    await new Promise<void>((resolve) => {
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    const port =
      typeof address === "object" && address !== null ? address.port : 0;
    const baseUrl = `http://127.0.0.1:${port}`;
    client = createClient(
      ElizaService,
      createConnectWebSocketDraft1Transport({
        baseUrl,
        unaryTransport: createConnectTransport({ baseUrl }),
      }),
    );
  });

  after(
    () =>
      new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      }),
  );

  it("unary: say never upgrades", async () => {
    const before = upgrades;
    const res = await client.say({ sentence: "unary hello" });
    assert.ok(
      res.sentence.includes("unary hello"),
      `response ${JSON.stringify(res.sentence)} does not echo the request`,
    );
    assert.strictEqual(
      upgrades,
      before,
      "a unary RPC opened a WebSocket; it must stay on plain Connect HTTP",
    );
  });

  it("streaming: one socket per RPC", async () => {
    const before = upgrades;
    for await (const _ of client.introduce({ name: "counted" })) {
      // Drain.
    }
    assert.strictEqual(
      upgrades,
      before + 1,
      "expected exactly one upgrade for one streaming RPC",
    );
  });

  exerciseStreams(() => client);
});

// Draft 1 across languages. The Go fixture already gives draft 5 the bare
// procedure URLs, so draft 1 is mounted under a prefix there; the procedure
// is the path's last two segments either way, which is exactly what a
// prefixed base URL exercises.
describe("TS Draft1 WebSocket client <-> Go server", {
  skip: goAvailable ? false : "go not found in PATH",
}, () => {
  let goServer: GoServer;
  let client: ElizaClient;

  before(async () => {
    goServer = await startGoServer();
    client = createClient(
      ElizaService,
      createConnectWebSocketDraft1Transport({
        baseUrl: `${goServer.baseUrl}/websocket-draft1`,
        unaryTransport: createConnectTransport({ baseUrl: goServer.baseUrl }),
      }),
    );
  });

  after(async () => {
    await goServer.stop();
  });

  it("unary: say over plain Connect HTTP", async () => {
    const res = await client.say({ sentence: "unary hello" });
    assert.ok(
      res.sentence.includes("unary hello"),
      `response ${JSON.stringify(res.sentence)} does not echo the request`,
    );
  });

  exerciseStreams(() => client);
});

// The same, with JSON payloads. The envelope head stays binary whatever the
// codec, which is the trade draft 1 makes and draft 5 refuses.
describe("TS Draft1 JSON WebSocket client <-> Go server", {
  skip: goAvailable ? false : "go not found in PATH",
}, () => {
  let goServer: GoServer;
  let client: ElizaClient;

  before(async () => {
    goServer = await startGoServer();
    client = createClient(
      ElizaService,
      createConnectWebSocketDraft1Transport({
        baseUrl: `${goServer.baseUrl}/websocket-draft1`,
        useBinaryFormat: false,
        unaryTransport: createConnectTransport({ baseUrl: goServer.baseUrl }),
      }),
    );
  });

  after(async () => {
    await goServer.stop();
  });

  exerciseStreams(() => client);
});

describe("TS Draft5 WebSocket client <-> TS Node server", () => {
  let server: http.Server;
  let client: ElizaClient;
  let upgrades = 0;

  before(async () => {
    const router = createConnectRouter();
    router.service(ElizaService, elizaImpl);
    // The very same server answers unary RPCs over plain Connect HTTP...
    server = http.createServer(
      connectNodeAdapter({
        routes: (routes) => {
          routes.service(ElizaService, elizaImpl);
        },
      }),
    );
    server.on("upgrade", () => {
      upgrades++;
    });
    // ...and streaming RPCs by upgrading the same URLs.
    createBidiWebSocketDraft5Handler(router).upgrade(server);
    await new Promise<void>((resolve) => {
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    const port =
      typeof address === "object" && address !== null ? address.port : 0;
    const baseUrl = `http://127.0.0.1:${port}`;
    client = createClient(
      ElizaService,
      createConnectWebSocketDraft5Transport({
        baseUrl,
        unaryTransport: createConnectTransport({ baseUrl }),
      }),
    );
  });

  after(
    () =>
      new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      }),
  );

  it("unary: say never upgrades", async () => {
    const before = upgrades;
    const res = await client.say({ sentence: "unary hello" });
    assert.ok(
      res.sentence.includes("unary hello"),
      `response ${JSON.stringify(res.sentence)} does not echo the request`,
    );
    assert.strictEqual(
      upgrades,
      before,
      "a unary RPC opened a WebSocket; it must stay on plain Connect HTTP",
    );
  });

  it("streaming: one socket per RPC", async () => {
    const before = upgrades;
    for await (const _ of client.introduce({ name: "counted" })) {
      // Drain.
    }
    assert.strictEqual(
      upgrades,
      before + 1,
      "expected exactly one upgrade for one streaming RPC",
    );
  });

  exerciseStreams(() => client);
});

// Draft 5 across languages. The Go fixture mounts draft5 in place of
// connecthttp, so the same URLs serve both dispatch paths there too.
describe("TS Draft5 WebSocket client <-> Go server", {
  skip: goAvailable ? false : "go not found in PATH",
}, () => {
  let goServer: GoServer;
  let client: ElizaClient;

  before(async () => {
    goServer = await startGoServer();
    client = createClient(
      ElizaService,
      createConnectWebSocketDraft5Transport({
        baseUrl: goServer.baseUrl,
        unaryTransport: createConnectTransport({ baseUrl: goServer.baseUrl }),
      }),
    );
  });

  after(async () => {
    await goServer.stop();
  });

  it("unary: say over plain Connect HTTP", async () => {
    const res = await client.say({ sentence: "unary hello" });
    assert.ok(
      res.sentence.includes("unary hello"),
      `response ${JSON.stringify(res.sentence)} does not echo the request`,
    );
  });

  exerciseStreams(() => client);
});

// The same, with JSON payloads: data messages stay binary whatever the
// codec, because the separator owns the empty text message.
describe("TS Draft5 JSON WebSocket client <-> Go server", {
  skip: goAvailable ? false : "go not found in PATH",
}, () => {
  let goServer: GoServer;
  let client: ElizaClient;

  before(async () => {
    goServer = await startGoServer();
    client = createClient(
      ElizaService,
      createConnectWebSocketDraft5Transport({
        baseUrl: goServer.baseUrl,
        useBinaryFormat: false,
        unaryTransport: createConnectTransport({ baseUrl: goServer.baseUrl }),
      }),
    );
  });

  after(async () => {
    await goServer.stop();
  });

  exerciseStreams(() => client);
});

// -- Draft 7: the Connect-over-WebSocket specification -------------------------

describe("TS Draft7 WebSocket client <-> TS Node server", () => {
  let server: http.Server;
  let client: ElizaClient;
  let overSocket: ElizaClient;
  let upgrades = 0;

  before(async () => {
    const router = createConnectRouter();
    router.service(ElizaService, elizaImpl);
    // The very same server answers unary RPCs over plain Connect HTTP...
    server = http.createServer(
      connectNodeAdapter({
        routes: (routes) => {
          routes.service(ElizaService, elizaImpl);
        },
      }),
    );
    server.on("upgrade", () => {
      upgrades++;
    });
    // ...and every RPC by upgrading the same URLs.
    createBidiWebSocketDraft7Handler(router).upgrade(server);
    await new Promise<void>((resolve) => {
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    const port =
      typeof address === "object" && address !== null ? address.port : 0;
    const baseUrl = `http://127.0.0.1:${port}`;
    client = createClient(
      ElizaService,
      createConnectWebSocketDraft7Transport({
        baseUrl,
        unaryTransport: createConnectTransport({ baseUrl }),
      }),
    );
    overSocket = createClient(
      ElizaService,
      createConnectWebSocketDraft7Transport({ baseUrl }),
    );
  });

  after(
    () =>
      new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      }),
  );

  it("unary: say stays on HTTP with a unary transport", async () => {
    const before = upgrades;
    const res = await client.say({ sentence: "unary hello" });
    assert.ok(
      res.sentence.includes("unary hello"),
      `response ${JSON.stringify(res.sentence)} does not echo the request`,
    );
    assert.strictEqual(upgrades, before, "a unary RPC opened a WebSocket");
  });

  it("unary: say upgrades without one", async () => {
    const before = upgrades;
    const res = await overSocket.say({ sentence: "socket hello" });
    assert.ok(
      res.sentence.includes("socket hello"),
      `response ${JSON.stringify(res.sentence)} does not echo the request`,
    );
    assert.strictEqual(
      upgrades,
      before + 1,
      "expected exactly one upgrade for one unary RPC over the socket",
    );
  });

  it("streaming: one socket per RPC", async () => {
    const before = upgrades;
    for await (const _ of client.introduce({ name: "counted" })) {
      // Drain.
    }
    assert.strictEqual(
      upgrades,
      before + 1,
      "expected exactly one upgrade for one streaming RPC",
    );
  });

  it("deadline: a silent client is answered, not parked", async () => {
    // A deadline shorter than the handler's work: the server ends the RPC
    // with deadline_exceeded in S rather than holding the socket.
    const input =
      createPushIterable<MessageInitShape<typeof ConverseRequestSchema>>();
    const responses = client
      .converse(input, { timeoutMs: 200 })
      [Symbol.asyncIterator]();
    await assert.rejects(
      () => responses.next(),
      (err: unknown) => err instanceof Error && /deadline/i.test(err.message),
    );
  });

  exerciseStreams(() => client);
});

// Draft 7 across languages. The Go fixture mounts draft 7's WebSocket side
// under a prefix — the specification's own provision for sharing an origin
// — so the client is told the prefix too.
describe("TS Draft7 WebSocket client <-> Go server", {
  skip: goAvailable ? false : "go not found in PATH",
}, () => {
  let goServer: GoServer;
  let client: ElizaClient;

  before(async () => {
    goServer = await startGoServer();
    client = createClient(
      ElizaService,
      createConnectWebSocketDraft7Transport({
        baseUrl: goServer.baseUrl,
        pathPrefix: "/websocket-draft7",
        unaryTransport: createConnectTransport({ baseUrl: goServer.baseUrl }),
      }),
    );
  });

  after(async () => {
    await goServer.stop();
  });

  it("unary: say over plain Connect HTTP", async () => {
    const res = await client.say({ sentence: "unary hello" });
    assert.ok(
      res.sentence.includes("unary hello"),
      `response ${JSON.stringify(res.sentence)} does not echo the request`,
    );
  });

  exerciseStreams(() => client);
});

// The same with JSON — every message a text frame — and unary carried
// over the socket as the specification's worked example has it.
describe("TS Draft7 JSON WebSocket client <-> Go server", {
  skip: goAvailable ? false : "go not found in PATH",
}, () => {
  let goServer: GoServer;
  let client: ElizaClient;

  before(async () => {
    goServer = await startGoServer();
    client = createClient(
      ElizaService,
      createConnectWebSocketDraft7Transport({
        baseUrl: goServer.baseUrl,
        pathPrefix: "/websocket-draft7",
        useBinaryFormat: false,
      }),
    );
  });

  after(async () => {
    await goServer.stop();
  });

  it("unary: say over the socket", async () => {
    const res = await client.say({ sentence: "unary hello" });
    assert.ok(
      res.sentence.includes("unary hello"),
      `response ${JSON.stringify(res.sentence)} does not echo the request`,
    );
  });

  exerciseStreams(() => client);
});
