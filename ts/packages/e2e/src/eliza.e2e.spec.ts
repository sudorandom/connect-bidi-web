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
import { createConnectTransport } from "@connectrpc/connect-web";
import { createBidiWebSocketHandler } from "@sudorandom/connect-bidi-node";
import type { ConnectWebSocketTransport } from "@sudorandom/connect-bidi-web";
import {
  createCompositeTransport,
  createConnectWebSocketTransport,
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

describe("TS WebSocket client <-> TS Node server", () => {
  let server: http.Server;
  let transport: ConnectWebSocketTransport;
  let client: ElizaClient;

  before(async () => {
    const router = createConnectRouter();
    router.service(ElizaService, elizaImpl);
    server = http.createServer((_req, res) => {
      res.writeHead(404);
      res.end();
    });
    createBidiWebSocketHandler(router).upgrade(server);
    await new Promise<void>((resolve) => {
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    const port =
      typeof address === "object" && address !== null ? address.port : 0;
    transport = createConnectWebSocketTransport({
      baseUrl: `http://127.0.0.1:${port}`,
    });
    client = createClient(ElizaService, transport);
  });

  after(
    () =>
      new Promise<void>((resolve, reject) => {
        // The shared multiplexed connection outlives individual RPCs and
        // would otherwise keep server.close() (and the process) waiting.
        transport.close();
        server.close((err) => (err ? reject(err) : resolve()));
      }),
  );

  exerciseStreams(() => client);
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
      const match = output.match(/READY ws:\/\/(\S+)\/websocket/);
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

describe("TS composite client <-> Go server", {
  skip: goAvailable ? false : "go not found in PATH",
}, () => {
  let goServer: GoServer;
  let wsTransport: ConnectWebSocketTransport;
  let client: ElizaClient;

  before(async () => {
    goServer = await startGoServer();
    // Unary over plain Connect HTTP, streams over WebSocket — the
    // recommended production setup from the README.
    wsTransport = createConnectWebSocketTransport({
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
