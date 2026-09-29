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
// End-to-end test for createFallbackTransport: a ladder whose first rung
// points at an unreachable server degrades to the working rung and sticks
// to it.
//

import * as assert from "node:assert";
import * as http from "node:http";
import { after, before, describe, it } from "node:test";
import type { ServiceImpl } from "@connectrpc/connect";
import { createClient, createConnectRouter } from "@connectrpc/connect";
import { createBidiWebSocketDraft3Handler } from "@sudorandom/connect-bidi-node";
import type { FallbackTransport } from "@sudorandom/connect-bidi-web";
import {
  createConnectWebSocketDraft3Transport,
  createFallbackTransport,
} from "@sudorandom/connect-bidi-web";
import { ElizaService } from "./gen/connectbidi/eliza/v1/eliza_pb.js";

const elizaImpl: ServiceImpl<typeof ElizaService> = {
  say: (req) => ({ sentence: req.sentence }),
  converse: async function* (reqs) {
    for await (const req of reqs) {
      yield { sentence: `heard: ${req.sentence}` };
    }
  },
  introduce: async function* (req) {
    yield { sentence: `Hello, ${req.name}.` };
  },
};

describe("createFallbackTransport()", () => {
  let server: http.Server;
  let transport: FallbackTransport;
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
    transport = createFallbackTransport(
      // Port 1 on loopback: connection refused, fails fast with
      // Code.Unavailable.
      createConnectWebSocketDraft3Transport({ baseUrl: "http://127.0.0.1:1" }),
      createConnectWebSocketDraft3Transport({
        baseUrl: `http://127.0.0.1:${port}`,
      }),
    );
  });

  after(
    () =>
      new Promise<void>((resolve, reject) => {
        transport.close();
        server.close((err) => (err ? reject(err) : resolve()));
      }),
  );

  it("degrades past an unreachable rung and sticks to the working one", async () => {
    const client = createClient(ElizaService, transport);

    const first: string[] = [];
    for await (const res of client.introduce({ name: "ladder" })) {
      first.push(res.sentence);
    }
    assert.ok(first[0]?.includes("ladder"));

    // A second RPC goes straight to the remembered rung: still exactly one
    // upgrade on the working server (the shared connection is reused, and
    // the dead rung is not retried).
    const second: string[] = [];
    for await (const res of client.introduce({ name: "again" })) {
      second.push(res.sentence);
    }
    assert.ok(second[0]?.includes("again"));
    assert.strictEqual(upgrades, 1);
  });
});
