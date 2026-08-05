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
// Interop fixture for the Go connectwebsocket client. Serves ElizaService
// over a plain ws:// WebSocket (no TLS) using @sudorandom/connect-bidi-node.
// Prints "READY <url>" on stdout once listening.
//
// Run with: npx tsx interop/server.ts [port]
// Or:       npm run interop:eliza -w packages/node -- [port]
// Port defaults to 8080; override with the PORT env var or a CLI argument.
//

import * as http from "node:http";
import type { ServiceImpl } from "@connectrpc/connect";
import { createConnectRouter } from "@connectrpc/connect";
import {
  createBidiWebSocketHandler,
  defaultBidiWebSocketPath,
} from "../src/index.js";
import { ElizaService } from "../src/gen/connectbidi/eliza/v1/eliza_pb.js";

const elizaImpl: ServiceImpl<typeof ElizaService> = {
  say: (req) => ({ sentence: `Eliza says: ${req.sentence}` }),
  converse: async function* (reqs) {
    for await (const req of reqs) {
      yield { sentence: `Eliza hears: ${req.sentence}` };
    }
  },
  introduce: async function* (req) {
    yield { sentence: `Hello, ${req.name}. I am Eliza.` };
    yield { sentence: "How are you feeling today? I am always listening." };
  },
};

const router = createConnectRouter();
router.service(ElizaService, elizaImpl);

const server = http.createServer((_req, res) => {
  res.writeHead(404);
  res.end();
});
createBidiWebSocketHandler(router).upgrade(server);

const port = Number(process.env.PORT ?? process.argv[2] ?? 8080);
// Bind on all interfaces: this fixture is meant to be driven by a client
// process that may not be on localhost in every environment it runs in.
server.listen(port, "0.0.0.0", () => {
  // With PORT=0 the OS assigns the port; report the actual one.
  const address = server.address();
  const actualPort =
    typeof address === "object" && address !== null ? address.port : port;
  console.log(`READY ws://localhost:${actualPort}${defaultBidiWebSocketPath}`);
});
