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

// The Cloudflare Workers demo serves the Eliza demo service two ways at once:
// standard Connect protocol on the fetch handler (unary + server streaming),
// and full bidi streaming over WebSocket via
// @sudorandom/connect-bidi-cloudflare. The static demo site is served by
// Workers Assets (see wrangler.jsonc); only non-asset requests reach this
// code.

import type { ServiceImpl } from "@connectrpc/connect";
import { createConnectRouter } from "@connectrpc/connect";
import { createFetchHandler } from "@connectrpc/connect/protocol";
import { createBidiWebSocketHandler } from "@sudorandom/connect-bidi-cloudflare";
import { ElizaService } from "./gen/connectbidi/eliza/v1/eliza_pb.js";

const elizaImpl: ServiceImpl<typeof ElizaService> = {
  say: (req) => ({
    sentence: "Workers Eliza says: " + req.sentence,
  }),
  converse: async function* (reqs) {
    for await (const req of reqs) {
      yield { sentence: "Workers Eliza says: " + req.sentence };
    }
  },
  introduce: async function* (req) {
    yield {
      sentence: "Hi " + req.name + ", I'm Eliza on Cloudflare Workers!",
    };
    yield {
      sentence:
        "Unary and server-streaming RPCs run over plain Connect HTTP; " +
        "bidi streams run over WebSocket.",
    };
    yield { sentence: "How can I help you today?" };
  },
};

const router = createConnectRouter();
router.service(ElizaService, elizaImpl);

// Workers terminate HTTP/2+ at the edge; advertise HTTP/2 so streaming
// handlers are not rejected as running over HTTP/1.1.
const rpcHandlers = new Map(
  router.handlers.map((handler) => [
    handler.requestPath,
    createFetchHandler(handler, { httpVersion: "2" }),
  ]),
);

const handleWebSocketUpgrade = createBidiWebSocketHandler(router.handlers, {
  // Cost control: a connection that sends nothing for a minute is torn
  // down instead of pinning this invocation until the platform reaps it
  // (which shows up as a "hung request" error). The demo client re-dials
  // transparently on the next message.
  idleTimeoutMs: 60_000,
  onError: (error) => console.error("bidi websocket error:", error),
});

// WebTransport is not available on Cloudflare Workers; the demo UI probes
// this endpoint and offers WebSocket only.
const capabilities = JSON.stringify({ webtransport: false });

// Same permissive CORS as the Go demo server, so the demo site on one origin
// can target a server on another.
const corsHeaders = {
  "access-control-allow-origin": "*",
  "access-control-allow-headers": "*",
  "access-control-allow-methods": "*",
};

function withCors(res: Response): Response {
  const headers = new Headers(res.headers);
  for (const [key, value] of Object.entries(corsHeaders)) {
    headers.set(key, value);
  }
  return new Response(res.body, {
    status: res.status,
    statusText: res.statusText,
    headers,
  });
}

export default {
  async fetch(request: Request): Promise<Response> {
    // Bidi streaming: one WebSocket connection, RPCs multiplexed by stream ID.
    const upgraded = handleWebSocketUpgrade(request);
    if (upgraded !== null) {
      return upgraded;
    }

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    const url = new URL(request.url);
    if (url.pathname === "/capabilities.json") {
      return withCors(
        new Response(capabilities, {
          headers: { "content-type": "application/json" },
        }),
      );
    }

    // Unary and server-streaming RPCs over standard Connect HTTP.
    const rpcHandler = rpcHandlers.get(url.pathname);
    if (rpcHandler !== undefined) {
      return withCors(await rpcHandler(request));
    }

    return new Response("not found", { status: 404 });
  },
};
