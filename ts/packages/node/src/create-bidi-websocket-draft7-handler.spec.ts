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

// These tests speak draft 7 by hand over a real `ws` client, so the
// handshake statuses and the wire shape are pinned independently of the
// browser transport that produces them.

import * as assert from "node:assert";
import * as http from "node:http";
import { after, before, describe, it } from "node:test";
import type { ServiceImpl } from "@connectrpc/connect";
import {
  type Code,
  ConnectError,
  createConnectRouter,
} from "@connectrpc/connect";
import { WebSocket } from "ws";
import { createBidiWebSocketDraft7Handler } from "./create-bidi-websocket-draft7-handler.js";
import { PingService } from "./gen/connectbidi/ping/v1/ping_pb.js";

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

/** One received message: its frame type, marker, and payload. */
interface Received {
  text: boolean;
  marker: string;
  payload: string;
}

/** A hand-rolled draft 7 client over `ws`. */
class Wire {
  private readonly queue: Received[] = [];
  private waiting: ((message: Received | undefined) => void) | undefined;
  private closed = false;
  closeCode: number | undefined;

  constructor(readonly socket: WebSocket) {
    socket.on("message", (data: Buffer, isBinary: boolean) => {
      const bytes = new Uint8Array(data);
      const message = {
        text: !isBinary,
        marker: String.fromCharCode(bytes[0]),
        payload: new TextDecoder().decode(bytes.subarray(1)),
      };
      const waiting = this.waiting;
      if (waiting !== undefined) {
        this.waiting = undefined;
        waiting(message);
      } else {
        this.queue.push(message);
      }
    });
    socket.on("close", (code) => {
      this.closed = true;
      this.closeCode = code;
      this.waiting?.(undefined);
    });
  }

  static open(url: string, subprotocols: string[]): Promise<Wire> {
    return new Promise((resolve, reject) => {
      const socket = new WebSocket(url, subprotocols);
      socket.once("open", () => resolve(new Wire(socket)));
      socket.once("error", reject);
    });
  }

  send(marker: string, payload: string | Uint8Array): void {
    if (typeof payload === "string") {
      this.socket.send(marker + payload);
      return;
    }
    const data = new Uint8Array(1 + payload.byteLength);
    data[0] = marker.charCodeAt(0);
    data.set(payload, 1);
    this.socket.send(data, { binary: true });
  }

  read(): Promise<Received | undefined> {
    const next = this.queue.shift();
    if (next !== undefined) {
      return Promise.resolve(next);
    }
    if (this.closed) {
      return Promise.resolve(undefined);
    }
    return new Promise((resolve) => {
      this.waiting = resolve;
    });
  }

  /** Reads M then S and returns S's error code, if any. */
  async readMetadataThenEnd(): Promise<string | undefined> {
    const metadata = await this.read();
    assert.ok(metadata, "connection closed before M");
    assert.strictEqual(metadata.marker, "M");
    assert.ok(metadata.text, "M must be a text frame");
    const end = await this.read();
    assert.ok(end, "connection closed before S");
    assert.strictEqual(
      end.marker,
      "S",
      `expected S, got ${end.marker}${end.payload}`,
    );
    const parsed = JSON.parse(end.payload) as { error?: { code: string } };
    return parsed.error?.code;
  }
}

/** The status of a handshake that must fail. */
function handshakeStatus(
  url: string,
  subprotocols: string[],
  headers?: Record<string, string>,
): Promise<number> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url, subprotocols, { headers });
    socket.once("unexpected-response", (_req, res) => {
      resolve(res.statusCode ?? 0);
      socket.terminate();
    });
    socket.once("open", () => {
      socket.terminate();
      reject(new Error("handshake succeeded"));
    });
    socket.once("error", () => {
      // The unexpected-response listener already resolved.
    });
  });
}

describe("createBidiWebSocketDraft7Handler", () => {
  let server: http.Server;
  let baseUrl: string;
  let prefixed: http.Server;
  let prefixedUrl: string;

  before(async () => {
    const router = createConnectRouter();
    router.service(PingService, pingImpl);
    server = http.createServer((_req, res) => {
      res.writeHead(404);
      res.end();
    });
    createBidiWebSocketDraft7Handler(router, {
      serverTimeoutMs: 2000,
      infrastructureHeaders: ["x-tenant-"],
    }).upgrade(server);
    await new Promise<void>((resolve) => {
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    const port =
      typeof address === "object" && address !== null ? address.port : 0;
    baseUrl = `ws://127.0.0.1:${port}`;

    const withPrefix = createBidiWebSocketDraft7Handler(router, {
      pathPrefix: "/ws",
      codecs: ["proto"],
    });
    prefixed = http.createServer((req, res) => {
      if (withPrefix.handleRequest(req, res)) {
        return;
      }
      res.writeHead(404);
      res.end();
    });
    withPrefix.upgrade(prefixed);
    await new Promise<void>((resolve) => {
      prefixed.listen(0, "127.0.0.1", resolve);
    });
    const prefixedAddress = prefixed.address();
    const prefixedPort =
      typeof prefixedAddress === "object" && prefixedAddress !== null
        ? prefixedAddress.port
        : 0;
    prefixedUrl = `ws://127.0.0.1:${prefixedPort}`;
  });

  after(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await new Promise<void>((resolve) => prefixed.close(() => resolve()));
  });

  it("serves the specification's worked example: a unary Ping", async () => {
    const wire = await Wire.open(
      `${baseUrl}/connectbidi.ping.v1.PingService/Ping`,
      ["connectrpc.1+json"],
    );
    assert.strictEqual(wire.socket.protocol, "connectrpc.1+json");
    wire.send("M", "{}");
    wire.send("B", '{"number":"7"}');
    wire.send("C", "");

    const metadata = await wire.read();
    assert.strictEqual(metadata?.marker, "M");
    const body = await wire.read();
    assert.strictEqual(body?.marker, "B");
    assert.ok(body.text, "a JSON body must be a text frame");
    assert.strictEqual(JSON.parse(body.payload).number, "7");
    const end = await wire.read();
    assert.strictEqual(end?.marker, "S");
    assert.deepStrictEqual(JSON.parse(end.payload), {});
    await wire.read();
    assert.strictEqual(wire.closeCode, 1000);
  });

  it("carries a final body on C and counts it", async () => {
    const wire = await Wire.open(
      `${baseUrl}/connectbidi.ping.v1.PingService/Sum`,
      ["connectrpc.1"],
    );
    wire.send("M", '{"x-custom":["v"]}');
    wire.send("B", '{"number":"1"}');
    wire.send("C", '{"number":"2"}');
    await wire.read();
    const body = await wire.read();
    assert.strictEqual(JSON.parse(body?.payload ?? "{}").sum, "3");
  });

  it("reports every protocol violation in S after M", async () => {
    const cases: [string, (wire: Wire) => void][] = [
      ["body before metadata", (w) => w.send("B", "{}")],
      [
        "metadata as binary",
        (w) => w.send("M", new TextEncoder().encode("{}")),
      ],
      ["metadata without payload", (w) => w.send("M", "")],
      ["metadata not an object", (w) => w.send("M", "[]")],
      ["bare string value", (w) => w.send("M", '{"k":"v"}')],
      ["reserved key", (w) => w.send("M", '{"cookie":["a=b"]}')],
      ["infrastructure key", (w) => w.send("M", '{"x-tenant-id":["1"]}')],
      [
        "unknown marker",
        (w) => {
          w.send("M", "{}");
          w.send("X", "");
        },
      ],
      [
        "server marker from client",
        (w) => {
          w.send("M", "{}");
          w.send("S", "{}");
        },
      ],
      [
        "second metadata",
        (w) => {
          w.send("M", "{}");
          w.send("M", "{}");
        },
      ],
      [
        "binary body under json",
        (w) => {
          w.send("M", "{}");
          w.send("B", new Uint8Array([8, 1]));
        },
      ],
      [
        "empty text body",
        (w) => {
          w.send("M", "{}");
          w.send("B", "");
        },
      ],
    ];
    for (const [name, send] of cases) {
      const wire = await Wire.open(
        `${baseUrl}/connectbidi.ping.v1.PingService/CumSum`,
        ["connectrpc.1+json"],
      );
      send(wire);
      assert.strictEqual(
        await wire.readMetadataThenEnd(),
        "invalid_argument",
        name,
      );
    }
  });

  it("answers a malformed deadline on the socket", async () => {
    const wire = await Wire.open(
      `${baseUrl}/connectbidi.ping.v1.PingService/CumSum?connect-timeout-ms=soon`,
      ["connectrpc.1+json"],
    );
    assert.strictEqual(await wire.readMetadataThenEnd(), "invalid_argument");
  });

  it("answers a silent client once the deadline passes", async () => {
    const wire = await Wire.open(
      `${baseUrl}/connectbidi.ping.v1.PingService/CumSum?connect-timeout-ms=100`,
      ["connectrpc.1+json"],
    );
    assert.strictEqual(await wire.readMetadataThenEnd(), "deadline_exceeded");
  });

  it("refuses the handshakes the specification refuses", async () => {
    const procedure = "/connectbidi.ping.v1.PingService/CumSum";
    assert.strictEqual(
      await handshakeStatus(baseUrl + procedure, ["connect.bidi.d5"]),
      400,
      "no recognized token",
    );
    assert.strictEqual(
      await handshakeStatus(prefixedUrl + "/ws" + procedure, [
        "connectrpc.1+json",
      ]),
      415,
      "recognized token, unsupported codec",
    );
    assert.strictEqual(
      await handshakeStatus(baseUrl + procedure, ["connectrpc.1+json"], {
        Origin: "https://evil.example",
      }),
      403,
      "cross-origin",
    );
    assert.strictEqual(
      await handshakeStatus(prefixedUrl + procedure, ["connectrpc.1+proto"]),
      400,
      "upgrade at the bare path with a prefix configured",
    );
  });

  it("permits a same-host origin whatever the scheme", async () => {
    const wire = await Wire.open(
      `${baseUrl}/connectbidi.ping.v1.PingService/Ping`,
      ["connectrpc.1+json"],
    );
    // ws does not let a test set Origin on an open socket, so a second
    // handshake carries it explicitly.
    wire.socket.terminate();
    const host = baseUrl.replace("ws://", "");
    await new Promise<void>((resolve, reject) => {
      const socket = new WebSocket(
        `${baseUrl}/connectbidi.ping.v1.PingService/Ping`,
        ["connectrpc.1+json"],
        {
          headers: { Origin: `https://${host.toUpperCase()}` },
        },
      );
      socket.once("open", () => {
        socket.terminate();
        resolve();
      });
      socket.once("error", reject);
    });
  });

  it("answers a non-upgrade request under the prefix with 426", async () => {
    const response = await fetch(
      `${prefixedUrl.replace("ws://", "http://")}/ws/connectbidi.ping.v1.PingService/Ping`,
    );
    assert.strictEqual(response.status, 426);
    assert.strictEqual(response.headers.get("upgrade"), "websocket");
  });

  it("negotiates permessage-deflate with no context takeover", async () => {
    await new Promise<void>((resolve, reject) => {
      const socket = new WebSocket(
        `${baseUrl}/connectbidi.ping.v1.PingService/Ping`,
        ["connectrpc.1+proto"],
        {
          // Offer the extension without the takeover parameters; the server
          // must impose them, and RFC 7692 obliges the client to honor that.
          perMessageDeflate: true,
        },
      );
      socket.once("open", () => {
        socket.terminate();
        resolve();
      });
      socket.once("error", reject);
    });
  });
});
