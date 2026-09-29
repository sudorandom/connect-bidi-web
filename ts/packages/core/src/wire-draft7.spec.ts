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
import { Code, ConnectError } from "@connectrpc/connect";
import {
  checkDraft7BodyFrame,
  decodeDraft7Message,
  decodeDraft7Metadata,
  draft7OriginIsSameHost,
  draft7ReservedHeaderReason,
  encodeDraft7Message,
  encodeDraft7Metadata,
  parseDraft7Timeout,
  selectDraft7Subprotocol,
} from "./wire-draft7.js";

const decoder = new TextDecoder();

function codeOf(fn: () => unknown): Code | undefined {
  try {
    fn();
  } catch (err) {
    return err instanceof ConnectError ? err.code : undefined;
  }
  return undefined;
}

describe("draft 7 subprotocol selection", () => {
  it("selects the first offered token it recognizes and serves", () => {
    assert.deepStrictEqual(
      selectDraft7Subprotocol(
        ["connectrpc.1+json", "connectrpc.1+proto"],
        ["proto", "json"],
      ),
      { token: "connectrpc.1+json", recognized: true },
    );
    assert.deepStrictEqual(
      selectDraft7Subprotocol(
        ["connectrpc.1+json", "connectrpc.1+proto"],
        ["proto"],
      ),
      { token: "connectrpc.1+proto", recognized: true },
    );
  });

  it("treats the base token as JSON", () => {
    assert.strictEqual(
      selectDraft7Subprotocol(["connectrpc.1"], ["json"]).token,
      "connectrpc.1",
    );
  });

  it("distinguishes unrecognized from unsupported", () => {
    assert.deepStrictEqual(selectDraft7Subprotocol(["graphql-ws"], ["json"]), {
      token: undefined,
      recognized: false,
    });
    assert.deepStrictEqual(
      selectDraft7Subprotocol(["connectrpc.1"], ["proto"]),
      {
        token: undefined,
        recognized: true,
      },
    );
  });
});

describe("draft 7 message framing", () => {
  it("puts the marker first", () => {
    const message = encodeDraft7Message(0x42, new Uint8Array([1, 2]), false);
    assert.deepStrictEqual([...message.data], [0x42, 1, 2]);
    assert.deepStrictEqual(decodeDraft7Message(message), {
      marker: 0x42,
      payload: new Uint8Array([1, 2]),
    });
  });

  it("rejects an empty message and a high-bit marker", () => {
    assert.strictEqual(
      codeOf(() =>
        decodeDraft7Message({ text: true, data: new Uint8Array(0) }),
      ),
      Code.InvalidArgument,
    );
    assert.strictEqual(
      codeOf(() =>
        decodeDraft7Message({ text: false, data: new Uint8Array([0x80, 1]) }),
      ),
      Code.InvalidArgument,
    );
  });

  it("checks the frame type against the codec", () => {
    const body = new Uint8Array([0x7b, 0x7d]);
    assert.strictEqual(
      codeOf(() =>
        checkDraft7BodyFrame({ text: true, data: body }, body, "proto"),
      ),
      Code.InvalidArgument,
    );
    assert.strictEqual(
      codeOf(() =>
        checkDraft7BodyFrame({ text: false, data: body }, body, "json"),
      ),
      Code.InvalidArgument,
    );
    // An empty text body is not a message in any JSON codec.
    assert.strictEqual(
      codeOf(() =>
        checkDraft7BodyFrame(
          { text: true, data: new Uint8Array(0) },
          new Uint8Array(0),
          "json",
        ),
      ),
      Code.InvalidArgument,
    );
    // An empty binary body is the empty Protobuf message.
    assert.strictEqual(
      codeOf(() =>
        checkDraft7BodyFrame(
          { text: false, data: new Uint8Array(0) },
          new Uint8Array(0),
          "proto",
        ),
      ),
      undefined,
    );
  });
});

describe("draft 7 metadata", () => {
  it("encodes a flat object with lower-case sorted keys and array values", () => {
    const headers = new Headers();
    headers.set("X-Zebra", "z");
    headers.set("Acme-Tenant", "t-1");
    headers.set("X-Data-Bin", "/wA=");
    assert.strictEqual(
      decoder.decode(encodeDraft7Metadata(headers)),
      '{"acme-tenant":["t-1"],"x-data-bin":["/wA"],"x-zebra":["z"]}',
    );
    assert.strictEqual(
      decoder.decode(encodeDraft7Metadata(new Headers())),
      "{}",
    );
  });

  it("decodes case-insensitively and keeps every value", () => {
    const headers = decodeDraft7Metadata(
      new TextEncoder().encode(
        '{"Acme-Tenant":["a","b"],"x-data-bin":["/wAB"]}',
      ),
    );
    assert.strictEqual(headers.get("acme-tenant"), "a, b");
    assert.strictEqual(headers.get("x-data-bin"), "/wAB");
  });

  it("rejects what the specification says a receiver must reject", () => {
    for (const payload of [
      "",
      "[]",
      '{"k":"v"}',
      '{"k":[1]}',
      '{"K":["a"],"k":["b"]}',
      '{"bad key":["a"]}',
      '{"k":["a\\rb"]}',
      '{"k-bin":["***"]}',
    ]) {
      assert.strictEqual(
        codeOf(() => decodeDraft7Metadata(new TextEncoder().encode(payload))),
        Code.InvalidArgument,
        `accepted ${JSON.stringify(payload)}`,
      );
    }
  });

  it("names the three kinds of reserved key", () => {
    assert.ok(draft7ReservedHeaderReason("cookie"));
    assert.ok(draft7ReservedHeaderReason("sec-websocket-key"));
    assert.ok(draft7ReservedHeaderReason("proxy-authorization"));
    assert.ok(draft7ReservedHeaderReason("x-forwarded-for"));
    assert.ok(draft7ReservedHeaderReason("connect-timeout-ms"));
    assert.ok(draft7ReservedHeaderReason("content-type"));
    assert.strictEqual(draft7ReservedHeaderReason("authorization"), undefined);
    assert.strictEqual(
      draft7ReservedHeaderReason("x-forwarded-for", ["x-tenant-"]),
      undefined,
    );
    assert.ok(draft7ReservedHeaderReason("x-tenant-id", ["x-tenant-"]));
  });
});

describe("draft 7 handshake helpers", () => {
  it("compares origin host to request host, scheme aside", () => {
    assert.ok(draft7OriginIsSameHost(null, "example.com"));
    assert.ok(draft7OriginIsSameHost("https://Example.com", "example.com"));
    assert.ok(
      draft7OriginIsSameHost("http://example.com:8080", "example.com:8080"),
    );
    assert.ok(!draft7OriginIsSameHost("https://evil.example", "example.com"));
    assert.ok(
      !draft7OriginIsSameHost("https://example.com", "example.com:8080"),
    );
    assert.ok(!draft7OriginIsSameHost("not a url", "example.com"));
  });

  it("parses the timeout query parameter strictly", () => {
    assert.strictEqual(parseDraft7Timeout(""), undefined);
    assert.strictEqual(parseDraft7Timeout("?connect-timeout-ms=1500"), 1500);
    for (const search of [
      "?connect-timeout-ms=soon",
      "?connect-timeout-ms=1.5",
      "?connect-timeout-ms=12345678901",
      "?connect-timeout-ms=1&connect-timeout-ms=2",
    ]) {
      assert.strictEqual(
        codeOf(() => parseDraft7Timeout(search)),
        Code.InvalidArgument,
        `accepted ${search}`,
      );
    }
  });
});
