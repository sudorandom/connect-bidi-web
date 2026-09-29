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
import {
  decodeDraft1Envelope,
  draft1EnvelopeHeadLength,
  draft1ProcedureFromPath,
  encodeDraft1Envelope,
} from "./wire-draft1.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

// These assertions mirror the Go package's wire tests; the two
// implementations must agree byte for byte or the wire breaks between them.
describe("draft 1 envelope", () => {
  it("puts the flag first and the length in big-endian", () => {
    const message = encodeDraft1Envelope(0x00, encoder.encode("hello"));
    assert.deepStrictEqual(
      Array.from(message),
      [0x00, 0x00, 0x00, 0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f],
    );
  });

  it("round-trips every frame kind, empty payloads included", () => {
    for (const [flag, payload] of [
      [0x00, ""],
      [0x00, "message"],
      [0x02, ""],
      [0x02, '{"metadata":{}}'],
      [0x06, '{"metadata":{"content-type":["application/connect+proto"]}}'],
    ] as [number, string][]) {
      const message = encodeDraft1Envelope(flag, encoder.encode(payload));
      const decoded = decodeDraft1Envelope(message);
      assert.strictEqual(decoded.flag, flag);
      assert.strictEqual(decoder.decode(decoded.payload), payload);
    }
  });

  it("rejects a message that is not exactly one envelope", () => {
    // Too short to hold a head.
    assert.throws(() => decodeDraft1Envelope(new Uint8Array(0)));
    assert.throws(() =>
      decodeDraft1Envelope(new Uint8Array(draft1EnvelopeHeadLength - 1)),
    );
    // Declares more payload than the message carries.
    assert.throws(() =>
      decodeDraft1Envelope(
        new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x09, 0x61]),
      ),
    );
    // Declares less: the message carries a second envelope's worth of
    // bytes, which draft 1 never allows.
    assert.throws(() =>
      decodeDraft1Envelope(
        new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x01, 0x61, 0x62]),
      ),
    );
  });

  it("decodes a view into a larger buffer", () => {
    // subarray() keeps the original buffer, so decoding must respect
    // byteOffset rather than reading from the start of the buffer.
    const backing = new Uint8Array(16);
    backing.set(encodeDraft1Envelope(0x06, encoder.encode("hi")), 4);
    const decoded = decodeDraft1Envelope(backing.subarray(4, 4 + 5 + 2));
    assert.strictEqual(decoded.flag, 0x06);
    assert.strictEqual(decoder.decode(decoded.payload), "hi");
  });
});

// Mirrors TestProcedureFromPath in the Go package.
describe("draft 1 procedure resolution", () => {
  it("takes the last two path segments", () => {
    const procedure = "/connectbidi.ping.v1.PingService/CumSum";
    for (const path of [
      procedure,
      `/websocket-draft1${procedure}`,
      `/a/b${procedure}`,
    ]) {
      assert.strictEqual(draft1ProcedureFromPath(path), procedure);
    }
  });

  it("returns an empty string for paths that name no procedure", () => {
    for (const path of ["", "/", "/OnlyOneSegment", "/Service/", "//Method"]) {
      assert.strictEqual(draft1ProcedureFromPath(path), "");
    }
  });
});
