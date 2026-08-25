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
  decodeDraft4StreamFrame,
  draft4FrameFlagsMask,
  draft4FrameTypeData,
  draft4FrameTypeEndStream,
  draft4FrameTypeHeaders,
  draft4FrameTypeMask,
  draft4FrameTypeOf,
  draft4FrameTypeReset,
  encodeDraft4StreamFrame,
} from "./wire-draft4.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const types = [
  draft4FrameTypeData,
  draft4FrameTypeHeaders,
  draft4FrameTypeEndStream,
  draft4FrameTypeReset,
];

// These assertions mirror TestFrameTypeOf in the Go package; the two
// implementations must agree bit for bit or the wire breaks between them.
describe("draft 4 flags field", () => {
  it("partitions the byte exactly", () => {
    assert.strictEqual(
      draft4FrameTypeMask & draft4FrameFlagsMask,
      0,
      "the type and flag masks overlap",
    );
    assert.strictEqual(
      draft4FrameTypeMask | draft4FrameFlagsMask,
      0xff,
      "the masks leave gaps",
    );
  });

  it("keeps every defined type inside the type mask", () => {
    for (const type of types) {
      assert.strictEqual(
        type & draft4FrameFlagsMask,
        0,
        `frame type ${type} overflows into the flag bits`,
      );
    }
  });

  it("recovers the type with any combination of flags set", () => {
    for (const type of types) {
      for (const flag of [0x08, 0x10, 0x20, 0x40, 0x80]) {
        assert.strictEqual(
          draft4FrameTypeOf(type | flag),
          type,
          `type ${type} with flag 0x${flag.toString(16)}`,
        );
      }
      assert.strictEqual(
        draft4FrameTypeOf(type | draft4FrameFlagsMask),
        type,
        `type ${type} with every flag set`,
      );
    }
  });

  it("round-trips a flagged frame through the wire form", () => {
    const flagFuture = 0x08; // the first free bit
    const frame = encodeDraft4StreamFrame(
      7,
      draft4FrameTypeData | flagFuture,
      encoder.encode("payload"),
    );
    // The head is still three fields; the flags value is just a bigger
    // number, which is what keeps the wire readable.
    assert.strictEqual(decoder.decode(frame), "7|8|payload");

    const decoded = decodeDraft4StreamFrame(frame);
    assert.strictEqual(decoded.streamId, 7);
    assert.strictEqual(draft4FrameTypeOf(decoded.flags), draft4FrameTypeData);
    assert.notStrictEqual(
      decoded.flags & flagFuture,
      0,
      "the flag bit did not survive the round trip",
    );
    assert.strictEqual(decoder.decode(decoded.payload), "payload");
  });

  it("parses a reserved frame type rather than rejecting the frame", () => {
    // Types 4-7 are reserved: they must decode, so a consumer can report an
    // unknown type instead of a broken frame.
    const decoded = decodeDraft4StreamFrame(encoder.encode("1|5|{}"));
    assert.strictEqual(draft4FrameTypeOf(decoded.flags), 5);
  });

  it("does not escape separators in the payload", () => {
    const payload = 'a|b||c|{"pipe":"|"}';
    const frame = encodeDraft4StreamFrame(
      3,
      draft4FrameTypeData,
      encoder.encode(payload),
    );
    const decoded = decodeDraft4StreamFrame(frame);
    assert.strictEqual(decoded.streamId, 3);
    assert.strictEqual(decoder.decode(decoded.payload), payload);
  });

  it("rejects a malformed head", () => {
    for (const bad of [
      "",
      "1",
      "1|0",
      "|0|x",
      "1||x",
      "abc|0|x",
      "1|data|x",
      "-1|0|x",
      "4294967296|0|x",
      "1|256|x",
      `${"9".repeat(64)}|0|x`,
    ]) {
      assert.throws(
        () => decodeDraft4StreamFrame(encoder.encode(bad)),
        `decodeDraft4StreamFrame(${JSON.stringify(bad)}) should throw`,
      );
    }
  });
});
