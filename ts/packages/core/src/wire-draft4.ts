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

/**
 * Draft 4 of the WebSocket wire protocol: an ASCII text frame head instead
 * of packed binary, so the wire is readable in a browser's network
 * inspector without a decoder. Every message is
 *
 *     <stream ID> "|" <flags> "|" <payload>
 *
 * with both fields in unpadded decimal ASCII, e.g. `7|1|{"metadata":...}`.
 * Parsers split on the first two "|" bytes only, so the payload is never
 * escaped: a "|" inside JSON or protobuf bytes is just a payload byte.
 * Compression is left to the WebSocket's permessage-deflate extension, as
 * there is no compression flag. Must match the Go
 * connectwebsocket/draft4 package and @sudorandom/connect-bidi-web's draft
 * 4 client transport.
 */

/**
 * The frame head's second field is an 8-bit "flags" value in decimal ASCII,
 * partitioned so it can be read either as a small enum or with bit math:
 *
 * ```text
 *  7   6   5   4   3   2   1   0
 * +---+---+---+---+---+---+---+---+
 * |      flags        |   type    |
 * +---+---+---+---+---+---+---+---+
 * ```
 *
 * The low 3 bits are the frame type; the high 5 bits are independent flags,
 * none defined yet. So today's frames read as the plain numbers 0-3, and a
 * later revision can OR a flag in without moving anything or breaking the
 * wire. Must match the Go connectwebsocket/draft4 package.
 */
/** Selects the frame type from the flags field. */
export const draft4FrameTypeMask = 0x07;
/** Selects the (currently all reserved) flag bits from the flags field. */
export const draft4FrameFlagsMask = 0xf8;

/** Frame type carrying one RPC message encoded with the selected codec. */
export const draft4FrameTypeData = 0;
/** Frame type marking the leading metadata frame; payload is always JSON. */
export const draft4FrameTypeHeaders = 1;
/** Frame type half-closing a stream. */
export const draft4FrameTypeEndStream = 2;
/** Frame type aborting a single stream, with an empty payload. */
export const draft4FrameTypeReset = 3;

/**
 * Extracts the frame type from a flags field, discarding the flag bits.
 *
 * Discarding is deliberate: unknown flags are *ignored*, not rejected, so a
 * later revision can define one and still be understood by peers built
 * against this one — the same forward-compatibility rule HTTP/2 uses for
 * its frame flags. An unknown frame *type* (4-7) is a different matter and
 * is rejected, because its payload semantics would be anyone's guess.
 */
export function draft4FrameTypeOf(flags: number): number {
  return flags & draft4FrameTypeMask;
}

/** The field separator, "|". */
const separator = 0x7c;

/**
 * How far a parser scans for the two separators: ten digits of uint32
 * stream ID, three digits of uint8 flags, and the two separators. Bounding
 * the scan keeps a hostile peer from making us search a whole large message
 * for a separator that isn't there. The window fits the whole uint8 flags
 * range, not just the four types defined today, so a reserved type parses
 * and is reported as an unknown frame type rather than a broken frame.
 */
const maxHeadLength = 10 + 1 + 3 + 1;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * One draft 4 WebSocket message, split into the stream it belongs to, the
 * flags field, and the payload.
 */
export interface Draft4StreamFrame {
  streamId: number;
  /**
   * The flags field, whole: apply `draft4FrameTypeOf` for the frame type.
   * It is kept unmasked so a flag defined by a later revision reaches the
   * consumer rather than being dropped at the parser.
   */
  flags: number;
  payload: Uint8Array;
}

/**
 * A draft 4 frame ready to send, and how to send it. A text WebSocket
 * message is what makes the frame render as text in devtools and packet
 * captures; `text` may only be set when `data` is valid UTF-8.
 */
export interface Draft4OutgoingFrame {
  readonly data: Uint8Array;
  readonly text: boolean;
}

/**
 * A full-duplex, message-oriented connection carrying multiplexed draft 4
 * streams. Like `DuplexMessageStream`, but the writable takes the message
 * type alongside the bytes, and the readable yields the bytes of text and
 * binary messages alike: draft 4 treats the opcode as a legibility hint,
 * not protocol data, so receivers accept either.
 */
export interface Draft4DuplexMessageStream {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Draft4OutgoingFrame>;
  /**
   * Close the underlying connection, optionally with a WebSocket close
   * code and reason the peer can surface to its callers.
   */
  close?: (code?: number, reason?: string) => void;
}

/**
 * Encode one draft 4 WebSocket message: the ASCII head followed by the
 * payload.
 */
export function encodeDraft4StreamFrame(
  streamId: number,
  flags: number,
  payload: Uint8Array,
): Uint8Array {
  const head = encoder.encode(`${streamId}|${flags}|`);
  const frame = new Uint8Array(head.byteLength + payload.byteLength);
  frame.set(head, 0);
  frame.set(payload, head.byteLength);
  return frame;
}

/**
 * Split one draft 4 WebSocket message. Throws on a malformed head: missing
 * separators, non-numeric fields, or values out of range. The payload is
 * returned as a subarray of `message`, not a copy.
 */
export function decodeDraft4StreamFrame(
  message: Uint8Array,
): Draft4StreamFrame {
  const head = message.subarray(0, Math.min(message.byteLength, maxHeadLength));
  const firstSep = head.indexOf(separator);
  if (firstSep < 0) {
    throw new Error(
      `frame head has no "|" separator in its first ${head.byteLength} bytes`,
    );
  }
  const secondSepInRest = head.subarray(firstSep + 1).indexOf(separator);
  if (secondSepInRest < 0) {
    throw new Error(
      `frame head has only one "|" separator in its first ${head.byteLength} bytes`,
    );
  }
  const secondSep = firstSep + 1 + secondSepInRest;

  const streamId = parseField(
    head.subarray(0, firstSep),
    0xffffffff,
    "stream ID",
  );
  const flags = parseField(
    head.subarray(firstSep + 1, secondSep),
    0xff,
    "flags",
  );
  return { streamId, flags, payload: message.subarray(secondSep + 1) };
}

/**
 * Parse one unpadded decimal ASCII field, rejecting anything Number()
 * would otherwise accept: an empty field, a sign, whitespace, exponents,
 * and values above the field's range.
 */
function parseField(field: Uint8Array, max: number, name: string): number {
  if (field.byteLength === 0) {
    throw new Error(`empty ${name} field`);
  }
  let value = 0;
  for (const byte of field) {
    if (byte < 0x30 || byte > 0x39) {
      throw new Error(
        `invalid ${name} ${JSON.stringify(decoder.decode(field))}`,
      );
    }
    value = value * 10 + (byte - 0x30);
    if (value > max) {
      throw new Error(
        `${name} ${JSON.stringify(decoder.decode(field))} exceeds its ${max} maximum`,
      );
    }
  }
  return value;
}
