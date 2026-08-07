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
 * Envelope flag used only by the bidi wire protocol (not part of the
 * Connect-over-HTTP wire format) to mark the leading metadata frame of a
 * request or response. Must match the flag of the same name used by
 * @sudorandom/connect-bidi-web's client transports. Flags are complete
 * byte values, not bitmasks; 0x08 and up are reserved for extended flags.
 */
export const flagEnvelopeHeaders = 0x06;

/**
 * Envelope flag used only by the WebSocket wire protocol to abort a single
 * stream on a multiplexed connection, with an empty payload. Transports
 * with one stream per connection (WebTransport) simply close the stream
 * instead.
 */
export const flagEnvelopeReset = 0x07;

/**
 * The "no flags set" value for a data envelope. Not a distinct flag, just
 * named for readability at call sites.
 */
export const flagEnvelopeData = 0x00;

/**
 * Length of the prefix identifying the stream on every WebSocket message:
 * a 4-byte big-endian stream ID, followed by one Connect envelope (1 flag
 * byte, 4-byte big-endian payload length, payload).
 */
export const streamIdLength = 4;

/**
 * One WebSocket message, split into the stream it belongs to and the
 * Connect envelope it carries.
 */
export interface StreamFrame {
  streamId: number;
  /** The envelope's flag byte (the first byte after the stream ID). */
  flag: number;
  /** The complete envelope: flag, length, and payload. */
  envelope: Uint8Array;
}

/**
 * Encode one WebSocket message: the stream ID followed by one envelope.
 */
export function encodeStreamFrame(
  streamId: number,
  envelope: Uint8Array,
): Uint8Array {
  const frame = new Uint8Array(streamIdLength + envelope.byteLength);
  new DataView(frame.buffer).setUint32(0, streamId);
  frame.set(envelope, streamIdLength);
  return frame;
}

/**
 * Split one WebSocket message into stream ID and envelope. Throws on a
 * malformed frame: too short, or not exactly one complete envelope.
 */
export function decodeStreamFrame(message: Uint8Array): StreamFrame {
  const envelopeHeadLength = 5;
  if (message.byteLength < streamIdLength + envelopeHeadLength) {
    throw new Error(`frame too short: ${message.byteLength} bytes`);
  }
  const view = new DataView(
    message.buffer,
    message.byteOffset,
    message.byteLength,
  );
  const streamId = view.getUint32(0);
  const flag = view.getUint8(streamIdLength);
  const declared = view.getUint32(streamIdLength + 1);
  const actual = message.byteLength - streamIdLength - envelopeHeadLength;
  if (declared !== actual) {
    throw new Error(
      `envelope declares ${declared} payload bytes but frame carries ${actual}`,
    );
  }
  return { streamId, flag, envelope: message.subarray(streamIdLength) };
}

/**
 * Concatenate a list of byte chunks into a single Uint8Array, avoiding a
 * copy when there is only one chunk.
 */
export function concatBytes(chunks: Uint8Array[]): Uint8Array {
  if (chunks.length === 1) {
    return chunks[0];
  }
  const total = chunks.reduce((n, c) => n + c.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

/**
 * Read and concatenate every chunk of an AsyncIterable<Uint8Array>.
 */
export async function concatAsyncIterable(
  it: AsyncIterable<Uint8Array>,
): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of it) {
    chunks.push(chunk);
  }
  return concatBytes(chunks);
}
