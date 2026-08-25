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
 * Draft 1 of the WebSocket wire protocol: the draft that stays closest to
 * the Connect HTTP protocol. Every message is
 *
 *     [4-byte big-endian stream ID][one 5-byte Connect envelope][payload]
 *
 * so the payload length is stated twice — once by the envelope, once by the
 * WebSocket message — which is exactly the redundancy the later drafts
 * dropped. Compression is Connect's own, negotiated with
 * `connect-content-encoding`/`connect-accept-encoding` metadata in the
 * headers frame and signaled by the envelope's compressed flag. Must match
 * the Go connectwebsocket/draft1 package and @sudorandom/connect-bidi-web's
 * draft 1 client transport.
 *
 * The envelope flag values themselves live in ./wire.js, because the
 * WebTransport protocol shares them.
 */

/**
 * Length of the prefix identifying the stream on every WebSocket message:
 * a 4-byte big-endian stream ID, followed by one Connect envelope (1 flag
 * byte, 4-byte big-endian payload length, payload).
 */
export const draft1StreamIdLength = 4;

/**
 * One draft 1 WebSocket message, split into the stream it belongs to and
 * the Connect envelope it carries.
 */
export interface Draft1StreamFrame {
  streamId: number;
  /** The envelope's flag byte (the first byte after the stream ID). */
  flag: number;
  /** The complete envelope: flag, length, and payload. */
  envelope: Uint8Array;
}

/**
 * Encode one draft 1 WebSocket message: the stream ID followed by one
 * envelope.
 */
export function encodeDraft1StreamFrame(
  streamId: number,
  envelope: Uint8Array,
): Uint8Array {
  const frame = new Uint8Array(draft1StreamIdLength + envelope.byteLength);
  new DataView(frame.buffer).setUint32(0, streamId);
  frame.set(envelope, draft1StreamIdLength);
  return frame;
}

/**
 * Split one draft 1 WebSocket message into stream ID and envelope. Throws
 * on a malformed frame: too short, or not exactly one complete envelope.
 */
export function decodeDraft1StreamFrame(
  message: Uint8Array,
): Draft1StreamFrame {
  const envelopeHeadLength = 5;
  if (message.byteLength < draft1StreamIdLength + envelopeHeadLength) {
    throw new Error(`frame too short: ${message.byteLength} bytes`);
  }
  const view = new DataView(
    message.buffer,
    message.byteOffset,
    message.byteLength,
  );
  const streamId = view.getUint32(0);
  const flag = view.getUint8(draft1StreamIdLength);
  const declared = view.getUint32(draft1StreamIdLength + 1);
  const actual = message.byteLength - draft1StreamIdLength - envelopeHeadLength;
  if (declared !== actual) {
    throw new Error(
      `envelope declares ${declared} payload bytes but frame carries ${actual}`,
    );
  }
  return { streamId, flag, envelope: message.subarray(draft1StreamIdLength) };
}
