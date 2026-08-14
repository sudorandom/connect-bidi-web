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
 * Draft 3 of the WebSocket wire protocol: draft 2's framing with
 * compression as a protocol option. Every message is
 *
 *     [4-byte big-endian stream ID][1-byte descriptor][payload]
 *
 * where the descriptor carries the frame type in bits 0-6 and the
 * compressed flag in bit 7. Compression (per-frame raw DEFLATE, no shared
 * window) is negotiated once per connection through the WebSocket
 * subprotocols below. Must match the Go connectwebsocket/draft3 package
 * and @sudorandom/connect-bidi-web's draft 3 client transport.
 */

/** Subprotocol enabling per-frame raw DEFLATE. */
export const draft3SubprotocolDeflate = "connect.bidi.d3.deflate";

/** Subprotocol carrying every frame uncompressed. */
export const draft3SubprotocolIdentity = "connect.bidi.d3";

export const draft3FrameTypeData = 0x00;
export const draft3FrameTypeHeaders = 0x01;
export const draft3FrameTypeEndStream = 0x02;
export const draft3FrameTypeReset = 0x03;

/** Bit 7 of the descriptor: the payload is one raw DEFLATE stream. */
export const draft3FrameCompressed = 0x80;

/**
 * Sender-side threshold below which payloads stay uncompressed; matching
 * the Go implementation and the benchmark finding that compressing tiny
 * messages costs both size and time.
 */
export const draft3CompressMinBytes = 512;

const streamIdLength = 4;
const frameHeadLength = streamIdLength + 1;

/**
 * One draft 3 WebSocket message, split into the stream it belongs to, the
 * descriptor, and the (possibly still compressed) payload.
 */
export interface Draft3StreamFrame {
  streamId: number;
  /** Frame type, bits 0-6 of the descriptor. */
  type: number;
  /** Whether the payload is one raw DEFLATE stream (descriptor bit 7). */
  compressed: boolean;
  payload: Uint8Array;
}

/**
 * Encode one draft 3 WebSocket message. The payload must already be in
 * its wire form (compressed or not, matching `compressed`).
 */
export function encodeDraft3StreamFrame(
  streamId: number,
  type: number,
  compressed: boolean,
  payload: Uint8Array,
): Uint8Array {
  const frame = new Uint8Array(frameHeadLength + payload.byteLength);
  new DataView(frame.buffer).setUint32(0, streamId);
  frame[streamIdLength] = compressed ? type | draft3FrameCompressed : type;
  frame.set(payload, frameHeadLength);
  return frame;
}

/**
 * Split one draft 3 WebSocket message. Throws on a malformed frame
 * (shorter than the 5-byte head). The payload is returned as-is; callers
 * inflate it when `compressed` is set.
 */
export function decodeDraft3StreamFrame(
  message: Uint8Array,
): Draft3StreamFrame {
  if (message.byteLength < frameHeadLength) {
    throw new Error(`frame too short: ${message.byteLength} bytes`);
  }
  const view = new DataView(
    message.buffer,
    message.byteOffset,
    message.byteLength,
  );
  const descriptor = view.getUint8(streamIdLength);
  return {
    streamId: view.getUint32(0),
    type: descriptor & ~draft3FrameCompressed & 0xff,
    compressed: (descriptor & draft3FrameCompressed) !== 0,
    payload: message.subarray(frameHeadLength),
  };
}
