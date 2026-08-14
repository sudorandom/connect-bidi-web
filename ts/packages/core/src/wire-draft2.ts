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
 * Draft 2 of the WebSocket wire protocol. Where draft 1 (wire.ts) follows
 * each stream ID with a standard 5-byte Connect envelope, draft 2 delegates
 * the envelope's two jobs to the WebSocket itself — permessage-deflate for
 * compression, message boundaries for length — so a frame is just:
 *
 *     [4-byte big-endian stream ID][1-byte frame type][payload]
 *
 * The payload is the remainder of the WebSocket message and may be empty.
 * Frame types are complete byte values, not bitmasks; 0x04 and up are
 * reserved. Must match the values used by @sudorandom/connect-bidi-web's
 * draft 2 client transport and the Go connectwebsocket/draft2 package.
 */

/** Carries one RPC message encoded with the selected codec. */
export const draft2FrameTypeData = 0x00;

/** Marks the leading metadata frame of a request or response. */
export const draft2FrameTypeHeaders = 0x01;

/**
 * Half-closes a stream: empty on requests, the Connect EndStreamResponse
 * JSON on responses.
 */
export const draft2FrameTypeEndStream = 0x02;

/** Aborts a single stream on a multiplexed connection, with an empty payload. */
export const draft2FrameTypeReset = 0x03;

const streamIdLength = 4;
const frameHeadLength = streamIdLength + 1;

/**
 * One draft 2 WebSocket message, split into the stream it belongs to, the
 * frame type, and the payload.
 */
export interface Draft2StreamFrame {
  streamId: number;
  type: number;
  payload: Uint8Array;
}

/**
 * Encode one draft 2 WebSocket message.
 */
export function encodeDraft2StreamFrame(
  streamId: number,
  type: number,
  payload: Uint8Array,
): Uint8Array {
  const frame = new Uint8Array(frameHeadLength + payload.byteLength);
  new DataView(frame.buffer).setUint32(0, streamId);
  frame[streamIdLength] = type;
  frame.set(payload, frameHeadLength);
  return frame;
}

/**
 * Split one draft 2 WebSocket message into stream ID, frame type, and
 * payload. Throws on a malformed frame (shorter than the 5-byte head).
 */
export function decodeDraft2StreamFrame(
  message: Uint8Array,
): Draft2StreamFrame {
  if (message.byteLength < frameHeadLength) {
    throw new Error(`frame too short: ${message.byteLength} bytes`);
  }
  const view = new DataView(
    message.buffer,
    message.byteOffset,
    message.byteLength,
  );
  return {
    streamId: view.getUint32(0),
    type: view.getUint8(streamIdLength),
    payload: message.subarray(frameHeadLength),
  };
}
