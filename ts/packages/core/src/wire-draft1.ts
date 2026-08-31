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
 * the Connect HTTP protocol, and the shape the later drafts are variations
 * on. One WebSocket carries one streaming RPC, and every message on it is
 * exactly one Connect envelope:
 *
 *     [1 flag byte][4-byte big-endian payload length][payload]
 *
 * The length restates what the WebSocket message boundary already says.
 * That redundancy is deliberate — the envelope is Connect's own, byte for
 * byte — and it is the one thing draft 5 dropped when it went further.
 *
 * There is no stream ID, because nothing is multiplexed, and no reset
 * frame, because cancelling is closing the socket. Compression is the
 * native permessage-deflate extension, so the envelope's compressed-data
 * flag goes unused. Must match the Go connectwebsocket/draft1 package and
 * @sudorandom/connect-bidi-web's draft 1 client transport.
 *
 * The envelope flag values themselves live in ./wire.js, because the
 * WebTransport protocol shares them.
 */

import { Code, ConnectError } from "@connectrpc/connect";

/** The WebSocket subprotocol every draft 1 handshake must offer. */
export const draft1Subprotocol = "connect.bidi.d1";

/**
 * Length of a Connect envelope head: one flag byte and a 4-byte big-endian
 * payload length.
 */
export const draft1EnvelopeHeadLength = 5;

/**
 * A message-oriented full-duplex connection carrying exactly one RPC, each
 * message being one whole Connect envelope. Draft 1 messages are always
 * binary, so unlike draft 5 there is no opcode to carry alongside them.
 */
export interface Draft1MessageStream {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  /** Close the underlying connection once the RPC has finished. */
  close?: (code?: number, reason?: string) => void;
}

/** One draft 1 message, split into the envelope's flag and payload. */
export interface Draft1Envelope {
  flag: number;
  payload: Uint8Array;
}

/**
 * Encode one draft 1 message: a Connect envelope head followed by its
 * payload.
 */
export function encodeDraft1Envelope(
  flag: number,
  payload: Uint8Array,
): Uint8Array {
  const message = new Uint8Array(draft1EnvelopeHeadLength + payload.byteLength);
  const view = new DataView(message.buffer);
  view.setUint8(0, flag);
  view.setUint32(1, payload.byteLength);
  message.set(payload, draft1EnvelopeHeadLength);
  return message;
}

/**
 * Split one draft 1 message into its flag and payload. Throws on a
 * malformed message: too short to hold an envelope head, or declaring a
 * length the message does not match. A message carries one whole envelope
 * and nothing else, so a disagreement between the declared length and the
 * message boundary means the peer framed something this protocol cannot
 * represent.
 */
export function decodeDraft1Envelope(message: Uint8Array): Draft1Envelope {
  if (message.byteLength < draft1EnvelopeHeadLength) {
    throw new ConnectError(
      `envelope too short: ${message.byteLength} bytes`,
      Code.InvalidArgument,
    );
  }
  const view = new DataView(
    message.buffer,
    message.byteOffset,
    message.byteLength,
  );
  const declared = view.getUint32(1);
  const actual = message.byteLength - draft1EnvelopeHeadLength;
  if (declared !== actual) {
    throw new ConnectError(
      `envelope declares ${declared} payload bytes but the message carries ${actual}`,
      Code.InvalidArgument,
    );
  }
  return {
    flag: view.getUint8(0),
    payload: message.subarray(draft1EnvelopeHeadLength),
  };
}

/**
 * The procedure a request URL addresses: its last two path segments,
 * `/package.Service/Method`.
 *
 * Draft 1 takes the procedure from the URL rather than from a frame, so a
 * handler must recover it from the path. Taking the last two segments
 * rather than the whole path is what lets one handler serve whether it is
 * mounted on the Connect procedure URLs themselves or under a prefix.
 * Returns an empty string when the path does not name a procedure.
 */
export function draft1ProcedureFromPath(path: string): string {
  const method = path.lastIndexOf("/");
  if (method <= 0) {
    return "";
  }
  const service = path.lastIndexOf("/", method - 1);
  if (service < 0 || method === path.length - 1 || method - service === 1) {
    return "";
  }
  return path.slice(service);
}
