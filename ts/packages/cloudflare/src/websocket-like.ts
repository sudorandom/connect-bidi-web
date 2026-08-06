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

import type { DuplexMessageStream } from "@sudorandom/connect-bidi-core";

/**
 * The subset of the Workers `WebSocket` interface that `wrapWebSocket`
 * needs. Kept narrow and independent of `@cloudflare/workers-types` so it
 * can be satisfied by a plain mock object in unit tests, while a real
 * accepted Workers `WebSocket` also satisfies it structurally.
 */
export interface BidiWebSocketLike {
  /**
   * Per the WebSocket spec this defaults to "blob", and workerd honors that
   * default — `wrapWebSocket` sets it to "arraybuffer" when present, since
   * the envelope parser needs bytes synchronously and cannot await
   * `Blob.arrayBuffer()`.
   */
  binaryType?: string;
  addEventListener(
    type: "message",
    listener: (event: { data: unknown }) => void,
  ): void;
  addEventListener(type: "close", listener: () => void): void;
  addEventListener(type: "error", listener: () => void): void;
  send(message: ArrayBuffer | ArrayBufferView | string): void;
  close(code?: number, reason?: string): void;
}

/**
 * Adapts a `BidiWebSocketLike` (an accepted Workers `WebSocket`, or a mock
 * of one in tests) into the `DuplexMessageStream` that
 * `@sudorandom/connect-bidi-core`'s `handleMuxedBidiSocket` bridges to
 * Connect `UniversalHandler`s. Message boundaries are preserved, as the
 * muxed protocol requires: each message becomes exactly one readable
 * chunk, and each written chunk is sent as one WebSocket message.
 *
 * Message frames are converted to `Uint8Array` regardless of whether they
 * arrive as text or binary, matching `@sudorandom/connect-bidi-web`'s
 * browser transports, which always send binary frames but whose peer could
 * in principle be any WebSocket client.
 */
export function wrapWebSocket(
  socket: BidiWebSocketLike,
): DuplexMessageStream {
  // Receive binary frames as ArrayBuffer, not the spec-default Blob.
  socket.binaryType = "arraybuffer";
  // The socket keeps firing events after the consumer cancels the stream
  // (canceling closes the socket, which fires "close"). Touching the
  // controller of a closed or canceled stream throws, and an exception
  // escaping a Workers event listener tears down the connection before
  // buffered frames are flushed to the peer — so every listener must
  // become a no-op once the stream has ended either way.
  let ended = false;
  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
      socket.addEventListener("message", (event) => {
        if (ended) {
          return;
        }
        try {
          controller.enqueue(toBytes(event.data));
        } catch (err) {
          ended = true;
          controller.error(err);
        }
      });
      socket.addEventListener("close", () => {
        if (ended) {
          return;
        }
        ended = true;
        controller.close();
      });
      socket.addEventListener("error", () => {
        if (ended) {
          return;
        }
        ended = true;
        controller.error(new Error("WebSocket error"));
      });
    },
    cancel() {
      ended = true;
      socket.close();
    },
  });

  const writable = new WritableStream<Uint8Array>({
    write(chunk) {
      socket.send(chunk);
    },
    close() {
      socket.close();
    },
    abort() {
      socket.close();
    },
  });

  return {
    readable,
    writable,
    close: () => {
      socket.close();
    },
  };
}

/**
 * Normalizes a WebSocket message event's `data` (per the WebSocket API,
 * either a `string` or an `ArrayBuffer`, though Workers' `MessageEvent.data`
 * is typed as `any`) to a `Uint8Array`.
 */
function toBytes(data: unknown): Uint8Array {
  if (typeof data === "string") {
    return new TextEncoder().encode(data);
  }
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }
  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }
  throw new Error(
    `unsupported WebSocket message data type: ${Object.prototype.toString.call(data)}`,
  );
}
