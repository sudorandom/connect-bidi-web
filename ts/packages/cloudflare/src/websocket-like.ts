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

import type {
  Draft4DuplexMessageStream,
  Draft4OutgoingFrame,
  Draft5Message,
  Draft5MessageStream,
  Draft7Message,
  Draft7MessageStream,
  DuplexMessageStream,
} from "@sudorandom/connect-bidi-core";

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
 * `@sudorandom/connect-bidi-core`'s `handleMuxedBidiSocketDraft3` bridges to
 * Connect `UniversalHandler`s. Message boundaries are preserved, as the
 * muxed protocol requires: each message becomes exactly one readable
 * chunk, and each written chunk is sent as one WebSocket message.
 *
 * Message frames are converted to `Uint8Array` regardless of whether they
 * arrive as text or binary, matching `@sudorandom/connect-bidi-web`'s
 * browser transports, which always send binary frames but whose peer could
 * in principle be any WebSocket client.
 */
export function wrapWebSocket(socket: BidiWebSocketLike): DuplexMessageStream {
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
      closeQuietly(socket);
    },
  });

  const writable = new WritableStream<Uint8Array>({
    write(chunk) {
      socket.send(chunk);
    },
    close() {
      closeQuietly(socket);
    },
    abort() {
      closeQuietly(socket);
    },
  });

  return {
    readable,
    writable,
    close: (code?: number, reason?: string) => {
      closeQuietly(socket, code, reason);
    },
  };
}

/**
 * The draft 4 counterpart of `wrapWebSocket`, for
 * `handleMuxedBidiSocketDraft4`. Reading is identical -- both adapters
 * already accept text and binary messages alike -- but writing carries the
 * message type: draft 4 sends a frame as a text message whenever its
 * payload is UTF-8, which is what makes the wire readable in devtools.
 */
export function wrapDraft4WebSocket(
  socket: BidiWebSocketLike,
): Draft4DuplexMessageStream {
  const { readable, close } = wrapWebSocket(socket);
  const writable = new WritableStream<Draft4OutgoingFrame>({
    write(frame) {
      // A text message must be sent as a string; Workers infers the opcode
      // from the argument type, with no separate flag.
      socket.send(frame.text ? decoder.decode(frame.data) : frame.data);
    },
    close() {
      closeQuietly(socket);
    },
    abort() {
      closeQuietly(socket);
    },
  });
  return { readable, writable, close };
}

/**
 * The draft 5 counterpart of `wrapWebSocket`, for `handleBidiSocketDraft5`.
 * This one carries the message type in *both* directions, where the other
 * adapters discard it on the way in: in draft 5 the opcode is the
 * protocol's only framing, so a zero-length binary message is an empty
 * protobuf message while a zero-length text message is the separator.
 */
export function wrapDraft5WebSocket(
  socket: BidiWebSocketLike,
): Draft5MessageStream {
  socket.binaryType = "arraybuffer";
  // As in wrapWebSocket: every listener must become a no-op once the stream
  // has ended, because touching a closed controller throws and an exception
  // escaping a Workers listener tears the connection down.
  let ended = false;
  const readable = new ReadableStream<Draft5Message>({
    start(controller) {
      socket.addEventListener("message", (event) => {
        if (ended) {
          return;
        }
        try {
          controller.enqueue({
            text: typeof event.data === "string",
            data: toBytes(event.data),
          });
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
      closeQuietly(socket);
    },
  });

  const writable = new WritableStream<Draft5Message>({
    write(message) {
      // Workers infers the opcode from the argument type: a string is a
      // text message, bytes are a binary one.
      socket.send(message.text ? decoder.decode(message.data) : message.data);
    },
    close() {
      closeQuietly(socket);
    },
    abort() {
      closeQuietly(socket);
    },
  });

  return {
    readable,
    writable,
    close: (code?: number, reason?: string) => {
      closeQuietly(socket, code, reason);
    },
  };
}

/**
 * The draft 7 counterpart of `wrapWebSocket`, for `handleBidiSocketDraft7`.
 * Like draft 5's, it carries the frame type in both directions: in draft 7
 * the frame type names the payload encoding, text for JSON and binary for
 * Protobuf, and a receiver checks it against the negotiated codec.
 */
export function wrapDraft7WebSocket(
  socket: BidiWebSocketLike,
): Draft7MessageStream {
  socket.binaryType = "arraybuffer";
  let ended = false;
  const readable = new ReadableStream<Draft7Message>({
    start(controller) {
      socket.addEventListener("message", (event) => {
        if (ended) {
          return;
        }
        try {
          controller.enqueue({
            text: typeof event.data === "string",
            data: toBytes(event.data),
          });
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
      closeQuietly(socket);
    },
  });

  const writable = new WritableStream<Draft7Message>({
    write(message) {
      // Workers infers the frame type from the argument type: a string is
      // a text message, bytes are a binary one.
      socket.send(message.text ? decoder.decode(message.data) : message.data);
    },
    close() {
      closeQuietly(socket);
    },
    abort() {
      closeQuietly(socket);
    },
  });

  return {
    readable,
    writable,
    close: (code?: number, reason?: string) => {
      closeQuietly(socket, code, reason);
    },
  };
}

const decoder = new TextDecoder();

/**
 * Closes the socket, tolerating a socket that is already closed: Workers
 * throw on a second `close()`, and teardown legitimately reaches several
 * close paths (idle-timeout close, stream cancel, writer close).
 */
function closeQuietly(
  socket: BidiWebSocketLike,
  code?: number,
  reason?: string,
): void {
  try {
    socket.close(code ?? 1000, reason);
  } catch {
    // Already closed.
  }
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
