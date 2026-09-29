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
import type { RawData, WebSocket } from "ws";

/**
 * Adapts a `ws` WebSocket connection to a `DuplexMessageStream` for
 * `handleMuxedBidiSocketDraft3`. Message boundaries are preserved, as the muxed
 * protocol requires: each binary message becomes exactly one readable
 * chunk, and each written chunk is sent as one binary WebSocket message.
 * Closing or erroring the socket in either direction propagates to both
 * the readable and the writable side.
 */
export function websocketToDuplexMessageStream(
  ws: WebSocket,
): DuplexMessageStream {
  // The socket keeps firing events after the consumer cancels the stream --
  // cancelling closes the socket, which fires "close" -- and touching the
  // controller of a closed or cancelled stream throws. Every listener has to
  // become a no-op once the stream has ended either way.
  let ended = false;
  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
      ws.on("message", (data: RawData, isBinary: boolean) => {
        if (ended) {
          return;
        }
        if (!isBinary) {
          ended = true;
          controller.error(
            new Error(
              "received a text WebSocket frame on a binary-only bidi connection",
            ),
          );
          return;
        }
        controller.enqueue(toUint8Array(data));
      });
      ws.on("close", () => {
        if (ended) {
          return;
        }
        ended = true;
        controller.close();
      });
      ws.on("error", (err: Error) => {
        if (ended) {
          return;
        }
        ended = true;
        controller.error(err);
      });
    },
    cancel() {
      ended = true;
      ws.close(1000);
    },
  });

  const writable = new WritableStream<Uint8Array>({
    write(chunk) {
      return new Promise<void>((resolve, reject) => {
        ws.send(chunk, { binary: true }, (err) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      });
    },
    abort() {
      ws.close(1000);
    },
  });

  return {
    readable,
    writable,
    close: (code?: number, reason?: string) => {
      ws.close(code ?? 1000, reason);
    },
  };
}

function toUint8Array(data: RawData): Uint8Array {
  if (Array.isArray(data)) {
    // Only occurs for fragmented messages delivered without reassembly;
    // `ws` reassembles by default, but concatenate defensively.
    let total = 0;
    for (const chunk of data) {
      total += chunk.byteLength;
    }
    const out = new Uint8Array(total);
    let offset = 0;
    for (const chunk of data) {
      out.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return out;
  }
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }
  // A Node Buffer, which is a Uint8Array subclass; `ws` hands us one
  // per message, so a zero-copy view is safe.
  return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
}
