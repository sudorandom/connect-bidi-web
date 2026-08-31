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
  Draft5Message,
  Draft5MessageStream,
} from "@sudorandom/connect-bidi-core";
import type { RawData, WebSocket } from "ws";

/**
 * Adapts a `ws` WebSocket connection to a `Draft5MessageStream` for
 * `handleBidiSocketDraft5`.
 *
 * Unlike the other drafts' adapters, this one carries the message type
 * through rather than discarding it. In draft 5 the opcode is the
 * protocol's only framing: an empty text message is the separator that ends
 * a direction's data phase, while an empty binary message is an ordinary
 * empty protobuf message. Losing the distinction here would lose the
 * protocol.
 */
export function websocketToDraft5MessageStream(
  ws: WebSocket,
): Draft5MessageStream {
  // The socket keeps firing events after the consumer cancels the stream --
  // cancelling closes the socket, which fires "close" -- and touching the
  // controller of a closed or cancelled stream throws. Every listener has to
  // become a no-op once the stream has ended either way.
  let ended = false;
  const readable = new ReadableStream<Draft5Message>({
    start(controller) {
      ws.on("message", (data: RawData, isBinary: boolean) => {
        if (ended) {
          return;
        }
        controller.enqueue({ text: !isBinary, data: toUint8Array(data) });
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

  const writable = new WritableStream<Draft5Message>({
    write(message) {
      return new Promise<void>((resolve, reject) => {
        ws.send(message.data, { binary: !message.text }, (err) => {
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
  // A Node Buffer, which is a Uint8Array subclass; `ws` hands us one per
  // message, so a zero-copy view is safe.
  return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
}
