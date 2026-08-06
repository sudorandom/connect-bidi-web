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

import { Code, ConnectError } from "@connectrpc/connect";
import type { UniversalHandler } from "@connectrpc/connect/protocol";
import type { HandleBidiSocketOptions } from "./handle-bidi-socket.js";
import { handleBidiSocket } from "./handle-bidi-socket.js";
import type { StreamFrame } from "./wire.js";
import {
  decodeStreamFrame,
  encodeStreamFrame,
  flagEnvelopeHeaders,
  flagEnvelopeReset,
} from "./wire.js";

/**
 * A full-duplex, message-oriented connection carrying any number of
 * multiplexed RPC streams -- in practice, a WebSocket. Unlike
 * DuplexByteStream, message boundaries are significant: the readable must
 * yield exactly one WebSocket message per chunk, and every chunk written to
 * the writable must be sent as one WebSocket message, because each message
 * begins with the stream ID it belongs to.
 */
export interface DuplexMessageStream {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  /**
   * Close the underlying connection. Called once the connection's read side
   * has ended and every in-flight RPC has finished.
   */
  close?: () => void;
}

interface StreamEntry {
  /** Feeds incoming envelopes to the stream's handleBidiSocket. */
  controller: ReadableStreamDefaultController<Uint8Array>;
  /** Aborts the stream's RPC (client reset or connection teardown). */
  abort: AbortController;
  /** Set on reset: suppresses further writes for this stream. */
  reset: boolean;
}

/**
 * Bridges a multiplexed bidi connection (a WebSocket) to UniversalHandlers
 * from `@connectrpc/connect`. Use `createConnectRouter(...).handlers` to
 * obtain the handlers array.
 *
 * Every message on the wire is a 4-byte big-endian stream ID followed by
 * one Connect envelope, matching `@sudorandom/connect-bidi-web`'s client
 * transports byte-for-byte. A headers envelope (flag 0x07) with an unknown
 * stream ID starts a new RPC; a reset envelope (flag 0x0F) cancels an
 * in-flight one; frames for finished streams are dropped. Any number of
 * RPCs run concurrently on one connection.
 *
 * The returned promise settles once the connection's read side has ended
 * and every RPC started on it has finished.
 */
export async function handleMuxedBidiSocket(
  socket: DuplexMessageStream,
  handlers: readonly UniversalHandler[],
  options?: HandleBidiSocketOptions,
): Promise<void> {
  const reader = socket.readable.getReader();
  const writer = socket.writable.getWriter();
  const streams = new Map<number, StreamEntry>();
  const running = new Set<Promise<void>>();

  function startStream(streamId: number, headersEnvelope: Uint8Array): void {
    // start() runs synchronously in the ReadableStream constructor, so the
    // controller is assigned before it is first used.
    let controller!: ReadableStreamDefaultController<Uint8Array>;
    const readable = new ReadableStream<Uint8Array>({
      start(readableController) {
        controller = readableController;
      },
    });
    const entry: StreamEntry = {
      controller,
      abort: new AbortController(),
      reset: false,
    };
    // Each write is one envelope; prefix it with this stream's ID and send
    // it as one message on the shared connection. Writes for a reset stream
    // fail so the handler stops streaming; handleBidiSocket treats a failed
    // write like a peer disconnect.
    const writable = new WritableStream<Uint8Array>({
      write(chunk) {
        if (entry.reset) {
          return Promise.reject(
            new ConnectError("stream reset by client", Code.Canceled),
          );
        }
        return writer.write(encodeStreamFrame(streamId, chunk));
      },
    });
    entry.controller.enqueue(headersEnvelope);
    streams.set(streamId, entry);
    const done = handleBidiSocket(
      { readable, writable, close: () => streams.delete(streamId) },
      handlers,
      { signal: entry.abort.signal, contextValues: options?.contextValues },
    );
    running.add(done);
    void done
      .catch(() => {
        // handleBidiSocket reports RPC-level failures to the client itself;
        // a rejection means the stream broke, which teardown handles.
      })
      .finally(() => {
        running.delete(done);
        streams.delete(streamId);
      });
  }

  const teardown = (reason: unknown): void => {
    for (const entry of streams.values()) {
      entry.reset = true;
      entry.abort.abort(reason);
      try {
        entry.controller.error(reason);
      } catch {
        // The stream may already be closed or errored.
      }
    }
    streams.clear();
  };

  if (options?.signal !== undefined) {
    const signal = options.signal;
    if (signal.aborted) {
      teardown(signal.reason);
    } else {
      signal.addEventListener("abort", () => teardown(signal.reason), {
        once: true,
      });
    }
  }

  try {
    for (;;) {
      let result: ReadableStreamReadResult<Uint8Array>;
      try {
        result = await reader.read();
      } catch {
        // The connection broke; teardown in finally aborts the streams.
        return;
      }
      if (result.done) {
        return;
      }
      let frame: StreamFrame;
      try {
        frame = decodeStreamFrame(result.value);
      } catch {
        // Malformed frame: the connection is unusable as a whole, since
        // framing has been lost. Stop serving it.
        return;
      }
      const entry = streams.get(frame.streamId);
      if (entry === undefined) {
        if (frame.flag === flagEnvelopeHeaders) {
          startStream(frame.streamId, frame.envelope);
        }
        // Anything else is a late frame for a finished stream; drop it.
        continue;
      }
      if (frame.flag === flagEnvelopeReset) {
        streams.delete(frame.streamId);
        entry.reset = true;
        const reason = new ConnectError(
          "stream reset by client",
          Code.Canceled,
        );
        entry.abort.abort(reason);
        try {
          entry.controller.error(reason);
        } catch {
          // The stream may already be closed or errored.
        }
        continue;
      }
      entry.controller.enqueue(frame.envelope);
    }
  } finally {
    teardown(
      new ConnectError("websocket connection closed", Code.Unavailable),
    );
    await Promise.allSettled(running);
    await reader.cancel().catch(() => {
      // Ignore: the stream may already be closed or errored.
    });
    await writer.close().catch(() => {
      // Ignore: the stream may already be closed or errored.
    });
    socket.close?.();
  }
}
