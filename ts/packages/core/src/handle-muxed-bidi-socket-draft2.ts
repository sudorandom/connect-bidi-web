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
import { compressedFlag, encodeEnvelope } from "@connectrpc/connect/protocol";
import { endStreamFlag } from "@connectrpc/connect/protocol-connect";
import { handleBidiSocket } from "./handle-bidi-socket.js";
import type {
  DuplexMessageStream,
  HandleMuxedBidiSocketOptions,
} from "./muxed-bidi-socket.js";
import { flagEnvelopeData, flagEnvelopeHeaders } from "./wire.js";
import type { Draft2StreamFrame } from "./wire-draft2.js";
import {
  decodeDraft2StreamFrame,
  draft2FrameTypeData,
  draft2FrameTypeEndStream,
  draft2FrameTypeHeaders,
  draft2FrameTypeReset,
  encodeDraft2StreamFrame,
} from "./wire-draft2.js";

interface StreamEntry {
  /** Feeds incoming envelopes to the stream's handleBidiSocket. */
  controller: ReadableStreamDefaultController<Uint8Array>;
  /** Aborts the stream's RPC (client reset or connection teardown). */
  abort: AbortController;
  /** Set on reset: suppresses further writes for this stream. */
  reset: boolean;
}

/** Sentinel resolved by the read-with-timeout race when the peer is quiet. */
const idle = Symbol("idle");

const envelopeHeadLength = 5;

/**
 * Translate a draft 2 frame type into the flag of the internal 5-byte
 * envelope representation consumed by handleBidiSocket. Returns undefined
 * for frame types that never reach a stream (reset) or are unknown.
 */
function envelopeFlagForFrameType(type: number): number | undefined {
  switch (type) {
    case draft2FrameTypeData:
      return flagEnvelopeData;
    case draft2FrameTypeHeaders:
      return flagEnvelopeHeaders;
    case draft2FrameTypeEndStream:
      return endStreamFlag;
    default:
      return undefined;
  }
}

/**
 * Translate the flag of an internal envelope written by handleBidiSocket
 * into a draft 2 frame type. Throws on the compressed flag: draft 2 has no
 * per-message compression (permessage-deflate replaces it), so a compressed
 * envelope cannot be represented on the wire.
 */
function frameTypeForEnvelopeFlag(flag: number): number {
  switch (flag) {
    case flagEnvelopeData:
      return draft2FrameTypeData;
    case flagEnvelopeHeaders:
      return draft2FrameTypeHeaders;
    case endStreamFlag:
      return draft2FrameTypeEndStream;
    case compressedFlag:
      throw new ConnectError(
        "draft 2 has no per-message compression",
        Code.Internal,
      );
    default:
      throw new ConnectError(
        `unknown envelope flag 0x${flag.toString(16)}`,
        Code.Internal,
      );
  }
}

/**
 * Bridges a multiplexed bidi connection (a WebSocket) speaking draft 2 of
 * the wire protocol to UniversalHandlers from `@connectrpc/connect`. Use
 * `createConnectRouter(...).handlers` to obtain the handlers array. The
 * draft 1 equivalent is `handleMuxedBidiSocketDraft1`; the two wire protocols are
 * incompatible, so a connection must be served by the matching bridge.
 *
 * Every message on the wire is a 4-byte big-endian stream ID, a 1-byte
 * frame type, and the payload (see wire-draft2.ts), matching
 * `@sudorandom/connect-bidi-web`'s draft 2 client transport byte-for-byte.
 * A headers frame (0x01) with an unknown stream ID starts a new RPC; a
 * reset frame (0x03) cancels an in-flight one; frames for finished streams
 * are dropped. Any number of RPCs run concurrently on one connection.
 *
 * The returned promise settles once the connection's read side has ended
 * and every RPC started on it has finished.
 */
export async function handleMuxedBidiSocketDraft2(
  socket: DuplexMessageStream,
  handlers: readonly UniversalHandler[],
  options?: HandleMuxedBidiSocketOptions,
): Promise<void> {
  const reader = socket.readable.getReader();
  const writer = socket.writable.getWriter();
  const streams = new Map<number, StreamEntry>();
  const running = new Set<Promise<void>>();
  const idleTimeoutMs = options?.idleTimeoutMs;
  let idledOut = false;

  // Reads the next message, resolving to the idle sentinel if the peer
  // sends nothing for idleTimeoutMs. The abandoned read settles later,
  // when the finally block cancels the reader.
  function readNext(): Promise<
    ReadableStreamReadResult<Uint8Array> | typeof idle
  > {
    const read = reader.read();
    if (idleTimeoutMs === undefined) {
      return read;
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => resolve(idle), idleTimeoutMs);
      read.then(
        (result) => {
          clearTimeout(timer);
          resolve(result);
        },
        (err: unknown) => {
          clearTimeout(timer);
          reject(err);
        },
      );
    });
  }

  function startStream(streamId: number, headersPayload: Uint8Array): void {
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
    // Each write is one internal envelope; translate it into a draft 2
    // frame, prefix it with this stream's ID, and send it as one message on
    // the shared connection. Writes for a reset stream fail so the handler
    // stops streaming; handleBidiSocket treats a failed write like a peer
    // disconnect.
    const writable = new WritableStream<Uint8Array>({
      write(chunk) {
        if (entry.reset) {
          return Promise.reject(
            new ConnectError("stream reset by client", Code.Canceled),
          );
        }
        if (chunk.byteLength < envelopeHeadLength) {
          return Promise.reject(
            new ConnectError(
              `envelope too short: ${chunk.byteLength} bytes`,
              Code.Internal,
            ),
          );
        }
        const view = new DataView(
          chunk.buffer,
          chunk.byteOffset,
          chunk.byteLength,
        );
        const declared = view.getUint32(1);
        if (declared !== chunk.byteLength - envelopeHeadLength) {
          return Promise.reject(
            new ConnectError(
              `expected exactly one envelope per write, got ${declared} declared payload bytes in a ${chunk.byteLength}-byte chunk`,
              Code.Internal,
            ),
          );
        }
        let type: number;
        try {
          type = frameTypeForEnvelopeFlag(view.getUint8(0));
        } catch (err) {
          return Promise.reject(err);
        }
        return writer.write(
          encodeDraft2StreamFrame(
            streamId,
            type,
            chunk.subarray(envelopeHeadLength),
          ),
        );
      },
    });
    entry.controller.enqueue(
      encodeEnvelope(flagEnvelopeHeaders, headersPayload),
    );
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

  // abortStream tears one stream down with reason, leaving the rest of
  // the connection running.
  function abortStream(
    streamId: number,
    entry: StreamEntry,
    reason: ConnectError,
  ): void {
    streams.delete(streamId);
    entry.reset = true;
    entry.abort.abort(reason);
    try {
      entry.controller.error(reason);
    } catch {
      // The stream may already be closed or errored.
    }
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
      let result: ReadableStreamReadResult<Uint8Array> | typeof idle;
      try {
        result = await readNext();
      } catch {
        // The connection broke; teardown in finally aborts the streams.
        return;
      }
      if (result === idle) {
        idledOut = true;
        // Close with an explicit reason before the generic cleanup below
        // races to close the socket without one, so the peer can tell an
        // expected idle disconnect from a failure.
        socket.close?.(1000, "idle timeout");
        return;
      }
      if (result.done) {
        return;
      }
      let frame: Draft2StreamFrame;
      try {
        frame = decodeDraft2StreamFrame(result.value);
      } catch {
        // Malformed frame: the connection is unusable as a whole, since
        // framing has been lost. Stop serving it.
        return;
      }
      const entry = streams.get(frame.streamId);
      if (entry === undefined) {
        if (frame.type === draft2FrameTypeHeaders) {
          startStream(frame.streamId, frame.payload);
        }
        // Anything else is a late frame for a finished stream; drop it.
        continue;
      }
      if (frame.type === draft2FrameTypeReset) {
        abortStream(
          frame.streamId,
          entry,
          new ConnectError("stream reset by client", Code.Canceled),
        );
        continue;
      }
      const flag = envelopeFlagForFrameType(frame.type);
      if (flag === undefined) {
        // An unknown frame type is a protocol error for this stream; the
        // rest of the connection is still well-framed.
        abortStream(
          frame.streamId,
          entry,
          new ConnectError(
            `unknown frame type 0x${frame.type.toString(16)}`,
            Code.Internal,
          ),
        );
        continue;
      }
      entry.controller.enqueue(encodeEnvelope(flag, frame.payload));
    }
  } finally {
    teardown(
      new ConnectError(
        idledOut
          ? "connection closed after idle timeout"
          : "websocket connection closed",
        Code.Unavailable,
      ),
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
