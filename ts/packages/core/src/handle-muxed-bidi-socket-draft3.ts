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
import { deflateRaw, inflateRaw } from "./deflate.js";
import { handleBidiSocket } from "./handle-bidi-socket.js";
import type {
  DuplexMessageStream,
  HandleMuxedBidiSocketOptions,
} from "./muxed-bidi-socket.js";
import { flagEnvelopeData, flagEnvelopeHeaders } from "./wire.js";
import type { Draft3StreamFrame } from "./wire-draft3.js";
import {
  decodeDraft3StreamFrame,
  draft3CompressMinBytes,
  draft3FrameTypeData,
  draft3FrameTypeEndStream,
  draft3FrameTypeHeaders,
  draft3FrameTypeReset,
  encodeDraft3StreamFrame,
} from "./wire-draft3.js";

export interface HandleMuxedBidiSocketDraft3Options
  extends HandleMuxedBidiSocketOptions {
  /**
   * Whether the connection's handshake negotiated the
   * `connect.bidi.d3.deflate` subprotocol. The adapter that accepted the
   * WebSocket knows the selection and must pass it here; it applies to
   * both directions for the connection's lifetime.
   */
  compression: boolean;
}

interface StreamEntry {
  controller: ReadableStreamDefaultController<Uint8Array>;
  abort: AbortController;
  reset: boolean;
}

/** Sentinel resolved by the read-with-timeout race when the peer is quiet. */
const idle = Symbol("idle");

const envelopeHeadLength = 5;

function envelopeFlagForFrameType(type: number): number | undefined {
  switch (type) {
    case draft3FrameTypeData:
      return flagEnvelopeData;
    case draft3FrameTypeHeaders:
      return flagEnvelopeHeaders;
    case draft3FrameTypeEndStream:
      return endStreamFlag;
    default:
      return undefined;
  }
}

function frameTypeForEnvelopeFlag(flag: number): number {
  switch (flag) {
    case flagEnvelopeData:
      return draft3FrameTypeData;
    case flagEnvelopeHeaders:
      return draft3FrameTypeHeaders;
    case endStreamFlag:
      return draft3FrameTypeEndStream;
    case compressedFlag:
      throw new ConnectError(
        "draft 3 has no per-message Connect compression",
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
 * Bridges a multiplexed bidi connection (a WebSocket) speaking draft 3 of
 * the wire protocol to UniversalHandlers from `@connectrpc/connect`.
 * Draft 3 is a packed binary head plus protocol-level compression: the
 * handshake negotiates it by subprotocol (see `options.compression`), and
 * each frame's descriptor bit 7 marks a raw-DEFLATE payload. The draft 1
 * and 2 equivalents are `handleMuxedBidiSocketDraft1` and
 * `handleMuxedBidiSocketDraft1` and `handleMuxedBidiSocketDraft4`; the wire
 * protocols are incompatible, so
 * a connection must be served by the matching bridge.
 *
 * The returned promise settles once the connection's read side has ended
 * and every RPC started on it has finished.
 */
export async function handleMuxedBidiSocketDraft3(
  socket: DuplexMessageStream,
  handlers: readonly UniversalHandler[],
  options: HandleMuxedBidiSocketDraft3Options,
): Promise<void> {
  const reader = socket.readable.getReader();
  const writer = socket.writable.getWriter();
  const streams = new Map<number, StreamEntry>();
  const running = new Set<Promise<void>>();
  const idleTimeoutMs = options.idleTimeoutMs;
  const compression = options.compression;
  let idledOut = false;

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

  // Compresses eligible outgoing payloads: at or above the threshold and
  // only when deflate actually shrinks them (the sender's choice draft 3
  // grants).
  async function wireForm(
    payload: Uint8Array,
  ): Promise<{ compressed: boolean; payload: Uint8Array }> {
    if (!compression || payload.byteLength < draft3CompressMinBytes) {
      return { compressed: false, payload };
    }
    const deflated = await deflateRaw(payload);
    if (deflated.byteLength >= payload.byteLength) {
      return { compressed: false, payload };
    }
    return { compressed: true, payload: deflated };
  }

  function startStream(streamId: number, headersPayload: Uint8Array): void {
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
    // Each write is one internal envelope; translate it into a draft 3
    // frame — compressing the payload when eligible — and send it as one
    // message. WritableStream serializes writes, so async compression
    // cannot reorder frames.
    const writable = new WritableStream<Uint8Array>({
      write: async (chunk) => {
        if (entry.reset) {
          throw new ConnectError("stream reset by client", Code.Canceled);
        }
        if (chunk.byteLength < envelopeHeadLength) {
          throw new ConnectError(
            `envelope too short: ${chunk.byteLength} bytes`,
            Code.Internal,
          );
        }
        const view = new DataView(
          chunk.buffer,
          chunk.byteOffset,
          chunk.byteLength,
        );
        const declared = view.getUint32(1);
        if (declared !== chunk.byteLength - envelopeHeadLength) {
          throw new ConnectError(
            `expected exactly one envelope per write, got ${declared} declared payload bytes in a ${chunk.byteLength}-byte chunk`,
            Code.Internal,
          );
        }
        const type = frameTypeForEnvelopeFlag(view.getUint8(0));
        const wire = await wireForm(chunk.subarray(envelopeHeadLength));
        await writer.write(
          encodeDraft3StreamFrame(
            streamId,
            type,
            wire.compressed,
            wire.payload,
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
      { signal: entry.abort.signal, contextValues: options.contextValues },
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

  if (options.signal !== undefined) {
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
        socket.close?.(1000, "idle timeout");
        return;
      }
      if (result.done) {
        return;
      }
      let frame: Draft3StreamFrame;
      try {
        frame = decodeDraft3StreamFrame(result.value);
      } catch {
        // Malformed frame: framing has been lost; stop serving the
        // connection.
        return;
      }
      let payload = frame.payload;
      if (frame.compressed) {
        if (!compression) {
          // A compressed frame on an identity connection: the peer is not
          // speaking the negotiated protocol.
          return;
        }
        try {
          payload = await inflateRaw(payload);
        } catch {
          // A payload that fails to inflate poisons the connection's
          // framing trust; stop serving it.
          return;
        }
      }
      const entry = streams.get(frame.streamId);
      if (entry === undefined) {
        if (frame.type === draft3FrameTypeHeaders) {
          startStream(frame.streamId, payload);
        }
        // Anything else is a late frame for a finished stream; drop it.
        continue;
      }
      if (frame.type === draft3FrameTypeReset) {
        abortStream(
          frame.streamId,
          entry,
          new ConnectError("stream reset by client", Code.Canceled),
        );
        continue;
      }
      const flag = envelopeFlagForFrameType(frame.type);
      if (flag === undefined) {
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
      entry.controller.enqueue(encodeEnvelope(flag, payload));
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
