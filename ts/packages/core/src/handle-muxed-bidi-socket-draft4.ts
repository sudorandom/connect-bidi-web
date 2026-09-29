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
import { decodeHeadersFrame } from "./headers-frame.js";
import type { HandleMuxedBidiSocketOptions } from "./muxed-bidi-socket.js";
import { flagEnvelopeData, flagEnvelopeHeaders } from "./wire.js";
import type {
  Draft4DuplexMessageStream,
  Draft4StreamFrame,
} from "./wire-draft4.js";
import {
  decodeDraft4StreamFrame,
  draft4FrameTypeData,
  draft4FrameTypeEndStream,
  draft4FrameTypeHeaders,
  draft4FrameTypeOf,
  draft4FrameTypeReset,
  encodeDraft4StreamFrame,
} from "./wire-draft4.js";

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
 * Translate a draft 4 frame type into the flag of the internal 5-byte
 * envelope representation consumed by handleBidiSocket. Returns undefined
 * for frame types that never reach a stream (reset) or are reserved.
 */
function envelopeFlagForFrameType(type: number): number | undefined {
  switch (type) {
    case draft4FrameTypeData:
      return flagEnvelopeData;
    case draft4FrameTypeHeaders:
      return flagEnvelopeHeaders;
    case draft4FrameTypeEndStream:
      return endStreamFlag;
    default:
      return undefined;
  }
}

/**
 * Translate the flag of an internal envelope written by handleBidiSocket
 * into a draft 4 frame type. Throws on the compressed flag: draft 4 has no
 * compression of its own (permessage-deflate replaces it), so a compressed
 * envelope cannot be represented on the wire.
 */
function frameTypeForEnvelopeFlag(flag: number): number {
  switch (flag) {
    case flagEnvelopeData:
      return draft4FrameTypeData;
    case flagEnvelopeHeaders:
      return draft4FrameTypeHeaders;
    case endStreamFlag:
      return draft4FrameTypeEndStream;
    case compressedFlag:
      throw new ConnectError(
        "draft 4 has no per-message compression",
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
 * Whether a frame of this type carries UTF-8, and so can be sent as a text
 * WebSocket message. Control payloads are always JSON in draft 4; a data
 * payload is text only when the negotiated codec is JSON.
 */
function frameIsText(type: number, codecIsJson: boolean): boolean {
  return type === draft4FrameTypeData ? codecIsJson : true;
}

/**
 * Read the request content type out of a headers frame to decide whether
 * this stream's data frames are UTF-8. A payload that doesn't parse is
 * left to the stream code to reject; assuming binary here is the safe
 * default, since receivers accept either message type.
 */
function streamCodecIsJson(headersPayload: Uint8Array): boolean {
  try {
    const { headers } = decodeHeadersFrame(headersPayload);
    return headers.get("content-type")?.includes("json") === true;
  } catch {
    return false;
  }
}

/**
 * Bridges a multiplexed bidi connection (a WebSocket) speaking draft 4 of
 * the wire protocol to UniversalHandlers from `@connectrpc/connect`. Use
 * `createConnectRouter(...).handlers` to obtain the handlers array. The
 * other bridges are `handleMuxedBidiSocketDraft3` and, for the
 * unmultiplexed draft 5, `handleBidiSocketDraft5`; the wire protocols are
 * incompatible, so a connection must be served by the bridge that speaks
 * its draft.
 *
 * Every message on the wire is an ASCII head, `<stream ID>|<flags>|`,
 * followed by the payload (see wire-draft4.ts), matching
 * `@sudorandom/connect-bidi-web`'s draft 4 client transport byte-for-byte.
 * A headers frame (type 1) with an unknown stream ID starts a new RPC; a
 * reset frame (type 3) cancels an in-flight one; frames for finished
 * streams are dropped. Any number of RPCs run concurrently on one
 * connection.
 *
 * Outgoing frames are sent as text WebSocket messages whenever their
 * payload is UTF-8 — always for control frames, and for data frames when
 * the stream's codec is JSON — so tooling renders them as text.
 *
 * The returned promise settles once the connection's read side has ended
 * and every RPC started on it has finished.
 */
export async function handleMuxedBidiSocketDraft4(
  socket: Draft4DuplexMessageStream,
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
    const codecIsJson = streamCodecIsJson(headersPayload);
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
    // Each write is one internal envelope; translate it into a draft 4
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
        return writer.write({
          data: encodeDraft4StreamFrame(
            streamId,
            type,
            chunk.subarray(envelopeHeadLength),
          ),
          text: frameIsText(type, codecIsJson),
        });
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
      let frame: Draft4StreamFrame;
      try {
        frame = decodeDraft4StreamFrame(result.value);
      } catch {
        // Malformed frame: the connection is unusable as a whole, since
        // framing has been lost. Stop serving it.
        return;
      }
      const entry = streams.get(frame.streamId);
      if (entry === undefined) {
        if (draft4FrameTypeOf(frame.flags) === draft4FrameTypeHeaders) {
          startStream(frame.streamId, frame.payload);
        }
        // Anything else is a late frame for a finished stream; drop it.
        continue;
      }
      if (draft4FrameTypeOf(frame.flags) === draft4FrameTypeReset) {
        abortStream(
          frame.streamId,
          entry,
          new ConnectError("stream reset by client", Code.Canceled),
        );
        continue;
      }
      const flag = envelopeFlagForFrameType(draft4FrameTypeOf(frame.flags));
      if (flag === undefined) {
        // A reserved frame type is a protocol error for this stream; the
        // rest of the connection is still well-framed.
        abortStream(
          frame.streamId,
          entry,
          new ConnectError(
            `unknown frame type ${draft4FrameTypeOf(frame.flags)}`,
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
