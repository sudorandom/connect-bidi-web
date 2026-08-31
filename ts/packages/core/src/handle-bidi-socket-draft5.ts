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

import type { ContextValues } from "@connectrpc/connect";
import { Code, ConnectError } from "@connectrpc/connect";
import type { UniversalHandler } from "@connectrpc/connect/protocol";
import { compressedFlag, encodeEnvelope } from "@connectrpc/connect/protocol";
import { endStreamFlag } from "@connectrpc/connect/protocol-connect";
import { handleBidiSocket } from "./handle-bidi-socket.js";
import { decodeHeadersFrame, encodeHeadersFrame } from "./headers-frame.js";
import { flagEnvelopeData, flagEnvelopeHeaders } from "./wire.js";

/** The WebSocket subprotocol every draft 5 handshake must offer. */
export const draft5Subprotocol = "connect.bidi.d5";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
/** Rejects invalid UTF-8 instead of substituting replacement characters. */
const strictDecoder = new TextDecoder("utf-8", { fatal: true });
const empty = new Uint8Array(0);

/** Whether a payload may travel as a text WebSocket message. */
function isValidUtf8(payload: Uint8Array): boolean {
  try {
    strictDecoder.decode(payload);
    return true;
  } catch {
    return false;
  }
}

/**
 * One message on a draft 5 connection, with the opcode that framed it. The
 * opcode is the protocol's only framing, so it travels with every message
 * rather than being discarded at the edge: a zero-length binary message is
 * an empty protobuf message, while a zero-length text message is the
 * separator that ends a direction's data phase.
 */
export interface Draft5Message {
  /** True when the message is text: JSON metadata, or the separator. */
  text: boolean;
  data: Uint8Array;
}

/**
 * A message-oriented full-duplex connection carrying exactly one RPC, such
 * as a WebSocket that was upgraded from a Connect procedure URL.
 */
export interface Draft5MessageStream {
  readonly readable: ReadableStream<Draft5Message>;
  readonly writable: WritableStream<Draft5Message>;
  /** Close the underlying connection once the RPC has finished. */
  close?: (code?: number, reason?: string) => void;
}

export interface HandleBidiSocketDraft5Options {
  /**
   * The procedure the upgrade request addressed — the URL's path, such as
   * `/connectrpc.eliza.v1.ElizaService/Converse`. In draft 5 the handshake
   * is the request, so the path comes from the URL and is never repeated in
   * the headers message.
   */
  path: string;
  /**
   * The headers the upgrade request carried. They are the base of the
   * request metadata; the headers message overrides them key by key, so a
   * Cookie the platform attached by itself still reaches the handler while
   * anything the Connect client set explicitly wins.
   */
  requestHeaders?: Headers;
  /** Aborts the in-flight RPC, for example for graceful shutdown. */
  signal?: AbortSignal;
  /** Context values made available to the handler implementation. */
  contextValues?: ContextValues;
}

/**
 * Headers belonging to the WebSocket handshake rather than to the RPC.
 * They are stripped before the upgrade request's headers become request
 * metadata, along with every `sec-websocket-*` header.
 */
const handshakeHeaders = new Set([
  "connection",
  "upgrade",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
]);

/**
 * Bridges one WebSocket speaking draft 5 of the wire protocol to
 * UniversalHandlers from `@connectrpc/connect`. Use
 * `createConnectRouter(...).handlers` to obtain the handlers array.
 *
 * Draft 5 carries one RPC per connection, so unlike
 * `handleMuxedBidiSocketDraft3` and `handleMuxedBidiSocketDraft4` there
 * is nothing to demultiplex here — and nothing to frame. Each direction
 * sends a JSON metadata message, then its RPC messages as plain codec
 * output, then an empty text message to end the data phase; the server adds
 * the Connect EndStreamResponse after its own. Exactly one message shape is
 * reserved -- an empty text message is the separator -- and position tells
 * metadata from data. A payload is sent as text when it is non-empty and
 * valid UTF-8, and as binary otherwise, including when empty: an empty
 * protobuf message encodes to zero bytes and must never read as a
 * half-close.
 *
 * Unary RPCs never arrive here. They are ordinary Connect HTTP requests,
 * served by whatever already serves the procedure URL; draft 5 upgrades
 * only streaming RPCs.
 *
 * The returned promise settles once the RPC has finished and its response
 * has been written.
 */
export async function handleBidiSocketDraft5(
  socket: Draft5MessageStream,
  handlers: readonly UniversalHandler[],
  options: HandleBidiSocketDraft5Options,
): Promise<void> {
  const reader = socket.readable.getReader();
  const writer = socket.writable.getWriter();

  // Translate the request direction into the internal envelope
  // representation handleBidiSocket consumes.
  let sawHeaders = false;
  let halfClosed = false;
  const readable = new ReadableStream<Uint8Array>({
    async pull(controller) {
      for (;;) {
        let result: ReadableStreamReadResult<Draft5Message>;
        try {
          result = await reader.read();
        } catch (err) {
          controller.error(err);
          return;
        }
        if (result.done) {
          controller.close();
          return;
        }
        const message = result.value;
        const isSeparator = message.text && message.data.byteLength === 0;
        if (!isSeparator && sawHeaders) {
          // A data message, text or binary alike.
          if (halfClosed) {
            controller.error(
              new ConnectError(
                "protocol error: request message after the separator",
                Code.InvalidArgument,
              ),
            );
            return;
          }
          controller.enqueue(encodeEnvelope(flagEnvelopeData, message.data));
          return;
        }
        if (!isSeparator && !message.text) {
          controller.error(
            new ConnectError(
              "protocol error: expected a request headers message first",
              Code.InvalidArgument,
            ),
          );
          return;
        }
        if (message.data.byteLength === 0) {
          if (halfClosed) {
            // A second separator; the client is done talking either way.
            continue;
          }
          halfClosed = true;
          // The separator is the client's half-close, which the envelope
          // protocol spells as an empty end-stream frame.
          controller.enqueue(encodeEnvelope(endStreamFlag, empty));
          return;
        }
        // Only the first message reaches here: once sawHeaders is set,
        // every non-separator message is taken as data above.
        sawHeaders = true;
        controller.enqueue(
          encodeEnvelope(
            flagEnvelopeHeaders,
            requestHeadersEnvelope(message.data, options),
          ),
        );
        return;
      }
    },
    cancel(reason) {
      void reader.cancel(reason).catch(() => {
        // The stream may already be closed or errored.
      });
    },
  });

  // ...and the response direction back out. Each write is one envelope.
  const writable = new WritableStream<Uint8Array>({
    async write(chunk) {
      const { flag, payload } = splitEnvelope(chunk);
      switch (flag) {
        case flagEnvelopeHeaders:
          await writer.write({ text: true, data: payload });
          return;
        case flagEnvelopeData: {
          // Text when the payload is non-empty and valid UTF-8, so JSON is
          // readable on the wire; binary otherwise, including when empty --
          // an empty text message is the separator.
          const text = payload.byteLength > 0 && isValidUtf8(payload);
          await writer.write({ text, data: payload });
          return;
        }
        case endStreamFlag:
          // One envelope, two messages: the separator says the data phase
          // is over, and the message after it carries the status.
          await writer.write({ text: true, data: empty });
          await writer.write({ text: true, data: payload });
          return;
        case compressedFlag:
          throw new ConnectError(
            "draft 5 has no per-message compression",
            Code.Internal,
          );
        default:
          throw new ConnectError(
            `unknown envelope flag 0x${flag.toString(16)}`,
            Code.Internal,
          );
      }
    },
  });

  try {
    await handleBidiSocket({ readable, writable }, handlers, {
      signal: options.signal,
      contextValues: options.contextValues,
    });
  } finally {
    await reader.cancel().catch(() => {
      // The stream may already be closed or errored.
    });
    await writer.close().catch(() => {
      // The stream may already be closed or errored.
    });
    socket.close?.(1000);
  }
}

/**
 * Build the headers frame handleBidiSocket expects: the handshake's own
 * headers, overridden by the headers message, plus the `:path` pseudo-header
 * naming the procedure the URL addressed.
 */
function requestHeadersEnvelope(
  payload: Uint8Array,
  options: HandleBidiSocketDraft5Options,
): Uint8Array {
  const headers = new Headers();
  options.requestHeaders?.forEach((value, key) => {
    const lower = key.toLowerCase();
    if (handshakeHeaders.has(lower) || lower.startsWith("sec-websocket-")) {
      return;
    }
    headers.append(key, value);
  });
  const { headers: fromMessage } = decodeHeadersFrame(payload);
  fromMessage.forEach((value, key) => {
    headers.set(key, value);
  });
  return encodeHeadersFrame(headers, { ":path": options.path });
}

/** Split one internal envelope into its flag and payload. */
function splitEnvelope(chunk: Uint8Array): {
  flag: number;
  payload: Uint8Array;
} {
  const envelopeHeadLength = 5;
  if (chunk.byteLength < envelopeHeadLength) {
    throw new ConnectError(
      `envelope too short: ${chunk.byteLength} bytes`,
      Code.Internal,
    );
  }
  const view = new DataView(chunk.buffer, chunk.byteOffset, chunk.byteLength);
  const declared = view.getUint32(1);
  if (declared !== chunk.byteLength - envelopeHeadLength) {
    throw new ConnectError(
      `expected exactly one envelope per write, got ${declared} declared payload bytes in a ${chunk.byteLength}-byte chunk`,
      Code.Internal,
    );
  }
  return {
    flag: view.getUint8(0),
    payload: chunk.subarray(envelopeHeadLength),
  };
}

/** Decode a text message's bytes, for adapters that hand over strings. */
export function draft5TextMessage(text: string): Draft5Message {
  return { text: true, data: encoder.encode(text) };
}

/** Encode a text message's bytes back to a string for sending. */
export function draft5MessageText(message: Draft5Message): string {
  return decoder.decode(message.data);
}
