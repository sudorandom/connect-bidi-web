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
import { compressedFlag } from "@connectrpc/connect/protocol";
import { endStreamFlag } from "@connectrpc/connect/protocol-connect";
import { handleBidiSocket } from "./handle-bidi-socket.js";
import { decodeHeadersFrame, encodeHeadersFrame } from "./headers-frame.js";
import type { Draft1Envelope, Draft1MessageStream } from "./wire-draft1.js";
import {
  decodeDraft1Envelope,
  encodeDraft1Envelope,
  draft1ProcedureFromPath,
  draft1Subprotocol,
} from "./wire-draft1.js";
import { flagEnvelopeData, flagEnvelopeHeaders } from "./wire.js";

export { draft1ProcedureFromPath, draft1Subprotocol };
export type { Draft1Envelope, Draft1MessageStream };

export interface HandleBidiSocketDraft1Options {
  /**
   * The procedure the upgrade request addressed — the URL's path, such as
   * `/connectrpc.eliza.v1.ElizaService/Converse`. In draft 1 the URL names
   * the procedure, so the headers message never repeats it.
   *
   * Pass the full request path; `draft1ProcedureFromPath` reduces it to the
   * procedure, so a handler mounted under a prefix works unchanged.
   */
  path: string;
  /**
   * The headers the upgrade request carried. They are the base of the
   * request metadata; the headers envelope overrides them key by key, so a
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
 * Bridges one WebSocket speaking draft 1 of the wire protocol to
 * UniversalHandlers from `@connectrpc/connect`. Use
 * `createConnectRouter(...).handlers` to obtain the handlers array.
 *
 * Draft 1 carries one RPC per connection, so unlike
 * `handleMuxedBidiSocketDraft3` and `handleMuxedBidiSocketDraft4` there is
 * nothing to demultiplex. And unlike draft 5 there is nothing to reframe
 * either: every message is already exactly one Connect envelope, which is
 * the representation `handleBidiSocket` consumes. The only translation is
 * putting the URL's procedure into the request headers envelope as the
 * `:path` pseudo-header, since the wire does not carry it.
 *
 * Unary RPCs never arrive here. They are ordinary Connect HTTP requests,
 * served by whatever already serves the procedure URL; draft 1 upgrades
 * only streaming RPCs.
 *
 * The returned promise settles once the RPC has finished and its response
 * has been written.
 */
export async function handleBidiSocketDraft1(
  socket: Draft1MessageStream,
  handlers: readonly UniversalHandler[],
  options: HandleBidiSocketDraft1Options,
): Promise<void> {
  const reader = socket.readable.getReader();
  const writer = socket.writable.getWriter();

  // The request direction. Messages pass through as they are, except the
  // first: the headers envelope gains the procedure the URL named.
  let sawHeaders = false;
  let halfClosed = false;
  const readable = new ReadableStream<Uint8Array>({
    async pull(controller) {
      for (;;) {
        let result: ReadableStreamReadResult<Uint8Array>;
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
        let envelope: Draft1Envelope;
        try {
          envelope = decodeDraft1Envelope(result.value);
        } catch (err) {
          controller.error(err);
          return;
        }
        const { flag, payload } = envelope;
        if (halfClosed) {
          controller.error(
            new ConnectError(
              "protocol error: an envelope after the request end-stream",
              Code.InvalidArgument,
            ),
          );
          return;
        }
        if (!sawHeaders) {
          if (flag !== flagEnvelopeHeaders) {
            controller.error(
              new ConnectError(
                "protocol error: expected a request headers envelope first",
                Code.InvalidArgument,
              ),
            );
            return;
          }
          sawHeaders = true;
          controller.enqueue(
            encodeDraft1Envelope(
              flagEnvelopeHeaders,
              requestHeadersPayload(payload, options),
            ),
          );
          return;
        }
        switch (flag) {
          case flagEnvelopeData:
            controller.enqueue(encodeDraft1Envelope(flag, payload));
            return;
          case endStreamFlag:
            halfClosed = true;
            controller.enqueue(encodeDraft1Envelope(endStreamFlag, payload));
            return;
          case flagEnvelopeHeaders:
            controller.error(
              new ConnectError(
                "protocol error: a second request headers envelope",
                Code.InvalidArgument,
              ),
            );
            return;
          case compressedFlag:
            controller.error(
              new ConnectError(
                "draft 1 has no per-message compression; use permessage-deflate",
                Code.InvalidArgument,
              ),
            );
            return;
          default:
            controller.error(
              new ConnectError(
                `unknown envelope flag 0x${flag.toString(16)}`,
                Code.InvalidArgument,
              ),
            );
            return;
        }
      }
    },
    cancel(reason) {
      void reader.cancel(reason).catch(() => {
        // The stream may already be closed or errored.
      });
    },
  });

  // ...and the response direction back out. Each write is one envelope, and
  // one envelope is one message, so this only checks the framing before
  // handing the bytes to the socket unchanged.
  const writable = new WritableStream<Uint8Array>({
    async write(chunk) {
      const { flag } = decodeDraft1Envelope(chunk);
      if (flag === compressedFlag) {
        throw new ConnectError(
          "draft 1 has no per-message compression; use permessage-deflate",
          Code.Internal,
        );
      }
      await writer.write(chunk);
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
 * Build the headers payload handleBidiSocket expects: the handshake's own
 * headers, overridden by the headers envelope, plus the `:path`
 * pseudo-header naming the procedure the URL addressed.
 */
function requestHeadersPayload(
  payload: Uint8Array,
  options: HandleBidiSocketDraft1Options,
): Uint8Array {
  const headers = new Headers();
  options.requestHeaders?.forEach((value, key) => {
    const lower = key.toLowerCase();
    if (handshakeHeaders.has(lower) || lower.startsWith("sec-websocket-")) {
      return;
    }
    headers.append(key, value);
  });
  const { headers: fromEnvelope } = decodeHeadersFrame(payload);
  fromEnvelope.forEach((value, key) => {
    headers.set(key, value);
  });
  return encodeHeadersFrame(headers, {
    ":path": draft1ProcedureFromPath(options.path),
  });
}
