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

import type { DescMethod } from "@bufbuild/protobuf";
import type { ContextValues } from "@connectrpc/connect";
import { Code, ConnectError } from "@connectrpc/connect";
import type {
  UniversalHandler,
  UniversalServerRequest,
  UniversalServerResponse,
} from "@connectrpc/connect/protocol";
import {
  createAsyncIterable,
  encodeEnvelope,
} from "@connectrpc/connect/protocol";
import {
  codeFromHttpStatus,
  contentTypeStreamJson,
  contentTypeStreamProto,
  contentTypeUnaryJson,
  contentTypeUnaryProto,
  endStreamFlag,
  endStreamFromJson,
  endStreamToJson,
  errorFromJsonBytes,
  headerTimeout,
  trailerDemux,
  validateResponse,
} from "@connectrpc/connect/protocol-connect";
import { concatBytes, flagEnvelopeData } from "./wire.js";
import type {
  Draft7Codec,
  Draft7Message,
  Draft7MessageStream,
} from "./wire-draft7.js";
import {
  checkDraft7BodyFrame,
  decodeDraft7Message,
  decodeDraft7Metadata,
  draft7DefaultInfrastructureHeaders,
  draft7MarkerBody,
  draft7MarkerClientEndStream,
  draft7MarkerLeadingMetadata,
  draft7MarkerServerEndStream,
  draft7ReservedHeaderReason,
  encodeDraft7Message,
  encodeDraft7Metadata,
  unknownDraft7MarkerError,
} from "./wire-draft7.js";

/**
 * The server timeout applied when `serverTimeoutMs` is not given: an hour,
 * because the protocol exists for long-lived streams, and finite because a
 * server waits for the client's opening message before it dispatches the
 * RPC, so an unbounded one would let a silent peer park the connection.
 */
export const draft7DefaultServerTimeoutMs = 3_600_000;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export interface HandleBidiSocketDraft7Options {
  /**
   * The procedure the upgrade request addressed — the URL's path with any
   * WebSocket path prefix removed, such as
   * `/connectrpc.eliza.v1.ElizaService/Converse`.
   */
  path: string;
  /** The codec the subprotocol negotiated in the handshake. */
  codec: Draft7Codec;
  /**
   * The headers the upgrade request carried. Every one of them is made
   * available to the handler as request metadata; the client's
   * leading-metadata message then replaces values key by key.
   */
  requestHeaders?: Headers;
  /**
   * The deadline the handshake URI asked for with `connect-timeout-ms`,
   * in milliseconds. The effective deadline is the shorter of this and
   * `serverTimeoutMs`, and it bounds the whole RPC from now.
   */
  timeoutMs?: number;
  /**
   * The server's own deadline for every RPC, in milliseconds. Defaults to
   * `draft7DefaultServerTimeoutMs`; zero disables it.
   */
  serverTimeoutMs?: number;
  /**
   * The deny list of header names the server's infrastructure sets, which
   * a client's leading-metadata message must not carry. Replaces the
   * default (`forwarded`, `x-forwarded-*`, `x-real-ip`). An entry ending
   * in "-" is a prefix.
   */
  infrastructureHeaders?: readonly string[];
  /** Aborts the in-flight RPC, for example for graceful shutdown. */
  signal?: AbortSignal;
  /** Context values made available to the handler implementation. */
  contextValues?: ContextValues;
}

/**
 * Bridges one WebSocket speaking draft 7 of the wire protocol — the
 * Connect-over-WebSocket specification — to UniversalHandlers from
 * `@connectrpc/connect`. Use `createConnectRouter(...).handlers` to obtain
 * the handlers array.
 *
 * One connection carries one RPC. The client sends M, then bodies as B,
 * then C (optionally carrying the final body); the server answers M, then
 * bodies, then S with the Connect EndStreamResponse. Every failure of the
 * RPC, including a protocol violation by the client, is reported in S.
 *
 * The returned promise settles once the RPC has finished and its response
 * has been written.
 */
export async function handleBidiSocketDraft7(
  socket: Draft7MessageStream,
  handlers: readonly UniversalHandler[],
  options: HandleBidiSocketDraft7Options,
): Promise<void> {
  const reader = socket.readable.getReader();
  const writer = socket.writable.getWriter();
  const controller = new AbortController();
  const forwardAbort = () => controller.abort(options.signal?.reason);
  if (options.signal?.aborted === true) {
    forwardAbort();
  } else {
    options.signal?.addEventListener("abort", forwardAbort, { once: true });
  }
  // The reader's `closed` promise rejects if the socket errors; observe it
  // even while nobody is reading so request.signal reflects a disconnect.
  reader.closed.catch(() => controller.abort());

  const codec = options.codec;
  const serverTimeoutMs =
    options.serverTimeoutMs ?? draft7DefaultServerTimeoutMs;
  const effectiveTimeoutMs = shorter(options.timeoutMs, serverTimeoutMs);
  const deadline =
    effectiveTimeoutMs === undefined
      ? undefined
      : Date.now() + effectiveTimeoutMs;

  // The last protocol violation the request reader caught. It outranks
  // whatever the handler produced once its request stream failed under it.
  let protocolError: ConnectError | undefined;
  const request = new RequestReader(reader, codec, (err) => {
    protocolError ??= err;
    controller.abort(err);
  });

  const writeMessage = (marker: number, payload: Uint8Array, text: boolean) =>
    writer.write(encodeDraft7Message(marker, payload, text));
  const writeMetadata = (headers: Headers) =>
    writeMessage(
      draft7MarkerLeadingMetadata,
      encodeDraft7Metadata(headers),
      true,
    );
  const writeEndStream = (error: ConnectError | undefined, metadata: Headers) =>
    writeMessage(
      draft7MarkerServerEndStream,
      encoder.encode(
        JSON.stringify(endStreamToJson(metadata, error, undefined)),
      ),
      true,
    );
  const failBeforeDispatch = async (error: ConnectError) => {
    protocolError ??= error;
    await ignoringPeerDisconnect(
      (async () => {
        await writeMetadata(new Headers());
        await writeEndStream(error, new Headers());
      })(),
    );
  };

  try {
    // The mandatory first message, bounded by the deadline: a peer that
    // upgrades and then goes silent is answered, not parked.
    let effectiveHeaders: Headers;
    try {
      effectiveHeaders = await withDeadline(
        request.readLeadingMetadata(
          options.requestHeaders,
          options.infrastructureHeaders ?? draft7DefaultInfrastructureHeaders,
        ),
        deadline,
      );
    } catch (err) {
      await failBeforeDispatch(ConnectError.from(err, Code.Internal));
      return;
    }

    const handler = handlers.find((h) => h.requestPath === options.path);
    if (handler === undefined) {
      await failBeforeDispatch(
        new ConnectError(
          `unknown procedure: ${options.path}`,
          Code.Unimplemented,
        ),
      );
      return;
    }
    const methodKind = handler.method.methodKind;
    const unary = methodKind === "unary";
    // What connect-es's own handler needs to negotiate the Connect
    // protocol: the content type says the codec and, for streaming, the
    // framing; the timeout header is how it applies the deadline.
    effectiveHeaders.set(
      "content-type",
      unary
        ? codec === "proto"
          ? contentTypeUnaryProto
          : contentTypeUnaryJson
        : codec === "proto"
          ? contentTypeStreamProto
          : contentTypeStreamJson,
    );
    effectiveHeaders.set("connect-protocol-version", "1");
    if (deadline !== undefined) {
      effectiveHeaders.set(
        headerTimeout,
        String(Math.max(1, deadline - Date.now())),
      );
    }

    const universalRequest: UniversalServerRequest = {
      // connect-es rejects bidi_streaming methods when httpVersion starts
      // with "1."; the socket is full-duplex whatever the method kind.
      httpVersion: "2",
      method: "POST",
      url: new URL(options.path, "https://bidi.invalid").toString(),
      header: effectiveHeaders,
      body: unary
        ? createAsyncIterable([await request.readUnaryBody()])
        : request.streamingBody(),
      signal: controller.signal,
      contextValues: options.contextValues,
    };

    let response: UniversalServerResponse;
    try {
      response = await handler(universalRequest);
    } catch (e) {
      // UniversalHandlers catch their own errors; reaching here is a bug
      // in a handler implementation. Surface it rather than leaving the
      // client waiting forever.
      await ignoringPeerDisconnect(
        (async () => {
          await writeMetadata(new Headers());
          await writeEndStream(
            protocolError ?? ConnectError.from(e, Code.Internal),
            new Headers(),
          );
        })(),
      );
      return;
    }

    await ignoringPeerDisconnect(
      writeResponse(
        methodKind,
        response,
        codec,
        writeMetadata,
        writeMessage,
        writeEndStream,
        () => protocolError,
      ),
    );
  } finally {
    controller.abort();
    options.signal?.removeEventListener("abort", forwardAbort);
    await reader.cancel().catch(() => {
      // The stream may already be closed or errored.
    });
    await writer.close().catch(() => {
      // The stream may already be closed or errored.
    });
    socket.close?.(1000);
  }
}

/** The smaller of two optional durations. */
function shorter(
  a: number | undefined,
  b: number | undefined,
): number | undefined {
  const candidates = [a, b].filter(
    (value): value is number => value !== undefined && value > 0,
  );
  if (candidates.length === 0) {
    return undefined;
  }
  return Math.min(...candidates);
}

/** Rejects with deadline_exceeded once `deadline` passes. */
function withDeadline<T>(
  promise: Promise<T>,
  deadline: number | undefined,
): Promise<T> {
  if (deadline === undefined) {
    return promise;
  }
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(
      () =>
        reject(
          new ConnectError(
            "deadline exceeded before the leading-metadata message",
            Code.DeadlineExceeded,
          ),
        ),
      Math.max(0, deadline - Date.now()),
    );
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (err: unknown) => {
        clearTimeout(timer);
        reject(err);
      },
    );
  });
}

/**
 * Awaits a response-writing operation, swallowing its failure: a write can
 * only fail when the peer is gone, which is a normal way for an RPC to end.
 */
async function ignoringPeerDisconnect(write: Promise<void>): Promise<void> {
  try {
    await write;
  } catch {
    // Peer is gone; nothing left to deliver.
  }
}

/**
 * Reads the request direction: the leading-metadata message, then bodies
 * until the client end-of-stream. Every violation is reported through
 * `onProtocolError` and thrown, so the handler's request stream fails and
 * the S message carries the violation.
 */
class RequestReader {
  private ended = false;

  constructor(
    private readonly reader: ReadableStreamDefaultReader<Draft7Message>,
    private readonly codec: Draft7Codec,
    private readonly onProtocolError: (err: ConnectError) => void,
  ) {}

  private fail(err: ConnectError): never {
    this.onProtocolError(err);
    throw err;
  }

  /** The next message, or undefined once the connection has ended. */
  private async next(): Promise<
    { message: Draft7Message; marker: number; payload: Uint8Array } | undefined
  > {
    const result = await this.reader.read();
    if (result.done) {
      return undefined;
    }
    try {
      const { marker, payload } = decodeDraft7Message(result.value);
      return { message: result.value, marker, payload };
    } catch (err) {
      return this.fail(ConnectError.from(err, Code.InvalidArgument));
    }
  }

  /**
   * Reads the M message and builds the effective headers: the upgrade
   * request's, replaced key by key by the message's — unless a key is
   * reserved, which ends the RPC.
   */
  async readLeadingMetadata(
    handshake: Headers | undefined,
    infrastructure: readonly string[],
  ): Promise<Headers> {
    const next = await this.next();
    if (next === undefined) {
      throw new ConnectError(
        "connection closed before the leading-metadata message",
        Code.Canceled,
      );
    }
    if (next.marker !== draft7MarkerLeadingMetadata) {
      this.fail(
        new ConnectError(
          `protocol error: expected the leading-metadata message first, got marker ${JSON.stringify(String.fromCharCode(next.marker))}`,
          Code.InvalidArgument,
        ),
      );
    }
    if (!next.message.text) {
      this.fail(
        new ConnectError(
          "protocol error: the leading-metadata message must be a text frame",
          Code.InvalidArgument,
        ),
      );
    }
    if (next.payload.byteLength === 0) {
      this.fail(
        new ConnectError(
          "protocol error: the leading-metadata message has no payload; send {} for no metadata",
          Code.InvalidArgument,
        ),
      );
    }
    let fromMessage: Headers;
    try {
      fromMessage = decodeDraft7Metadata(next.payload);
    } catch (err) {
      return this.fail(ConnectError.from(err, Code.InvalidArgument));
    }
    const effective = new Headers();
    handshake?.forEach((value, key) => {
      effective.append(key, value);
    });
    const replaced = new Set<string>();
    fromMessage.forEach((value, key) => {
      const reason = draft7ReservedHeaderReason(key, infrastructure);
      if (reason !== undefined) {
        this.fail(
          new ConnectError(
            `protocol error: metadata key ${JSON.stringify(key)} is reserved: ${reason}`,
            Code.InvalidArgument,
          ),
        );
      }
      if (!replaced.has(key)) {
        replaced.add(key);
        effective.delete(key);
      }
      effective.append(key, value);
    });
    return effective;
  }

  /**
   * Reads one body-bearing message (B, or C with a payload) and returns
   * its payload, or undefined at a bare C. Anything else is a violation.
   */
  private async nextBody(): Promise<Uint8Array | undefined> {
    if (this.ended) {
      return undefined;
    }
    const next = await this.next();
    if (next === undefined) {
      // The connection ended without C: the client is gone.
      const err = new ConnectError(
        "connection closed before the client ended its stream",
        Code.Canceled,
      );
      this.onProtocolError(err);
      throw err;
    }
    switch (next.marker) {
      case draft7MarkerBody:
        this.checkBody(next.message, next.payload);
        return next.payload;
      case draft7MarkerClientEndStream:
        this.ended = true;
        if (next.payload.byteLength > 0) {
          this.checkBody(next.message, next.payload);
          return next.payload;
        }
        if (!next.message.text) {
          this.fail(
            new ConnectError(
              "protocol error: a bare client end-of-stream must be a text frame",
              Code.InvalidArgument,
            ),
          );
        }
        return undefined;
      case draft7MarkerLeadingMetadata:
        return this.fail(
          new ConnectError(
            "protocol error: a second leading-metadata message",
            Code.InvalidArgument,
          ),
        );
      case draft7MarkerServerEndStream:
        return this.fail(
          new ConnectError(
            "protocol error: server end-of-stream marker from a client",
            Code.InvalidArgument,
          ),
        );
      default:
        return this.fail(unknownDraft7MarkerError(next.marker));
    }
  }

  private checkBody(message: Draft7Message, payload: Uint8Array): void {
    try {
      checkDraft7BodyFrame(message, payload, this.codec);
    } catch (err) {
      this.fail(ConnectError.from(err, Code.InvalidArgument));
    }
  }

  /**
   * Keeps reading after C so that a message the client sends afterwards
   * is caught — it is a protocol error — and a client that disappears
   * mid-response cancels the handler.
   */
  private async watchAfterEnd(): Promise<void> {
    const next = await this.next().catch(() => undefined);
    if (next === undefined) {
      return;
    }
    this.onProtocolError(
      new ConnectError(
        `protocol error: message with marker ${JSON.stringify(String.fromCharCode(next.marker))} after the client end-of-stream`,
        Code.InvalidArgument,
      ),
    );
  }

  /** The single request message of a unary call. */
  async readUnaryBody(): Promise<Uint8Array> {
    const chunks: Uint8Array[] = [];
    for (;;) {
      const payload = await this.nextBody();
      if (payload === undefined) {
        break;
      }
      chunks.push(payload);
    }
    void this.watchAfterEnd();
    return concatBytes(chunks);
  }

  /** The request messages of a streaming call, enveloped for connect-es. */
  async *streamingBody(): AsyncIterable<Uint8Array> {
    for (;;) {
      const payload = await this.nextBody();
      if (payload === undefined) {
        void this.watchAfterEnd();
        return;
      }
      yield encodeEnvelope(flagEnvelopeData, payload);
    }
  }
}

/** Writes a UniversalServerResponse to the wire as M, bodies, and S. */
async function writeResponse(
  methodKind: DescMethod["methodKind"],
  response: UniversalServerResponse,
  codec: Draft7Codec,
  writeMetadata: (headers: Headers) => Promise<void>,
  writeMessage: (
    marker: number,
    payload: Uint8Array,
    text: boolean,
  ) => Promise<void>,
  writeEndStream: (
    error: ConnectError | undefined,
    metadata: Headers,
  ) => Promise<void>,
  protocolError: () => ConnectError | undefined,
): Promise<void> {
  const bodyIsText = codec === "json";
  if (response.status === 200) {
    if (methodKind === "unary") {
      // connect-es muxes unary trailers into the header block with a
      // "trailer-" prefix; demux them back out for S.
      const [header, trailer] = trailerDemux(response.header ?? new Headers());
      stripTransportHeaders(header);
      await writeMetadata(header);
      const bodyBytes = response.body
        ? await concatAsyncIterable(response.body)
        : new Uint8Array();
      const violation = protocolError();
      if (violation === undefined) {
        await writeMessage(draft7MarkerBody, bodyBytes, bodyIsText);
      }
      await writeEndStream(violation, trailer);
      return;
    }
    // Streaming: connect-es frames the body as envelopes, its own
    // end-stream envelope last. Each becomes one message.
    const header = new Headers(response.header ?? new Headers());
    stripTransportHeaders(header);
    await writeMetadata(header);
    let sentEndStream = false;
    if (response.body) {
      for await (const envelope of splitEnvelopes(response.body)) {
        if (envelope.flags === endStreamFlag) {
          const { error, metadata } = endStreamFromJson(envelope.data);
          await writeEndStream(protocolError() ?? error, metadata);
          sentEndStream = true;
          break;
        }
        if (envelope.flags !== flagEnvelopeData) {
          throw new ConnectError(
            `unexpected envelope flag 0x${envelope.flags.toString(16)} in a response`,
            Code.Internal,
          );
        }
        await writeMessage(draft7MarkerBody, envelope.data, bodyIsText);
      }
    }
    if (!sentEndStream) {
      await writeEndStream(protocolError(), new Headers());
    }
    return;
  }

  // Non-200: a unary business error (the body is a Connect error as JSON)
  // or an early protocol rejection with no body. codeFromHttpStatus() is
  // only the fallback when there is no structured error body to parse.
  const header = response.header ?? new Headers();
  let error: ConnectError;
  try {
    const validated = validateResponse(
      methodKind,
      false,
      response.status,
      header,
    );
    if (!validated.isUnaryError) {
      throw new ConnectError(
        `HTTP ${response.status}`,
        codeFromHttpStatus(response.status),
      );
    }
    const bodyBytes = response.body
      ? await concatAsyncIterable(response.body)
      : new Uint8Array();
    error = errorFromJsonBytes(bodyBytes, header, validated.unaryError);
  } catch (e) {
    error =
      e instanceof ConnectError
        ? e
        : new ConnectError(
            `HTTP ${response.status}`,
            codeFromHttpStatus(response.status),
          );
  }
  const [demuxedHeader, trailer] = trailerDemux(header);
  stripTransportHeaders(demuxedHeader);
  await writeMetadata(demuxedHeader);
  await writeEndStream(protocolError() ?? error, trailer);
}

/**
 * Removes the headers connect-es sets for the HTTP transport it thinks it
 * is on. The subprotocol names the codec and there is no message-level
 * compression, so none of them belongs in M.
 */
function stripTransportHeaders(headers: Headers): void {
  for (const name of [
    "content-type",
    "content-encoding",
    "connect-content-encoding",
    "connect-accept-encoding",
    "accept-encoding",
  ]) {
    headers.delete(name);
  }
}

async function concatAsyncIterable(
  iterable: AsyncIterable<Uint8Array>,
): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of iterable) {
    chunks.push(chunk);
  }
  return concatBytes(chunks);
}

/**
 * Splits an envelope-framed byte stream into envelopes, whatever the
 * chunking: connect-es usually yields one envelope per chunk, but nothing
 * promises that.
 */
async function* splitEnvelopes(
  body: AsyncIterable<Uint8Array>,
): AsyncIterable<{ flags: number; data: Uint8Array }> {
  let buffer: Uint8Array = new Uint8Array(0);
  for await (const chunk of body) {
    buffer = buffer.byteLength === 0 ? chunk : concatBytes([buffer, chunk]);
    for (;;) {
      if (buffer.byteLength < 5) {
        break;
      }
      const view = new DataView(
        buffer.buffer,
        buffer.byteOffset,
        buffer.byteLength,
      );
      const length = view.getUint32(1);
      if (buffer.byteLength < 5 + length) {
        break;
      }
      yield { flags: view.getUint8(0), data: buffer.slice(5, 5 + length) };
      buffer = buffer.subarray(5 + length);
    }
  }
  if (buffer.byteLength !== 0) {
    throw new ConnectError(
      `${buffer.byteLength} trailing bytes after the last envelope`,
      Code.Internal,
    );
  }
}

/** Decode a text message's bytes, for adapters that hand over strings. */
export function draft7TextMessage(text: string): Draft7Message {
  return { text: true, data: encoder.encode(text) };
}

/** Encode a text message's bytes back to a string for sending. */
export function draft7MessageText(message: Draft7Message): string {
  return decoder.decode(message.data);
}
