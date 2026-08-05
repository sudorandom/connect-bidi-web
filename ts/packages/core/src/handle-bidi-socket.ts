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
  EnvelopedMessage,
  UniversalHandler,
  UniversalServerRequest,
  UniversalServerResponse,
} from "@connectrpc/connect/protocol";
import {
  compressedFlag,
  createAsyncIterable,
  createEnvelopeReadableStream,
  encodeEnvelope,
} from "@connectrpc/connect/protocol";
import {
  codeFromHttpStatus,
  contentTypeUnaryJson,
  contentTypeUnaryProto,
  createEndStreamSerialization,
  endStreamFlag,
  errorFromJsonBytes,
  headerContentType,
  headerStreamAcceptEncoding,
  headerStreamEncoding,
  headerUnaryAcceptEncoding,
  headerUnaryEncoding,
  parseContentType,
  trailerDemux,
  validateResponse,
} from "@connectrpc/connect/protocol-connect";
import { decodeHeadersFrame, encodeHeadersFrame } from "./headers-frame.js";
import {
  concatAsyncIterable,
  concatBytes,
  flagEnvelopeData,
  flagEnvelopeHeaders,
} from "./wire.js";

/**
 * A minimal abstraction of a full-duplex byte-stream connection, such as a
 * WebSocket adapted to a byte stream, or a WebTransport bidirectional
 * stream. Each DuplexByteStream carries exactly one RPC.
 */
export interface DuplexByteStream {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  /**
   * Close the underlying connection. Called once the RPC has finished and
   * the response has been fully written.
   */
  close?: () => void;
}

export interface HandleBidiSocketOptions {
  /**
   * Aborts the in-flight RPC, for example to support graceful server
   * shutdown. Aborting this signal does not close the socket; callers that
   * want the socket closed too should also call `close()` themselves, or
   * rely on the caller of `handleBidiSocket` to do so once the returned
   * promise settles.
   */
  signal?: AbortSignal;

  /**
   * Context values made available to the handler implementation.
   */
  contextValues?: ContextValues;
}

const endStreamSerialization = createEndStreamSerialization(undefined);

/**
 * Bridges a duplex byte stream carrying a single Connect RPC to a
 * UniversalHandler from `@connectrpc/connect`. Use
 * `createConnectRouter(...).handlers` to obtain the handlers array.
 *
 * The wire format read from and written to `socket` is the bidi-web
 * envelope protocol: every frame is a 5-byte envelope (1 flag byte, 4-byte
 * big-endian payload length) followed by the payload, matching
 * `@sudorandom/connect-bidi-web`'s client transports byte-for-byte.
 */
export async function handleBidiSocket(
  socket: DuplexByteStream,
  handlers: readonly UniversalHandler[],
  options?: HandleBidiSocketOptions,
): Promise<void> {
  const controller = new AbortController();
  const forwardAbort = (reason: unknown) => controller.abort(reason);
  if (options?.signal) {
    if (options.signal.aborted) {
      controller.abort(options.signal.reason);
    } else {
      options.signal.addEventListener(
        "abort",
        () => forwardAbort(options.signal?.reason),
        { once: true },
      );
    }
  }

  const envReader = createEnvelopeReadableStream(socket.readable).getReader();
  // The reader's `closed` promise rejects if the underlying byte stream
  // errors. Observe it even while we are not actively calling `read()` (for
  // example, while a unary handler implementation is doing slow work) so
  // that request.signal reflects an abrupt disconnect.
  envReader.closed.catch(forwardAbort);
  const writer = socket.writable.getWriter();

  try {
    let first: ReadableStreamReadResult<EnvelopedMessage>;
    try {
      first = await envReader.read();
    } catch {
      // Malformed or truncated initial frame. There is nothing to route or
      // respond to yet, so we simply stop.
      return;
    }
    if (first.done || first.value.flags !== flagEnvelopeHeaders) {
      return;
    }
    const { headers: requestHeaders, pseudo } = decodeHeadersFrame(
      first.value.data,
    );
    const path = pseudo[":path"] ?? "";

    const handler = handlers.find((h) => h.requestPath === path);
    if (!handler) {
      await writeErrorResponse(
        writer,
        new ConnectError(`unknown procedure: ${path}`, Code.Unimplemented),
      );
      return;
    }

    const methodKind = handler.method.methodKind;
    if (methodKind === "unary") {
      // Interop gotcha: our bidi wire always carries requests enveloped, so
      // some clients (the Go connectwebsocket/connectwebtransport client,
      // see internal/bidiprotocol/client_stream.go sendHeaders()) send the
      // streaming-style content-type and compression headers unconditionally,
      // even for unary calls. connect-es's unary handler requires the bare
      // application/proto|json content-type (contentTypeUnaryRegExp) and the
      // unary-named compression headers, or it rejects with 415. Normalize
      // in place before dispatch; TS clients already send the bare form (see
      // requestHeader() in protocol-connect), so this is a no-op for them.
      normalizeUnaryRequestHeaders(requestHeaders);
    }
    const request: UniversalServerRequest = {
      // Gotcha: connect-es rejects bidi_streaming methods outright when
      // httpVersion starts with "1." (see negotiateProtocol in
      // universal-handler.ts). Our socket is always full-duplex regardless
      // of methodKind, so we always advertise HTTP/2.
      httpVersion: "2",
      method: "POST",
      url: new URL(path, "https://bidi.invalid").toString(),
      header: requestHeaders,
      body:
        methodKind === "unary"
          ? createAsyncIterable([await readUnaryRequestBody(envReader)])
          : streamingRequestBody(envReader),
      signal: controller.signal,
      contextValues: options?.contextValues,
    };

    let response: UniversalServerResponse;
    try {
      response = await handler(request);
    } catch (e) {
      // UniversalHandlers are expected to catch their own errors and
      // return a structured response; reaching here indicates a bug in the
      // handler implementation or an unexpected failure. Surface it rather
      // than leaving the client waiting forever.
      await writeErrorResponse(writer, ConnectError.from(e, Code.Internal));
      return;
    }

    await writeResponse(writer, methodKind, response);
  } finally {
    controller.abort();
    await envReader.cancel().catch(() => {
      // Ignore: the stream may already be closed or errored.
    });
    await writer.close().catch(() => {
      // Ignore: the stream may already be closed or errored.
    });
    socket.close?.();
  }
}

/**
 * Rewrites streaming-shaped request headers to their unary equivalents in
 * place: `application/connect+proto|json` content-type to the bare
 * `application/proto|json`, and the `Connect-Content-Encoding` /
 * `Connect-Accept-Encoding` compression headers to their unary-named
 * counterparts `Content-Encoding` / `Accept-Encoding`. See the Go client
 * interop note where this is called for why this is necessary. The bridge
 * still never decompresses anything itself either way -- this only lets
 * connect-es's own unary handler find the compression headers it looks for.
 */
function normalizeUnaryRequestHeaders(headers: Headers): void {
  const parsedType = parseContentType(headers.get(headerContentType));
  if (parsedType?.stream) {
    headers.set(
      headerContentType,
      parsedType.binary ? contentTypeUnaryProto : contentTypeUnaryJson,
    );
  }
  const streamEncoding = headers.get(headerStreamEncoding);
  if (streamEncoding !== null) {
    headers.set(headerUnaryEncoding, streamEncoding);
    headers.delete(headerStreamEncoding);
  }
  const streamAcceptEncoding = headers.get(headerStreamAcceptEncoding);
  if (streamAcceptEncoding !== null) {
    headers.set(headerUnaryAcceptEncoding, streamAcceptEncoding);
    headers.delete(headerStreamAcceptEncoding);
  }
}

/**
 * Mirrors a unary response's `Content-Encoding` header (if the handler
 * compressed the body) to `Connect-Content-Encoding`, since bidiprotocol's
 * Go client -- and our own streaming request/response handling -- always
 * looks for the streaming-named header regardless of methodKind (see
 * internal/bidiprotocol/client_stream.go Receive()). Both headers are kept:
 * `Content-Encoding` is still what this file's own unary response framing
 * checks to choose the data envelope's flag.
 */
function mirrorUnaryResponseEncoding(headers: Headers): void {
  const encoding = headers.get(headerUnaryEncoding);
  if (encoding !== null) {
    headers.set(headerStreamEncoding, encoding);
  }
}

/**
 * Builds the raw request body for a unary call: read envelopes until the
 * request half-closes (an explicit end-stream envelope, or the underlying
 * stream ending), concatenating any data payloads. connect-es's unary
 * handlers expect a plain, un-enveloped byte stream (see
 * `readUnaryMessageFromBody` in handler-factory.ts), unlike streaming
 * handlers, which expect the envelope framing to remain intact.
 */
async function readUnaryRequestBody(
  envReader: ReadableStreamDefaultReader<EnvelopedMessage>,
): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  for (;;) {
    const { done, value } = await envReader.read();
    if (done || value.flags === endStreamFlag) {
      break;
    }
    if (value.flags === flagEnvelopeHeaders) {
      throw new ConnectError(
        "protocol error: unexpected headers envelope in request body",
        Code.Internal,
      );
    }
    chunks.push(value.data);
  }
  return concatBytes(chunks);
}

/**
 * Builds the request body for a streaming call: re-encode each envelope
 * exactly as read (flags and payload untouched, including the compressed
 * flag) and yield it as a raw byte chunk, so that connect-es's own
 * envelope splitter reconstructs identical frames regardless of how the
 * underlying transport happened to chunk the bytes. Stops when the request
 * half-closes.
 */
async function* streamingRequestBody(
  envReader: ReadableStreamDefaultReader<EnvelopedMessage>,
): AsyncIterable<Uint8Array> {
  for (;;) {
    const { done, value } = await envReader.read();
    if (done || value.flags === endStreamFlag) {
      return;
    }
    if (value.flags === flagEnvelopeHeaders) {
      throw new ConnectError(
        "protocol error: unexpected headers envelope in request body",
        Code.Internal,
      );
    }
    yield encodeEnvelope(value.flags, value.data);
  }
}

/**
 * Writes a UniversalServerResponse to the wire.
 */
async function writeResponse(
  writer: WritableStreamDefaultWriter<Uint8Array>,
  methodKind: DescMethod["methodKind"],
  response: UniversalServerResponse,
): Promise<void> {
  if (response.status === 200) {
    if (methodKind === "unary") {
      // connect-es muxes unary trailers into the header block with a
      // "trailer-" prefix (see trailer-mux.ts); demux them back out so
      // they can be carried in our own end-stream envelope instead.
      const [header, trailer] = trailerDemux(response.header ?? new Headers());
      mirrorUnaryResponseEncoding(header);
      await writer.write(
        encodeEnvelope(flagEnvelopeHeaders, encodeHeadersFrame(header)),
      );
      const bodyBytes = response.body
        ? await concatAsyncIterable(response.body)
        : new Uint8Array();
      const encoding = header.get(headerUnaryEncoding);
      const dataFlag =
        encoding !== null && encoding.toLowerCase() !== "identity"
          ? compressedFlag
          : flagEnvelopeData;
      await writer.write(encodeEnvelope(dataFlag, bodyBytes));
      await writer.write(
        encodeEnvelope(
          endStreamFlag,
          endStreamSerialization.serialize({ metadata: trailer }),
        ),
      );
      return;
    }
    // Streaming: response.header carries the headers frame; response.body
    // is already envelope-framed by connect-es (via transformJoinEnvelopes),
    // including its own trailing end-stream envelope, so it is forwarded
    // verbatim without re-parsing or re-encoding.
    await writer.write(
      encodeEnvelope(
        flagEnvelopeHeaders,
        encodeHeadersFrame(response.header ?? new Headers()),
      ),
    );
    if (response.body) {
      for await (const chunk of response.body) {
        await writer.write(chunk);
      }
      return;
    }
    // Defensive: connect-es always sets a body for a 200 streaming
    // response, but guard against a missing one so the client isn't left
    // waiting for an end-stream envelope that never arrives.
    await writer.write(
      encodeEnvelope(
        endStreamFlag,
        endStreamSerialization.serialize({ metadata: new Headers() }),
      ),
    );
    return;
  }

  // Non-200: either a unary business error (the body carries a Connect
  // error as JSON) or an early protocol rejection from content negotiation,
  // which has no body at all (unsupported media type, method not allowed,
  // or an HTTP/1.1 bidi_streaming rejection). validateResponse() throws in
  // the latter case and returns `isUnaryError` in the former; either way,
  // codeFromHttpStatus() is only the fallback when there is no structured
  // error body to parse, since it is a lossy, many-to-one mapping (for
  // example codeFromHttpStatus(400) is Code.Internal, not the
  // Code.InvalidArgument that produced the 400 in the first place).
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
      // Unreachable in practice: validateResponse() only returns without
      // throwing when isUnaryError is true, or when status is 200 (which
      // is excluded here). Guard anyway so the types check out.
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
  // Harmless when absent: only a compressed unary error body (rare -- error
  // JSON is usually well under the default compression threshold) would
  // have Content-Encoding set here at all.
  mirrorUnaryResponseEncoding(demuxedHeader);
  await writeErrorResponse(writer, error, demuxedHeader, trailer);
}

async function writeErrorResponse(
  writer: WritableStreamDefaultWriter<Uint8Array>,
  error: ConnectError,
  header?: Headers,
  trailer?: Headers,
): Promise<void> {
  await writer.write(
    encodeEnvelope(
      flagEnvelopeHeaders,
      encodeHeadersFrame(header ?? new Headers()),
    ),
  );
  await writer.write(
    encodeEnvelope(
      endStreamFlag,
      endStreamSerialization.serialize({
        metadata: trailer ?? new Headers(),
        error,
      }),
    ),
  );
}
