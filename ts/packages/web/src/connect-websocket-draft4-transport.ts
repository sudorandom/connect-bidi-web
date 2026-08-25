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
  BinaryReadOptions,
  BinaryWriteOptions,
  DescMessage,
  JsonReadOptions,
  JsonWriteOptions,
  MessageInitShape,
  DescMethodUnary,
  DescMethodStreaming,
} from "@bufbuild/protobuf";
import type {
  Interceptor,
  StreamResponse,
  Transport,
  UnaryResponse,
  ContextValues,
  StreamRequest,
} from "@connectrpc/connect";
import { Code, ConnectError, createContextValues } from "@connectrpc/connect";
import {
  createClientMethodSerializers,
  createMethodUrl,
  runStreamingCall,
} from "@connectrpc/connect/protocol";
import {
  endStreamFromJson,
  requestHeader,
} from "@connectrpc/connect/protocol-connect";
import { decodeHeadersFrame, encodeHeadersFrame } from "./headers-frame.js";

// Draft 4 of the WebSocket wire protocol. Where draft 3 packs the
// frame head into five binary bytes, draft 4 spends a few more on an ASCII
// head so the wire is readable in the browser's own Network tab without a
// decoder:
//
//     <stream ID> "|" <flags> "|" <payload>
//
// Both fields are unpadded decimal ASCII. Parsers split on the first two
// "|" bytes only, so a payload containing "|" needs no escaping. Frames
// whose payload is UTF-8 are sent as text WebSocket messages, which is what
// makes devtools render them as text -- with the default JSON format, that
// is every frame on the connection. Compression is left to
// permessage-deflate, so there is no compression flag. Must
// match the Go connectwebsocket/draft4 package and
// @sudorandom/connect-bidi-core's draft 4 server bridge byte-for-byte.
// The flags field is partitioned: low 3 bits frame type, high 5 bits
// independent flags (none defined yet). Receivers mask out the type and
// ignore unknown flag bits, so a later revision can OR one in without
// breaking peers built against this one.
const frameTypeMask = 0x07;
const frameTypeData = 0;
const frameTypeHeaders = 1;
const frameTypeEndStream = 2;
const frameTypeReset = 3;

/** Extracts the frame type from a flags field, discarding the flag bits. */
function frameTypeOf(flags: number): number {
  return flags & frameTypeMask;
}

/**
 * How far the parser scans for the two separators: ten digits of uint32
 * stream ID, three digits of uint8 flags, and the two separators.
 */
const maxHeadLength = 10 + 1 + 3 + 1;
const separator = "|";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** One frame of a single stream, without the stream ID. */
interface Draft4Frame {
  type: number;
  payload: Uint8Array;
}

/**
 * A connect-es Transport carrying streaming RPCs over WebSocket using
 * draft 4 of the wire protocol, plus control over the shared multiplexed
 * connection.
 */
export interface ConnectWebSocketDraft4Transport extends Transport {
  /**
   * Closes the shared multiplexed connection, if one is open, terminating
   * any RPCs still running on it. The transport remains usable: the next
   * RPC dials a new connection. Useful outside the browser (tests, CLI
   * tools), where an open WebSocket keeps the process alive.
   */
  close(): void;
}

export interface ConnectWebSocketDraft4TransportOptions {
  baseUrl: string;
  /**
   * Send protobuf binary rather than JSON. Defaults to false, and leaving
   * it that way is what makes every frame on the connection readable:
   * protobuf data payloads are not UTF-8, so their frames must be sent as
   * binary messages (their ASCII head is still readable in a hex view).
   */
  useBinaryFormat?: boolean;
  interceptors?: Interceptor[];
  jsonOptions?: Partial<JsonReadOptions & JsonWriteOptions>;
  binaryOptions?: Partial<BinaryReadOptions & BinaryWriteOptions>;
  defaultTimeoutMs?: number;
  /**
   * Dial a dedicated WebSocket connection for each streaming RPC instead of
   * multiplexing all RPCs onto one shared connection. A shared connection
   * is subject to head-of-line blocking: one stream with a large message or
   * a slow consumer delays every other stream behind it. Dedicated
   * connections trade a WebSocket handshake per RPC for full isolation.
   * Frames carry a stream ID either way.
   */
  connectionPerStream?: boolean;
}

interface MuxStreamEntry {
  controller: ReadableStreamDefaultController<Draft4Frame>;
  /** The server finished this stream with an end-stream frame. */
  endSeen: boolean;
}

/**
 * One WebSocket connection carrying any number of concurrent RPC streams
 * with draft 4 framing. A single onmessage handler routes frames to the
 * stream they belong to. The socket is opened lazily and re-opened if it
 * failed.
 */
class WebSocketMuxDraft4 {
  private readonly url: string;
  private readonly closeWhenIdle: boolean;
  /**
   * Whether data frames carry UTF-8, and so can be sent as text messages.
   * Control frames are always JSON and always text.
   */
  private readonly dataIsText: boolean;
  private socket: WebSocket | undefined;
  private opening: Promise<WebSocket> | undefined;
  private nextStreamId = 1;
  private readonly entries = new Map<number, MuxStreamEntry>();

  constructor(url: string, closeWhenIdle: boolean, dataIsText: boolean) {
    this.url = url;
    this.closeWhenIdle = closeWhenIdle;
    this.dataIsText = dataIsText;
  }

  /**
   * Open a new stream on the connection, dialing it if necessary. The
   * returned readable yields the stream's incoming frames; each frame
   * written to the writable is sent as one WebSocket message. Closing the
   * writable sends an end-stream frame (half-close). Call `closeStream`
   * once the RPC is finished or abandoned.
   */
  async openStream(): Promise<{
    streamId: number;
    readable: ReadableStream<Draft4Frame>;
    writable: WritableStream<Draft4Frame>;
  }> {
    const socket = await this.open();
    const streamId = this.nextStreamId++;
    // start() runs synchronously in the ReadableStream constructor, so the
    // controller is assigned before it is first used.
    let controller!: ReadableStreamDefaultController<Draft4Frame>;
    const readable = new ReadableStream<Draft4Frame>({
      start(readableController) {
        controller = readableController;
      },
    });
    this.entries.set(streamId, { controller, endSeen: false });
    const writable = new WritableStream<Draft4Frame>({
      write: (frame) => {
        this.send(socket, streamId, frame.type, frame.payload);
      },
      close: () => {
        // Half-close: an explicit end-stream frame, since neither the
        // stream nor the WebSocket has a send-direction close of its own.
        this.send(socket, streamId, frameTypeEndStream, new Uint8Array());
      },
      abort: () => {
        this.closeStream(streamId);
      },
    });
    return { streamId, readable, writable };
  }

  /**
   * Close the connection, terminating every stream on it. The mux remains
   * usable: the next `openStream` dials a new connection.
   */
  close(): void {
    const socket = this.socket;
    this.socket = undefined;
    this.failAll(new ConnectError("WebSocket closed", Code.Unavailable));
    socket?.close(1000);
  }

  /**
   * Release a stream. If the server hasn't finished it, a reset frame tells
   * it to stop work. With `closeWhenIdle`, the connection is closed once no
   * streams remain.
   */
  closeStream(streamId: number): void {
    const entry = this.entries.get(streamId);
    if (entry === undefined) {
      return;
    }
    this.entries.delete(streamId);
    if (
      !entry.endSeen &&
      this.socket !== undefined &&
      this.socket.readyState === WebSocket.OPEN
    ) {
      this.send(this.socket, streamId, frameTypeReset, new Uint8Array());
    }
    try {
      entry.controller.close();
    } catch {
      // The stream may already be closed or errored.
    }
    if (
      this.closeWhenIdle &&
      this.entries.size === 0 &&
      this.socket !== undefined
    ) {
      this.socket.close(1000);
      this.socket = undefined;
    }
  }

  /**
   * Send one frame as one WebSocket message. A frame whose payload is UTF-8
   * goes out as a text message -- passing a string to `send` is how the
   * browser API picks the text opcode -- so devtools renders it as text.
   */
  private send(
    socket: WebSocket,
    streamId: number,
    type: number,
    payload: Uint8Array,
  ): void {
    const head = `${streamId}${separator}${type}${separator}`;
    if (type === frameTypeData && !this.dataIsText) {
      socket.send(concatFrame(head, payload));
      return;
    }
    socket.send(head + decoder.decode(payload));
  }

  private async open(): Promise<WebSocket> {
    if (
      this.socket !== undefined &&
      this.socket.readyState === WebSocket.OPEN
    ) {
      return this.socket;
    }
    this.opening ??= new Promise<WebSocket>((resolve, reject) => {
      const socket = new WebSocket(this.url);
      socket.binaryType = "arraybuffer";
      socket.onopen = () => {
        this.opening = undefined;
        this.socket = socket;
        resolve(socket);
      };
      socket.onerror = () => {
        this.opening = undefined;
        reject(
          new ConnectError("WebSocket connection failed", Code.Unavailable),
        );
      };
      socket.onclose = (event) => {
        this.opening = undefined;
        if (this.socket === socket) {
          this.socket = undefined;
        }
        // Surface the server's close reason (e.g. "idle timeout") so
        // callers can tell an expected disconnect from a failure.
        this.failAll(
          new ConnectError(
            event.reason !== ""
              ? `WebSocket closed: ${event.reason}`
              : "WebSocket closed",
            Code.Unavailable,
          ),
        );
      };
      socket.onmessage = (event) => {
        // Text and binary messages are both accepted: draft 4 treats the
        // opcode as a legibility hint, not protocol data, so a peer may
        // send any frame either way.
        const data: unknown = event.data;
        this.route(
          typeof data === "string"
            ? encoder.encode(data)
            : new Uint8Array(data as ArrayBuffer),
        );
      };
    });
    this.opening.catch(() => {});
    return this.opening;
  }

  /** Route one incoming message to the stream it belongs to. */
  private route(message: Uint8Array): void {
    const frame = decodeFrame(message);
    if (frame === undefined) {
      // A malformed head means framing has been lost; nothing after it can
      // be trusted, so take the connection down rather than skip the frame.
      this.close();
      return;
    }
    const entry = this.entries.get(frame.streamId);
    if (entry === undefined) {
      // A late frame for a stream already closed on this side; drop it.
      return;
    }
    const type = frameTypeOf(frame.flags);
    if (type === frameTypeReset) {
      this.entries.delete(frame.streamId);
      try {
        entry.controller.error(
          new ConnectError("stream reset by server", Code.Canceled),
        );
      } catch {
        // The stream may already be closed or errored.
      }
      return;
    }
    if (type === frameTypeEndStream) {
      entry.endSeen = true;
    }
    entry.controller.enqueue({ type, payload: frame.payload });
  }

  /** Terminate every stream after the connection failed or closed. */
  private failAll(reason: ConnectError): void {
    for (const entry of this.entries.values()) {
      try {
        entry.controller.error(reason);
      } catch {
        // The stream may already be closed or errored.
      }
    }
    this.entries.clear();
  }
}

/** Join an ASCII head and a binary payload into one frame. */
function concatFrame(
  head: string,
  payload: Uint8Array,
): Uint8Array<ArrayBuffer> {
  const headBytes = encoder.encode(head);
  const frame = new Uint8Array(headBytes.byteLength + payload.byteLength);
  frame.set(headBytes, 0);
  frame.set(payload, headBytes.byteLength);
  return frame;
}

/**
 * Split one draft 4 message into stream ID, flags, and payload, or
 * undefined if the head is malformed.
 */
function decodeFrame(
  message: Uint8Array,
): { streamId: number; flags: number; payload: Uint8Array } | undefined {
  const separatorByte = 0x7c;
  const head = message.subarray(0, Math.min(message.byteLength, maxHeadLength));
  const firstSep = head.indexOf(separatorByte);
  if (firstSep < 0) {
    return undefined;
  }
  const secondSepInRest = head.subarray(firstSep + 1).indexOf(separatorByte);
  if (secondSepInRest < 0) {
    return undefined;
  }
  const secondSep = firstSep + 1 + secondSepInRest;
  const streamId = parseField(head.subarray(0, firstSep), 0xffffffff);
  const flags = parseField(head.subarray(firstSep + 1, secondSep), 0xff);
  if (streamId === undefined || flags === undefined) {
    return undefined;
  }
  return { streamId, flags, payload: message.subarray(secondSep + 1) };
}

/**
 * Parse one unpadded decimal ASCII field, rejecting anything Number() would
 * otherwise accept: an empty field, a sign, whitespace, and values above
 * the field's range.
 */
function parseField(field: Uint8Array, max: number): number | undefined {
  if (field.byteLength === 0) {
    return undefined;
  }
  let value = 0;
  for (const byte of field) {
    if (byte < 0x30 || byte > 0x39) {
      return undefined;
    }
    value = value * 10 + (byte - 0x30);
    if (value > max) {
      return undefined;
    }
  }
  return value;
}

export function createConnectWebSocketDraft4Transport(
  options: ConnectWebSocketDraft4TransportOptions,
): ConnectWebSocketDraft4Transport {
  const useBinaryFormat = options.useBinaryFormat ?? false;
  const url = new URL("/websocket-draft4", options.baseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  const wsUrl = url.toString();
  const connectionPerStream = options.connectionPerStream ?? false;
  // All RPCs share one multiplexed connection unless connectionPerStream is
  // set, in which case each RPC gets a mux of its own that closes when the
  // RPC finishes.
  const sharedMux = new WebSocketMuxDraft4(wsUrl, false, !useBinaryFormat);

  return {
    close(): void {
      sharedMux.close();
    },

    async unary<I extends DescMessage, O extends DescMessage>(
      _method: DescMethodUnary<I, O>,
      _signal: AbortSignal | undefined,
      _timeoutMs: number | undefined,
      _header: HeadersInit | undefined,
      _message: MessageInitShape<I>,
      _contextValues?: ContextValues,
    ): Promise<UnaryResponse<I, O>> {
      throw new ConnectError(
        "Unary not implemented over WS",
        Code.Unimplemented,
      );
    },

    async stream<I extends DescMessage, O extends DescMessage>(
      method: DescMethodStreaming<I, O>,
      signal: AbortSignal | undefined,
      timeoutMs: number | undefined,
      header: HeadersInit | undefined,
      input: AsyncIterable<MessageInitShape<I>>,
      contextValues?: ContextValues,
    ): Promise<StreamResponse<I, O>> {
      const { serialize, parse } = createClientMethodSerializers(
        method,
        useBinaryFormat,
        options.jsonOptions,
        options.binaryOptions,
      );
      timeoutMs =
        timeoutMs === undefined
          ? options.defaultTimeoutMs
          : timeoutMs <= 0
            ? undefined
            : timeoutMs;

      return await runStreamingCall<I, O>({
        interceptors: options.interceptors,
        timeoutMs,
        signal,
        req: {
          stream: true,
          service: method.parent,
          method,
          requestMethod: "POST",
          url: createMethodUrl(options.baseUrl, method),
          header: requestHeader(
            method.methodKind,
            useBinaryFormat,
            timeoutMs,
            header,
            false,
          ),
          contextValues: contextValues ?? createContextValues(),
          message: input,
        },
        next: async (
          req: StreamRequest<I, O>,
        ): Promise<StreamResponse<I, O>> => {
          const mux = connectionPerStream
            ? new WebSocketMuxDraft4(wsUrl, true, !useBinaryFormat)
            : sharedMux;
          const { streamId, readable, writable } = await mux.openStream();

          const path = new URL(req.url).pathname;
          const writer = writable.getWriter();
          await writer.write({
            type: frameTypeHeaders,
            payload: encodeHeadersFrame(req.header, { ":path": path }),
          });

          // Write request messages asynchronously so the response can be
          // read concurrently.
          const writePromise = (async () => {
            try {
              for await (const msg of req.message) {
                await writer.write({
                  type: frameTypeData,
                  payload: serialize(msg),
                });
              }
              await writer.close();
            } catch (err) {
              await writer.abort(err).catch(() => {});
              throw err;
            } finally {
              writer.releaseLock();
            }
          })();
          writePromise.catch(() => {});
          // A failed write must also abort reads; derive the guard promise
          // once instead of per received frame. It stays pending forever
          // when the writes succeed.
          const writeFailed = writePromise.then(
            () => new Promise<never>(() => {}),
          );
          writeFailed.catch(() => {});

          const frameReader = readable.getReader();
          const firstResult = await Promise.race([
            frameReader.read(),
            writeFailed,
          ]);
          if (firstResult.done) {
            mux.closeStream(streamId);
            throw new ConnectError(
              "protocol error: missing response headers",
              Code.Internal,
            );
          }
          if (firstResult.value.type !== frameTypeHeaders) {
            mux.closeStream(streamId);
            throw new ConnectError(
              `protocol error: expected headers frame, got ${firstResult.value.type}`,
              Code.Internal,
            );
          }
          const { headers: responseHeaders } = decodeHeadersFrame(
            firstResult.value.payload,
          );

          const responseTrailers = new Headers();

          async function* iterate() {
            try {
              for (;;) {
                const result = await Promise.race([
                  frameReader.read(),
                  writeFailed,
                ]);
                if (result.done) {
                  break;
                }
                const frame = result.value;
                if (frame.type === frameTypeEndStream) {
                  const { error, metadata } = endStreamFromJson(frame.payload);
                  metadata.forEach((val, key) => {
                    responseTrailers.append(key, val);
                  });
                  if (error) {
                    throw error;
                  }
                  break;
                }
                if (frame.type !== frameTypeData) {
                  throw new ConnectError(
                    `protocol error: unexpected frame type ${frame.type}`,
                    Code.Internal,
                  );
                }
                yield parse(frame.payload);
              }
              await writePromise;
            } finally {
              frameReader.releaseLock();
              mux.closeStream(streamId);
            }
          }

          return {
            stream: true,
            service: method.parent,
            method,
            header: responseHeaders,
            trailer: responseTrailers,
            message: iterate(),
          };
        },
      });
    },
  };
}
