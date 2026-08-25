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

// Draft 2 of the WebSocket wire protocol. Where draft 1
// (connect-websocket-draft1-transport.ts) follows each stream ID with a standard
// 5-byte Connect envelope, draft 2 delegates the envelope's two jobs to the
// WebSocket itself — permessage-deflate for compression, message boundaries
// for length — so every message is:
//
//     [4-byte big-endian stream ID][1-byte frame type][payload]
//
// Frame types are complete byte values, not bitmasks; 0x04 and up are
// reserved. Must match the Go connectwebsocket/draft2 package and
// @sudorandom/connect-bidi-core's draft 2 server bridge byte-for-byte.
const frameTypeData = 0x00;
const frameTypeHeaders = 0x01;
const frameTypeEndStream = 0x02;
const frameTypeReset = 0x03;
const streamIdLength = 4;
const frameHeadLength = streamIdLength + 1;

/** One frame of a single stream, without the stream ID. */
interface Draft2Frame {
  type: number;
  payload: Uint8Array;
}

/**
 * A connect-es Transport carrying streaming RPCs over WebSocket using
 * draft 2 of the wire protocol, plus control over the shared multiplexed
 * connection.
 */
export interface ConnectWebSocketDraft2Transport extends Transport {
  /**
   * Closes the shared multiplexed connection, if one is open, terminating
   * any RPCs still running on it. The transport remains usable: the next
   * RPC dials a new connection. Useful outside the browser (tests, CLI
   * tools), where an open WebSocket keeps the process alive.
   */
  close(): void;
}

export interface ConnectWebSocketDraft2TransportOptions {
  baseUrl: string;
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
  controller: ReadableStreamDefaultController<Draft2Frame>;
  /** The server finished this stream with an end-stream frame. */
  endSeen: boolean;
}

/**
 * One WebSocket connection carrying any number of concurrent RPC streams
 * with draft 2 framing. A single onmessage handler routes frames to the
 * stream they belong to. The socket is opened lazily and re-opened if it
 * failed.
 */
class WebSocketMuxDraft2 {
  private readonly url: string;
  private readonly closeWhenIdle: boolean;
  private socket: WebSocket | undefined;
  private opening: Promise<WebSocket> | undefined;
  private nextStreamId = 1;
  private readonly entries = new Map<number, MuxStreamEntry>();

  constructor(url: string, closeWhenIdle: boolean) {
    this.url = url;
    this.closeWhenIdle = closeWhenIdle;
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
    readable: ReadableStream<Draft2Frame>;
    writable: WritableStream<Draft2Frame>;
  }> {
    const socket = await this.open();
    const streamId = this.nextStreamId++;
    // start() runs synchronously in the ReadableStream constructor, so the
    // controller is assigned before it is first used.
    let controller!: ReadableStreamDefaultController<Draft2Frame>;
    const readable = new ReadableStream<Draft2Frame>({
      start(readableController) {
        controller = readableController;
      },
    });
    this.entries.set(streamId, { controller, endSeen: false });
    const writable = new WritableStream<Draft2Frame>({
      write: (frame) => {
        socket.send(encodeFrame(streamId, frame.type, frame.payload));
      },
      close: () => {
        // Half-close: an explicit end-stream frame, since neither the
        // stream nor the WebSocket has a send-direction close of its own.
        socket.send(
          encodeFrame(streamId, frameTypeEndStream, new Uint8Array()),
        );
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
      this.socket.send(encodeFrame(streamId, frameTypeReset, new Uint8Array()));
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
        this.route(new Uint8Array(event.data as ArrayBuffer));
      };
    });
    this.opening.catch(() => {});
    return this.opening;
  }

  /** Route one incoming message to the stream it belongs to. */
  private route(message: Uint8Array): void {
    if (message.byteLength < frameHeadLength) {
      return;
    }
    const view = new DataView(
      message.buffer,
      message.byteOffset,
      message.byteLength,
    );
    const streamId = view.getUint32(0);
    const type = view.getUint8(streamIdLength);
    const entry = this.entries.get(streamId);
    if (entry === undefined) {
      // A late frame for a stream already closed on this side; drop it.
      return;
    }
    if (type === frameTypeReset) {
      this.entries.delete(streamId);
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
    entry.controller.enqueue({
      type,
      payload: message.subarray(frameHeadLength),
    });
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

function encodeFrame(
  streamId: number,
  type: number,
  payload: Uint8Array,
): Uint8Array<ArrayBuffer> {
  const frame = new Uint8Array(frameHeadLength + payload.byteLength);
  new DataView(frame.buffer).setUint32(0, streamId);
  frame[streamIdLength] = type;
  frame.set(payload, frameHeadLength);
  return frame;
}

export function createConnectWebSocketDraft2Transport(
  options: ConnectWebSocketDraft2TransportOptions,
): ConnectWebSocketDraft2Transport {
  const useBinaryFormat = options.useBinaryFormat ?? false;
  const url = new URL("/websocket-draft2", options.baseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  const wsUrl = url.toString();
  const connectionPerStream = options.connectionPerStream ?? false;
  // All RPCs share one multiplexed connection unless connectionPerStream is
  // set, in which case each RPC gets a mux of its own that closes when the
  // RPC finishes.
  const sharedMux = new WebSocketMuxDraft2(wsUrl, false);

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
            ? new WebSocketMuxDraft2(wsUrl, true)
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
              `protocol error: expected headers frame, got 0x${firstResult.value.type.toString(16)}`,
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
                    `protocol error: unexpected frame type 0x${frame.type.toString(16)}`,
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
