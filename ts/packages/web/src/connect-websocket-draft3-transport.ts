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

// Draft 3 of the WebSocket wire protocol: a packed binary head
// ([4-byte stream ID][1-byte descriptor][payload], no length) with
// compression moved into the protocol. The handshake negotiates it once
// per connection through the subprotocols below, and descriptor bit 7
// marks a frame whose payload is one complete raw DEFLATE stream. Must
// match the Go connectwebsocket/draft3 package and
// @sudorandom/connect-bidi-core's draft 3 server bridge byte-for-byte.
const subprotocolDeflate = "connect.bidi.d3.deflate";
const subprotocolIdentity = "connect.bidi.d3";
const frameTypeData = 0x00;
const frameTypeHeaders = 0x01;
const frameTypeEndStream = 0x02;
const frameTypeReset = 0x03;
const frameCompressed = 0x80;
const compressMinBytes = 512;
const streamIdLength = 4;
const frameHeadLength = streamIdLength + 1;

/** One frame of a single stream, decompressed, without the stream ID. */
interface Draft3Frame {
  type: number;
  payload: Uint8Array;
}

function deflateRaw(data: Uint8Array): Promise<Uint8Array> {
  return transform(data, new CompressionStream("deflate-raw"));
}

function inflateRaw(data: Uint8Array): Promise<Uint8Array> {
  return transform(data, new DecompressionStream("deflate-raw"));
}

async function transform(
  data: Uint8Array,
  stream: CompressionStream | DecompressionStream,
): Promise<Uint8Array> {
  const writer = stream.writable.getWriter();
  // Write and read concurrently: awaiting the write before reading could
  // deadlock once the transform's internal buffer fills. The write is
  // still awaited at the end so its errors surface.
  const writeDone = writer
    .write(data as BufferSource)
    .then(() => writer.close());
  writeDone.catch(() => {});
  const out = new Uint8Array(await new Response(stream.readable).arrayBuffer());
  await writeDone;
  return out;
}

/**
 * A connect-es Transport carrying streaming RPCs over WebSocket using
 * draft 3 of the wire protocol, plus control over the shared multiplexed
 * connection.
 */
export interface ConnectWebSocketDraft3Transport extends Transport {
  /**
   * Closes the shared multiplexed connection, if one is open, terminating
   * any RPCs still running on it. The transport remains usable: the next
   * RPC dials a new connection.
   */
  close(): void;
}

export interface ConnectWebSocketDraft3TransportOptions {
  baseUrl: string;
  useBinaryFormat?: boolean;
  interceptors?: Interceptor[];
  jsonOptions?: Partial<JsonReadOptions & JsonWriteOptions>;
  binaryOptions?: Partial<BinaryReadOptions & BinaryWriteOptions>;
  defaultTimeoutMs?: number;
  /**
   * Dial a dedicated WebSocket connection for each streaming RPC instead of
   * multiplexing all RPCs onto one shared connection; see the draft 4
   * transports.
   */
  connectionPerStream?: boolean;
  /**
   * Offer only the identity subprotocol (connect.bidi.d3), so no frame is
   * ever compressed in either direction. The default offers
   * connect.bidi.d3.deflate first: per-frame raw DEFLATE for payloads of
   * 512 bytes and up, negotiated once per connection.
   */
  withoutCompression?: boolean;
}

interface MuxStreamEntry {
  controller: ReadableStreamDefaultController<Draft3Frame>;
  /** The server finished this stream with an end-stream frame. */
  endSeen: boolean;
}

/**
 * One WebSocket connection carrying any number of concurrent RPC streams
 * with draft 3 framing. A single onmessage handler routes frames to the
 * stream they belong to, inflating compressed payloads in arrival order.
 * The socket is opened lazily and re-opened if it failed.
 */
class WebSocketMuxDraft3 {
  private readonly url: string;
  private readonly closeWhenIdle: boolean;
  private readonly offered: string[];
  private socket: WebSocket | undefined;
  private opening: Promise<WebSocket> | undefined;
  private compression = false;
  private nextStreamId = 1;
  private readonly entries = new Map<number, MuxStreamEntry>();
  /** Serializes async inflation so frames keep their arrival order. */
  private routeChain: Promise<void> = Promise.resolve();

  constructor(
    url: string,
    closeWhenIdle: boolean,
    withoutCompression: boolean,
  ) {
    this.url = url;
    this.closeWhenIdle = closeWhenIdle;
    this.offered = withoutCompression
      ? [subprotocolIdentity]
      : [subprotocolDeflate, subprotocolIdentity];
  }

  async openStream(): Promise<{
    streamId: number;
    readable: ReadableStream<Draft3Frame>;
    writable: WritableStream<Draft3Frame>;
  }> {
    const socket = await this.open();
    const streamId = this.nextStreamId++;
    let controller!: ReadableStreamDefaultController<Draft3Frame>;
    const readable = new ReadableStream<Draft3Frame>({
      start(readableController) {
        controller = readableController;
      },
    });
    this.entries.set(streamId, { controller, endSeen: false });
    const writable = new WritableStream<Draft3Frame>({
      write: async (frame) => {
        socket.send(
          await this.encodeFrame(streamId, frame.type, frame.payload),
        );
      },
      close: async () => {
        // Half-close: an explicit end-stream frame.
        socket.send(
          await this.encodeFrame(
            streamId,
            frameTypeEndStream,
            new Uint8Array(),
          ),
        );
      },
      abort: () => {
        this.closeStream(streamId);
      },
    });
    return { streamId, readable, writable };
  }

  close(): void {
    const socket = this.socket;
    this.socket = undefined;
    this.failAll(new ConnectError("WebSocket closed", Code.Unavailable));
    socket?.close(1000);
  }

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
      // Reset frames are empty, never compressed; encode inline.
      const frame = new Uint8Array(frameHeadLength);
      new DataView(frame.buffer).setUint32(0, streamId);
      frame[streamIdLength] = frameTypeReset;
      this.socket.send(frame);
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
   * Frames one payload for the wire, deflating it when the connection
   * negotiated compression, the payload meets the threshold, and deflate
   * actually shrinks it.
   */
  private async encodeFrame(
    streamId: number,
    type: number,
    payload: Uint8Array,
  ): Promise<Uint8Array<ArrayBuffer>> {
    let descriptor = type;
    if (this.compression && payload.byteLength >= compressMinBytes) {
      const deflated = await deflateRaw(payload);
      if (deflated.byteLength < payload.byteLength) {
        payload = deflated;
        descriptor = type | frameCompressed;
      }
    }
    const frame = new Uint8Array(frameHeadLength + payload.byteLength);
    new DataView(frame.buffer).setUint32(0, streamId);
    frame[streamIdLength] = descriptor;
    frame.set(payload, frameHeadLength);
    return frame;
  }

  private async open(): Promise<WebSocket> {
    if (
      this.socket !== undefined &&
      this.socket.readyState === WebSocket.OPEN
    ) {
      return this.socket;
    }
    this.opening ??= new Promise<WebSocket>((resolve, reject) => {
      const socket = new WebSocket(this.url, this.offered);
      socket.binaryType = "arraybuffer";
      socket.onopen = () => {
        this.opening = undefined;
        if (
          socket.protocol !== subprotocolDeflate &&
          socket.protocol !== subprotocolIdentity
        ) {
          socket.close(1002);
          reject(
            new ConnectError(
              `server did not negotiate a draft 3 subprotocol (got ${JSON.stringify(socket.protocol)})`,
              Code.Unavailable,
            ),
          );
          return;
        }
        this.compression = socket.protocol === subprotocolDeflate;
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
        // Chain messages so async inflation cannot reorder a stream's
        // frames.
        const message = new Uint8Array(event.data as ArrayBuffer);
        this.routeChain = this.routeChain.then(() => this.route(message));
      };
    });
    this.opening.catch(() => {});
    return this.opening;
  }

  /** Route one incoming message to the stream it belongs to. */
  private async route(message: Uint8Array): Promise<void> {
    if (message.byteLength < frameHeadLength) {
      return;
    }
    const view = new DataView(
      message.buffer,
      message.byteOffset,
      message.byteLength,
    );
    const streamId = view.getUint32(0);
    const descriptor = view.getUint8(streamIdLength);
    const type = descriptor & ~frameCompressed & 0xff;
    const entry = this.entries.get(streamId);
    if (entry === undefined) {
      // A late frame for a stream already closed on this side; drop it.
      return;
    }
    let payload = message.subarray(frameHeadLength);
    if ((descriptor & frameCompressed) !== 0) {
      if (!this.compression) {
        this.failConnection(
          new ConnectError(
            "compressed frame on a connection that negotiated identity",
            Code.Internal,
          ),
        );
        return;
      }
      try {
        payload = await inflateRaw(payload);
      } catch (err) {
        this.failConnection(
          new ConnectError(
            `failed to inflate frame: ${ConnectError.from(err).rawMessage}`,
            Code.Internal,
          ),
        );
        return;
      }
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
    entry.controller.enqueue({ type, payload });
  }

  /** Tear the connection down after a protocol violation. */
  private failConnection(reason: ConnectError): void {
    const socket = this.socket;
    this.socket = undefined;
    this.failAll(reason);
    socket?.close(1002);
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

export function createConnectWebSocketDraft3Transport(
  options: ConnectWebSocketDraft3TransportOptions,
): ConnectWebSocketDraft3Transport {
  const useBinaryFormat = options.useBinaryFormat ?? false;
  const url = new URL("/websocket-draft3", options.baseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  const wsUrl = url.toString();
  const connectionPerStream = options.connectionPerStream ?? false;
  const withoutCompression = options.withoutCompression ?? false;
  const sharedMux = new WebSocketMuxDraft3(wsUrl, false, withoutCompression);

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
            ? new WebSocketMuxDraft3(wsUrl, true, withoutCompression)
            : sharedMux;
          const { streamId, readable, writable } = await mux.openStream();

          const path = new URL(req.url).pathname;
          const writer = writable.getWriter();
          await writer.write({
            type: frameTypeHeaders,
            payload: encodeHeadersFrame(req.header, { ":path": path }),
          });

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
