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
  DescMethod,
  DescMethodStreaming,
  DescMethodUnary,
  JsonReadOptions,
  JsonWriteOptions,
  MessageInitShape,
} from "@bufbuild/protobuf";
import type {
  ContextValues,
  Interceptor,
  StreamRequest,
  StreamResponse,
  Transport,
  UnaryRequest,
  UnaryResponse,
} from "@connectrpc/connect";
import { Code, ConnectError, createContextValues } from "@connectrpc/connect";
import {
  createClientMethodSerializers,
  createMethodUrl,
  runStreamingCall,
  runUnaryCall,
} from "@connectrpc/connect/protocol";
import {
  endStreamFromJson,
  requestHeader,
} from "@connectrpc/connect/protocol-connect";
import type { Draft7Codec, Draft7Message } from "./wire-draft7.js";
import {
  checkDraft7BodyFrame,
  decodeDraft7Message,
  decodeDraft7Metadata,
  draft7MarkerBody,
  draft7MarkerClientEndStream,
  draft7MarkerLeadingMetadata,
  draft7MarkerServerEndStream,
  draft7ProtocolControlledHeaders,
  draft7SubprotocolForCodec,
  draft7TimeoutQueryParameter,
  encodeDraft7Message,
  encodeDraft7Metadata,
  unknownDraft7MarkerError,
} from "./wire-draft7.js";

// Draft 7 of the WebSocket wire protocol: the Connect-over-WebSocket
// specification. One WebSocket carries one RPC, dialed on the procedure's
// own URL, and every message is a one-byte marker and its payload:
//
//     -> [text]   M{"acme-tenant":["t-1"]}   leading metadata, first
//     -> [text]   B{"sentence":"hi"}         a body, in the negotiated codec
//     -> [text]   C                          client end-of-stream
//     <- [text]   M{}
//     <- [text]   B{"sentence":"hello"}
//     <- [text]   S{"metadata":{...}}        the Connect EndStreamResponse
//
// The frame type names the encoding: JSON is text, Protobuf binary is
// binary. The codec is selected by the subprotocol (connectrpc.1+json or
// connectrpc.1+proto), the deadline rides on the URI as
// connect-timeout-ms, and compression is permessage-deflate. Must match the
// Go connectwebsocket/draft7 package and @sudorandom/connect-bidi-core's
// draft 7 server bridge byte for byte.

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export interface ConnectWebSocketDraft7TransportOptions {
  baseUrl: string;
  /**
   * The WebSocket path prefix the server was configured with, if any: the
   * handshake URI is base + prefix + procedure, while unary RPCs delegated
   * to `unaryTransport` keep the bare procedure paths.
   */
  pathPrefix?: string;
  /**
   * The transport that carries unary RPCs, typically
   * `createConnectTransport({ baseUrl })` from `@connectrpc/connect-web`
   * pointed at the same base URL: every procedure URL answers POST with
   * Connect and an upgrade with this protocol. Without it, unary RPCs go
   * over a WebSocket of their own, exactly as streaming ones do.
   */
  unaryTransport?: Transport;
  /** Send protobuf binary rather than JSON. Defaults to true. */
  useBinaryFormat?: boolean;
  interceptors?: Interceptor[];
  jsonOptions?: Partial<JsonReadOptions & JsonWriteOptions>;
  binaryOptions?: Partial<BinaryReadOptions & BinaryWriteOptions>;
  defaultTimeoutMs?: number;
}

/** One received message, split into its marker and payload. */
interface Received {
  message: Draft7Message;
  marker: number;
  payload: Uint8Array;
}

/**
 * One WebSocket carrying one RPC. There is no multiplexing to do and no
 * connection to keep: the socket opens when the RPC starts and closes when
 * it ends, so cancellation is just closing it.
 */
class Draft7Socket {
  private readonly socket: WebSocket;
  private readonly queue: Draft7Message[] = [];
  private waiting: ((result: Draft7Message | undefined) => void) | undefined;
  private failure: ConnectError | undefined;
  private closed = false;

  private constructor(
    socket: WebSocket,
    private readonly codec: Draft7Codec,
  ) {
    this.socket = socket;
    socket.onmessage = (event: MessageEvent) => {
      const data: unknown = event.data;
      this.push(
        typeof data === "string"
          ? { text: true, data: encoder.encode(data) }
          : { text: false, data: new Uint8Array(data as ArrayBuffer) },
      );
    };
    socket.onerror = () => {
      this.fail(
        new ConnectError("WebSocket connection failed", Code.Unavailable),
      );
    };
    socket.onclose = (event: CloseEvent) => {
      // A close is never a success by itself, whatever its code: the S
      // message is the only place a status can appear, and the reader
      // checks for it before it looks here. A browser reports an absent
      // close message as 1006, which is why this only records a reason.
      this.closed = true;
      if (this.failure === undefined) {
        this.failure = new ConnectError(
          event.reason !== ""
            ? `WebSocket closed: ${event.reason}`
            : `WebSocket closed (code ${event.code}) before the end-of-stream message`,
          Code.Unavailable,
        );
      }
      this.push(undefined);
    };
  }

  /**
   * Open one WebSocket for one RPC, resolving once the server has echoed
   * the subprotocol for the codec in use. A browser cannot read the
   * handshake response, so the compression parameters the server chose
   * cannot be verified here; the Go client does.
   */
  static open(
    url: string,
    codec: Draft7Codec,
    signal?: AbortSignal,
  ): Promise<Draft7Socket> {
    const token = draft7SubprotocolForCodec(codec);
    return new Promise<Draft7Socket>((resolve, reject) => {
      let socket: WebSocket;
      try {
        socket = new WebSocket(url, [token]);
      } catch (err) {
        reject(
          new ConnectError(
            `failed to open WebSocket: ${String(err)}`,
            Code.Unavailable,
          ),
        );
        return;
      }
      socket.binaryType = "arraybuffer";
      const onAbort = () => {
        socket.close(1000);
        reject(new ConnectError("RPC canceled", Code.Canceled));
      };
      signal?.addEventListener("abort", onAbort, { once: true });
      socket.onopen = () => {
        signal?.removeEventListener("abort", onAbort);
        if (socket.protocol !== token) {
          // The handshake succeeded, but not with a token this client
          // offered; the peer is not speaking this protocol.
          socket.close();
          reject(
            new ConnectError(
              `server selected subprotocol ${JSON.stringify(socket.protocol)}, not ${token}`,
              Code.Unavailable,
            ),
          );
          return;
        }
        resolve(new Draft7Socket(socket, codec));
      };
      socket.onerror = () => {
        signal?.removeEventListener("abort", onAbort);
        // A failed handshake is indistinguishable from any other connection
        // failure here: JavaScript cannot read its status or headers. That
        // is why the protocol reports RPC-level errors on the socket.
        reject(
          new ConnectError("WebSocket connection failed", Code.Unavailable),
        );
      };
    });
  }

  private send(message: Draft7Message): void {
    if (this.socket.readyState !== WebSocket.OPEN) {
      return;
    }
    if (message.text) {
      this.socket.send(decoder.decode(message.data));
      return;
    }
    // encodeDraft7Message allocates a fresh buffer per message, so this is
    // never a view onto a shared one; the cast just tells TypeScript that.
    this.socket.send(message.data as Uint8Array<ArrayBuffer>);
  }

  /** Send the leading-metadata message. */
  sendMetadata(headers: Headers): void {
    this.send(
      encodeDraft7Message(
        draft7MarkerLeadingMetadata,
        encodeDraft7Metadata(headers),
        true,
      ),
    );
  }

  /** Send one body: text under JSON, binary under Protobuf. */
  sendBody(payload: Uint8Array): void {
    this.send(
      encodeDraft7Message(draft7MarkerBody, payload, this.codec === "json"),
    );
  }

  /** Send a bare client end-of-stream. */
  sendEndStream(): void {
    this.send(
      encodeDraft7Message(draft7MarkerClientEndStream, new Uint8Array(0), true),
    );
  }

  /** Read the next message, or undefined once the socket has ended. */
  read(): Promise<Draft7Message | undefined> {
    const next = this.queue.shift();
    if (next !== undefined) {
      return Promise.resolve(next);
    }
    if (this.closed) {
      return Promise.resolve(undefined);
    }
    return new Promise<Draft7Message | undefined>((resolve) => {
      this.waiting = resolve;
    });
  }

  /** The error that ended the socket, if it ended badly. */
  error(): ConnectError | undefined {
    return this.failure;
  }

  /**
   * Close the socket, ending the RPC: with 1000 when the RPC completed,
   * and with no code at all otherwise. The browser WebSocket API permits a
   * script only 1000 and the 3000–4999 range, so the 1009 the
   * specification suggests for an oversized message cannot be sent from
   * JavaScript; abandoning the connection says the same thing.
   */
  close(clean: boolean): void {
    if (
      this.socket.readyState === WebSocket.OPEN ||
      this.socket.readyState === WebSocket.CONNECTING
    ) {
      if (clean) {
        this.socket.close(1000);
      } else {
        this.socket.close();
      }
    }
  }

  private push(message: Draft7Message | undefined): void {
    const waiting = this.waiting;
    if (waiting !== undefined) {
      this.waiting = undefined;
      waiting(message);
      return;
    }
    if (message !== undefined) {
      this.queue.push(message);
    }
  }

  private fail(err: ConnectError): void {
    this.failure ??= err;
    this.closed = true;
    this.push(undefined);
  }
}

/**
 * The response side of one RPC: reads M, then bodies, then S, applying the
 * receiver's rules and failing the RPC on a violation by the server.
 */
class ResponseReader {
  readonly trailers = new Headers();
  private ended = false;

  constructor(
    private readonly socket: Draft7Socket,
    private readonly codec: Draft7Codec,
    private readonly writeFailed: Promise<never>,
  ) {}

  private async next(): Promise<Received> {
    const message = await Promise.race([this.socket.read(), this.writeFailed]);
    if (message === undefined) {
      throw (
        this.socket.error() ??
        new ConnectError(
          "connection closed before the end-of-stream message",
          Code.Unavailable,
        )
      );
    }
    try {
      const { marker, payload } = decodeDraft7Message(message);
      return { message, marker, payload };
    } catch (err) {
      throw this.violation(ConnectError.from(err, Code.Internal));
    }
  }

  /** A violation by the server ends the RPC and drops the connection. */
  private violation(err: ConnectError): ConnectError {
    this.socket.close(false);
    return err;
  }

  /** The mandatory first message. */
  async readMetadata(): Promise<Headers> {
    const first = await this.next();
    if (first.marker !== draft7MarkerLeadingMetadata) {
      throw this.violation(
        new ConnectError(
          `protocol error: expected the leading-metadata message first, got marker ${JSON.stringify(String.fromCharCode(first.marker))}`,
          Code.Internal,
        ),
      );
    }
    if (!first.message.text || first.payload.byteLength === 0) {
      throw this.violation(
        new ConnectError(
          "protocol error: the leading-metadata message must be a non-empty text frame",
          Code.Internal,
        ),
      );
    }
    try {
      return decodeDraft7Metadata(first.payload);
    } catch (err) {
      throw this.violation(ConnectError.from(err, Code.Internal));
    }
  }

  /**
   * The next body, or undefined once S has been read — in which case the
   * trailers are populated and an error carried by S is thrown.
   */
  async readBody(): Promise<Uint8Array | undefined> {
    if (this.ended) {
      return undefined;
    }
    const next = await this.next();
    switch (next.marker) {
      case draft7MarkerBody:
        try {
          checkDraft7BodyFrame(next.message, next.payload, this.codec);
        } catch (err) {
          throw this.violation(ConnectError.from(err, Code.Internal));
        }
        return next.payload;
      case draft7MarkerServerEndStream: {
        if (!next.message.text || next.payload.byteLength === 0) {
          throw this.violation(
            new ConnectError(
              "protocol error: the end-of-stream message must be a non-empty text frame",
              Code.Internal,
            ),
          );
        }
        let end: ReturnType<typeof endStreamFromJson>;
        try {
          end = endStreamFromJson(next.payload);
        } catch (err) {
          throw this.violation(ConnectError.from(err, Code.Internal));
        }
        this.ended = true;
        end.metadata.forEach((value, key) => {
          this.trailers.append(key, value);
        });
        if (end.error) {
          throw end.error;
        }
        return undefined;
      }
      case draft7MarkerLeadingMetadata:
        throw this.violation(
          new ConnectError(
            "protocol error: a second leading-metadata message from the server",
            Code.Internal,
          ),
        );
      case draft7MarkerClientEndStream:
        throw this.violation(
          new ConnectError(
            "protocol error: client end-of-stream marker from a server",
            Code.Internal,
          ),
        );
      default:
        throw this.violation(unknownDraft7MarkerError(next.marker));
    }
  }

  /** Whether S has been read: the RPC completed and may close politely. */
  finished(): boolean {
    return this.ended;
  }
}

/**
 * createConnectWebSocketDraft7Transport creates a connect-es Transport that
 * carries RPCs over a WebSocket per RPC, using draft 7 of the wire
 * protocol — the Connect-over-WebSocket specification. Streaming RPCs
 * always upgrade; unary RPCs are delegated to `unaryTransport` when one is
 * given, and upgrade too otherwise.
 */
export function createConnectWebSocketDraft7Transport(
  options: ConnectWebSocketDraft7TransportOptions,
): Transport {
  const useBinaryFormat = options.useBinaryFormat ?? true;
  const codec: Draft7Codec = useBinaryFormat ? "proto" : "json";
  const unaryTransport = options.unaryTransport;
  const prefix = normalizePrefix(options.pathPrefix);

  /** The handshake URI: base + prefix + procedure, deadline as a query. */
  function handshakeUrl(
    method: DescMethod,
    timeoutMs: number | undefined,
  ): string {
    const url = new URL(createMethodUrl(options.baseUrl, method));
    if (prefix !== "") {
      url.pathname = joinPath(
        url.pathname.slice(
          0,
          url.pathname.length - procedurePath(method).length,
        ),
        prefix + procedurePath(method),
      );
    }
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    if (timeoutMs !== undefined) {
      url.searchParams.set(
        draft7TimeoutQueryParameter,
        String(Math.max(1, Math.round(timeoutMs))),
      );
    }
    return url.toString();
  }

  /**
   * The metadata that goes in M: everything the caller and interceptors
   * set, minus the names the protocol carries elsewhere — the codec is the
   * subprotocol, the deadline is on the URI, and there is no message-level
   * compression.
   */
  function metadataFor(header: Headers): Headers {
    const metadata = new Headers();
    header.forEach((value, key) => {
      if (!draft7ProtocolControlledHeaders.has(key.toLowerCase())) {
        metadata.append(key, value);
      }
    });
    return metadata;
  }

  function resolveTimeout(timeoutMs: number | undefined): number | undefined {
    return timeoutMs === undefined
      ? options.defaultTimeoutMs
      : timeoutMs <= 0
        ? undefined
        : timeoutMs;
  }

  return {
    async unary<I extends DescMessage, O extends DescMessage>(
      method: DescMethodUnary<I, O>,
      signal: AbortSignal | undefined,
      timeoutMs: number | undefined,
      header: HeadersInit | undefined,
      message: MessageInitShape<I>,
      contextValues?: ContextValues,
    ): Promise<UnaryResponse<I, O>> {
      if (unaryTransport !== undefined) {
        // The endpoint is an ordinary Connect endpoint, so this is an
        // ordinary Connect POST.
        return unaryTransport.unary(
          method,
          signal,
          timeoutMs,
          header,
          message,
          contextValues,
        );
      }
      const { serialize, parse } = createClientMethodSerializers(
        method,
        useBinaryFormat,
        options.jsonOptions,
        options.binaryOptions,
      );
      timeoutMs = resolveTimeout(timeoutMs);
      return await runUnaryCall<I, O>({
        interceptors: options.interceptors,
        timeoutMs,
        signal,
        req: {
          stream: false,
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
          message,
        },
        next: async (req: UnaryRequest<I, O>): Promise<UnaryResponse<I, O>> => {
          // The specification's worked example: M, B, C; then M, B, S.
          const socket = await Draft7Socket.open(
            handshakeUrl(method, timeoutMs),
            codec,
            req.signal,
          );
          const onAbort = () => socket.close(false);
          req.signal.addEventListener("abort", onAbort, { once: true });
          const reader = new ResponseReader(
            socket,
            codec,
            new Promise<never>(() => {}),
          );
          try {
            socket.sendMetadata(metadataFor(req.header));
            socket.sendBody(serialize(req.message));
            socket.sendEndStream();
            const responseHeaders = await reader.readMetadata();
            const body = await reader.readBody();
            if (body === undefined) {
              throw new ConnectError(
                "unary response has zero messages",
                Code.Unimplemented,
              );
            }
            if ((await reader.readBody()) !== undefined) {
              throw new ConnectError(
                "unary response has multiple messages",
                Code.Unimplemented,
              );
            }
            return {
              stream: false,
              service: method.parent,
              method,
              header: responseHeaders,
              trailer: reader.trailers,
              message: parse(body),
            };
          } finally {
            req.signal.removeEventListener("abort", onAbort);
            socket.close(reader.finished());
          }
        },
      });
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
      timeoutMs = resolveTimeout(timeoutMs);

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
          const socket = await Draft7Socket.open(
            handshakeUrl(method, timeoutMs),
            codec,
            req.signal,
          );

          let finished = false;
          const onAbort = () => {
            // One socket is one RPC, so cancelling is closing it.
            if (!finished) {
              socket.close(false);
            }
          };
          req.signal.addEventListener("abort", onAbort, { once: true });

          socket.sendMetadata(metadataFor(req.header));

          // Write request messages asynchronously so the response can be
          // read concurrently.
          const writePromise = (async () => {
            for await (const message of req.message) {
              socket.sendBody(serialize(message));
            }
            socket.sendEndStream();
          })();
          writePromise.catch(() => {});
          // A failed write must also abort reads. This promise stays pending
          // forever when the writes succeed.
          const writeFailed = writePromise.then(
            () => new Promise<never>(() => {}),
          );
          writeFailed.catch(() => {});

          const reader = new ResponseReader(socket, codec, writeFailed);
          const responseHeaders = await reader.readMetadata();

          async function* iterate() {
            try {
              for (;;) {
                const body = await reader.readBody();
                if (body === undefined) {
                  break;
                }
                yield parse(body);
              }
              await writePromise;
              finished = true;
            } finally {
              req.signal.removeEventListener("abort", onAbort);
              socket.close(finished);
            }
          }

          return {
            stream: true,
            service: method.parent,
            method,
            header: responseHeaders,
            trailer: reader.trailers,
            message: iterate(),
          };
        },
      });
    },
  };
}

/** The procedure path a method is served at: "/package.Service/Method". */
function procedurePath(method: DescMethod): string {
  return `/${method.parent.typeName}/${method.name}`;
}

/** A prefix beginning with "/" and not ending with one, or "" for none. */
function normalizePrefix(prefix: string | undefined): string {
  if (prefix === undefined || prefix === "" || prefix === "/") {
    return "";
  }
  const trimmed = prefix.replace(/\/+$/, "");
  return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
}

/** Joins a base path and a procedure path with exactly one "/" between. */
function joinPath(base: string, procedure: string): string {
  return `${base.replace(/\/+$/, "")}${procedure}`;
}
