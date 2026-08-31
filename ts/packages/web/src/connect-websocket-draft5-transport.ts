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
  UnaryResponse,
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

// Draft 5 of the WebSocket wire protocol. Where drafts 1, 3, and 4 rebuild
// HTTP inside one shared socket -- a stream ID on every frame, a frame type
// byte, a headers frame -- draft 5 gives each streaming RPC its own
// WebSocket and lets the handshake carry the request. What is left has no
// framing at all:
//
//     -> [text]   {"metadata":{...}}    the mandatory headers message
//     -> [text]   {"sentence":"hi"}     the codec's output, unadorned
//     -> [text]   ""                    the separator: half-close
//     <- [text]   {"metadata":{...}}
//     <- [text]   {"sentence":"hello"}
//     <- [text]   ""                    the separator: no more messages
//     <- [text]   {"metadata":{...}}    the Connect EndStreamResponse
//
// The opcode is the protocol's only framing, and RFC 6455 sends it anyway.
// Exactly one shape is reserved: an empty text message is the separator.
// Everything else is a message of the RPC, with position telling metadata
// from data. A payload goes out as text when it is non-empty and valid
// UTF-8 -- so a JSON stream reads as JSON in the Network tab -- and as
// binary otherwise, including when empty: an empty protobuf message
// encodes to zero bytes and must never read as a half-close. In JavaScript
// the distinction is free either way: a text message arrives as a string,
// a binary one as an ArrayBuffer.
//
// Unary RPCs never upgrade. They are delegated to an ordinary Connect
// transport, the same way the Go implementation takes an HTTP client
// alongside its WebSocket dialing. Must match the Go
// connectwebsocket/draft5 package and @sudorandom/connect-bidi-core's
// draft 5 server bridge byte for byte.

/** The WebSocket subprotocol every draft 5 handshake must offer. */
export const draft5Subprotocol = "connect.bidi.d5";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
/** Rejects invalid UTF-8 instead of substituting replacement characters. */
const strictDecoder = new TextDecoder("utf-8", { fatal: true });

/**
 * The payload as text, if it may travel as a text WebSocket message: it has
 * to be non-empty (an empty text message is the separator) and valid UTF-8.
 * Returns undefined when the message must go out as binary.
 */
function asText(payload: Uint8Array): string | undefined {
  if (payload.byteLength === 0) {
    return undefined;
  }
  try {
    return strictDecoder.decode(payload);
  } catch {
    return undefined;
  }
}

/** One message read off the socket, with the opcode that framed it. */
interface Draft5Message {
  /** True when the message arrived as text: metadata, or the separator. */
  text: boolean;
  data: Uint8Array;
}

export interface ConnectWebSocketDraft5TransportOptions {
  baseUrl: string;
  /**
   * The transport that carries unary RPCs. Draft 5 never upgrades for them:
   * a WebSocket handshake to carry one request and one response is a bad
   * trade, so unary calls are dispatched as ordinary Connect HTTP requests.
   *
   * Pass `createConnectTransport({ baseUrl })` from `@connectrpc/connect-web`
   * pointed at the same base URL. One endpoint serves both: the procedure
   * URL answers POST with Connect and GET+Upgrade with this protocol.
   */
  unaryTransport: Transport;
  /** Send protobuf binary rather than JSON. Defaults to true. */
  useBinaryFormat?: boolean;
  interceptors?: Interceptor[];
  jsonOptions?: Partial<JsonReadOptions & JsonWriteOptions>;
  binaryOptions?: Partial<BinaryReadOptions & BinaryWriteOptions>;
  defaultTimeoutMs?: number;
}

/**
 * One WebSocket carrying one RPC. There is no multiplexing to do and no
 * connection to keep: the socket opens when the RPC starts and closes when
 * it ends, so cancellation is just closing it.
 */
class Draft5Socket {
  private readonly socket: WebSocket;
  /** Messages received but not yet consumed by the reader. */
  private readonly queue: Draft5Message[] = [];
  /** Resolves the reader that is waiting for the next message. */
  private waiting: ((result: Draft5Message | undefined) => void) | undefined;
  private failure: ConnectError | undefined;
  private closed = false;

  private constructor(socket: WebSocket) {
    this.socket = socket;
    socket.onmessage = (event: MessageEvent) => {
      const data: unknown = event.data;
      // The opcode is the framing: a string is a text message (metadata, or
      // the separator when empty), an ArrayBuffer is an RPC message.
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
      // A close is only clean once the end-stream message has been read; the
      // reader decides, because the trailers are the only place a status can
      // appear. Signalling the end is enough here.
      this.closed = true;
      if (event.code !== 1000 && this.failure === undefined) {
        this.failure = new ConnectError(
          event.reason !== ""
            ? `WebSocket closed: ${event.reason}`
            : "WebSocket closed before the end-stream message",
          Code.Unavailable,
        );
      }
      this.push(undefined);
    };
  }

  /**
   * Open one WebSocket for one RPC, resolving once the server has selected
   * the draft 5 subprotocol.
   */
  static open(url: string, signal?: AbortSignal): Promise<Draft5Socket> {
    return new Promise<Draft5Socket>((resolve, reject) => {
      let socket: WebSocket;
      try {
        socket = new WebSocket(url, [draft5Subprotocol]);
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
        if (socket.protocol !== draft5Subprotocol) {
          // The handshake succeeded but the peer is not speaking draft 5.
          socket.close(1002);
          reject(
            new ConnectError(
              `server did not select the ${draft5Subprotocol} subprotocol`,
              Code.Unavailable,
            ),
          );
          return;
        }
        resolve(new Draft5Socket(socket));
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

  /** Send the JSON metadata of a headers message. */
  sendHeaders(payload: Uint8Array): void {
    this.socket.send(decoder.decode(payload));
  }

  /**
   * Send one RPC message. It travels as text when its payload is non-empty
   * and valid UTF-8, so JSON reads as JSON in the Network tab, and as
   * binary otherwise -- including when it is empty, because an empty text
   * message is the separator.
   */
  sendMessage(payload: Uint8Array): void {
    const text = asText(payload);
    if (text !== undefined) {
      this.socket.send(text);
      return;
    }
    // The codec allocates a fresh buffer per message, so this is never a
    // view onto a shared one; the cast just tells TypeScript that, rather
    // than paying for a copy on every message to prove it.
    this.socket.send(payload as Uint8Array<ArrayBuffer>);
  }

  /** Send the separator, half-closing the request direction. */
  sendSeparator(): void {
    if (this.socket.readyState === WebSocket.OPEN) {
      this.socket.send("");
    }
  }

  /** Read the next message, or undefined once the socket has ended. */
  read(): Promise<Draft5Message | undefined> {
    const next = this.queue.shift();
    if (next !== undefined) {
      return Promise.resolve(next);
    }
    if (this.closed) {
      return Promise.resolve(undefined);
    }
    return new Promise<Draft5Message | undefined>((resolve) => {
      this.waiting = resolve;
    });
  }

  /** The error that ended the socket, if it ended badly. */
  error(): ConnectError | undefined {
    return this.failure;
  }

  /** Close the socket, ending the RPC. */
  close(clean: boolean): void {
    if (
      this.socket.readyState === WebSocket.OPEN ||
      this.socket.readyState === WebSocket.CONNECTING
    ) {
      this.socket.close(clean ? 1000 : 1001);
    }
  }

  private push(message: Draft5Message | undefined): void {
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
 * createConnectWebSocketDraft5Transport creates a connect-es Transport that
 * carries streaming RPCs over a WebSocket per RPC, using draft 5 of the wire
 * protocol, and delegates unary RPCs to an ordinary Connect transport.
 */
export function createConnectWebSocketDraft5Transport(
  options: ConnectWebSocketDraft5TransportOptions,
): Transport {
  const useBinaryFormat = options.useBinaryFormat ?? true;
  const unaryTransport = options.unaryTransport;

  return {
    unary<I extends DescMessage, O extends DescMessage>(
      method: DescMethodUnary<I, O>,
      signal: AbortSignal | undefined,
      timeoutMs: number | undefined,
      header: HeadersInit | undefined,
      message: MessageInitShape<I>,
      contextValues?: ContextValues,
    ): Promise<UnaryResponse<I, O>> {
      // Never upgrades. The endpoint is an ordinary Connect endpoint, so
      // this is an ordinary Connect POST.
      return unaryTransport.unary(
        method,
        signal,
        timeoutMs,
        header,
        message,
        contextValues,
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
          // The procedure is the URL's path -- the handshake is the request
          // -- so nothing repeats it in the headers message.
          const socket = await Draft5Socket.open(
            webSocketUrl(req.url),
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

          socket.sendHeaders(encodeHeadersFrame(req.header));

          // Write request messages asynchronously so the response can be
          // read concurrently.
          const writePromise = (async () => {
            for await (const message of req.message) {
              socket.sendMessage(serialize(message));
            }
            socket.sendSeparator();
          })();
          writePromise.catch(() => {});
          // A failed write must also abort reads. This promise stays pending
          // forever when the writes succeed.
          const writeFailed = writePromise.then(
            () => new Promise<never>(() => {}),
          );
          writeFailed.catch(() => {});

          const readNext = async (): Promise<Draft5Message> => {
            const message = await Promise.race([socket.read(), writeFailed]);
            if (message === undefined) {
              throw (
                socket.error() ??
                new ConnectError(
                  "connection closed before the end-stream message",
                  Code.Unavailable,
                )
              );
            }
            return message;
          };

          // The mandatory first message is the response metadata.
          const first = await readNext();
          if (!first.text || first.data.byteLength === 0) {
            socket.close(false);
            throw new ConnectError(
              "protocol error: expected a response headers message first",
              Code.Internal,
            );
          }
          const { headers: responseHeaders } = decodeHeadersFrame(first.data);
          const responseTrailers = new Headers();

          async function* iterate() {
            try {
              for (;;) {
                const message = await readNext();
                if (!message.text || message.data.byteLength > 0) {
                  // A data message, text or binary alike: the opcode says
                  // only whether the payload was UTF-8 worth rendering.
                  yield parse(message.data);
                  continue;
                }
                // The separator: the next message is the end-stream
                // metadata, which is where the RPC's status lives.
                const end = await readNext();
                if (!end.text) {
                  throw new ConnectError(
                    "protocol error: expected an end-stream message after the separator",
                    Code.Internal,
                  );
                }
                const { error, metadata } = endStreamFromJson(end.data);
                metadata.forEach((value, key) => {
                  responseTrailers.append(key, value);
                });
                if (error) {
                  throw error;
                }
                break;
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
            trailer: responseTrailers,
            message: iterate(),
          };
        },
      });
    },
  };
}

/** Rewrite an RPC URL's scheme for a WebSocket dial. */
function webSocketUrl(rpcUrl: string): string {
  const url = new URL(rpcUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}
