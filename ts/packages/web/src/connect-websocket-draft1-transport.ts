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

// Draft 1 of the WebSocket wire protocol, and the shape the later drafts
// are variations on. Each streaming RPC gets its own WebSocket; unary calls
// never upgrade. Every message on the socket is exactly one Connect
// envelope:
//
//     -> [bin]  0x06 <len> {"metadata":{...}}   the headers envelope
//     -> [bin]  0x00 <len> <message>            one RPC message
//     -> [bin]  0x02 <len=0>                    end-stream: half-close
//     <- [bin]  0x06 <len> {"metadata":{...}}
//     <- [bin]  0x00 <len> <message>
//     <- [bin]  0x02 <len> {"metadata":{...}}   the EndStreamResponse
//
// There is no stream ID and no reset frame, because nothing is multiplexed
// and cancelling is closing the socket. Each direction opens with a headers
// envelope -- the server's as well as the client's -- because browser APIs
// can neither attach custom headers to the upgrade request nor read them
// off the response. The envelope's length field restates the WebSocket
// message boundary; it is kept so the envelope stays Connect's own, byte
// for byte, which is the redundancy draft 5 later dropped.
//
// Compression is the native permessage-deflate extension, negotiated by the
// browser during the handshake and invisible here. The envelope's
// compressed-data flag (0x01) therefore goes unused.
//
// Must match the Go connectwebsocket/draft1 package and
// @sudorandom/connect-bidi-core's draft 1 server bridge byte for byte.

/** The WebSocket subprotocol every draft 1 handshake must offer. */
export const draft1Subprotocol = "connect.bidi.d1";

/** Envelope flags. Complete byte values, not bitmasks. */
const flagData = 0x00;
const flagEndStream = 0x02;
const flagHeaders = 0x06;
const envelopeHeadLength = 5;

const empty = new Uint8Array(0);

/** Encode one message: a Connect envelope head followed by its payload. */
function encodeEnvelope(flag: number, payload: Uint8Array): Uint8Array {
  const message = new Uint8Array(envelopeHeadLength + payload.byteLength);
  const view = new DataView(message.buffer);
  view.setUint8(0, flag);
  view.setUint32(1, payload.byteLength);
  message.set(payload, envelopeHeadLength);
  return message;
}

/** One message read off the socket, split into flag and payload. */
interface Draft1Envelope {
  flag: number;
  payload: Uint8Array;
}

/**
 * Split one message into its flag and payload. A message carries one whole
 * envelope and nothing else, so a length that disagrees with the message
 * boundary means the peer framed something this protocol cannot represent.
 */
function decodeEnvelope(message: Uint8Array): Draft1Envelope {
  if (message.byteLength < envelopeHeadLength) {
    throw new ConnectError(
      `envelope too short: ${message.byteLength} bytes`,
      Code.Internal,
    );
  }
  const view = new DataView(
    message.buffer,
    message.byteOffset,
    message.byteLength,
  );
  const declared = view.getUint32(1);
  const actual = message.byteLength - envelopeHeadLength;
  if (declared !== actual) {
    throw new ConnectError(
      `envelope declares ${declared} payload bytes but the message carries ${actual}`,
      Code.Internal,
    );
  }
  return {
    flag: view.getUint8(0),
    payload: message.subarray(envelopeHeadLength),
  };
}

export interface ConnectWebSocketDraft1TransportOptions {
  /**
   * The endpoint the procedure is appended to, so a call to
   * `/pkg.Service/Method` dials `${baseUrl}/pkg.Service/Method`. An
   * `https://` base URL is accepted and rewritten for the dial.
   */
  baseUrl: string;
  /**
   * The transport that carries unary RPCs. Draft 1 never upgrades for them:
   * a WebSocket handshake to carry one request and one response is a bad
   * trade, and it spends one of the browser's limited per-host WebSocket
   * slots.
   *
   * Pass `createConnectTransport({ baseUrl })` from `@connectrpc/connect-web`
   * pointed at the same base URL, or compose the two with
   * `createCompositeTransport`.
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
class Draft1Socket {
  private readonly socket: WebSocket;
  /** Messages received but not yet consumed by the reader. */
  private readonly queue: Uint8Array[] = [];
  /** Resolves the reader that is waiting for the next message. */
  private waiting: ((result: Uint8Array | undefined) => void) | undefined;
  private failure: ConnectError | undefined;
  private closed = false;

  private constructor(socket: WebSocket) {
    this.socket = socket;
    socket.onmessage = (event: MessageEvent) => {
      const data: unknown = event.data;
      if (typeof data === "string") {
        // Draft 1 messages are always binary: an envelope is a binary
        // structure however its payload is encoded.
        this.fail(
          new ConnectError(
            "protocol error: expected a binary websocket message carrying an envelope",
            Code.Internal,
          ),
        );
        return;
      }
      this.push(new Uint8Array(data as ArrayBuffer));
    };
    socket.onerror = () => {
      this.fail(
        new ConnectError("WebSocket connection failed", Code.Unavailable),
      );
    };
    socket.onclose = (event: CloseEvent) => {
      // A close is only clean once the end-stream envelope has been read;
      // the reader decides, because the trailers are the only place a
      // status can appear. Signalling the end is enough here.
      this.closed = true;
      if (event.code !== 1000 && this.failure === undefined) {
        this.failure = new ConnectError(
          event.reason !== ""
            ? `WebSocket closed: ${event.reason}`
            : "WebSocket closed before the end-stream envelope",
          Code.Unavailable,
        );
      }
      this.push(undefined);
    };
  }

  /**
   * Open one WebSocket for one RPC, resolving once the server has selected
   * the draft 1 subprotocol.
   */
  static open(url: string, signal?: AbortSignal): Promise<Draft1Socket> {
    return new Promise<Draft1Socket>((resolve, reject) => {
      let socket: WebSocket;
      try {
        socket = new WebSocket(url, [draft1Subprotocol]);
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
        if (socket.protocol !== draft1Subprotocol) {
          // The handshake succeeded but the peer is not speaking draft 1.
          socket.close(1002);
          reject(
            new ConnectError(
              `server did not select the ${draft1Subprotocol} subprotocol`,
              Code.Unavailable,
            ),
          );
          return;
        }
        resolve(new Draft1Socket(socket));
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

  /** Send one envelope as one binary message. */
  send(flag: number, payload: Uint8Array): void {
    if (this.socket.readyState !== WebSocket.OPEN) {
      return;
    }
    // The codec allocates a fresh buffer per message, so this is never a
    // view onto a shared one; the cast just tells TypeScript that, rather
    // than paying for a copy on every message to prove it.
    this.socket.send(encodeEnvelope(flag, payload) as Uint8Array<ArrayBuffer>);
  }

  /** Read the next message, or undefined once the socket has ended. */
  read(): Promise<Uint8Array | undefined> {
    const next = this.queue.shift();
    if (next !== undefined) {
      return Promise.resolve(next);
    }
    if (this.closed) {
      return Promise.resolve(undefined);
    }
    return new Promise<Uint8Array | undefined>((resolve) => {
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

  private push(message: Uint8Array | undefined): void {
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
 * createConnectWebSocketDraft1Transport creates a connect-es Transport that
 * carries streaming RPCs over a WebSocket per RPC, using draft 1 of the
 * wire protocol, and delegates unary RPCs to an ordinary Connect transport.
 */
export function createConnectWebSocketDraft1Transport(
  options: ConnectWebSocketDraft1TransportOptions,
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
          // The URL names the procedure, so the headers envelope carries
          // metadata only -- no ":path".
          const socket = await Draft1Socket.open(
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

          socket.send(flagHeaders, encodeHeadersFrame(req.header));

          // Write request messages asynchronously so the response can be
          // read concurrently.
          const writePromise = (async () => {
            for await (const message of req.message) {
              socket.send(flagData, serialize(message));
            }
            socket.send(flagEndStream, empty);
          })();
          writePromise.catch(() => {});
          // A failed write must also abort reads. This promise stays pending
          // forever when the writes succeed.
          const writeFailed = writePromise.then(
            () => new Promise<never>(() => {}),
          );
          writeFailed.catch(() => {});

          const readNext = async (): Promise<Draft1Envelope> => {
            const message = await Promise.race([socket.read(), writeFailed]);
            if (message === undefined) {
              throw (
                socket.error() ??
                new ConnectError(
                  "connection closed before the end-stream envelope",
                  Code.Unavailable,
                )
              );
            }
            return decodeEnvelope(message);
          };

          // The mandatory first envelope is the response metadata.
          const first = await readNext();
          if (first.flag !== flagHeaders) {
            socket.close(false);
            throw new ConnectError(
              "protocol error: expected a response headers envelope first",
              Code.Internal,
            );
          }
          const { headers: responseHeaders } = decodeHeadersFrame(
            first.payload,
          );
          const responseTrailers = new Headers();

          async function* iterate() {
            try {
              for (;;) {
                const envelope = await readNext();
                if (envelope.flag === flagData) {
                  yield parse(envelope.payload);
                  continue;
                }
                if (envelope.flag !== flagEndStream) {
                  throw new ConnectError(
                    `protocol error: unexpected envelope flag 0x${envelope.flag.toString(16)}`,
                    Code.Internal,
                  );
                }
                // The end-stream envelope is where the RPC's status lives.
                const { error, metadata } = endStreamFromJson(envelope.payload);
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
