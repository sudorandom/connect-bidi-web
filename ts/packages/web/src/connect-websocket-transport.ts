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
  compressedFlag,
  createClientMethodSerializers,
  createMethodUrl,
  encodeEnvelope,
  runStreamingCall,
} from "@connectrpc/connect/protocol";
import {
  endStreamFlag,
  endStreamFromJson,
  requestHeader,
} from "@connectrpc/connect/protocol-connect";
import { runWebTransportCall } from "./webtransport-helper.js";

const flagEnvelopeData = 0x00;
const flagEnvelopeHeaders = 0x04;

export interface ConnectWebSocketTransportOptions {
  baseUrl: string;
  useBinaryFormat?: boolean;
  interceptors?: Interceptor[];
  jsonOptions?: Partial<JsonReadOptions & JsonWriteOptions>;
  binaryOptions?: Partial<BinaryReadOptions & BinaryWriteOptions>;
  defaultTimeoutMs?: number;
}

export function createConnectWebSocketTransport(
  options: ConnectWebSocketTransportOptions,
): Transport {
  const useBinaryFormat = options.useBinaryFormat ?? false;

  return {
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
          const url = new URL("/websocket", options.baseUrl);
          url.protocol = url.protocol === "https:" ? "wss:" : "ws:";

          const socket = new WebSocket(url.toString());
          socket.binaryType = "arraybuffer";

          await new Promise<void>((resolve, reject) => {
            socket.onopen = () => resolve();
            socket.onerror = () =>
              reject(
                new ConnectError(
                  "WebSocket connection failed",
                  Code.Unavailable,
                ),
              );
          });

          // Adapt WebSocket to ReadableStream/WritableStream pair
          const readable = new ReadableStream<Uint8Array>({
            start(controller) {
              socket.onmessage = (event) => {
                controller.enqueue(new Uint8Array(event.data));
              };
              socket.onclose = () => {
                controller.close();
              };
              socket.onerror = () => {
                controller.error(
                  new ConnectError(
                    "WebSocket closed with error",
                    Code.Unavailable,
                  ),
                );
              };
            },
          });

          const writable = new WritableStream<Uint8Array>({
            write(chunk) {
              socket.send(new Uint8Array(chunk));
            },
            close() {
              socket.send(encodeEnvelope(endStreamFlag, new Uint8Array()));
            },
            abort() {
              socket.close();
            },
          });

          const path = new URL(req.url).pathname;

          async function* rawMessageGenerator() {
            for await (const msg of req.message) {
              yield serialize(msg);
            }
          }

          const { responseHeaders, responseMessages } =
            await runWebTransportCall(
              {
                ready: Promise.resolve(),
                createBidirectionalStream: async () => ({ readable, writable }),
              },
              req.header,
              rawMessageGenerator(),
              flagEnvelopeHeaders,
              flagEnvelopeData,
              path,
            );

          const responseTrailers = new Headers();

          async function* iterate() {
            try {
              for await (const env of responseMessages) {
                if (env.flags === endStreamFlag) {
                  const { error, metadata } = endStreamFromJson(env.data);
                  metadata.forEach((val, key) => {
                    responseTrailers.append(key, val);
                  });
                  if (error) {
                    throw error;
                  }
                  break;
                }
                if (
                  env.flags !== flagEnvelopeData &&
                  env.flags !== compressedFlag
                ) {
                  throw new ConnectError(
                    `protocol error: unexpected envelope flag 0x${env.flags.toString(16)}`,
                    Code.Internal,
                  );
                }
                yield parse(env.data);
              }
            } finally {
              socket.close();
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
