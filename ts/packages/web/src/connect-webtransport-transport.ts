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
  UnaryRequest,
  StreamRequest,
} from "@connectrpc/connect";
import { Code, ConnectError, createContextValues } from "@connectrpc/connect";
import {
  compressedFlag,
  createClientMethodSerializers,
  createMethodUrl,
  runStreamingCall,
  runUnaryCall,
} from "@connectrpc/connect/protocol";
import {
  endStreamFlag,
  endStreamFromJson,
  requestHeader,
} from "@connectrpc/connect/protocol-connect";
import type { WebTransportSession } from "./webtransport-helper.js";
import { runWebTransportCall } from "./webtransport-helper.js";

const flagEnvelopeData = 0x00;
// 0x07 rather than a single bit: no combination of Connect's bitmask flags
// (0x01 compressed, 0x02 end-stream) can produce it.
const flagEnvelopeHeaders = 0x07;

export interface ConnectWebTransportTransportOptions {
  /**
   * Base URI for the WebTransport server (e.g. "https://example.com/webtransport").
   */
  baseUrl: string;

  /**
   * The WebTransport session, or a function/promise that returns one.
   */
  session:
    | WebTransportSession
    | (() => Promise<WebTransportSession> | WebTransportSession);

  useBinaryFormat?: boolean;
  interceptors?: Interceptor[];
  jsonOptions?: Partial<JsonReadOptions & JsonWriteOptions>;
  binaryOptions?: Partial<BinaryReadOptions & BinaryWriteOptions>;
  defaultTimeoutMs?: number;
}

/**
 * Create a Transport for the Connect protocol running over WebTransport.
 */
export function createConnectWebTransportTransport(
  options: ConnectWebTransportTransportOptions,
): Transport {
  const useBinaryFormat = options.useBinaryFormat ?? false;
  return {
    async unary<I extends DescMessage, O extends DescMessage>(
      method: DescMethodUnary<I, O>,
      signal: AbortSignal | undefined,
      timeoutMs: number | undefined,
      header: HeadersInit | undefined,
      message: MessageInitShape<I>,
      contextValues?: ContextValues,
    ): Promise<UnaryResponse<I, O>> {
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

      return await runUnaryCall<I, O>({
        interceptors: options.interceptors,
        signal,
        timeoutMs,
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
          const resolvedSession =
            typeof options.session === "function"
              ? await options.session()
              : await options.session;
          const path = `/${method.parent.typeName}/${method.name}`;

          await resolvedSession.ready;

          const requestMessageBytes = serialize(req.message);

          async function* singleMessage() {
            yield requestMessageBytes;
          }

          const { responseHeaders, responseMessages } =
            await runWebTransportCall(
              resolvedSession,
              req.header,
              singleMessage(),
              flagEnvelopeHeaders,
              flagEnvelopeData,
              path,
            );

          let responseMessage: Uint8Array | undefined;
          let endStreamReceived = false;
          const trailer = new Headers();

          for await (const env of responseMessages) {
            if (
              env.flags === flagEnvelopeData ||
              env.flags === compressedFlag
            ) {
              if (responseMessage !== undefined) {
                throw new ConnectError(
                  "unary stream has multiple messages",
                  Code.Unimplemented,
                );
              }
              responseMessage = env.data;
            } else if (env.flags === endStreamFlag) {
              endStreamReceived = true;
              const endStream = endStreamFromJson(env.data);
              if (endStream.error) {
                const error = endStream.error;
                responseHeaders.forEach((value, key) => {
                  error.metadata.append(key, value);
                });
                throw error;
              }
              endStream.metadata.forEach((value, key) => {
                trailer.set(key, value);
              });
            } else {
              throw new ConnectError(
                `protocol error: unexpected envelope flag 0x${env.flags.toString(16)}`,
                Code.Internal,
              );
            }
          }

          if (!endStreamReceived) {
            throw new ConnectError("missing EndStreamResponse", Code.Internal);
          }
          if (responseMessage === undefined) {
            throw new ConnectError(
              "unary stream has no message",
              Code.Unimplemented,
            );
          }

          return {
            stream: false,
            service: method.parent,
            method,
            header: responseHeaders,
            message: parse(responseMessage),
            trailer,
          };
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
          const resolvedSession =
            typeof options.session === "function"
              ? await options.session()
              : await options.session;
          const path = `/${method.parent.typeName}/${method.name}`;

          await resolvedSession.ready;

          async function* serializeRequestMessages() {
            for await (const msg of req.message) {
              yield serialize(msg);
            }
          }

          const { responseHeaders, responseMessages } =
            await runWebTransportCall(
              resolvedSession,
              req.header,
              serializeRequestMessages(),
              flagEnvelopeHeaders,
              flagEnvelopeData,
              path,
            );

          const trailer = new Headers();

          async function* parseResponseBody() {
            let endStreamReceived = false;
            for await (const env of responseMessages) {
              if (
                env.flags === flagEnvelopeData ||
                env.flags === compressedFlag
              ) {
                yield parse(env.data);
              } else if (env.flags === endStreamFlag) {
                endStreamReceived = true;
                const endStream = endStreamFromJson(env.data);
                if (endStream.error) {
                  const error = endStream.error;
                  responseHeaders.forEach((value, key) => {
                    error.metadata.append(key, value);
                  });
                  throw error;
                }
                endStream.metadata.forEach((value, key) => {
                  trailer.set(key, value);
                });
              } else {
                throw new ConnectError(
                  `protocol error: unexpected envelope flag 0x${env.flags.toString(16)}`,
                  Code.Internal,
                );
              }
            }
            if (!endStreamReceived) {
              throw new ConnectError(
                "missing EndStreamResponse",
                Code.Internal,
              );
            }
          }

          return {
            stream: true,
            service: method.parent,
            method,
            header: responseHeaders,
            trailer,
            message: parseResponseBody(),
          };
        },
      });
    },
  };
}
