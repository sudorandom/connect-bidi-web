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

export {
  createConnectWebSocketDraft1Transport,
  draft1Subprotocol,
} from "./connect-websocket-draft1-transport.js";
export type { ConnectWebSocketDraft1TransportOptions } from "./connect-websocket-draft1-transport.js";
export { createConnectWebSocketDraft3Transport } from "./connect-websocket-draft3-transport.js";
export type {
  ConnectWebSocketDraft3Transport,
  ConnectWebSocketDraft3TransportOptions,
} from "./connect-websocket-draft3-transport.js";
export { createConnectWebSocketDraft4Transport } from "./connect-websocket-draft4-transport.js";
export type {
  ConnectWebSocketDraft4Transport,
  ConnectWebSocketDraft4TransportOptions,
} from "./connect-websocket-draft4-transport.js";
export {
  createConnectWebSocketDraft5Transport,
  draft5Subprotocol,
} from "./connect-websocket-draft5-transport.js";
export type { ConnectWebSocketDraft5TransportOptions } from "./connect-websocket-draft5-transport.js";
export { createConnectWebTransportTransport } from "./connect-webtransport-transport.js";
export type { ConnectWebTransportTransportOptions } from "./connect-webtransport-transport.js";
export type {
  WebTransportBidirectionalStream,
  WebTransportSession,
} from "./webtransport-helper.js";
export {
  createCompositeTransport,
  createAutoTransport,
} from "./composite-transport.js";
export { createFallbackTransport } from "./fallback-transport.js";
export type { FallbackTransport } from "./fallback-transport.js";
export {
  decodeHeadersFrame,
  encodeHeadersFrame,
  type HeadersMessage,
} from "./headers-frame.js";
export { createConnectWebSocketDraft7Transport } from "./connect-websocket-draft7-transport.js";
export type { ConnectWebSocketDraft7TransportOptions } from "./connect-websocket-draft7-transport.js";
export type { Draft7Codec } from "./wire-draft7.js";
export {
  draft7SubprotocolForCodec,
  draft7Subprotocols,
  draft7TimeoutQueryParameter,
} from "./wire-draft7.js";
