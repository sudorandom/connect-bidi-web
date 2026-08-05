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

export { createConnectWebSocketTransport } from "./connect-websocket-transport.js";
export type { ConnectWebSocketTransportOptions } from "./connect-websocket-transport.js";
export { createConnectWebTransportTransport } from "./connect-webtransport-transport.js";
export type { ConnectWebTransportTransportOptions } from "./connect-webtransport-transport.js";
export { createCompositeTransport } from "./composite-transport.js";
export {
  decodeHeadersFrame,
  encodeHeadersFrame,
  type HeadersMessage,
} from "./headers-frame.js";
