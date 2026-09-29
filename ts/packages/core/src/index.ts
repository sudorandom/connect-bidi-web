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

export { handleBidiSocket } from "./handle-bidi-socket.js";
export type {
  DuplexByteStream,
  HandleBidiSocketOptions,
} from "./handle-bidi-socket.js";
export type {
  DuplexMessageStream,
  HandleMuxedBidiSocketOptions,
} from "./muxed-bidi-socket.js";
export { handleBidiSocketDraft1 } from "./handle-bidi-socket-draft1.js";
export type { HandleBidiSocketDraft1Options } from "./handle-bidi-socket-draft1.js";
export type { Draft1Envelope, Draft1MessageStream } from "./wire-draft1.js";
export {
  decodeDraft1Envelope,
  draft1EnvelopeHeadLength,
  draft1ProcedureFromPath,
  draft1Subprotocol,
  encodeDraft1Envelope,
} from "./wire-draft1.js";
export { handleMuxedBidiSocketDraft3 } from "./handle-muxed-bidi-socket-draft3.js";
export type { HandleMuxedBidiSocketDraft3Options } from "./handle-muxed-bidi-socket-draft3.js";
export {
  draft3SubprotocolDeflate,
  draft3SubprotocolIdentity,
} from "./wire-draft3.js";
export { handleMuxedBidiSocketDraft4 } from "./handle-muxed-bidi-socket-draft4.js";
export type {
  Draft4DuplexMessageStream,
  Draft4OutgoingFrame,
  Draft4StreamFrame,
} from "./wire-draft4.js";
export {
  decodeDraft4StreamFrame,
  draft4FrameFlagsMask,
  draft4FrameTypeData,
  draft4FrameTypeEndStream,
  draft4FrameTypeHeaders,
  draft4FrameTypeMask,
  draft4FrameTypeOf,
  draft4FrameTypeReset,
  encodeDraft4StreamFrame,
} from "./wire-draft4.js";
export { handleBidiSocketDraft5 } from "./handle-bidi-socket-draft5.js";
export type {
  Draft5Message,
  Draft5MessageStream,
  HandleBidiSocketDraft5Options,
} from "./handle-bidi-socket-draft5.js";
export {
  draft5MessageText,
  draft5Subprotocol,
  draft5TextMessage,
} from "./handle-bidi-socket-draft5.js";
export {
  draft7DefaultServerTimeoutMs,
  draft7MessageText,
  draft7TextMessage,
  handleBidiSocketDraft7,
} from "./handle-bidi-socket-draft7.js";
export type { HandleBidiSocketDraft7Options } from "./handle-bidi-socket-draft7.js";
export type {
  Draft7Codec,
  Draft7Message,
  Draft7MessageStream,
} from "./wire-draft7.js";
export {
  checkDraft7BodyFrame,
  decodeDraft7Message,
  decodeDraft7Metadata,
  draft7DefaultInfrastructureHeaders,
  draft7MarkerBody,
  draft7MarkerClientEndStream,
  draft7MarkerLeadingMetadata,
  draft7MarkerServerEndStream,
  draft7OriginIsSameHost,
  draft7ProtocolControlledHeaders,
  draft7ReservedHeaderReason,
  draft7SubprotocolForCodec,
  draft7Subprotocols,
  draft7TimeoutQueryParameter,
  encodeDraft7Message,
  encodeDraft7Metadata,
  parseDraft7Timeout,
  parseSubprotocolHeader,
  selectDraft7Subprotocol,
  supportedDraft7Subprotocols,
  unknownDraft7MarkerError,
} from "./wire-draft7.js";
