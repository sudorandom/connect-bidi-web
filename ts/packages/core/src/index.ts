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
export { handleMuxedBidiSocketDraft1 } from "./handle-muxed-bidi-socket-draft1.js";
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
