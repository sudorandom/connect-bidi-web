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

// The pieces every draft's Workers handler shares: the upgrade check and
// the common options. Each draft's own handler module adds whatever its
// wire protocol needs on top.

import type { HandleMuxedBidiSocketOptions } from "@sudorandom/connect-bidi-core";

export interface CreateBidiWebSocketHandlerOptions
  extends HandleMuxedBidiSocketOptions {
  /**
   * Called if the connection's handler rejects (a bug in a handler
   * implementation; protocol errors are reported to the client instead of
   * throwing). Defaults to a no-op -- the fetch handler never awaits the
   * RPCs, so an unset `onError` would otherwise surface as a silently
   * swallowed rejection.
   */
  onError?: (error: unknown) => void;
}

export function isWebSocketUpgrade(request: Request): boolean {
  return request.headers.get("upgrade")?.toLowerCase() === "websocket";
}
