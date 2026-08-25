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

// The layer every WebSocket wire protocol draft shares: a message-oriented
// connection carrying multiplexed streams, and the options common to
// serving one. The drafts differ only in what a single message contains,
// which is what their own wire-draftN modules describe.

import type { HandleBidiSocketOptions } from "./handle-bidi-socket.js";

/**
 * A full-duplex, message-oriented connection carrying any number of
 * multiplexed RPC streams -- in practice, a WebSocket. Unlike
 * DuplexByteStream, message boundaries are significant: the readable must
 * yield exactly one WebSocket message per chunk, and every chunk written to
 * the writable must be sent as one WebSocket message, because each message
 * begins with the stream ID it belongs to.
 */
export interface DuplexMessageStream {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  /**
   * Close the underlying connection, optionally with a WebSocket close
   * code and reason the peer can surface to its callers. Called once the
   * connection's read side has ended and every in-flight RPC has
   * finished, and eagerly (with a reason) when an idle timeout fires.
   */
  close?: (code?: number, reason?: string) => void;
}

export interface HandleMuxedBidiSocketOptions extends HandleBidiSocketOptions {
  /**
   * Tears the connection down after this many milliseconds without an
   * incoming frame: in-flight RPCs are aborted and the socket is closed.
   *
   * Recommended on pay-per-use runtimes such as Cloudflare Workers, where
   * an idle connection otherwise pins a live invocation until the platform
   * reaps it (and logs the request as hung). Streams that are actively
   * receiving are kept alive by their own traffic; only a fully quiet peer
   * is disconnected. Well-behaved clients dial a fresh connection on their
   * next RPC.
   */
  idleTimeoutMs?: number;
}
