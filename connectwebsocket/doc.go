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

// Package connectwebsocket holds the transport-agnostic pieces shared by
// the WebSocket wire protocol drafts in its subpackages.
//
// The drafts themselves are wire-incompatible with one another and each
// lives in its own subpackage, with its own constructors, so their designs
// can be compared directly:
//
//   - [github.com/sudorandom/connect-bidi-web/connectwebsocket/draft1]:
//     a WebSocket per streaming RPC, with every message wrapped in a
//     standard 5-byte Connect envelope and each direction opening with a
//     headers envelope. Unary RPCs stay on HTTP. It has no path of its
//     own, mounting on the Connect procedure URLs instead.
//   - [github.com/sudorandom/connect-bidi-web/connectwebsocket/draft3]:
//     a 5-byte binary frame head, with per-frame DEFLATE negotiated by
//     a WebSocket subprotocol.
//   - [github.com/sudorandom/connect-bidi-web/connectwebsocket/draft4]:
//     an ASCII text frame head ("id|type|payload") and JSON-only control
//     payloads, for tool visibility.
//   - [github.com/sudorandom/connect-bidi-web/connectwebsocket/draft5]:
//     no framing and no multiplexing at all: one RPC per WebSocket, with
//     the upgrade request serving as the RPC request. It has no path of
//     its own, mounting on the Connect procedure URLs instead.
//   - [github.com/sudorandom/connect-bidi-web/connectwebsocket/draft7]:
//     the Connect-over-WebSocket specification: draft 5's connection
//     model with a one-byte marker on every message, the codec selected
//     by subprotocol, and the deadline on the handshake URI. It mounts on
//     the procedure URLs, or under a path prefix.
package connectwebsocket
