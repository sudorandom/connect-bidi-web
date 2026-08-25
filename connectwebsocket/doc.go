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
// lives in its own subpackage, with its own constructors and default path,
// so their designs can be compared side by side:
//
//   - [github.com/sudorandom/connect-bidi-web/connectwebsocket/draft1]:
//     Connect envelopes and connect-*-encoding compression metadata.
//   - [github.com/sudorandom/connect-bidi-web/connectwebsocket/draft3]:
//     a 5-byte binary frame head, with per-frame DEFLATE negotiated by
//     a WebSocket subprotocol.
//   - [github.com/sudorandom/connect-bidi-web/connectwebsocket/draft4]:
//     an ASCII text frame head ("id|type|payload") and JSON-only control
//     payloads, for tool visibility.
package connectwebsocket
