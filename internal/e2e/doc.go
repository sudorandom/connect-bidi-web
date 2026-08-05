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

// Package e2e contains end-to-end tests that exercise the WebSocket and
// WebTransport transports over real network connections, including
// cross-language interop between the Go and TypeScript implementations.
//
// The pure-Go tests always run. The interop tests spawn the Node.js server
// fixture in ts/packages/node/interop and skip when Node.js or the installed
// ts/ workspace is unavailable; set E2E_REQUIRE_INTEROP=1 to fail instead of
// skip (used in CI).
//
// cmd/elizaserver is the mirror-image fixture: a plain-HTTP Go server used by
// the TypeScript e2e tests in ts/packages/web/e2e.
package e2e
