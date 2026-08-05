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

const SERVER_STORAGE_KEY = "connect-bidi-web:server-url";

/**
 * Reports whether `value` is a well-formed http(s) URL.
 *
 * Used to reject garbage input before it ever reaches `new URL(...)` calls
 * deeper in the transport setup (e.g. when deriving the WebTransport or
 * WebSocket endpoint from the server URL), which would otherwise throw an
 * unhandled `TypeError`.
 */
export function isValidServerUrl(value: string): boolean {
  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:";
  } catch {
    return false;
  }
}

/**
 * Resolves the base URL of the Connect API server backing the live demo.
 *
 * Resolution order: the `?server=` query parameter, a value saved earlier
 * through `saveServerUrl`, then `window.location.origin` as a same-origin
 * fallback. This lets the same bundle run against a local Go server (e.g.
 * `https://localhost:4433`) and against wherever the page is deployed,
 * without a rebuild. Malformed overrides are ignored in favor of the next
 * candidate in the resolution order.
 */
export function resolveServerUrl(): string {
  const fromQuery = new URL(window.location.href).searchParams.get("server");
  if (fromQuery !== null && isValidServerUrl(fromQuery.trim())) {
    return fromQuery.trim();
  }

  const stored = window.localStorage.getItem(SERVER_STORAGE_KEY);
  if (stored !== null && isValidServerUrl(stored.trim())) {
    return stored.trim();
  }

  return window.location.origin;
}

/** Persists a server URL override so it survives a page reload. */
export function saveServerUrl(url: string): void {
  window.localStorage.setItem(SERVER_STORAGE_KEY, url);
}
