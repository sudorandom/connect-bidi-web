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

/**
 * Reports whether the WebTransport API is available in this browser.
 *
 * Feature-detecting the global avoids depending on `navigator.userAgent`
 * sniffing. This only covers the browser side; whether the *server* can
 * terminate WebTransport is a separate question answered by
 * `probeServerWebTransport`.
 */
export function isWebTransportSupported(): boolean {
  return "WebTransport" in globalThis;
}

/**
 * Asks the demo server whether it terminates WebTransport, via its
 * `/capabilities.json` endpoint. The Go demo server reports `true`; the
 * Cloudflare Workers deployment reports `false` (Workers terminate HTTP but
 * not HTTP/3 WebTransport).
 *
 * Servers that don't expose the endpoint (or can't be reached) are assumed
 * to support it, so the option stays selectable against unknown servers and
 * a connection failure is surfaced normally.
 */
export async function probeServerWebTransport(
  serverUrl: string,
): Promise<boolean> {
  try {
    const res = await fetch(new URL("/capabilities.json", serverUrl), {
      headers: { accept: "application/json" },
    });
    if (!res.ok) {
      return true;
    }
    const data: unknown = await res.json();
    if (typeof data !== "object" || data === null) {
      return true;
    }
    return (data as { webtransport?: unknown }).webtransport !== false;
  } catch {
    return true;
  }
}
