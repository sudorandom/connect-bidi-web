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
 * HeadersMessage is the JSON payload of a headers envelope (flag 0x04). Like
 * Connect's EndStreamResponse, it is defined directly in code rather than as
 * a protobuf message. On the wire it looks like:
 *
 * ```json
 * {"metadata": {":path": ["/pkg.Service/Method"], "content-type": ["application/connect+proto"]}}
 * ```
 *
 * Metadata keys are HTTP-header-shaped; values are string lists. Keys
 * beginning with ":" are pseudo-headers (only ":path" is currently used) and
 * are not surfaced to applications. The Go implementation defines the
 * equivalent HeaderMessage type in internal/connectprotocol.
 */
export interface HeadersMessage {
  metadata?: Record<string, string[]>;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * Serialize request or response headers into the JSON payload of a headers
 * envelope. Pseudo-headers (such as ":path") can be passed via `pseudo`.
 */
export function encodeHeadersFrame(
  headers: Headers,
  pseudo?: Record<string, string>,
): Uint8Array {
  const metadata: Record<string, string[]> = {};
  headers.forEach((value, key) => {
    if (metadata[key] === undefined) {
      metadata[key] = [];
    }
    metadata[key].push(value);
  });
  if (pseudo !== undefined) {
    for (const [key, value] of Object.entries(pseudo)) {
      metadata[key] = [value];
    }
  }
  const message: HeadersMessage = { metadata };
  return encoder.encode(JSON.stringify(message));
}

/**
 * Parse the JSON payload of a headers envelope. Returns the application
 * headers (pseudo-headers omitted) and the pseudo-headers separately.
 * Tolerates single-string metadata values for robustness.
 */
export function decodeHeadersFrame(payload: Uint8Array): {
  headers: Headers;
  pseudo: Record<string, string>;
} {
  const message = JSON.parse(decoder.decode(payload)) as {
    metadata?: Record<string, string[] | string>;
  };
  const headers = new Headers();
  const pseudo: Record<string, string> = {};
  if (message.metadata !== undefined) {
    for (const [key, val] of Object.entries(message.metadata)) {
      const values = Array.isArray(val) ? val : [val];
      if (key.startsWith(":")) {
        if (values.length > 0) {
          pseudo[key] = values[0];
        }
        continue;
      }
      for (const v of values) {
        if (typeof v === "string") {
          headers.append(key, v);
        }
      }
    }
  }
  return { headers, pseudo };
}
