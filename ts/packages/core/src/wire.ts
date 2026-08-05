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
 * Envelope flag used only by the bidi wire protocol (not part of the
 * Connect-over-HTTP wire format) to mark the leading metadata frame of a
 * request or response. Must match the flag of the same name used by
 * @sudorandom/connect-bidi-web's client transports.
 */
export const flagEnvelopeHeaders = 0x04;

/**
 * The "no flags set" value for a data envelope. Not a distinct flag, just
 * named for readability at call sites.
 */
export const flagEnvelopeData = 0x00;

/**
 * Concatenate a list of byte chunks into a single Uint8Array, avoiding a
 * copy when there is only one chunk.
 */
export function concatBytes(chunks: Uint8Array[]): Uint8Array {
  if (chunks.length === 1) {
    return chunks[0];
  }
  const total = chunks.reduce((n, c) => n + c.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

/**
 * Read and concatenate every chunk of an AsyncIterable<Uint8Array>.
 */
export async function concatAsyncIterable(
  it: AsyncIterable<Uint8Array>,
): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of it) {
    chunks.push(chunk);
  }
  return concatBytes(chunks);
}
