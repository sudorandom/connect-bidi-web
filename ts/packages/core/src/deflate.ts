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
 * Per-frame raw DEFLATE (RFC 1951) via the web platform's
 * CompressionStream/DecompressionStream, available in every modern
 * browser, Node.js, and Cloudflare Workers. Each call processes one
 * complete, independent DEFLATE stream — draft 3 deliberately has no
 * shared window between frames, because these APIs cannot flush at
 * message boundaries.
 */

export function deflateRaw(data: Uint8Array): Promise<Uint8Array> {
  return transform(data, new CompressionStream("deflate-raw"));
}

export function inflateRaw(data: Uint8Array): Promise<Uint8Array> {
  return transform(data, new DecompressionStream("deflate-raw"));
}

async function transform(
  data: Uint8Array,
  stream: CompressionStream | DecompressionStream,
): Promise<Uint8Array> {
  const writer = stream.writable.getWriter();
  // Write and read concurrently: awaiting the write before reading could
  // deadlock once the transform's internal buffer fills. The write is
  // still awaited at the end so its errors surface.
  const writeDone = writer
    .write(data as BufferSource)
    .then(() => writer.close());
  writeDone.catch(() => {});
  const out = new Uint8Array(await new Response(stream.readable).arrayBuffer());
  await writeDone;
  return out;
}
