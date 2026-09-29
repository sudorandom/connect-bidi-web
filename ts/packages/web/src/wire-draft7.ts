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

// Draft 7 of the WebSocket wire protocol: the Connect-over-WebSocket
// specification. One RPC per WebSocket, the procedure named by the
// handshake URI, and every message a one-byte marker followed by its
// payload:
//
//     M  leading metadata     JSON object, text frame
//     B  body                 one RPC message; text under JSON, binary under proto
//     C  client end-of-stream a final body framed as B would be, or bare, text
//     S  server end-of-stream Connect EndStreamResponse JSON, text frame
//
// The codec is selected by the WebSocket subprotocol, the deadline travels
// as a `connect-timeout-ms` query parameter, and compression is
// permessage-deflate with no context takeover. Must match the Go
// connectwebsocket/draft7 package and @sudorandom/connect-bidi-core's draft
// 7 server bridge byte for byte. This is a copy of the core package's
// wire-draft7.ts, kept in sync by hand, because the browser package does
// not depend on the server one.

import { Code, ConnectError } from "@connectrpc/connect";

/** The codecs the subprotocol tokens select. */
export type Draft7Codec = "proto" | "json";

/** The subprotocol tokens, and the codec each selects. */
export const draft7Subprotocols: Readonly<Record<string, Draft7Codec>> = {
  "connectrpc.1": "json",
  "connectrpc.1+proto": "proto",
  "connectrpc.1+json": "json",
};

/** The token a client offers for a codec. */
export function draft7SubprotocolForCodec(codec: Draft7Codec): string {
  return codec === "proto" ? "connectrpc.1+proto" : "connectrpc.1+json";
}

/** The message markers. */
export const draft7MarkerBody = 0x42; // "B"
export const draft7MarkerLeadingMetadata = 0x4d; // "M"
export const draft7MarkerServerEndStream = 0x53; // "S"
export const draft7MarkerClientEndStream = 0x43; // "C"

/** The query parameter carrying the client's deadline, in milliseconds. */
export const draft7TimeoutQueryParameter = "connect-timeout-ms";

/**
 * One message on a draft 7 connection, with the frame type that carried
 * it. The frame type names the payload encoding — text is JSON, binary is
 * Protobuf — so it travels with every message rather than being discarded
 * at the edge.
 */
export interface Draft7Message {
  /** True when the message arrived as, or must be sent as, a text frame. */
  text: boolean;
  /** The whole message: marker first, then the payload. */
  data: Uint8Array;
}

/**
 * A message-oriented full-duplex connection carrying exactly one RPC, such
 * as a WebSocket that was upgraded from a Connect procedure URL.
 */
export interface Draft7MessageStream {
  readonly readable: ReadableStream<Draft7Message>;
  readonly writable: WritableStream<Draft7Message>;
  /** Close the underlying connection once the RPC has finished. */
  close?: (code?: number, reason?: string) => void;
}

/**
 * Selects the first offered token the server both recognizes and can
 * serve. `recognized` says whether any token was one of this protocol's
 * at all, which decides between a 400 and a 415 when nothing is selected.
 */
export function selectDraft7Subprotocol(
  offered: readonly string[],
  codecs: readonly Draft7Codec[],
): { token: string | undefined; recognized: boolean } {
  let recognized = false;
  for (const candidate of offered) {
    const codec = draft7Subprotocols[candidate];
    if (codec === undefined) {
      continue;
    }
    recognized = true;
    if (codecs.includes(codec)) {
      return { token: candidate, recognized };
    }
  }
  return { token: undefined, recognized };
}

/** Splits a `Sec-WebSocket-Protocol` header value into its tokens. */
export function parseSubprotocolHeader(value: string | null): string[] {
  if (value === null) {
    return [];
  }
  return value
    .split(",")
    .map((token) => token.trim())
    .filter((token) => token !== "");
}

/** The tokens a server with these codecs serves, for the body of a 415. */
export function supportedDraft7Subprotocols(
  codecs: readonly Draft7Codec[],
): string[] {
  return Object.entries(draft7Subprotocols)
    .filter(([, codec]) => codecs.includes(codec))
    .map(([token]) => token);
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Frames one message: the marker, then the payload. */
export function encodeDraft7Message(
  marker: number,
  payload: Uint8Array,
  text: boolean,
): Draft7Message {
  const data = new Uint8Array(1 + payload.byteLength);
  data[0] = marker;
  data.set(payload, 1);
  return { text, data };
}

/**
 * Splits a received message into its marker and payload, applying the
 * rules every receiver shares: a message with no marker, a marker with
 * the reserved high bit set, and a text frame that is not UTF-8 are
 * protocol errors.
 */
export function decodeDraft7Message(message: Draft7Message): {
  marker: number;
  payload: Uint8Array;
} {
  if (message.data.byteLength === 0) {
    throw new ConnectError(
      "protocol error: empty message: no marker",
      Code.InvalidArgument,
    );
  }
  const marker = message.data[0];
  if (marker >= 0x80) {
    throw new ConnectError(
      `protocol error: marker 0x${marker.toString(16)} has the reserved high bit set`,
      Code.InvalidArgument,
    );
  }
  return { marker, payload: message.data.subarray(1) };
}

/** The protocol error for a marker this revision does not define. */
export function unknownDraft7MarkerError(marker: number): ConnectError {
  return new ConnectError(
    `protocol error: unknown marker ${JSON.stringify(String.fromCharCode(marker))}`,
    Code.InvalidArgument,
  );
}

/**
 * The frame-type rule for bodies: binary under Protobuf, text under JSON,
 * and never an empty text body — an empty JSON message is `{}`.
 */
export function checkDraft7BodyFrame(
  message: Draft7Message,
  payload: Uint8Array,
  codec: Draft7Codec,
): void {
  if (message.text && codec === "proto") {
    throw new ConnectError(
      "protocol error: body arrived as a text frame (JSON) but the negotiated codec is proto",
      Code.InvalidArgument,
    );
  }
  if (!message.text && codec === "json") {
    throw new ConnectError(
      "protocol error: body arrived as a binary frame (Protobuf) but the negotiated codec is json",
      Code.InvalidArgument,
    );
  }
  if (message.text && payload.byteLength === 0) {
    throw new ConnectError(
      "protocol error: an empty text body; an empty JSON message is {}",
      Code.InvalidArgument,
    );
  }
}

// -- Leading metadata ---------------------------------------------------------

/** RFC 9110 token characters, which every metadata key must consist of. */
const tokenRegExp = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;

/** Base64 with the standard alphabet, padded or not. */
const base64RegExp = /^[A-Za-z0-9+/]*={0,2}$/;

function validBase64(value: string): boolean {
  if (!base64RegExp.test(value)) {
    return false;
  }
  const unpadded = value.replace(/=+$/, "");
  return unpadded.length % 4 !== 1;
}

/** Strips base64 padding; the wire form is unpadded. */
function unpadBase64(value: string): string {
  return value.replace(/=+$/, "");
}

/**
 * Serializes headers as the payload of an M message: a flat JSON object
 * of lower-case keys to arrays of strings, `{}` for none. Values are
 * validated on the way out, so a sender cannot produce a message the
 * receiver is required to reject.
 */
export function encodeDraft7Metadata(headers: Headers): Uint8Array {
  const object: Record<string, string[]> = {};
  headers.forEach((value, key) => {
    const name = key.toLowerCase();
    if (!tokenRegExp.test(name)) {
      throw new ConnectError(
        `metadata key ${JSON.stringify(key)} is not a valid HTTP field name`,
        Code.InvalidArgument,
      );
    }
    let outgoing = value;
    if (name.endsWith("-bin")) {
      if (!validBase64(value)) {
        throw new ConnectError(
          `metadata ${key}: value is not base64`,
          Code.InvalidArgument,
        );
      }
      outgoing = unpadBase64(value);
    } else if (/[\r\n\0]/.test(value)) {
      throw new ConnectError(
        `metadata ${key}: value contains CR, LF, or NUL`,
        Code.InvalidArgument,
      );
    }
    object[name] = [outgoing];
  });
  // Sorted keys, so the wire form is deterministic across implementations.
  const sorted: Record<string, string[]> = {};
  for (const key of Object.keys(object).sort()) {
    sorted[key] = object[key];
  }
  return encoder.encode(JSON.stringify(sorted));
}

/**
 * Parses the payload of an M message. Keys are folded to lower case; two
 * keys that fold to the same name, a value that is not an array of
 * strings, an invalid field name or value, and a `-bin` value that is not
 * base64 are all protocol errors.
 */
export function decodeDraft7Metadata(payload: Uint8Array): Headers {
  let parsed: unknown;
  try {
    parsed = JSON.parse(decoder.decode(payload));
  } catch (err) {
    throw new ConnectError(
      `protocol error: metadata message is not JSON: ${String(err)}`,
      Code.InvalidArgument,
    );
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new ConnectError(
      "protocol error: metadata message is not a JSON object",
      Code.InvalidArgument,
    );
  }
  const headers = new Headers();
  const seen = new Set<string>();
  for (const [key, value] of Object.entries(parsed)) {
    const name = key.toLowerCase();
    if (!tokenRegExp.test(name)) {
      throw new ConnectError(
        `protocol error: metadata key ${JSON.stringify(key)} is not a valid HTTP field name`,
        Code.InvalidArgument,
      );
    }
    if (seen.has(name)) {
      throw new ConnectError(
        `protocol error: metadata key ${JSON.stringify(key)} appears more than once`,
        Code.InvalidArgument,
      );
    }
    seen.add(name);
    if (
      !Array.isArray(value) ||
      !value.every((entry) => typeof entry === "string")
    ) {
      throw new ConnectError(
        `protocol error: metadata ${key}: value must be an array of strings`,
        Code.InvalidArgument,
      );
    }
    for (const entry of value as string[]) {
      if (name.endsWith("-bin")) {
        if (!validBase64(entry)) {
          throw new ConnectError(
            `protocol error: metadata ${key}: value is not base64`,
            Code.InvalidArgument,
          );
        }
      } else if (/[\r\n\0]/.test(entry)) {
        throw new ConnectError(
          `protocol error: metadata ${key}: value contains CR, LF, or NUL`,
          Code.InvalidArgument,
        );
      }
      headers.append(name, entry);
    }
  }
  return headers;
}

// -- Reserved names -----------------------------------------------------------

/** The Fetch standard's forbidden request-header names, lower-case. */
const fetchForbiddenHeaders = new Set([
  "accept-charset",
  "accept-encoding",
  "access-control-request-headers",
  "access-control-request-method",
  "connection",
  "content-length",
  "cookie",
  "cookie2",
  "date",
  "dnt",
  "expect",
  "host",
  "keep-alive",
  "origin",
  "referer",
  "set-cookie",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "via",
  "x-http-method",
  "x-http-method-override",
  "x-method-override",
]);

/**
 * The names this protocol controls: the subprotocol implies the protocol
 * version and the codec, the query string carries the deadline, and
 * compression has no per-message form here. A client strips these from
 * what it sends; a server rejects them.
 */
export const draft7ProtocolControlledHeaders: ReadonlySet<string> = new Set([
  "connect-protocol-version",
  "connect-timeout-ms",
  "content-type",
  "content-encoding",
  "connect-content-encoding",
  "connect-accept-encoding",
]);

/**
 * The default deny list of names the server's own infrastructure sets,
 * which a client must not be able to forge. An entry ending in "-" is a
 * prefix.
 */
export const draft7DefaultInfrastructureHeaders: readonly string[] = [
  "forwarded",
  "x-forwarded-",
  "x-real-ip",
];

/**
 * Why a lower-case header name may not appear in a client's
 * leading-metadata message, or undefined if it may.
 */
export function draft7ReservedHeaderReason(
  name: string,
  infrastructure: readonly string[] = draft7DefaultInfrastructureHeaders,
): string | undefined {
  if (
    fetchForbiddenHeaders.has(name) ||
    name.startsWith("proxy-") ||
    name.startsWith("sec-")
  ) {
    return "a forbidden request header";
  }
  if (draft7ProtocolControlledHeaders.has(name)) {
    return "controlled by the protocol";
  }
  for (const entry of infrastructure) {
    const lower = entry.toLowerCase();
    if (lower.endsWith("-") ? name.startsWith(lower) : name === lower) {
      return "set by the server's infrastructure";
    }
  }
  return undefined;
}

// -- Handshake helpers --------------------------------------------------------

/**
 * Whether a handshake's `Origin` is the request's own host: host and port
 * compared case-insensitively, scheme deliberately left out, because a
 * deployment terminating TLS at a proxy sees `https` in the Origin and its
 * own plaintext Host. A handshake with no Origin is not from a browser and
 * passes.
 */
export function draft7OriginIsSameHost(
  origin: string | null,
  host: string | null,
): boolean {
  if (origin === null || origin === "") {
    return true;
  }
  let originHost: string;
  try {
    originHost = new URL(origin).host;
  } catch {
    return false;
  }
  return host !== null && originHost.toLowerCase() === host.toLowerCase();
}

/**
 * Parses the `connect-timeout-ms` query parameter off a handshake URI's
 * search string. Returns undefined when absent, and throws
 * invalid_argument for a value that is not a base-10 integer of at most
 * ten digits — reported on the socket, since the upgrade has already
 * succeeded by the time anyone looks.
 */
export function parseDraft7Timeout(search: string): number | undefined {
  const values = new URLSearchParams(search).getAll(
    draft7TimeoutQueryParameter,
  );
  if (values.length === 0) {
    return undefined;
  }
  if (values.length > 1) {
    throw new ConnectError(
      `protocol error: ${draft7TimeoutQueryParameter} must be given once`,
      Code.InvalidArgument,
    );
  }
  const raw = values[0];
  if (!/^-?\d{1,10}$/.test(raw)) {
    throw new ConnectError(
      `protocol error: invalid ${draft7TimeoutQueryParameter} ${JSON.stringify(raw)}`,
      Code.InvalidArgument,
    );
  }
  return Number(raw);
}
