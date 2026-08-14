# connectwebsocket/draft3

Draft 3 of the WebSocket wire protocol. Drafts
[1](../README.md) and [2](../draft2/README.md) coexist with it, each with
its own constructors and default path (`/websocket-draft3`), so the
designs can be compared.

Draft 3 keeps draft 2's framing and adds one idea: **compression is an
option of the protocol itself**, negotiated once per connection through
the WebSocket handshake and signaled per frame with one bit. Draft 1 made
compression Connect metadata (`connect-*-encoding` headers, a compressed
envelope flag); draft 2 delegated it entirely to the WebSocket's
permessage-deflate extension. Both have measured problems that draft 3
exists to fix:

- **Draft 1's per-message gzip compresses everything**, including tiny
  messages, which the benchmarks show makes them ~50% *larger* and bidi
  round trips ~2× slower. It also never covers the headers or end-stream
  JSON.
- **Draft 2's permessage-deflate is best-effort and bootstrap-bound.** It
  does not exist on the HTTP/2 extended-CONNECT bootstrap (RFC 8441
  carries no extension negotiation), is disabled by default in common
  server runtimes (`ws`), is gated by a compatibility flag on Cloudflare
  Workers, and browser/undici clients never compress what they send even
  when it is negotiated.

Draft 3's compression is negotiated by the protocol, so it behaves
identically over every bootstrap — HTTP/1.1 upgrade and HTTP/2 extended
CONNECT alike — and both directions are under the endpoints' control.

## Usage

Server (one handler serves both bootstraps; HTTP/2 extended CONNECT
additionally needs the process to run with `GODEBUG=http2xconnect=1`):

```go
connectServer := connect.NewServer()
pingv1connect.RegisterPingServiceHandler(connectServer, pingServer{})

http.Handle("/websocket-draft3", draft3.NewHandler(connectServer))
```

Client:

```go
// HTTP/1.1 upgrade bootstrap:
transport := draft3.NewTransport("wss://example.com/websocket-draft3")

// ...or HTTP/2 extended CONNECT (golang.org/x/net/http2):
transport = draft3.NewH2Transport("https://example.com/websocket-draft3", nil)

client := pingv1connect.NewPingServiceClient(connect.NewClient(transport))
```

Compression is on by default (the client offers
`connect.bidi.d3.deflate` first); `WithoutCompression()` makes the client
offer only the identity subprotocol. The `Subprotocols` and
`CompressionMode` fields of custom dial/accept options are overridden:
the subprotocols are the protocol's negotiation surface, and
permessage-deflate is never used.

## Negotiation: WebSocket subprotocols

The handshake's subprotocol mechanism (`Sec-WebSocket-Protocol`) is the
one negotiation surface the WebSocket exposes on every bootstrap and to
browser JavaScript (`new WebSocket(url, protocols)`, echoed selection in
`ws.protocol`; on HTTP/2 the header travels in the extended CONNECT
request and its 200 response). Draft 3 defines these subprotocol tokens:

| Token | Meaning |
| --- | --- |
| `connect.bidi.d3` | Draft 3, no compression |
| `connect.bidi.d3.deflate` | Draft 3, per-frame raw DEFLATE available |

The client offers every token it supports, most preferred first — a
compressing client offers `connect.bidi.d3.deflate, connect.bidi.d3`. The
server selects the first offered token it supports and echoes it; the
selection applies to both directions for the connection's lifetime.

A response without one of these tokens is a failed negotiation: the
client must close the connection and report the RPCs as failed. This also
makes draft 3 connections self-identifying, where drafts 1 and 2 are
distinguished only by path.

Compression is connection-scoped by design. It is not per-RPC metadata:
unlike `content-type`, which legitimately varies per RPC and stays in the
headers frame, there is no per-RPC reason to vary the *algorithm*, and
per-frame applicability is already covered by the compressed bit below.

## Frame format

As draft 2, with the frame type byte split into a type and one flag bit:

```text
  0                   1                   2                   3
  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                           Stream ID                           |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |C|  Frame type |                 Payload ...                   |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- `Stream ID` is an unsigned 32-bit big-endian integer, exactly as in
  draft 2.
- `C` (bit 7 of the second-header byte) marks the payload as compressed.
- `Frame type` (bits 0–6) is one of draft 2's values: `0x00` data, `0x01`
  headers, `0x02` end-stream, `0x03` reset. Values `0x04` and up are
  reserved.
- The payload is the remainder of the WebSocket message, exactly as in
  draft 2: no length field.

Everything else — connection mapping, stream IDs, control payload JSON,
request and response sequences, cancellation, half-close — is identical
to [draft 2](../draft2/README.md#protocol).

## Compression rules

- The `C` bit is meaningful only when a `.deflate` token was negotiated;
  otherwise a set `C` bit is a protocol error.
- A compressed payload is one complete raw DEFLATE stream (RFC 1951 — no
  zlib or gzip wrapper). **No context takeover**: every frame decompresses
  independently, with no shared window between frames or streams. This is
  what keeps the scheme implementable with the web platform's
  `CompressionStream`/`DecompressionStream` (`deflate-raw`), which cannot
  flush at message boundaries and therefore cannot share a window.
- Either peer may set `C` on any frame type with a non-empty payload —
  data, headers, and response end-stream alike (covering the JSON control
  payloads is a measured draft 2 win that draft 1 never had). Empty
  payloads (request end-stream, reset) are never compressed.
- Senders decide per frame. They SHOULD leave payloads below ~512 bytes
  uncompressed: the benchmarks show compressing tiny messages costs both
  size and time. Receivers MUST accept every frame type in either form.
- **No double compression**: a server that selected a `.deflate`
  subprotocol MUST NOT accept the permessage-deflate WebSocket extension
  on the same connection (browsers offer it unconditionally; the server
  simply declines it). The `connect-content-encoding` and
  `connect-accept-encoding` headers are not used, as in draft 2.

## Relationship to the other drafts

| | Draft 1 | Draft 2 | Draft 3 |
| --- | --- | --- | --- |
| Frame prefix | 9 bytes (ID + Connect envelope) | 5 bytes (ID + type) | 5 bytes (ID + C/type) |
| Payload length | explicit u32 | message boundary | message boundary |
| Compression unit | per message (Connect gzip) | per message (permessage-deflate) | per frame (protocol deflate) |
| Negotiated via | Connect metadata headers | WebSocket extension | WebSocket subprotocol |
| Covers control payloads | no | yes | yes |
| Small-message opt-out | no | yes (extension threshold) | yes (sender's choice) |
| Works over HTTP/2 bootstrap | yes (metadata) | **no** | yes |
| Works when intermediaries lack extensions | yes | no | yes |
| Shared compression window | no | optional (context takeover) | no |

The cost draft 3 accepts: like draft 2's no-context-takeover mode, every
frame carries a fresh DEFLATE stream, so long runs of small, similar
messages compress worse than permessage-deflate with context takeover
could. The benchmark suite (internal/bench, and `npm run bench` for the
TypeScript packages) includes draft 3 cases to quantify that trade.
