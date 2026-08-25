# connectwebsocket/draft3

Draft 3 of the WebSocket wire protocol. Drafts [1](../draft1/README.md)
and [4](../draft4/README.md) coexist with it, each with its own
constructors and default path (`/websocket-draft3`), so the designs can be
compared. Everything the drafts share — connection mapping, control payload
JSON, request and response sequences, cancellation, half-close — is
documented once in the [parent README](../README.md#shared-protocol).

Draft 3 pairs a minimal binary frame head with one idea: **compression is
an option of the protocol itself**, negotiated once per connection through
the WebSocket handshake and signaled per frame with one bit. The two
alternatives both have measured problems that draft 3 exists to fix:

- **Connect-metadata compression** (draft 1's `connect-*-encoding` headers
  and compressed envelope flag) has no size threshold, so per-message gzip
  compresses everything — the benchmarks show that makes tiny messages
  ~50% *larger* and bidi round trips ~2× slower. It also never covers the
  headers or end-stream JSON.
- **The WebSocket's own permessage-deflate** (draft 4's choice) is
  best-effort and environment-bound. It is disabled by default in common
  server runtimes (`ws`), gated by a compatibility flag on Cloudflare
  Workers, has to be implemented separately for every bootstrap, and
  whether a client compresses what it *sends* is up to that client. Chrome
  and Firefox do (a 16 KiB compressible upload leaves as ~106 B and ~168 B
  respectively); Node's global WebSocket negotiates the extension, inflates
  what it receives, and then sends every message uncompressed. Nothing in
  the protocol can require otherwise: RSV1 is a per-message choice each
  endpoint makes on its own.

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
`connect.bidi.d3.deflate` first, and the server selects it). Even then it
is selective per frame: payloads under 512 bytes, and payloads deflate
cannot shrink, are sent uncompressed. `WithoutCompression()` disables it
entirely on whichever side it's applied to — the client stops offering
the deflate subprotocol, the server stops selecting it. The `Subprotocols` and
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
makes draft 3 connections self-identifying, where drafts 1 and 4 are
distinguished only by path.

Compression is connection-scoped by design. It is not per-RPC metadata:
unlike `content-type`, which legitimately varies per RPC and stays in the
headers frame, there is no per-RPC reason to vary the *algorithm*, and
per-frame applicability is already covered by the compressed bit below.

## Frame format

A 4-byte stream ID and one byte split into a frame type and one flag bit:

```text
  0                   1                   2                   3
  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                           Stream ID                           |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |C|  Frame type |                 Payload ...                   |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- `Stream ID` is an unsigned 32-bit big-endian integer.
- `C` (bit 7 of the second-header byte) marks the payload as compressed.
- `Frame type` (bits 0–6) is `0x00` data, `0x01` headers, `0x02`
  end-stream, or `0x03` reset. Values `0x04` and up are reserved.
- The payload is the remainder of the WebSocket message: no length
  field.

Everything else — connection mapping, stream IDs, control payload JSON,
request and response sequences, cancellation, half-close — is the
[shared protocol](../README.md#shared-protocol).

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
  payloads is a measured win that draft 1's per-message scheme never
  had). Empty
  payloads (request end-stream, reset) are never compressed.
- Senders decide per frame. They SHOULD leave payloads below ~512 bytes
  uncompressed: the benchmarks show compressing tiny messages costs both
  size and time. Receivers MUST accept every frame type in either form.
- **No double compression**: a server that selected a `.deflate`
  subprotocol MUST NOT accept the permessage-deflate WebSocket extension
  on the same connection (browsers offer it unconditionally; the server
  simply declines it). The `connect-content-encoding` and
  `connect-accept-encoding` headers are not used.

## Relationship to the other drafts

| | Draft 1 | Draft 3 | Draft 4 |
| --- | --- | --- | --- |
| Frame head | 9 bytes (ID + Connect envelope) | 5 bytes (ID + C/type) | 4–15 bytes, ASCII |
| Payload length | explicit u32 | message boundary | message boundary |
| Compression unit | per message (Connect gzip) | per frame (protocol deflate) | per message (permessage-deflate) |
| Negotiated via | Connect metadata headers | WebSocket subprotocol | WebSocket extension |
| Covers control payloads | no | yes | yes |
| Small-message opt-out | no | yes (sender's choice) | yes (extension threshold) |
| Compression needs a per-bootstrap implementation | no (metadata) | no (protocol) | **yes** (one per WebSocket stack) |
| Works when intermediaries lack extensions | yes | yes | no |
| Shared compression window | no | no | optional (context takeover) |
| Readable without a decoder | no | no | **yes** |

The cost draft 3 accepts: every frame carries a fresh DEFLATE stream with
no shared window, so long runs of small, similar messages compress worse
than permessage-deflate with context takeover could — which is the one
thing draft 4 keeps and draft 3 gives up. The benchmark suite
(`internal/bench`, and `npm run bench` for the TypeScript packages)
quantifies that trade.
