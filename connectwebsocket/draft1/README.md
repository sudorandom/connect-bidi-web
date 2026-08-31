# connectwebsocket/draft1

Draft 1 of the WebSocket wire protocol, and the shape the other drafts are
variations on. Drafts [3](../draft3/README.md), [4](../draft4/README.md),
[5](../draft5/README.md), and 6 coexist with it, each wire-incompatible
with the others, so the designs can be compared.

**Draft 1 does not share the [shared protocol](../README.md#shared-protocol)
the multiplexing drafts document once in the parent README.** It has no
stream IDs and no reset frame, because it has nothing to multiplex.

## The proposal

The best way for Connect to support streaming in the browser is WebSockets.
Draft 1 is the shape that proposes itself, made of four decisions.

**Each streaming RPC creates its own WebSocket.** Unary calls keep using
HTTP. Multiplexing every RPC — unary and streaming — onto one connection was
considered and rejected: it is too complicated and too easy to get wrong,
and drafts 3 and 4 exist to show what it costs, in a stream ID on every
frame and a reset frame in the protocol. Two consequences follow. Cancelling
an RPC is closing its socket, so there is nothing else to design. And
browsers enforce a global cap on how many WebSockets a page may hold open to
one host — 255 in Chrome — which a page opening streams in a loop needs to
know about.

**The first message carries the headers.** Browser APIs cannot attach custom
headers to the upgrade request, and cannot read custom headers off the
response. So each direction opens with a metadata frame — like the existing
Connect `EndStreamResponse`, but at the start, and sent by the server as
well as the client.

**Each Connect message is wrapped in a Connect envelope**, one envelope per
WebSocket message. The envelope's length field is redundant once the message
boundary already states it, and it is kept anyway: the envelope is Connect's
own, byte for byte. The cost is that a JSON payload is no longer a
prettified, expandable object in devtools — it is five bytes of head
followed by the JSON. Draft 5 is the draft that takes the other side of that
trade.

**Compression is the native `permessage-deflate` extension.** It is
transparent and every browser has it. It is negotiated only with
`no_context_takeover` on both sides; if a peer will not agree to that,
compression is disabled entirely rather than run with a window shared across
messages. The alternative — Connect's own per-message compression inside the
envelope — would make devtools show ciphertext-looking noise for every
message, and buys little that the extension does not already provide.

WebSocket support is **opt-in**. Most people do not need browser bidi
streaming, so it stays off by default: a server and a client each have to
initialize a WebSocket transport explicitly.

## Usage

Server. Draft 1 has no path of its own, so `Intercept` wraps the handler
that already serves the Connect procedure URLs: each one answers `POST`
with ordinary Connect over HTTP and `GET`+`Upgrade` with this protocol.

```go
connectServer := connect.NewServer()
pingv1connect.RegisterPingServiceHandler(connectServer, pingServer{})

mux := http.NewServeMux()
connecthttp.Mount(mux, connectServer)
http.ListenAndServe(addr, draft1.Intercept(mux, connectServer))
```

Anything that is not a draft 1 upgrade passes straight through, so the
wrapped handler keeps serving everything it did before — including some
other WebSocket protocol on the same origin, which is how this repository's
demo server runs several drafts at once.

`Mount` is the alternative, for serving draft 1 on URLs of its own:
`draft1.Mount(mux, connectServer, draft1.DefaultPathPrefix)` registers one
route per *streaming* procedure under that prefix. Two handlers cannot
register the same pattern on one `http.ServeMux`, which is why co-mounting
is `Intercept`'s job and not `Mount`'s.

Use `WithAcceptOptions` to configure the upgrade, `OriginPatterns` in
particular, which browsers need.

Client. The WebSocket transport carries streaming RPCs only, so it is
composed with an HTTP one:

```go
transport := connectwebsocket.NewCompositeTransport(
	connecthttp.NewTransport(httpClient, "https://example.com"),
	draft1.NewTransport("wss://example.com"),
)
client := pingv1connect.NewPingServiceClient(connect.NewClient(transport))
```

The base URL is the endpoint the procedure is appended to, so a call to
`/connectrpc.eliza.v1.ElizaService/Converse` dials
`wss://example.com/connectrpc.eliza.v1.ElizaService/Converse`. An `https://`
base URL is accepted and normalized, so a caller configuring one endpoint
for both dispatch paths does not have to spell it twice.

In TypeScript the same composition is
`createConnectWebSocketDraft1Transport` inside `createCompositeTransport`;
see [`ts/packages/web`](../../ts/packages/web).

To bootstrap over HTTP/2 extended CONNECT instead of an HTTP/1.1 upgrade,
use `NewH2Transport` (`GODEBUG=http2xconnect=1` must be set in the *server*
process, and Go clients must use `golang.org/x/net/http2` directly —
`net/http` rejects the `:protocol` pseudo-header before HTTP/2 sees it):

```go
transport := draft1.NewH2Transport("https://example.com", &http2.Transport{})
```

`NewHandler` serves both bootstraps automatically.

## Protocol

### Connection mapping

One WebSocket carries one streaming RPC, and closes when it ends:

```text
RPC A ── WebSocket connection 1
RPC B ── WebSocket connection 2
RPC C ── WebSocket connection 3
```

The handshake must offer the `connect.bidi.d1` subprotocol; a server that
does not select it does not speak this protocol. The subprotocol is where
the version lives because it is the one thing a browser *can* set on the
handshake.

The URL names the procedure: it is the request path's last two segments,
`/package.Service/Method`. Nothing in the protocol says what comes before
them, so a handler serves whether it is mounted on the procedure URLs
themselves or under a prefix — as this repository's demo server does, since
several drafts share one origin.

Only binary WebSocket messages are used. Each message carries exactly one
complete Connect envelope. Message boundaries are significant: an envelope
never spans messages, and a message never carries more than one envelope.
Text messages are invalid protocol data.

### Frame format

Every message is a standard 5-byte Connect envelope and its payload:

```text
  0                   1                   2                   3
  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |     Flags     |              Payload length                   |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 | Payload len.  |                 Payload ...                   |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- `Flags` is one byte.
- `Payload length` is an unsigned 32-bit big-endian integer.
- The length counts payload bytes only and may be zero. It must equal the
  remaining bytes of the WebSocket message — the message boundary already
  states the same thing, and a disagreement between the two is a protocol
  error.
- The maximum representable payload is `2^32 - 1` bytes. Configured send and
  receive limits may impose smaller bounds.

The defined flag values are:

| Value | Name | Payload |
| --- | --- | --- |
| `0x00` | data | One RPC message encoded with the selected codec |
| `0x02` | end-stream | Empty on requests; Connect `EndStreamResponse` JSON on responses |
| `0x06` | headers | JSON metadata object (`{"metadata": ...}`) |

These are complete flag-byte values, not bitmasks. Data and end-stream
envelopes are byte-for-byte the Connect protocol's; `0x06` is a
transport-specific frame type. Values `0x08` and up are reserved for
extended flags.

Two flags the other drafts define are deliberately absent. Connect's
compressed-data flag (`0x01`) is unused, because compression is
permessage-deflate's job here — a receiver that sees it should treat it as a
protocol error rather than guess at an encoding. The reset flag (`0x07`) is
unused because a connection carrying one RPC cancels by closing.

Other values, and a second headers envelope in the same direction, are
protocol errors.

### Control payloads

The headers envelope (`0x06`) uses this JSON object schema:

```json
{
  "metadata": {
    "content-type": ["application/connect+proto"]
  }
}
```

There is no `:path`: the URL already named the procedure.

The response end-stream envelope (`0x02`) reuses the Connect protocol's
standard JSON EndStreamResponse:

```json
{
  "error": {
    "code": "unavailable",
    "message": "service unavailable",
    "details": [
      {
        "type": "google.rpc.RetryInfo",
        "value": "BASE64_PROTOBUF_VALUE",
        "debug": {}
      }
    ]
  },
  "metadata": {
    "trailer-name": ["value"]
  }
}
```

Both members are optional. Each detail maps directly to
`connect.ErrorDetail`: `type` is the fully qualified protobuf message name,
`value` is its unpadded base64-encoded binary value, and `debug` is an
optional best-effort JSON representation. `metadata` contains response
trailers as arrays of strings.

### Request sequence

The client sends envelopes in this order:

```text
headers, zero or more data messages, end stream
```

The request headers payload must include:

- `Content-Type`: `application/connect+proto` or
  `application/connect+json`.

It may also include application request headers and:

- `Connect-Timeout-Ms`: remaining RPC timeout in milliseconds.

The upgrade request's own headers are the base of the request metadata, and
the headers envelope overrides them key by key. That is what lets a Cookie
the browser attached by itself still reach the handler, while anything the
Connect client set explicitly wins. Handshake headers — `Connection`,
`Upgrade`, and every `Sec-WebSocket-*` — are stripped.

The request end-stream envelope has an empty payload. It represents
`CloseSend`: no more request messages will be sent, while the stream stays
open for response messages. This explicit envelope is necessary because
neither the RPC nor the WebSocket has a send-direction close of its own —
closing the socket would end the response too.

### Response sequence

The server sends:

```text
headers, zero or more data messages, end stream
```

Response headers are always sent, including when the RPC fails before
producing a message: a browser cannot read them off the handshake, so this
is the only place they can arrive. They include the response `Content-Type`.

The final response end-stream payload is the Connect EndStreamResponse JSON
described above. A successful RPC omits `error`; a failed RPC includes the
Connect code, message, and error details. Application response trailers are
carried in `metadata`. Receiving a successful end stream produces `io.EOF`
for the caller. Closing the WebSocket before a valid response end stream is
a protocol error.

Client-, server-, and bidirectional-streaming RPCs all use this sequence,
with the number and timing of data envelopes appropriate to the method type.
Unary RPCs never appear: they are ordinary Connect HTTP requests, and a
draft 1 transport refuses them with `unimplemented` rather than spending a
handshake and one of the browser's connection slots on a single
request-and-response.

### Compression

Compression is the WebSocket's `permessage-deflate` extension (RFC 7692),
negotiated in the handshake and applied to the whole message — envelope head
included. It is transparent to everything above: the flag byte, the length,
and the payload semantics are unchanged.

The extension is only accepted with `client_no_context_takeover` and
`server_no_context_takeover`. A shared compression window across messages is
what lets an attacker-influenced payload reveal the size of a secret one, so
a peer that will not agree to no-context-takeover gets no compression at
all. Every browser that offers permessage-deflate offers it with
no-context-takeover, so this costs nothing in practice — beyond compressing
long runs of small similar messages less well than a shared window would.

`WithoutCompression` turns the extension off. That is worth considering for
streams of small messages: nothing under a few hundred bytes compresses, so
all that arrives is the negotiation in the handshake — which draft 1 pays
once per RPC, having no connection reuse to amortize it over.

On the HTTP/2 bootstrap the extension is implemented in this package rather
than inherited from the WebSocket library, which cannot accept over HTTP/2.
RFC 8441 §5 keeps `Sec-WebSocket-Extensions`, so the negotiation is the
ordinary one; only the framing underneath is ours.

### Cancellation and connection lifetime

The connection carries this RPC and nothing else, so closing it says
everything a reset frame would. A client that cancels or abandons an RPC
before its response finished tears the socket down; the server cancels that
RPC's context and the handler stops. There is no reset envelope, and no
question of what happens to the other streams on the connection, because
there are none.

The server keeps reading past the client's half-close for exactly this
reason. A server-streaming RPC half-closes immediately and then streams for
as long as it likes; that outstanding read is the only thing that will
notice a browser tab closing mid-stream.

Head-of-line blocking cannot happen: there is no line to be at the head of.
That is the compensation for the cost this design does pay, which is a
handshake per streaming RPC.

That cost is answered by the bootstrap rather than by the protocol. Over
[RFC 8441](https://datatracker.ietf.org/doc/html/rfc8441) extended CONNECT,
a new WebSocket is a new HTTP/2 stream on a connection that is already open:
no TCP connection, no TLS handshake, HPACK compressing the repeated request
headers down to a few bytes, and no per-host WebSocket slot consumed. On the
HTTP/1.1 bootstrap the cost stands, and it is the honest price of the
design. Draft 6 is the draft that answers it a second way, by pooling
connections across RPCs.

## Relationship to the Connect HTTP protocol

Message serialization, status codes, error details, the 5-byte envelope
layout, and the EndStreamResponse JSON follow Connect semantics exactly. The
headers envelope is the one transport-specific addition, and it exists
because a raw WebSocket gives Connect no per-RPC headers after the upgrade —
in either direction. The overall exchange is still not the Connect HTTP wire
protocol and is not intended for a normal Connect HTTP handler.

---

See the demo site's
[conclusions](https://connect-bidi-web.kmcd.dev/#conclusions) for what
comparing the drafts settled.
