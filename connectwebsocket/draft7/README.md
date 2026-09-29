# connectwebsocket/draft7

Draft 7 of the WebSocket wire protocol: the implementation of the
**Connect-over-WebSocket Protocol** specification. Drafts
[1](../draft1/README.md), [3](../draft3/README.md),
[4](../draft4/README.md), [5](../draft5/README.md), and 6 coexist with it,
each wire-incompatible with the others, so the designs can be compared.

**Draft 7 does not share the [shared protocol](../README.md#shared-protocol)
the multiplexing drafts document in the parent README.** It has no stream
IDs and no reset frame, because one WebSocket carries one RPC.

## Where the drafts converged

Draft 7 is [draft 5](../draft5/README.md)'s connection model with the
loose ends tied. It keeps the two ideas that made draft 5 cheap — one RPC
per WebSocket, and the handshake as the request, so the URL names the
procedure and there is nothing to multiplex or reset — and changes what
draft 5 left to position and to the opcode:

```text
→  [text]    M{"acme-tenant":["t-1"]}      leading metadata, required, first
→  [text]    B{"sentence":"Hello, Eliza!"}  a body, in the negotiated codec
→  [text]    C                              client end-of-stream
←  [text]    M{}
←  [text]    B{"sentence":"Eliza hears: Hello, Eliza!"}
←  [text]    S{"metadata":{"x-request-id":["7f3a"]}}
   [close 1000]
```

**Every message begins with a marker.** One printable byte — `M`, `B`,
`C`, or `S` — says what the message is, so a receiver never infers it from
position and a reader of the Network tab never has to. Draft 5 reserved a
single message shape (an empty text frame) and let position tell metadata
from data; draft 7 spends one byte per message to make that explicit. The
high bit of the marker is reserved: a first byte of `0x80` or above is a
protocol error, which leaves a later revision a signal no conforming
implementation of this one can be emitting.

**The frame type names the encoding.** A text frame is JSON, a binary
frame is Protobuf. `M` and `S` are always JSON and always text; `B` follows
the codec; a bare `C` is text, and a `C` carrying a final body follows the
codec as `B` does. A bare binary `B` is the empty Protobuf message; a bare
text `B` is an error, because an empty JSON message is `{}`. A body in the
wrong frame type for the negotiated codec is rejected, naming the
encoding, rather than failing later as a corrupt message.

**The subprotocol selects the codec.** A client offers `connectrpc.1+proto`
or `connectrpc.1+json` (the base token `connectrpc.1` means JSON), in
order of preference; the server echoes the first it both recognizes and
serves. Nothing is recognized: `400`. Something is recognized but its codec
is not served: `415`, naming the ones that are. Draft 5 put the codec in a
`Content-Type` header inside the first message; here it is settled before
the first message exists.

**The deadline rides on the handshake URI**, as `connect-timeout-ms`, a
positive integer of at most ten digits. A browser cannot set headers on a
handshake, and metadata cannot bound the read of the message that carries
it. The deadline bounds the whole RPC from the moment the server accepts
the handshake — it is not an inactivity timeout — and the server imposes a
deadline of its own besides (`WithServerTimeout`, an hour by default), so
a peer that upgrades and then goes silent is answered rather than parked.
The effective deadline is the shorter of the two.

**Metadata is a flat object with rules.** The payload of `M` is
`{"name":["value",...]}` — not draft 5's `{"metadata":{...}}`. Keys fold
to lower case, so two keys that differ only in case are one duplicate and
a protocol error; values are arrays of strings, never bare strings; a key
ending in `-bin` carries base64 with no padding; and a client's `M` must
not carry a reserved name — the Fetch standard's forbidden request headers,
anything a proxy in front of the server sets (`Forwarded`, `X-Forwarded-*`,
`X-Real-IP`, configurable with `WithInfrastructureHeaders`), or a name the
protocol controls (`Content-Type`, `Connect-Timeout-Ms`, ...). A reserved
name ends the RPC rather than being dropped, because dropping it would leave
the two ends disagreeing about the effective headers with nothing on the
wire to show it. The handshake's own headers are the base of the request
metadata, every one of them, and the message replaces them key by key.

**The handshake is HTTP/1.1 in the specification, and HTTP/2 too here.**
The specification does not adopt [RFC 8441](https://datatracker.ietf.org/doc/html/rfc8441)
and asks a server to refuse an extended CONNECT. This implementation
departs from that on purpose. A browser that has seen
`SETTINGS_ENABLE_CONNECT_PROTOCOL` on an existing HTTP/2 connection sends
its WebSocket handshake as an extended CONNECT on that connection, with no
fallback to HTTP/1.1, and a page cannot override the choice — so refusing
it means any deployment that enables the setting for anything else loses
the protocol in browsers entirely. The frames on the stream are identical
either way, and one WebSocket per RPC is the model that gains the most
from the HTTP/2 bootstrap: a handshake becomes one more stream on an open
connection instead of a TCP connection and a TLS handshake. Handlers
therefore serve both bootstraps; `WithH2Bootstrap` dials the HTTP/2 one
(the server process needs `GODEBUG=http2xconnect=1`). The RFC 6455
framing and permessage-deflate on that path are this package's own, as
they are in draft 5, because coder/websocket cannot accept over HTTP/2.

**Same-origin by default.** A handshake whose `Origin` host is not the
request's own `Host` is refused with `403` unless the accept options permit
it; the scheme is deliberately left out of the comparison, because a
deployment terminating TLS at a proxy sees `https` in the one and its own
plaintext host in the other. A handshake with no `Origin` is not from a
browser and is never refused on origin grounds.

**Compression is `permessage-deflate` with no context takeover, required.**
A server imposes `client_no_context_takeover` and
`server_no_context_takeover` on a client that did not offer them, as RFC
7692 allows, and a Go client verifies both were negotiated before it sends
a byte — a compression context shared across messages leaks plaintext
across trust boundaries. There is no message-level compression and no
marker for it.

**Size limits bound the message, not the frame.** A receiver applies
`WithReadMaxBytes` to the decompressed size of the whole message, reads at
most one byte past the limit, and reports the overrun without consuming the
rest: a server in `S` with `resource_exhausted`, a client by closing with
`1009` and failing the RPC. Either way the connection is then dropped
without a closing handshake, because the handshake would mean draining the
very message that was too big.

## Usage

Server. `Mount` registers a handler per procedure, each of which serves
both ordinary Connect HTTP and the WebSocket upgrade — unary procedures
included, because the specification lets any procedure upgrade:

```go
connectServer := connect.NewServer()
pingv1connect.RegisterPingServiceHandler(connectServer, pingServer{})

mux := http.NewServeMux()
draft7.Mount(mux, connectServer)
```

Client. One transport covers both dispatch paths — unary over `httpClient`
by default, streaming over a WebSocket dialed against the same base URL:

```go
transport := draft7.NewTransport(http.DefaultClient, "https://example.com")
client := pingv1connect.NewPingServiceClient(connect.NewClient(transport))
```

`WithUnaryOverWebSocket` sends unary RPCs over a WebSocket too, which is
the specification's own worked example. `WithProtoJSON` selects the JSON
codec, and with it the `connectrpc.1+json` subprotocol.

**A path prefix.** Some load balancers need to tell WebSocket traffic from
plain RPCs by URL alone. `WithPathPrefix("/ws")` on both sides puts every
handshake at prefix + procedure while plain HTTP RPCs keep the bare paths;
a non-upgrade request under the prefix is answered `426`, and an upgrade at
a bare procedure path `400`. It is also how draft 7 shares an origin with
another draft that already owns the procedure URLs: `MountWebSocket`
registers only the WebSocket side, under the prefix, which is how this
repository's demo and e2e servers run it beside draft 5.

Use `WithWebSocketAcceptOptions` to permit cross-origin handshakes
(`OriginPatterns`), and `WithoutCompression` to stop offering or accepting
permessage-deflate.

## Sequence

Each direction sends `M` first, then zero or more `B`, and ends: the client
with exactly one `C` (bare, or carrying the last body), the server with
exactly one `S`. A client that has already received `S` need not send `C`
— once the server has ended the RPC, the client simply closes. Anything the
client sends after `C` is a protocol error. A message the receiver does not
understand — an unknown marker, a marker valid only in the other direction,
an empty message with no marker at all — ends the stream: a server sends
`M` (if it has not yet) and an `S` carrying the error, then drops the
connection; a client fails the RPC and closes. A receiver never skips the
offending message and carries on.

`S` is Connect's `EndStreamResponse` JSON, unchanged: `error` absent on
success, trailers in `metadata`, `{}` when there is neither. A connection
that closes without `S` is a failed RPC whatever the close code said, and a
server whose stream is still open when the connection ends — no `C` seen,
no `S` sent — treats the RPC as canceled. `Receive` on the server tells
the two apart: `io.EOF` is the client's `C`; anything else is the client
gone.

## What it costs

One byte per message over draft 5, and nothing else on the wire: the
handshake, the connection-per-RPC model, and the compression are draft
5's. The unary rows in the benchmarks measure ordinary Connect over HTTP,
as draft 5's do, because `NewTransport` keeps unary there by default. What
the byte buys is a protocol that can be extended — every marker defined
before it is used, the high bit held in reserve — and a receiver that
never has to guess.

## Implementation

Like draft 5, the Go implementation is a fork of
[`connectrpc.com/connect/v2/connecthttp`](https://pkg.go.dev/connectrpc.com/connect/v2/connecthttp),
because the two dispatch paths share one configuration and
`connecthttp.Option` is opaque. Everything under the transport and handler
entry points is upstream code, kept verbatim so it can be re-synced; the
draft 7 behaviour lives in the files with no upstream counterpart: `ws.go`
(markers, subprotocols, the connection), `metadata.go` (the `M` payload
and the reserved-name rules), `ws_handler.go`, `ws_client.go`,
`ws_option.go`, and the HTTP/2 bootstrap in `ws_h2.go`, `ws_h2_conn.go`,
and `ws_compress.go`.
