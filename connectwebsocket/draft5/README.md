# connectwebsocket/draft5

Draft 5 of the WebSocket wire protocol. Drafts [3](../draft3/README.md)
and [4](../draft4/README.md) coexist with it, so the designs can be
compared.

Draft 5 shares almost nothing with them, including the
[shared protocol](../README.md#shared-protocol) the others document once in
the parent README. It has no stream IDs, no frame types, no envelopes, no
length fields, and no dedicated path. It asks a different question:

> The WebSocket handshake is already an HTTP request. What is left to
> design?

The answer is: nearly nothing. Drafts 3 and 4 both treat the WebSocket
as a byte pipe and rebuild HTTP inside it — a stream ID because one pipe
carries many RPCs, a headers frame because a pipe has no headers, a frame
type byte because the receiver must be told what it just got. Draft 5
deletes each of those by refusing the premise. One WebSocket carries one
RPC, so there is nothing to multiplex. The upgrade request carries the URL
and the headers, so most of the request is expressed before a single
message is sent. What remains on the wire is the RPC's messages, unadorned:

```text
→  [text]    {"metadata":{"content-type":["application/connect+json"]}}
→  [text]    {"sentence":"Hello, Eliza!"}
→  [text]    {"sentence":"How are you?"}
→  [text]    (empty)                       ← half-close
←  [text]    {"metadata":{"content-type":["application/connect+json"]}}
←  [text]    {"sentence":"Eliza hears: Hello, Eliza!"}
←  [text]    (empty)                       ← end of response messages
←  [text]    {"metadata":{"trailer":["v"]}}
   [close 1000]
```

Data messages are the codec's output and nothing else. There is no draft 5
frame to parse, because there is no draft 5 frame — which is also why a
JSON stream reads end to end in a browser's Network tab with nothing
decoding it. A payload that is empty, or that is not valid UTF-8, travels
as a binary message instead — which protobuf sometimes is and sometimes is
not; see [Messages](#messages).

## The two ideas

**One socket, one RPC.** Every other draft multiplexes, which is what
forces a stream ID onto every frame and a reset frame into the protocol.
Draft 5 gives each streaming RPC its own WebSocket. Cancellation is then
just closing the socket; there is no reset frame because there is nothing
else on the connection to leave running. Head-of-line blocking cannot
happen, because there is no line to be at the head of.

The obvious objection — a handshake per RPC is expensive — is answered by
the bootstrap rather than by the protocol. Over
[RFC 8441](https://datatracker.ietf.org/doc/html/rfc8441) extended CONNECT,
a new WebSocket is a new HTTP/2 stream on a connection that is already
open: no TCP connection, no TLS handshake, and HPACK compresses the
repeated request headers down to a few bytes. Draft 5 is the draft that
*wants* the HTTP/2 bootstrap, where the others merely tolerate it. On the
HTTP/1.1 bootstrap the objection stands, and it is the honest cost of the
design.

**Unary RPCs never upgrade.** They are dispatched as ordinary Connect HTTP
requests, exactly as they would be without this package: `POST` to the
procedure URL, Connect's own unary framing, unchanged. A WebSocket
handshake to carry one request and one response is a bad trade, and the
composite transport in the parent package exists precisely to avoid it.
Draft 5 folds that split into the transport itself, so there is one
transport to configure rather than two to compose.

That is why draft 5 has no path of its own. The other drafts are mounted at
`/websocket-draftN` and speak only to themselves. Draft 5 is mounted on the
Connect procedure URLs, and each of them answers both methods:

| Method | Handled as |
| --- | --- |
| `POST /connectrpc.eliza.v1.ElizaService/Say` | Connect unary over HTTP |
| `GET /connectrpc.eliza.v1.ElizaService/Converse` + `Upgrade: websocket` | this protocol |

A client that has never heard of draft 5 sees a normal Connect endpoint,
because that is what it is.

## Usage

Server. `Mount` registers a handler per procedure, each of which serves
both ordinary Connect HTTP and the WebSocket upgrade:

```go
connectServer := connect.NewServer()
pingv1connect.RegisterPingServiceHandler(connectServer, pingServer{})

mux := http.NewServeMux()
draft5.Mount(mux, connectServer)
```

Client. One transport covers both dispatch paths — unary over `httpClient`,
streaming over a WebSocket dialed against the same base URL:

```go
transport := draft5.NewTransport(http.DefaultClient, "https://example.com")
client := pingv1connect.NewPingServiceClient(connect.NewClient(transport))
```

A `wss://` or `ws://` base URL is accepted and normalized for the HTTP
path; an `https://` base URL is normalized for the WebSocket dial. The two
schemes name the same endpoint, so the transport does not make the caller
say it twice.

To bootstrap streaming RPCs over HTTP/2 extended CONNECT instead of an
HTTP/1.1 upgrade, supply the HTTP/2 transport to dial with
(`GODEBUG=http2xconnect=1` must be set in the *server* process, and Go
clients must use `golang.org/x/net/http2` directly — `net/http` rejects the
`:protocol` pseudo-header before HTTP/2 sees it):

```go
transport := draft5.NewTransport(
	httpClient, "https://example.com",
	draft5.WithH2Bootstrap(&http2.Transport{}),
)
```

## Handshake

The upgrade request is the RPC request. Its URL path is the procedure; its
headers are request metadata:

```http
GET /connectrpc.eliza.v1.ElizaService/Converse HTTP/1.1
Host: example.com
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Version: 13
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
Sec-WebSocket-Protocol: connect.bidi.d5
Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits
Cookie: session=...
```

The `connect.bidi.d5` subprotocol is **required**, and the server must
select it. It is what separates an RPC upgrade from any other WebSocket a
deployment might serve on the same origin, and — since it is one of the
two things a browser is allowed to put on a handshake — it is the only
identification that works from every client.

The server routes the upgrade to the procedure named by the path. It
rejects the handshake, with an ordinary HTTP response and no upgrade, only
for failures that are not about a particular RPC:

| Condition | Response |
| --- | --- |
| Subprotocol absent or unrecognized | `400 Bad Request` |
| Path is not a registered procedure | `404 Not Found` |
| Procedure is unary | `405 Method Not Allowed` |

Everything else — an unimplemented method, a failed interceptor, an
expired deadline, an application error thrown before the first response
message — is reported **after** a successful upgrade, in the end-stream
message. That is deliberate, and it is the browser's doing: JavaScript
cannot read the status code or the headers of a failed WebSocket
handshake. A rejected handshake surfaces in the browser as an
indistinguishable connection failure, so any error a client is expected to
handle programmatically has to arrive on the socket, where every client can
read it.

### Why the first message exists anyway

If the handshake carries the headers, the headers message looks redundant.
It is not, and the reason is the whole browser problem in one line:

```js
new WebSocket(url, protocols) // ← there is no third argument
```

A browser cannot set a request header on a WebSocket handshake, and cannot
read a response header from one either. `content-type`,
`connect-protocol-version`, `connect-timeout-ms`, and every piece of call
metadata an interceptor wants to attach are all unreachable from the client
this transport exists to serve.

So the **first message in each direction is always a JSON metadata object**,
and it is mandatory rather than conditional. Making it required is what
keeps browser and non-browser clients byte-identical on the wire: there is
one shape to implement, one shape to test, and no negotiation about which
of two mechanisms carried the content type on any given connection.

The handshake headers are not wasted. The server takes them as the base of
the request metadata and the message overrides them key by key, so a
`Cookie` or an `Authorization` the platform attached by itself still
reaches the handler, and anything the Connect client set explicitly wins.
Headers that belong to the handshake rather than the RPC are stripped
before the merge: `Connection`, `Upgrade`, `Keep-Alive`,
`Proxy-Authenticate`, `Proxy-Authorization`, `TE`, `Trailer`,
`Transfer-Encoding`, and every `Sec-WebSocket-*`.

## Messages

Draft 5 reserves exactly one message shape. **An empty text message is the
separator**, always and only. Everything else is a message of the RPC:

| Opcode | Payload | Meaning |
| --- | --- | --- |
| text | empty | the separator, and nothing else |
| text | non-empty | the headers, the end-stream, or an RPC message |
| binary | any | an RPC message |

Position tells data from metadata, so the opcode never has to: the first
message in each direction is the headers, and the one message after the
server's separator is the end-stream. Every other message is data.

Senders pick the opcode by what the payload is, not by what it means: a
message travels as **text when its payload is non-empty and valid UTF-8**,
and as **binary otherwise**. So a JSON stream is readable end to end in a
browser's Network tab, at no cost in bytes — the opcode is a bit in a frame
header RFC 6455 sends whether a protocol uses it or not.

The "non-empty" half of that rule is the load-bearing part, and it exists
for one edge case that is easy to miss and impossible to work around after
the fact: **an empty protobuf message encodes to zero bytes.** A
`SayRequest` with no fields set is a legitimate, zero-length data message.
Were it allowed to travel as text, it would be indistinguishable from the
separator, and a service whose messages are sometimes empty would
half-close at random. So an empty payload is always binary:

```text
[binary] (empty)  → an empty RPC message
[text]   (empty)  → the separator
```

Unlike [draft 4](../draft4/README.md), where the opcode is a legibility
hint and receivers accept either kind, a draft 5 receiver that ignored the
opcode could not parse the stream.

One consequence is worth stating plainly, because it is easy to get wrong.
A protobuf payload is sometimes valid UTF-8 and sometimes not, and the line
between them is narrow: the *whole* encoding has to decode, framing bytes
included. A string field is UTF-8 by protobuf's own rules, but the tag and
the length varint in front of it are not always in ASCII range:

| Encoding | Bytes | Valid UTF-8 |
| --- | --- | --- |
| field 1, 13-byte string | `0a 0d 48 65 …` | yes |
| field 1, 127-byte string | `0a 7f 61 61 …` | yes |
| field 1, **128**-byte string | `0a 80 01 61 …` | no |
| **field 16**, 2-byte string | `82 01 02 68 …` | no |

So a short message on a low field number usually goes as text, and the same
method's longer messages go as binary — the opcode flips as a payload
crosses 128 bytes. Nothing depends on which it was: receivers treat text
and binary data identically, and the only invariant is that an empty
payload is never text. But it does mean "proto is the binary one" is not a
rule you can rely on, and that a stream's readability in devtools varies
message by message. A codec-gated rule (text only when the content type is
`+json`) would be deterministic instead; this draft chose the
content-based rule so the parse never depends on the handshake.

### Metadata payloads

The headers message uses the same JSON object as every other draft's
headers frame, minus `:path` — the URL already carries the procedure, and
a second copy that could disagree with it is a bug waiting to happen:

```json
{
  "metadata": {
    "content-type": ["application/connect+proto"],
    "connect-protocol-version": ["1"],
    "connect-timeout-ms": ["5000"]
  }
}
```

`content-type` is required on the request and selects the codec for the
whole connection: `application/connect+proto` or `application/connect+json`.
Because one socket carries one RPC, the codec is genuinely a property of
the connection here — the reason the other drafts must keep it per-stream
does not apply.

The end-stream message is the Connect protocol's standard
`EndStreamResponse`, unchanged and documented in the
[parent README](../README.md#control-payloads):

```json
{
  "error": { "code": "unavailable", "message": "..." },
  "metadata": { "trailer-name": ["value"] }
}
```

## Sequence

```text
client:  headers ─ data* ─ separator
server:  headers ─ data* ─ separator ─ end-stream ─ close(1000)
```

The client's separator is `CloseSend`: no more request messages, the
response stream stays open. WebSocket has no half-close of its own — the
close handshake tears down both directions — which is why an in-band
signal is needed at all. Sending anything after it is a protocol error.

The server's separator says the data phase is over and the next message is
the end-stream metadata. It is what lets trailers be told apart from a
response message without a type byte on every frame, and it is why the
separator is symmetric even though the client has no trailers to send: one
rule, both directions, rather than a rule and an exception.

The server's headers message is written lazily — before the first response
message, or before the end-stream message if there are none — matching when
Connect over HTTP flushes response headers. A handler that fails before
writing anything therefore produces exactly two messages, headers and
end-stream, with the separator between them.

After the end-stream message the server closes with status 1000. A client
that reads a close before the end-stream message reports
`CodeUnavailable`: the trailers are the only place a status can appear, so
their absence is a broken stream, never an implicit success.

Cancellation, in either direction, is closing the WebSocket. No frame, no
code, nothing to define — the connection *is* the RPC.

## Compression

permessage-deflate, negotiated in the handshake, with no context takeover.
Draft 5 has no compression of its own and no room for one: with no
envelope, a data message has nowhere to carry a "this one is compressed"
bit, and Connect's `connect-content-encoding` mechanism needs exactly that.
So the WebSocket extension is not one of two options here, as it is in
draft 4 — it is the only one available, which is a consequence of deleting
the framing rather than a preference.

It covers metadata messages as well as data, and it is negotiated on both
bootstraps: RFC 8441 §5 keeps `Sec-WebSocket-Extensions` in the extended
CONNECT exchange. As in draft 4, the HTTP/2 path implements the extension
itself, because coder/websocket cannot accept over HTTP/2.

Whether a client compresses what it *sends* stays the client's own choice,
per message, as the extension defines it: browsers do, Node's global
WebSocket does not.

It does work in both directions here, which is worth stating because the
benchmark suite spent a while implying otherwise. Its large payloads were
all *unary*, and draft 5 never puts unary on a socket, so nothing in the
numbers ever exercised this — deflate looked inert. On a stream with
something to compress it is emphatic: `internal/bench`'s
`bidi_16KiB_repetitive` row goes from 65992/65854 bytes to **1242/1106**,
and `TestStreamingCompression` pins the same result directly.

## What it costs

Per message, draft 5 is the floor. There is nothing to subtract:

| | Draft 3 | Draft 4 | Draft 5 |
| --- | --- | --- | --- |
| Per-message overhead | 5 bytes | 4–15 bytes | **0 bytes** |
| Multiplexing | yes | yes | no |
| Frames defined | 4 | 4 | 0 |
| Reset frame | yes | yes | not needed |
| Codec scope | per stream | per stream | per connection |
| Unary over the socket | yes | yes | never |
| Handshakes per streaming RPC | shared | shared | **1** |

The bill arrives per *connection* instead. A streaming RPC costs a full
WebSocket handshake — an HTTP request and response, and on HTTP/1.1 quite
possibly a TCP connection and a TLS handshake with it — where the other
drafts cost 9, 5, or 4 bytes on a socket that is already open.

The surprise is how quickly that pays for itself. In `internal/bench`, a
100-roundtrip bidi stream on the HTTP/1.1 bootstrap, identity codec, where
every other draft reuses one already-open connection and draft 5 dials a
fresh one for the RPC:

| | rxB/op | txB/op |
| --- | --- | --- |
| Draft 3 | 1433 | 1060 |
| Draft 4 | 1535 | 1162 |
| **Draft 5** | **1200** | **763** |

Draft 5 is the cheapest of the three *including* the handshake it pays and
the others don't. Saving five bytes on each of 200 messages buys more than
a handshake costs, so the break-even is somewhere around 75 messages — far
earlier than "a handshake per RPC" suggests. Below that a stream is better
off on a shared connection; above it, framing overhead dominates and draft
5 wins. On the HTTP/2 bootstrap the crossover moves earlier still, because
there a handshake is one more stream on a connection that is already open.

Where draft 5 genuinely loses is unary, and it loses there on purpose. The
same benchmark's small unary call costs 229/182 bytes against draft 3's
144/84: a Connect POST spends real HTTP headers where a unary RPC on an
open WebSocket spends a five-byte frame head. That is the price of an
endpoint that stays an ordinary Connect endpoint, and on the HTTP/2
bootstrap HPACK takes most of it back (73/94 bytes on a warm connection).

A per-RPC connection also means paying for the *handshake's* options on
every RPC. Negotiating permessage-deflate costs about 102 bytes of
`Sec-WebSocket-Extensions` in each direction, and on a stream of small
messages nothing ever compresses, so it is a pure loss — the same
100-roundtrip benchmark costs 1302/865 bytes with deflate negotiated
against 1200/763 without. The multiplexing drafts pay that once per
connection, where it disappears into the noise; draft 5 pays it per RPC.
Turn it off with `WithoutCompression()` for small-message streams.

The second cost is subtler and does not show up in a benchmark at all:
**nothing is shared, so nothing is amortized**. Four concurrent streaming
RPCs are four sockets, four handshakes, four sets of TCP buffers and four
flow-control windows in every proxy along the path. Multiplexing is what
the other drafts spend their frame head on, and what they buy with it is
that the tenth concurrent RPC costs almost nothing. Draft 5's tenth RPC
costs exactly what the first did — and browsers cap how many WebSockets a
page may hold open, which the other drafts never have to think about.

What it buys, beyond the bytes, is that there is no protocol to get wrong.
No stream-ID allocator, no demultiplexer, no per-stream state machine, no
reset semantics, no lifetime rule for a connection outliving the RPCs on
it — the class of bug those things produce cannot occur here, because the
code that would contain it does not exist. For a service with a handful of
long-lived streams per client, that is a very good trade. For one with
many short, concurrent ones, the connection count is what will hurt, and
a multiplexing draft is the better fit.

## Implementation

Draft 5's Go implementation is a fork of
[`connectrpc.com/connect/v2/connecthttp`](https://pkg.go.dev/connectrpc.com/connect/v2/connecthttp)
rather than a package layered on top of it, because the two dispatch paths
have to share one configuration. Codecs, size limits, compression, and the
`connect-protocol-version` requirement all apply to unary HTTP requests and
to WebSocket streams alike, and `connecthttp.Option` is an opaque type: a
wrapper can pass options through to the HTTP path but cannot read the
resolved values back out to configure the WebSocket path to match. Forking
gives both paths one options struct and one place to change.

On the HTTP path the fork carries everything upstream does, gRPC and
gRPC-Web included, because the copy is worth more kept verbatim than
trimmed: pruning them would have meant edits across ten upstream files for
no behaviour anyone wanted. Only the Connect protocol upgrades — `WithGRPC`
and `WithGRPCWeb` keep every RPC on HTTP, since those protocols have their
own answers for bidirectional streaming. The one thing that could not be
copied as-is is `internal/bufferpool`, which a fork outside the upstream
module cannot import; it is vendored into this repository's own
`internal/bufferpool`.

Everything under the transport and handler entry points is upstream code,
kept as close to verbatim as possible so it can be re-synced. The draft 5
behaviour lives in the files that have no upstream counterpart.
