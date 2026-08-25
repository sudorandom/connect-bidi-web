# connectwebsocket/draft2

Draft 2 of the WebSocket wire protocol. Drafts
[1](../draft1/README.md), [3](../draft3/README.md), and
[4](../draft4/README.md) sit alongside it; all four are wire-incompatible
and coexist so their designs and implementations can be compared. Serve
them on different paths.

Draft 2 differs from draft 1 in one idea: where draft 1 wraps each message
in a standard 5-byte Connect envelope carrying a compression flag and a
payload length, draft 2 delegates both jobs to the WebSocket itself —
permessage-deflate for compression, message boundaries for length — so a
frame is just a stream ID, a frame type, and the payload.

## Design strategy

This transport reuses as much of the [Connect protocol](https://connectrpc.com/docs/protocol/) as possible — Connect
codecs, Connect codes, error details, trailers, and the standard Connect
EndStreamResponse JSON — and as much of the WebSocket as possible. Where the
WebSocket already provides a capability, the protocol uses it instead of
duplicating it: message boundaries delimit frames, so frames carry no payload
length, and permessage-deflate compresses messages, so there is no
per-message compression. Transport-specific protocol is limited to what a
WebSocket cannot provide itself:

- a **stream ID** on every frame, so several RPCs can share one connection
  and receivers can match each frame to the appropriate caller;
- an initial **headers frame**, standing in for the per-RPC HTTP headers
  that disappear after the upgrade;
- an explicit **end-stream frame** on requests for half-close semantics;
- a **reset frame** to cancel one stream without closing the connection.

Each of these replaces something HTTP provides to the Connect protocol for
free: stream identification, headers, half-close, and `RST_STREAM`.

## Usage

Server:

```go
connectServer := connect.NewServer()
pingv1connect.RegisterPingServiceHandler(connectServer, pingServer{})

http.Handle("/websocket-draft2", draft2.NewHandler(connectServer))
```

Use `WithAcceptOptions` to configure the WebSocket upgrade, including origin
checks and compression. By default the handler offers permessage-deflate
(`websocket.CompressionNoContextTakeover`); custom accept options replace
that default entirely.

Client:

```go
transport := draft2.NewTransport("wss://example.com/websocket-draft2")
client := pingv1connect.NewPingServiceClient(connect.NewClient(transport))

resp, err := client.Ping(ctx, &pingv1.PingRequest{Text: "hello"})
```

The transport is reusable and safe for concurrent RPCs. All RPCs are
multiplexed onto one shared WebSocket connection, dialed lazily on the first
RPC and re-dialed if it fails. `WithDialOptions` configures those
handshakes. The returned transport also implements `io.Closer`; `Close`
closes the shared connection.

Because a shared connection is subject to head-of-line blocking (see
[Cancellation and connection lifetime](#cancellation-and-connection-lifetime)),
`WithConnectionPerStream` makes the transport dial a dedicated connection
for each streaming RPC instead; unary RPCs stay on the shared connection.
This is a client-side choice only — the server serves both patterns with no
configuration, since a dedicated connection is simply a multiplexed
connection carrying a single stream.

## WebSocket over HTTP/2 (RFC 8441)

The protocol runs over a WebSocket however that WebSocket came to be. Two
bootstraps carry identical frames:

- **HTTP/1.1 Upgrade** (RFC 6455): `NewTransport` dials it; one TCP + TLS
  handshake per WebSocket connection. permessage-deflate is negotiated by
  default.
- **HTTP/2 extended CONNECT** (RFC 8441): `NewH2Transport` dials it; each
  WebSocket is one `:method=CONNECT, :protocol=websocket` stream on a
  shared HTTP/2 connection, so WebSockets to the same origin share TCP and
  TLS state. There is no `Sec-WebSocket-Key` handshake — the server
  accepts with a plain 200 — but RFC 6455 message framing still applies on
  the stream.

`NewHandler` serves both automatically: an extended CONNECT request and an
Upgrade request on the same path reach the same RPCs.

```go
h2 := &http2.Transport{} // golang.org/x/net/http2
transport := draft2.NewH2Transport("https://example.com/websocket-draft2", h2)
```

The HTTP/2 bootstrap has real constraints:

- **The server process must run with `GODEBUG=http2xconnect=1`.** Go's
  HTTP/2 stack supports extended CONNECT but ships with it disabled (as of
  Go 1.26). Without it the server never advertises
  `SETTINGS_ENABLE_CONNECT_PROTOCOL` and resets `:protocol` streams, so no
  client — browsers included — ever attempts this bootstrap.
- **The Go client needs `golang.org/x/net/http2.Transport` directly**
  (which is why `NewH2Transport` takes one): plain `net/http` clients
  reject the `:protocol` pseudo-header before the HTTP/2 layer sees it.
- **No permessage-deflate.** Extensions are not negotiated on this
  bootstrap, and HTTP/2 does not compress DATA frames, so draft 2 messages
  travel uncompressed over HTTP/2.
- **Browsers pick this bootstrap on their own.** `new WebSocket(...)`
  rides an existing HTTP/2 connection when the origin advertises extended
  CONNECT support; JavaScript can neither force nor observe the choice.
- Dialing a server that hasn't enabled extended CONNECT fails with
  `CodeUnavailable` ("extended connect not supported by peer") — the
  signal for falling back to the HTTP/1.1 bootstrap.

## Protocol

### Connection mapping

One WebSocket connection carries any number of concurrent RPCs:

```text
RPC A (stream 1) ─┐
RPC B (stream 2) ─┼─ WebSocket connection
RPC C (stream 3) ─┘
```

Every frame begins with the ID of the stream it belongs to. Stream IDs are
assigned by the client: the first stream on a connection is 1, and each new
stream increments the ID by one. IDs are never reused within a connection,
so a client may also choose to open a new connection per RPC (as
`WithConnectionPerStream` does) — such a connection simply carries a single
stream.

Only binary WebSocket messages are used. Each message carries exactly one
frame. Message boundaries are significant — a frame never spans messages,
and a message never carries more than one frame. Text messages are invalid
protocol data.

### Frame format

Every frame is a 4-byte big-endian stream ID and a 1-byte frame type; the
rest of the WebSocket message is the payload:

```text
  0                   1                   2                   3
  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                           Stream ID                           |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |  Frame type   |                 Payload ...                   |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- `Stream ID` is an unsigned 32-bit big-endian integer.
- `Frame type` is one byte.
- The payload has no length prefix: it is exactly the remaining bytes of the
  WebSocket message, and may be empty. The WebSocket's own framing delimits
  it. Frame size is bounded only by configured send and receive limits.

The defined frame types are:

| Value | Name | Payload |
| --- | --- | --- |
| `0x00` | data | One RPC message encoded with the selected codec |
| `0x01` | headers | JSON metadata object (`{"metadata": ...}`) |
| `0x02` | end stream | Empty on requests; Connect EndStreamResponse JSON on responses |
| `0x03` | reset | Empty; aborts the stream |

Values `0x04` and up are reserved for future frame types. Other values, and
a second headers frame in the same direction of the same stream, are
protocol errors.

### Control payloads

The headers frame (`0x01`) uses this JSON object schema:

```json
{
  "metadata": {
    ":path": ["/connectrpc.eliza.v1.ElizaService/Converse"],
    "content-type": ["application/connect+proto"]
  }
}
```

The response end-stream frame (`0x02`) reuses the Connect protocol's standard JSON
EndStreamResponse:

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
`value` is its unpadded base64-encoded binary value, and `debug` is an optional
best-effort JSON representation. `metadata` contains response trailers as
arrays of strings.

### Request sequence

For each stream, the client sends frames in this order:

```text
headers, zero or more data messages, end stream
```

A headers frame with a previously unseen stream ID opens a new stream;
the server routes every subsequent frame with that ID to the same RPC.
Frames of concurrent streams may interleave arbitrarily, but each stream's
own frames stay ordered.

The request headers payload must include:

- `:path`: the fully qualified RPC procedure, for example
  `/connect.ping.v1.PingService/Ping`.
- `Content-Type`: `application/connect+proto` or
  `application/connect+json`.

It may also include application request headers and:

- `Connect-Timeout-Ms`: remaining RPC timeout in milliseconds.

The request end-stream frame has an empty payload. It represents
`CloseSend`: no more request messages will be sent, while the stream stays
open for response messages. This explicit frame is necessary because
neither the stream nor the WebSocket has a send-direction close of its own.

### Response sequence

For each stream, the server sends:

```text
headers, zero or more data messages, end stream
```

Response headers are always sent, including when the RPC fails before
producing a message. They include the response `Content-Type`.

The final response end-stream payload is the Connect EndStreamResponse JSON
described above. A successful RPC omits `error`; a failed RPC includes the
Connect code, message, and error details. Application response trailers are
carried in `metadata`. Receiving a successful end stream produces `io.EOF` for
the caller. Closing the WebSocket before a valid response end stream is a
protocol error for every stream still in flight.

Unary RPCs contain exactly one request data frame and, on success, exactly
one response data frame. Client-, server-, and bidirectional-streaming RPCs
use the same sequence with the number and timing of data frames appropriate
to the method type.

### Compression

This protocol has no compression of its own. Compression is the WebSocket's
job: when client and server negotiate the permessage-deflate extension
([RFC 7692](https://datatracker.ietf.org/doc/html/rfc7692)) during the
upgrade, every frame — data, headers, and end-stream payloads alike — is
compressed transparently below this protocol. Frame payloads themselves are
always the uncompressed bytes, and the `Connect-Content-Encoding` and
`Connect-Accept-Encoding` headers are not used.

Compression is therefore best-effort: it applies only when both endpoints
and every WebSocket-terminating intermediary support the extension, and its
absence is invisible to this protocol. Both the handler and the transport
offer permessage-deflate by default
(`websocket.CompressionNoContextTakeover`); use `WithAcceptOptions`
(server) and `WithDialOptions` (client) to configure or disable it — custom
options replace the defaults entirely.

### Cancellation and connection lifetime

Closing the connection cannot cancel one RPC without killing the others, so
cancellation is a frame: a reset frame (`0x03`, empty payload) aborts the
stream it names. The client sends one when an RPC is canceled or abandoned
before the response finished; the server cancels that RPC's context,
stops sending frames for the stream, and keeps the connection and every
other stream running. Frames that arrive for a stream that has already
finished or been reset are dropped — stream IDs are never reused, so a late
frame is unambiguous. This mirrors HTTP/2, where `END_STREAM` marks a
graceful half-close and `RST_STREAM` aborts: both signals are needed, since
a client that already half-closed (a server-streaming RPC, say) has no other
way left to say "stop".

The connection itself stays open across RPCs, saving a WebSocket handshake
(and TLS setup) per call. Closing the connection terminates every stream on
it; in-flight handlers are canceled.

Multiplexing has one inherent cost: **head-of-line blocking**. All streams
share one TCP connection and one ordered byte stream, so a large message or
a stalled consumer on one stream delays frames of every other stream behind
it. There is no per-stream flow control (unlike HTTP/2 or QUIC).
`WithConnectionPerStream` restores full isolation by dialing a dedicated
connection per streaming RPC, at the cost of a handshake each — the protocol
is identical either way, so the server needs no configuration. WebTransport
does not have this trade-off at all: QUIC streams are independently
flow-controlled, which is why the WebTransport transport needs neither
stream IDs nor reset frames.

Standard HTTP is generally preferable for unary calls. The parent package's
`connectwebsocket.NewCompositeTransport` is transport-agnostic and can route
unary calls over HTTP while using this transport for streaming calls.

## Relationship to the Connect HTTP protocol

Message serialization, status codes, error details, and the EndStreamResponse
JSON follow Connect semantics. The framing does not: where Connect over HTTP
wraps each message in a 5-byte envelope carrying a compression flag and a
payload length, this protocol delegates both jobs to the WebSocket —
permessage-deflate for compression, message boundaries for length — and a
frame carries only a stream ID and a frame type. The stream ID, headers
frame, and reset frame are transport-specific additions because a raw
WebSocket provides none of what HTTP gives Connect: no way to tell
concurrent RPCs apart, no per-RPC headers after the upgrade, and no per-RPC
teardown. The overall exchange is not the Connect HTTP wire protocol and is
not intended for a normal Connect HTTP handler.
