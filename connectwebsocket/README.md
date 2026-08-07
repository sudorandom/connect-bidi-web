# connectwebsocket

## Design strategy

This transport reuses as much of the [Connect protocol](https://connectrpc.com/docs/protocol/) as possible. RPC
messages use Connect codecs, per-message compression, and the standard 5-byte
Connect envelope. Responses use Connect codes, error details, trailers, and
the standard Connect EndStreamResponse JSON. Transport-specific protocol is
limited to what a WebSocket cannot provide itself:

- a **stream ID** on every frame, so several RPCs can share one connection
  and receivers can match each frame to the appropriate caller;
- an initial **headers envelope**, standing in for the per-RPC HTTP headers
  that disappear after the upgrade;
- an explicit **end-stream envelope** on requests for half-close semantics;
- a **reset envelope** to cancel one stream without closing the connection.

Each of these replaces something HTTP provides to the Connect protocol for
free: stream identification, headers, half-close, and `RST_STREAM`.

## Usage

Server:

```go
connectServer := connect.NewServer()
pingv1connect.RegisterPingServiceHandler(connectServer, pingServer{})

http.Handle("/websocket", connectwebsocket.NewHandler(connectServer))
```

Use `WithAcceptOptions` to configure the WebSocket upgrade, including origin
checks and WebSocket-level compression.

Client:

```go
transport := connectwebsocket.NewTransport("wss://example.com/websocket")
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
frame: the stream ID followed by one complete Connect envelope. Message
boundaries are significant — an envelope never spans messages, and a message
never carries more than one envelope. Text messages are invalid protocol
data.

### Frame format

Every frame is a 4-byte big-endian stream ID followed by a standard 5-byte
Connect envelope and its payload:

```text
  0                   1                   2                   3
  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                           Stream ID                           |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |     Flags     |              Payload length                   |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 | Payload len.  |                 Payload ...                   |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- `Stream ID` is an unsigned 32-bit big-endian integer.
- `Flags` is one byte.
- `Payload length` is an unsigned 32-bit big-endian integer.
- The length counts payload bytes only and may be zero. It must equal the
  remaining bytes of the WebSocket message.
- The maximum representable payload is `2^32 - 1` bytes. Configured send and
  receive limits may impose smaller bounds.

The defined flag values are:

| Value | Name | Payload |
| --- | --- | --- |
| `0x00` | data | One uncompressed RPC message encoded with the selected codec |
| `0x01` | compressed data | One compressed, codec-encoded RPC message |
| `0x02` | end stream | Empty on requests; Connect EndStreamResponse JSON on responses |
| `0x06` | headers | JSON metadata object (`{"metadata": ...}`) |
| `0x07` | reset | Empty; aborts the stream |

These are complete flag-byte values in this protocol, not bitmasks. Data
and compressed-data envelopes are byte-for-byte the Connect protocol's
envelopes; `0x06` and `0x07` are transport-specific frame types. Values
`0x08` and up are reserved for extended flags. Other values, and a second
headers envelope in the same direction of the same stream, are protocol
errors.

### Control payloads

The headers envelope (`0x06`) uses this JSON object schema:

```json
{
  "metadata": {
    ":path": ["/connectrpc.eliza.v1.ElizaService/Converse"],
    "content-type": ["application/connect+proto"]
  }
}
```

The response end-stream envelope (`0x02`) reuses the Connect protocol's standard JSON
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

For each stream, the client sends envelopes in this order:

```text
headers, zero or more data messages, end stream
```

A headers envelope with a previously unseen stream ID opens a new stream;
the server routes every subsequent frame with that ID to the same RPC.
Frames of concurrent streams may interleave arbitrarily, but each stream's
own envelopes stay ordered.

The request headers payload must include:

- `:path`: the fully qualified RPC procedure, for example
  `/connect.ping.v1.PingService/Ping`.
- `Content-Type`: `application/connect+proto` or
  `application/connect+json`.

It may also include application request headers and:

- `Connect-Content-Encoding`: compression applied to compressed request data.
- `Connect-Accept-Encoding`: response compression algorithms accepted by the
  client.
- `Connect-Timeout-Ms`: remaining RPC timeout in milliseconds.

The request end-stream envelope has an empty payload. It represents
`CloseSend`: no more request messages will be sent, while the stream stays
open for response messages. This explicit envelope is necessary because
neither the stream nor the WebSocket has a send-direction close of its own.

### Response sequence

For each stream, the server sends:

```text
headers, zero or more data messages, end stream
```

Response headers are always sent, including when the RPC fails before
producing a message. They include the response `Content-Type` and, when used,
`Connect-Content-Encoding`.

The final response end-stream payload is the Connect EndStreamResponse JSON
described above. A successful RPC omits `error`; a failed RPC includes the
Connect code, message, and error details. Application response trailers are
carried in `metadata`. Receiving a successful end stream produces `io.EOF` for
the caller. Closing the WebSocket before a valid response end stream is a
protocol error for every stream still in flight.

Unary RPCs contain exactly one request data envelope and, on success, exactly
one response data envelope. Client-, server-, and bidirectional-streaming RPCs
use the same sequence with the number and timing of data envelopes appropriate
to the method type.

### Compression

Compression applies independently to each RPC message, not to headers or the
end-stream payload. An uncompressed message uses `0x00`; a compressed message
uses `0x01` and the applicable `Connect-Content-Encoding` header names the
algorithm. Gzip is registered by default.

WebSocket extension compression is a separate transport-layer feature. It may
compress the binary WebSocket message containing a frame, but does not
change the stream ID, Connect flags, or payload semantics.

### Cancellation and connection lifetime

Closing the connection cannot cancel one RPC without killing the others, so
cancellation is a frame: a reset envelope (`0x07`, empty payload) aborts the
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

Standard HTTP is generally preferable for unary calls. `NewCompositeTransport`
can route unary calls over HTTP while using WebSockets for streaming calls.

## Relationship to the Connect HTTP protocol

Message serialization, per-message compression, status codes, error details,
and the 5-byte envelope layout follow Connect semantics. The stream ID,
headers envelope, and reset envelope are transport-specific additions
because a raw WebSocket provides none of what HTTP gives Connect: no way to
tell concurrent RPCs apart, no per-RPC headers after the upgrade, and no
per-RPC teardown. The end-stream flag and JSON payload are reused directly
from the Connect streaming protocol. The overall exchange is still not the
Connect HTTP wire protocol and is not intended for a normal Connect HTTP
handler.
