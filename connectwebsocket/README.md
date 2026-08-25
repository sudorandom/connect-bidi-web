# connectwebsocket

Connect RPCs over WebSocket connections, with full bidirectional streaming
from environments such as web browsers.

The wire protocol exists in three **wire-incompatible drafts**, each in its
own subpackage with its own constructors and default path, so their designs
and implementations can be compared under identical benchmarks. The parent
package itself holds only the draft-agnostic `CompositeTransport`.

| Draft | Frame head | Compression negotiated via | Control payloads |
| --- | --- | --- | --- |
| [1](draft1/README.md) (`/websocket-draft1`) | 4-byte stream ID + 5-byte Connect envelope | `connect-*-encoding` metadata | JSON, per message |
| [3](draft3/README.md) (`/websocket-draft3`) | 4-byte stream ID + 1 type/flag byte | `connect.bidi.d3.deflate` subprotocol | JSON |
| [4](draft4/README.md) (`/websocket-draft4`) | ASCII `id\|flags\|` | permessage-deflate extension | JSON, always |

Every draft runs over two bootstraps carrying identical frames: the classic
HTTP/1.1 Upgrade handshake (`NewTransport`) and RFC 8441 extended CONNECT on
HTTP/2 (`NewH2Transport`), which additionally needs the server process to
run with `GODEBUG=http2xconnect=1`. Each draft's `NewHandler` serves both
automatically.

Each draft's README documents its own frame encoding and compression. What
follows is everything they share.

## Shared protocol

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

Each WebSocket message carries exactly one frame. Message boundaries are
significant: a frame never spans messages, and a message never carries more
than one frame. Drafts 1 and 3 use binary messages only. Draft 4 treats the
message type as a legibility hint and accepts either.

### Frame kinds

Every draft defines the same four kinds of frame. Only their encoding
differs — see each draft's README for the byte (or ASCII) values:

| Kind | Payload |
| --- | --- |
| data | One RPC message encoded with the selected codec |
| headers | JSON metadata object (`{"metadata": ...}`) |
| end-stream | Empty on requests; Connect `EndStreamResponse` JSON on responses |
| reset | Empty; aborts the stream |

A second headers frame in the same direction of the same stream is a
protocol error, as is an unrecognized frame kind.

### Control payloads

The headers frame uses this JSON object schema:

```json
{
  "metadata": {
    ":path": ["/connectrpc.eliza.v1.ElizaService/Converse"],
    "content-type": ["application/connect+proto"]
  }
}
```

The response end-stream frame reuses the Connect protocol's standard JSON
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
`value` is its unpadded base64-encoded binary value, and `debug` is an
optional best-effort JSON representation. `metadata` contains response
trailers as arrays of strings.

### Request sequence

For each stream, the client sends frames in this order:

```text
headers, zero or more data messages, end stream
```

A headers frame with a previously unseen stream ID opens a new stream; the
server routes every subsequent frame with that ID to the same RPC. Frames of
concurrent streams may interleave arbitrarily, but each stream's own frames
stay ordered.

The request headers payload must include:

- `:path`: the fully qualified RPC procedure, for example
  `/connect.ping.v1.PingService/Ping`.
- `Content-Type`: `application/connect+proto` or
  `application/connect+json`.

It may also include application request headers and:

- `Connect-Timeout-Ms`: remaining RPC timeout in milliseconds.

The request end-stream frame has an empty payload. It represents
`CloseSend`: no more request messages will be sent, while the stream stays
open for response messages. This explicit frame is necessary because neither
the stream nor the WebSocket has a send-direction close of its own.

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
carried in `metadata`. Receiving a successful end stream produces `io.EOF`
for the caller. Closing the WebSocket before a valid response end stream is
a protocol error for every stream still in flight.

Unary RPCs contain exactly one request data frame and, on success, exactly
one response data frame. Client-, server-, and bidirectional-streaming RPCs
use the same sequence with the number and timing of data frames appropriate
to the method type.

### Cancellation and connection lifetime

Closing the connection cannot cancel one RPC without killing the others, so
cancellation is a frame: a reset frame with an empty payload aborts the
stream it names. The client sends one when an RPC is canceled or abandoned
before the response finished; the server cancels that RPC's context, stops
sending frames for the stream, and keeps the connection and every other
stream running. Frames that arrive for a stream that has already finished or
been reset are dropped — stream IDs are never reused, so a late frame is
unambiguous. This mirrors HTTP/2, where `END_STREAM` marks a graceful
half-close and `RST_STREAM` aborts: both signals are needed, since a client
that already half-closed (a server-streaming RPC, say) has no other way left
to say "stop".

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

Standard HTTP is generally preferable for unary calls.
`connectwebsocket.NewCompositeTransport` is transport-agnostic and can route
unary calls over HTTP while using a WebSocket transport for streaming calls.

## Relationship to the Connect HTTP protocol

Message serialization, status codes, error details, and the
EndStreamResponse JSON follow Connect semantics. The framing does not: the
stream ID, headers frame, and reset frame are transport-specific additions,
because a raw WebSocket provides none of what HTTP gives Connect — no way to
tell concurrent RPCs apart, no per-RPC headers after the upgrade, and no
per-RPC teardown. The overall exchange is not the Connect HTTP wire protocol
and is not intended for a normal Connect HTTP handler.

---

See the demo site's
[conclusions](https://connect-bidi-web.kmcd.dev/#conclusions) for what
comparing the drafts settled.
