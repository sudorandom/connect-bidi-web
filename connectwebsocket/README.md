# connectwebsocket

## Design strategy

This transport reuses as much of the [Connect protocol](https://connectrpc.com/docs/protocol/) as possible. RPC
messages use Connect codecs, per-message compression, and the standard 5-byte
Connect envelope. Responses use Connect codes, error details, trailers, and
the standard Connect EndStreamResponse JSON. Transport-specific protocol is
limited to what a WebSocket cannot provide itself: an initial headers envelope
and an explicit request end-stream envelope for half-close semantics.

This package carries Connect RPCs over WebSockets. It uses one dedicated
WebSocket connection per RPC. It does not multiplex RPCs, assign stream IDs, or
wrap envelopes in an outer protobuf message.

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

The transport is reusable and safe for concurrent RPCs. Each call to
`NewClientStream` dials a new WebSocket. `WithDialOptions` configures those
handshakes. Closing an RPC closes only its WebSocket.

## Protocol

### Connection mapping

One WebSocket connection carries exactly one RPC:

```text
RPC A <-> WebSocket connection 1
RPC B <-> WebSocket connection 2
RPC C <-> WebSocket connection 3
```

A streaming RPC may carry any number of request and response messages, but all
messages belonging to that RPC remain on its connection. Concurrent RPCs never
share a WebSocket. There is therefore no stream ID, connection-level demuxer,
or `WebSocketFrame` protobuf.

Only binary WebSocket messages are used. Each outgoing Connect envelope is
written as one binary WebSocket message. Receivers parse the contents as a byte
stream, so reads do not depend on WebSocket message boundaries. Text messages
are invalid protocol data.

### Envelope format

Every protocol item uses the standard 5-byte Connect envelope followed by its
payload:

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
- The length counts payload bytes only and may be zero.
- The maximum representable payload is `2^32 - 1` bytes. Configured send and
  receive limits may impose smaller bounds.

The defined flag values are:

| Value | Name | Payload |
| --- | --- | --- |
| `0x00` | data | One uncompressed RPC message encoded with the selected codec |
| `0x01` | compressed data | One compressed, codec-encoded RPC message |
| `0x02` | end stream | Empty on requests; Connect EndStreamResponse JSON on responses |
| `0x04` | headers | JSON metadata object (`{"metadata": ...}`) |

These are complete flag-byte values in this protocol. Other values and a
second headers envelope in the same direction are protocol errors.

### Control payloads

The headers envelope (`0x04`) uses this JSON object schema:

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

The client sends envelopes in this order:

```text
headers, zero or more data messages, end stream
```

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
`CloseSend`: no more request messages will be sent, while the connection stays
open for response messages. This explicit envelope is necessary because a
WebSocket close is bidirectional and cannot represent a half-close.

### Response sequence

The server sends:

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
protocol error.

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
compress the binary WebSocket message containing an envelope, but does not
change the Connect flags or payload semantics.

### Cancellation and connection lifetime

Client cancellation or closing the client stream closes that RPC's WebSocket.
The server also closes the connection after sending the response end stream.
Since a connection carries only one RPC, connection failure or backpressure
cannot block an unrelated RPC.

The cost of this isolation is a WebSocket handshake for every RPC. Standard
HTTP is generally preferable for unary calls. `NewCompositeTransport` can
route unary calls over HTTP while using WebSockets for streaming calls.

## Relationship to the Connect HTTP protocol

Message serialization, per-message compression, status codes, error details,
and the 5-byte envelope layout follow Connect semantics. The headers envelope
is a transport-specific addition because a raw WebSocket does not provide
per-RPC HTTP headers after the upgrade. The end-stream flag and JSON payload are
reused directly from the Connect streaming protocol. The overall exchange is
still not the Connect HTTP wire protocol and is not intended for a normal
Connect HTTP handler.
