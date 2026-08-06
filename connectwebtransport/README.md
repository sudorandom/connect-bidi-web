# connectwebtransport

## Design strategy

This transport reuses as much of the [Connect protocol](https://connectrpc.com/docs/protocol/) as possible. RPC
messages use Connect codecs, per-message compression, and the standard 5-byte
Connect envelope. Responses use Connect codes, error details, trailers, and
the standard Connect EndStreamResponse JSON. Transport-specific protocol is
limited to what a raw WebTransport stream cannot provide itself: an initial
headers envelope and an optional explicit request end-stream marker for
adapters that cannot expose QUIC half-close. Native QUIC streams are used
instead of inventing application-level stream multiplexing.

This package carries Connect RPCs over WebTransport and HTTP/3. One persistent
WebTransport session can serve many concurrent RPCs, and every RPC gets its own
native bidirectional QUIC stream.

## Usage

Server:

```go
connectServer := connect.NewServer()
pingv1connect.RegisterPingServiceHandler(connectServer, pingServer{})
handler := connectwebtransport.NewHandler(connectServer)

wtServer := &webtransport.Server{
    H3: &http3.Server{
        Addr:      ":4433",
        TLSConfig: tlsConfig,
    },
}

http.Handle("/webtransport", handler.UpgradeHandler(wtServer))
```

`UpgradeHandler` upgrades the CONNECT request via `wtServer` and serves RPCs
on the accepted session. When sessions are accepted elsewhere, use
`HandleSession` directly:

```go
handler.HandleSession(ctx, session)
```

Client:

```go
wtClient := &webtransport.Transport{TLSClientConfig: tlsConfig}
defer wtClient.Close()

_, session, err := wtClient.Dial(
    ctx,
    "https://example.com/webtransport",
    nil,
)
if err != nil {
    log.Fatal(err)
}

transport := connectwebtransport.NewTransport(session)
client := pingv1connect.NewPingServiceClient(connect.NewClient(transport))

resp, err := client.Ping(ctx, &pingv1.PingRequest{Text: "hello"})
```

The transport reuses the session. Each call to `NewClientStream` opens a new
bidirectional WebTransport stream with `session.OpenStreamSync`.

## Protocol

### Session and stream mapping

The WebTransport HTTP/3 connection and session establishment are handled by
`webtransport-go`. After the session is established, RPCs map directly to QUIC
streams:

```text
WebTransport session
├── bidirectional stream 0 <-> RPC A
├── bidirectional stream 4 <-> RPC B
└── bidirectional stream 8 <-> RPC C
```

QUIC assigns stream IDs and provides multiplexing, per-stream ordering,
flow-control, cancellation, and independent loss recovery. This protocol does
not put a stream ID in its envelopes and does not multiplex multiple RPCs over
one QUIC stream. It also does not use a `WebSocketFrame` protobuf.

### Envelope format

Each bidirectional stream is an ordered byte stream containing standard 5-byte
Connect envelopes:

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
- Envelope boundaries are independent of QUIC write and packet boundaries;
  receivers read exactly five header bytes followed by the declared payload.

The defined flag values are:

| Value | Name | Payload |
| --- | --- | --- |
| `0x00` | data | One uncompressed RPC message encoded with the selected codec |
| `0x01` | compressed data | One compressed, codec-encoded RPC message |
| `0x02` | end stream | Empty on requests when used; Connect EndStreamResponse JSON on responses |
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

The client sends:

```text
headers, zero or more data messages, request end
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

The Go client represents `CloseSend` with a QUIC stream write-side close
(FIN), which leaves the read side available for responses. The server treats
EOF as the end of the request. An empty `0x02` request envelope is also
recognized as an explicit request end marker, which is useful for clients
whose stream adapter represents half-close in-band.

### Response sequence

The server sends:

```text
headers, zero or more data messages, end stream, QUIC FIN
```

Response headers are always sent, including when the RPC fails before
producing a message. They include the response `Content-Type` and, when used,
`Connect-Content-Encoding`.

The final end-stream payload is the Connect EndStreamResponse JSON described
above. A successful RPC omits `error`; a failed RPC includes the Connect code,
message, and error details. Application response trailers are carried in
`metadata`. Receiving a successful end stream produces `io.EOF` for the
caller. A QUIC FIN before a valid response end-stream envelope is a protocol
error.

Unary RPCs contain exactly one request data envelope and, on success, exactly
one response data envelope. Client-, server-, and bidirectional-streaming RPCs
use the same sequence with the number and timing of data envelopes appropriate
to the method type.

### Compression

Compression applies independently to each RPC message, not to headers or the
end-stream payload. An uncompressed message uses `0x00`; a compressed message
uses `0x01` and the applicable `Connect-Content-Encoding` header names the
algorithm. Gzip is registered by default.

For responses, the server selects the first supported value from the client's
`Connect-Accept-Encoding` list and reports it in
`Connect-Content-Encoding`. HTTP/3 and QUIC packet processing do not change the
envelope or compression semantics.

### Cancellation and lifetime

Canceling an RPC resets or closes only its QUIC stream; the WebTransport
session and unrelated RPC streams remain usable. Closing the session ends all
RPCs carried by it. The `Connect-Timeout-Ms` request header establishes a
server-side deadline for the individual RPC.

## Why use one QUIC stream per RPC?

Opening a new WebTransport session for each call would repeat an HTTP/3
handshake and discard QUIC's cheap native streams. Conversely, placing many
RPCs on one WebTransport stream would require custom stream IDs, demultiplexing,
and application-level flow control. A native stream per RPC lets QUIC provide
these capabilities and avoids head-of-line blocking between calls.

Standard Connect over HTTP/3 remains preferable when browser WebTransport APIs
or raw bidirectional streams are unnecessary. This transport exists for cases
where an application explicitly needs a WebTransport session.

## Relationship to the Connect HTTP protocol

Message serialization, per-message compression, status codes, error details,
and the 5-byte envelope layout follow Connect semantics. The headers envelope
is a transport-specific addition because a raw WebTransport stream does not
itself carry HTTP request/response metadata. The end-stream flag and JSON
payload are reused directly from the Connect streaming protocol. The overall
exchange is still not the Connect HTTP wire protocol and is not intended for a
normal Connect HTTP handler.
