# connectwebsocket/draft4

Draft 4 of the WebSocket wire protocol. Drafts [3](../draft3/README.md)
and [5](../draft5/README.md) coexist with it; drafts 3 and 4 each have
their own constructors and default path (`/websocket-draft4` here), so the
designs can be compared. Everything those two share — connection mapping,
control payload JSON, request and response sequences, cancellation,
half-close — is documented once in the
[parent README](../README.md#shared-protocol); draft 5 shares none of it.

Draft 4 optimizes for the client that actually matters. This transport
exists because **browsers** can't do bidirectional streaming over `fetch` —
browsers are the primary WebSocket client, and the browser's own Network
tab is where this traffic gets read. Draft 3 is opaque there: binary
frames render as hex or as a blob, so the one debugging surface every web
developer already has is useless on it.

Draft 4 fixes that, by making the frame text:

```text
7|1|{"metadata":{":path":["/connectrpc.eliza.v1.ElizaService/Converse"]}}
7|0|{"sentence":"hello"}
7|2|
```

That is the whole framing. A stream ID, a flags field, and a payload, all
separated by `|`, with no length fields, no bit packing, and no
compression flag. Frames whose payload is UTF-8 are sent as *text*
WebSocket messages, so devtools renders them as text rather than as bytes.
Open the Network tab, click the connection, and read the conversation —
no extension, no proxy, no decoder, no `tcpdump`.

Two other things follow from that goal:

- **Control payloads are always JSON.** The headers and end-stream frames
  were already JSON in every draft; draft 4 makes it a rule rather than a
  default, so no future revision can make the metadata unreadable.
- **Compression is the WebSocket's job.** Draft 3's per-frame DEFLATE
  needs a signal bit in the frame head, and a bit is exactly what a text
  head can't carry cheaply; permessage-deflate needs no protocol surface
  at all. It is negotiated in the handshake and works over both
  bootstraps: RFC 8441 §5 keeps `Sec-WebSocket-Extensions`, so the HTTP/2
  path negotiates it in the CONNECT exchange. Because coder/websocket
  cannot accept over HTTP/2, that path implements the extension itself
  (see `compress.go`), with no context takeover.

  It did not always. For a while the HTTP/2 path had no deflate at all, and
  a 16 KiB compressible payload went out at 16841 B there against 327 B
  over HTTP/1.1 — same code, same options, no error and no failing test.
  That is the hazard of putting a feature *below* your connection
  abstraction: swap the connection and the feature leaves with it. Draft 3
  keeps compression above that line and was never affected.

  The other thing the extension leaves to someone else is whether a client
  compresses what it *sends*: RSV1 is a per-message choice, so each endpoint
  decides on its own. Browsers do — Chrome sends a 16 KiB compressible
  upload as ~106 B, Firefox as ~168 B. Node's global WebSocket does not: it
  negotiates the extension, inflates what it receives, and uploads every
  message in full. Draft 3, which compresses in the protocol, is the same in
  both directions everywhere.

## Usage

Server (one handler serves both bootstraps; HTTP/2 extended CONNECT
additionally needs the process to run with `GODEBUG=http2xconnect=1`):

```go
connectServer := connect.NewServer()
pingv1connect.RegisterPingServiceHandler(connectServer, pingServer{})

http.Handle("/websocket-draft4", draft4.NewHandler(connectServer))
```

Client:

```go
// HTTP/1.1 upgrade bootstrap:
transport := draft4.NewTransport("wss://example.com/websocket-draft4")

// ...or HTTP/2 extended CONNECT (golang.org/x/net/http2):
transport = draft4.NewH2Transport("https://example.com/websocket-draft4", nil)

client := pingv1connect.NewPingServiceClient(connect.NewClient(transport))
```

For a fully readable connection, send JSON too — then no frame on the wire
is binary:

```go
transport := draft4.NewTransport(
	"wss://example.com/websocket-draft4",
	draft4.WithSendCodec(connect.CodecNameJSON),
	draft4.WithoutCompression(), // a compressed text frame is no more readable than a binary one
)
```

## Frame format

Every WebSocket message carries exactly one frame belonging to exactly one
stream:

```text
<stream ID> "|" <flags> "|" <payload>
```

- `stream ID` is an unsigned 32-bit integer in unpadded decimal ASCII.
  Assigned by the client, starting at 1, incrementing by one per stream,
  never reused within a connection — as in every other draft.
- `flags` is an unsigned 8-bit integer in unpadded decimal ASCII,
  partitioned so it reads as a small enum today and supports bit math
  tomorrow:

  ```text
   7   6   5   4   3   2   1   0
  +---+---+---+---+---+---+---+---+
  |      flags        |   type    |
  +---+---+---+---+---+---+---+---+
  ```

  The **low 3 bits** are the frame type:

  | Value | Name | Payload |
  | --- | --- | --- |
  | `0` | data | One RPC message encoded with the selected codec |
  | `1` | headers | JSON metadata object (`{"metadata": ...}`) |
  | `2` | end-stream | Empty on requests; Connect `EndStreamResponse` JSON on responses |
  | `3` | reset | Empty; aborts the stream |

  Types `4`–`7` are reserved. They must *parse* — a receiver rejects them
  as an unknown frame type, not as a malformed frame — because a frame
  whose payload semantics are unknown can't be safely handled.

  The **high 5 bits** (`8`, `16`, `32`, `64`, `128`) are independent flags.
  None is defined yet, so every frame today is simply `0`–`3`. A later
  revision ORs one in: a data frame carrying flag `8` is `type | 8` = `8`,
  and reads on the wire as `7|8|…`.

  **Receivers ignore unknown flag bits**, and mask with `& 7` to get the
  type. That asymmetry — ignore unknown flags, reject unknown types — is
  what makes a new flag deployable without a coordinated upgrade, and it's
  the same rule HTTP/2 applies to its own frame flags. It is the reason
  this field is called `flags` rather than `type`.
- `payload` is everything after the second `|`, delimited by the WebSocket
  message itself. There is no length field, exactly as in draft 3.

**The payload is never escaped.** Parsers split on the first two `|` bytes
and treat the rest as opaque, so a `|` inside a JSON string or a protobuf
field is just a payload byte. This is what keeps the text head free: no
escaping pass over the payload, no length prefix to keep consistent.

Senders emit no leading zeros. Receivers accept them, which costs nothing
and keeps hand-written frames working.

### Message type

The WebSocket opcode is a **legibility hint, not protocol data**: receivers
accept text and binary messages alike, and a peer may send every frame as
binary without violating anything. Senders here use a text message whenever
the whole frame is valid UTF-8 — always for control frames, and for data
frames when the codec is JSON — because that is what makes browser devtools
and packet captures render the frame as text instead of a hex dump. A proto
data payload is not UTF-8, so its frame is binary; its head is still ASCII
and still readable in a hex view.

Everything else — connection mapping, stream IDs, control payload JSON,
request and response sequences, cancellation, half-close — is the
[shared protocol](../README.md#shared-protocol).

## Negotiation

Draft 4 defines no WebSocket subprotocol; unlike draft 3, it is
identified by the path it is served on. Compression is negotiated purely as
the permessage-deflate extension, which the handler accepts by default
(`websocket.CompressionNoContextTakeover`) and `WithoutCompression()`
declines. The `CompressionMode` field of custom dial and accept options is
overridden either way: compression is the protocol's only compression, so
custom options can't silently disable it.

## The trade

The measured cost is stranger than "text is bigger". Draft 4's head is
*variable width* — 4 bytes for `"1|0|"`, 6 for `"201|0|"` — where draft 3
spends a fixed 5. So it depends entirely on how far a connection's
stream counter has climbed (`internal/bench`, 100-roundtrip bidi stream,
identity):

| Stream ID | Head | rxB/op vs draft 3 |
| --- | --- | --- |
| 1 digit | 4 bytes | 1331 vs 1433 — **draft 4 wins by 102 B** |
| 2 digits | 5 bytes | parity |
| 3 digits | 6 bytes | 1535 vs 1433 — draft 4 loses by 102 B |

Stream IDs are never reused within a connection, so a long-lived
multiplexed connection walks steadily rightward through that table: past
about 100 RPCs every frame carries a 6-byte head, past 1000 a 7-byte one.
That is the honest cost — not "text costs more than binary" but **framing
overhead that grows with connection age**, which no fixed-width head does.
A client that dials per RPC never leaves the first row.

Small unary calls and 16 KiB payloads come out at parity (144 vs 144 B, and
16534 vs 16534 B), because there the head is one frame's worth of a much
larger exchange. The `json` benchmark case measures the other half of the
bill: choosing the readable codec, which costs payload bytes, not head
bytes.

What the variable head buys is not performance:

| | Draft 3 | Draft 4 |
| --- | --- | --- |
| Frame head | 5 bytes | 4–15 bytes ASCII, growing with the stream counter |
| Readable without a decoder | no | no | **yes** |
| Payload length | explicit u32 | message boundary | message boundary |
| Compression unit | per message (Connect gzip) | per frame (protocol deflate) | per message (permessage-deflate) |
| Negotiated via | Connect metadata headers | WebSocket subprotocol | WebSocket extension |
| Covers control payloads | no | yes | yes |
| Compression needs a per-bootstrap implementation | no | no | **yes** |
| Control payloads guaranteed JSON | no | no | **yes** |

The summary: for a browser-facing service, draft 4 is a reasonable
*default*. Its cost is real but narrow — a head that grows with a
connection's age — while what it buys applies to every debugging session in
the client this transport was built for. Legibility in the primary client is not a developer luxury; it
is the difference between a protocol your users can support and one only
its authors can.

Draft 3 remains the pick where wire bytes are the binding constraint:
metered links, high-rate streams of tiny messages, or deployments that need
compression over the HTTP/2 bootstrap.
