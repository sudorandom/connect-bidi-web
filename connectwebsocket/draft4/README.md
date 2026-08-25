# connectwebsocket/draft4

Draft 4 of the WebSocket wire protocol. Drafts [1](../draft1/README.md),
[2](../draft2/README.md), and [3](../draft3/README.md) coexist with it, each
with its own constructors and default path (`/websocket-draft4`), so the
designs can be compared.

Draft 4 asks a different question than its predecessors. Drafts 2 and 3
optimized the frame head down to five packed binary bytes and then argued
about where to negotiate compression. Draft 4 gives up those bytes on
purpose, to find out what a **legible** wire protocol costs:

```text
7|1|{"metadata":{":path":["/connectrpc.eliza.v1.ElizaService/Converse"]}}
7|0|{"sentence":"hello"}
7|2|
```

That is the whole framing. A stream ID, a flags field, and a payload, all
separated by `|`, with no length fields, no bit packing, and no
compression flag. Open the Network tab in a browser, or point `tcpdump` at
the socket, and the conversation reads as text — which is exactly what
drafts 1 through 3 cannot do.

Two other things follow from that goal:

- **Control payloads are always JSON.** The headers and end-stream frames
  were already JSON in every draft; draft 4 makes it a rule rather than a
  default, so no future revision can make the metadata unreadable.
- **Compression is the WebSocket's job again**, as in draft 2. Draft 3's
  per-frame DEFLATE needed a signal bit in the frame head, and a bit is
  exactly what a text head can't carry cheaply. permessage-deflate needs
  no protocol surface at all. The cost is draft 2's cost, measured in
  [CONCLUSIONS.md](../../CONCLUSIONS.md): the extension does not exist over
  the HTTP/2 bootstrap, and browsers never compress what they *send* with
  it.

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
  message itself. There is no length field, exactly as in drafts 2 and 3.

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
request and response sequences, cancellation, half-close — is identical to
[draft 2](../draft2/README.md#protocol).

## Negotiation

Draft 4 defines no WebSocket subprotocol; like drafts 1 and 2, it is
identified by the path it is served on. Compression is negotiated purely as
the permessage-deflate extension, which the handler accepts by default
(`websocket.CompressionNoContextTakeover`) and `WithoutCompression()`
declines. The `CompressionMode` field of custom dial and accept options is
overridden either way: compression is the protocol's only compression, so
custom options can't silently disable it.

## The trade

The measured cost is stranger than "text is bigger". Draft 4's head is
*variable width* — 4 bytes for `"1|0|"`, 6 for `"201|0|"` — where drafts 2
and 3 spend a fixed 5. So it depends entirely on how far a connection's
stream counter has climbed (`internal/bench`, 100-roundtrip bidi stream,
identity):

| Stream ID | Head | rxB/op vs draft 2 |
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

| | Draft 1 | Draft 2 | Draft 3 | Draft 4 |
| --- | --- | --- | --- | --- |
| Frame head | 9 bytes | 5 bytes | 5 bytes | 4–15 bytes ASCII, growing with the stream counter |
| Readable without a decoder | no | no | no | **yes** |
| Payload length | explicit u32 | message boundary | message boundary | message boundary |
| Compression unit | per message (Connect gzip) | per message (permessage-deflate) | per frame (protocol deflate) | per message (permessage-deflate) |
| Negotiated via | Connect metadata headers | WebSocket extension | WebSocket subprotocol | WebSocket extension |
| Covers control payloads | no | yes | yes | yes |
| Works over HTTP/2 bootstrap | yes | **no** | yes | **no** |
| Control payloads guaranteed JSON | no | no | no | **yes** |

The honest summary: draft 4 is the draft to reach for while *debugging* a
bidi protocol, or when the operators of a system will spend more time
reading its traffic than paying for its bytes. For everything else, draft 3
remains the recommendation in [CONCLUSIONS.md](../../CONCLUSIONS.md).
