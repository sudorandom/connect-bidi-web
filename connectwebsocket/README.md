# connectwebsocket

Connect RPCs over WebSocket connections, with full bidirectional streaming
from environments such as web browsers.

The wire protocol exists in four **wire-incompatible drafts**, each in its
own subpackage with its own constructors and default path, so their designs
and implementations can be compared under identical benchmarks. The parent
package itself holds only the draft-agnostic `CompositeTransport`.

| Draft | Frame head | Compression negotiated via | Control payloads |
| --- | --- | --- | --- |
| [1](draft1/README.md) (`/websocket-draft1`) | 4-byte stream ID + 5-byte Connect envelope | `connect-*-encoding` metadata | JSON, per message |
| [2](draft2/README.md) (`/websocket-draft2`) | 4-byte stream ID + 1 type byte | permessage-deflate extension | JSON |
| [3](draft3/README.md) (`/websocket-draft3`) | 4-byte stream ID + 1 type/flag byte | `connect.bidi.d3.deflate` subprotocol | JSON |
| [4](draft4/README.md) (`/websocket-draft4`) | ASCII `id\|type\|` | permessage-deflate extension | JSON, always |

Every draft runs over two bootstraps carrying identical frames: the classic
HTTP/1.1 Upgrade handshake (`NewTransport`) and RFC 8441 extended CONNECT on
HTTP/2 (`NewH2Transport`), which additionally needs the server process to
run with `GODEBUG=http2xconnect=1`. Each draft's `NewHandler` serves both
automatically.

All four share the same shape above the frame format: one stream ID per
frame so concurrent RPCs multiplex onto one connection, a leading headers
frame standing in for the HTTP headers a raw socket doesn't have, an
explicit end-stream frame for half-close, and a reset frame to cancel one
stream without closing the connection.

See [CONCLUSIONS.md](../CONCLUSIONS.md) for the generalized findings the
drafts produced, and each draft's README for its own protocol reference.
