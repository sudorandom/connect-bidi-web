# connect-bidi-web

Full bidirectional streaming for [ConnectRPC](https://connectrpc.com) in the browser, over WebSockets and WebTransport.

Browsers can't do full bidi streaming with the plain Connect protocol because fetch can't stream request bodies in every browser and network path. This project adds pluggable transports that carry the Connect envelope protocol over:

- **WebSocket** - one shared connection, any number of concurrent RPCs multiplexed by a stream ID on every frame (an option gives each streaming RPC its own connection instead, avoiding head-of-line blocking). Runs over TCP, so it traverses the proxies, load balancers, gateways, and serverless platforms that already carry HTTP.
- **WebTransport** - one HTTP/3 session, one bidirectional stream per RPC. QUIC streams make multiplexing the transport's job: no stream IDs needed, no head-of-line blocking. Available in every major browser (Chrome 97+, Firefox 114+, Safari 26.4+), but it needs HTTP/3 over UDP end to end, which most infrastructure between the browser and your handler can't carry today.

A composite transport keeps unary RPCs on plain HTTP (caching, observability, proxies) and routes streaming RPCs over the bidi transport.

A fallback transport degrades through a ladder of transports, best-first,
remembering the rung that works. In Go the full ladder is WebTransport →
WebSocket over HTTP/2 (RFC 8441 extended CONNECT) → WebSocket over
HTTP/1.1 → fail:

```go
transport := connectfallback.New(
	connectwebtransport.NewDialTransport(wtURL, nil),
	draft3.NewH2Transport(wsURL, nil),
	draft3.NewTransport(wsURL),
)
```

In the browser (`createFallbackTransport`) the ladder is WebTransport →
WebSocket → fail: the browser privately chooses HTTP/2 or HTTP/1.1 for a
WebSocket — it bootstraps over an existing HTTP/2 connection when the
server advertises extended CONNECT support — so that rung cannot be split
from JavaScript.

## Packages

| Package | What it is |
|---|---|
| [`github.com/sudorandom/connect-bidi-web/connectwebsocket`](https://pkg.go.dev/github.com/sudorandom/connect-bidi-web/connectwebsocket) | Go client transports + `http.Handler` servers, one subpackage per wire protocol draft (`draft1`, `draft3`…`draft5`) |
| [`github.com/sudorandom/connect-bidi-web/connectwebtransport`](https://pkg.go.dev/github.com/sudorandom/connect-bidi-web/connectwebtransport) | Go client transport + WebTransport session handler |
| [`github.com/sudorandom/connect-bidi-web/connectfallback`](https://pkg.go.dev/github.com/sudorandom/connect-bidi-web/connectfallback) | Go degrading transport: tries a ladder of transports, best-first |
| [`@sudorandom/connect-bidi-web`](https://www.npmjs.com/package/@sudorandom/connect-bidi-web) | Browser client transports (WebSocket, WebTransport, composite) |
| [`@sudorandom/connect-bidi-core`](https://www.npmjs.com/package/@sudorandom/connect-bidi-core) | Runtime-neutral server bridge to `@connectrpc/connect` handlers |
| [`@sudorandom/connect-bidi-node`](https://www.npmjs.com/package/@sudorandom/connect-bidi-node) | Node.js WebSocket server adapter |
| [`@sudorandom/connect-bidi-cloudflare`](https://www.npmjs.com/package/@sudorandom/connect-bidi-cloudflare) | Cloudflare Workers WebSocket server adapter |

API references: [Go on pkg.go.dev](https://pkg.go.dev/github.com/sudorandom/connect-bidi-web) · [TypeScript on connect-bidi-web.kmcd.dev/docs](https://connect-bidi-web.kmcd.dev/docs/)

> [!NOTE]
> The Go packages build on connect-go **v2** and its new `Transport` API
> ([connectrpc/connect-go#951](https://github.com/connectrpc/connect-go/pull/951)),
> which is unreleased. This module pins a pseudo-version of the upstream `v2`
> branch; expect breaking changes until v2 ships.

> [!NOTE]
> The npm packages are **ESM-only** and require Node.js ≥ 20.19. CJS consumers can
> still `require()` them — Node 20.19+/22.12+ support `require(esm)` natively.

## Usage

### Go server

```go
server := connect.NewServer()
elizav1connect.RegisterElizaServiceHandler(server, &elizaServer{})

// Serve Connect RPCs over WebSocket alongside regular HTTP handlers:
http.Handle("/websocket-draft3", draft3.NewHandler(server))
```

### Go client

```go
transport := draft3.NewTransport("wss://api.example.com/websocket-draft3")
client := elizav1connect.NewElizaServiceClient(connect.NewClient(transport))
```

### Browser client (TypeScript)

```ts
import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import {
  createCompositeTransport,
  createConnectWebSocketDraft3Transport,
} from "@sudorandom/connect-bidi-web";

const transport = createCompositeTransport(
  createConnectTransport({ baseUrl: "https://api.example.com" }), // unary
  createConnectWebSocketDraft3Transport({ baseUrl: "https://api.example.com" }), // streams
);
const client = createClient(ElizaService, transport);
```

## Wire protocol

The WebSocket transport exists in several wire-incompatible drafts so their
designs and implementations can be compared; WebTransport has a single
protocol. Drafts 3 and 4 are served on paths of their own; drafts 1 and 5
are served on the Connect procedure URLs themselves.

**WebTransport** (`connectwebtransport`) uses Connect-style envelopes: a
flag byte and a big-endian u32 payload length. `0x00` data, `0x01`
compressed data, `0x02` end-stream (Connect `EndStreamResponse` JSON),
`0x06` headers (JSON metadata, includes `:path`). It needs neither stream
IDs nor resets, because each RPC has its own QUIC stream — which is the
same observation drafts 1 and 5 act on over WebSocket.

Every draft runs over two bootstraps carrying identical frames: the
classic HTTP/1.1 upgrade, and **HTTP/2 extended CONNECT**
([RFC 8441](https://datatracker.ietf.org/doc/html/rfc8441)) — one
WebSocket per h2 stream on a shared connection — via each package's
`NewH2Transport` (`WithH2Bootstrap` in draft 5); the handlers serve both
automatically. The server
process must run with `GODEBUG=http2xconnect=1` for HTTP/2 bootstrapping,
which is how browsers pick it too.

**WebSocket draft 1** (`connectwebsocket/draft1`) is the shape the others
are variations on, and the one this repository proposes. Each streaming RPC
gets **its own WebSocket**; unary calls stay on HTTP, so a WebSocket
transport is composed with an HTTP one rather than replacing it. That
removes stream IDs and the reset frame — cancelling is closing the socket —
at the cost of a handshake per streaming call and one of the browser's
per-host WebSocket slots (255 in Chrome). Every message is a standard
5-byte Connect envelope, one per WebSocket message: `0x00` data, `0x02`
end-stream, `0x06` headers. The length field is redundant beside the
message boundary and is kept anyway, so the envelope stays Connect's own.
Each direction opens with a headers envelope, the server's as well as the
client's, because browser APIs can neither set headers on the upgrade
request nor read them off the response. Compression is
**permessage-deflate**, accepted only with `no_context_takeover` on both
sides and disabled entirely otherwise.

**WebSocket draft 3** (`connectwebsocket/draft3`, `/websocket-draft3`)
packs the frame head into a 4-byte big-endian stream ID plus one byte
carrying the frame type (`0x00` data, `0x01` headers, `0x02` end-stream,
`0x03` reset) and a compressed bit, with the payload delimited by the
WebSocket message. Compression is the protocol's own: a
`connect.bidi.d3.deflate` WebSocket subprotocol negotiates per-frame raw
DEFLATE. Because that sits above the connection rather than inside it, it
behaves identically over both bootstraps and in both directions, without
depending on an extension being implemented on the path — or on the client
choosing to use it, which under permessage-deflate is the client's call
alone.

**WebSocket draft 4** (`connectwebsocket/draft4`, `/websocket-draft4`)
optimizes for the browser, which is the primary WebSocket client and the
place this traffic actually gets read: drafts 1 and 3 render as opaque hex
in the Network tab. The head is ASCII text,
`<stream ID>|<flags>|<payload>`, so a frame reads as
`7|1|{"metadata":…}` with no decoder, and frames whose payload is UTF-8
travel as *text* WebSocket messages so devtools renders them as text.
Control payloads are always JSON; parsers split on the first two `|` only,
so payloads are never escaped; and compression goes back to
permessage-deflate, since a text head can't cheaply carry a compressed
bit.

**WebSocket draft 5** (`connectwebsocket/draft5`) asks what is left to
design once you admit that a WebSocket handshake is already an HTTP
request. The answer is nearly nothing: it carries **one RPC per
connection**, so there are no stream IDs and no reset frames; the upgrade
request's URL is the procedure and its headers are request metadata, so
there is no envelope; and the WebSocket's own message boundaries delimit
each message, so there are no length fields. A data message is the codec's
output and nothing else.

The only framing is the WebSocket opcode, which RFC 6455 already sends on
every frame. Exactly one shape is reserved: an *empty text* message is the
separator that ends a direction's data phase. Everything else is a message
of the RPC, with position telling metadata from data. A payload travels as
text when it is non-empty and valid UTF-8 — so a JSON stream is readable
end to end in the browser's Network tab, at no cost in bytes — and as
binary otherwise, including whenever it is empty: an empty protobuf message
encodes to zero bytes, and must never read as a half-close. Unary RPCs never upgrade: they are dispatched as ordinary
Connect HTTP requests, which is why draft 5 needs no path of its own and no
composite transport. It is implemented as a fork of connect-go's
`connecthttp`, so `draft5.Mount` stands in for `connecthttp.Mount` and each
procedure URL answers `POST` with Connect and `GET`+`Upgrade` with this
protocol.

See the per-package READMEs for each draft's protocol reference, the
[benchmarks](https://connect-bidi-web.kmcd.dev/#benchmarks) for what each
choice costs, and the demo site's
[conclusions](https://connect-bidi-web.kmcd.dev/#conclusions) for what the
drafts settled.

## Demo

`demo/` contains an Eliza chat demo that doubles as the project site. The Go server serves the same service three ways at once — plain Connect HTTP, WebSocket, and WebTransport — and a Cloudflare Workers variant serves it from workerd with WebSocket only (Workers can't terminate WebTransport).

### Go server (all three transports)

```sh
mise install       # dev tools: go, node, buf, just, mkcert, wrangler, ...
just demo          # builds demo/web, then serves https://localhost:4433
```

The first run creates a locally-trusted TLS certificate with mkcert;
`mkcert -install` prompts for your password once so browsers trust it.
Then open <https://localhost:4433> and pick a transport in the live demo.

> [!TIP]
> Local WebTransport needs one browser tweak even after `mkcert -install`,
> because browsers treat WebTransport certificates more strictly than HTTPS:
>
> - **Chrome** only accepts WebTransport certificates that chain to a
>   *well-known* root — locally-installed CAs like mkcert's don't count.
>   Enable `chrome://flags/#webtransport-developer-mode`, which relaxes the
>   requirement to any trusted root, including locally-installed ones.
> - **Firefox** disables HTTP/3 — and with it WebTransport — whenever the
>   certificate chains to a third-party root. Set
>   `network.http.http3.disable_when_third_party_roots_found` to `false` in
>   `about:config`.

### Cloudflare Workers (WebSocket only)

```sh
just demo-worker   # wrangler dev at http://localhost:8787
```

`npm run deploy` in `demo/worker` deploys it to your Cloudflare account; see
[demo/worker/README.md](demo/worker/README.md).

## Development

Dev tooling is managed with [mise](https://mise.jdx.dev) and [just](https://just.systems):

```sh
mise install
just          # generate + build + test + lint
```

End-to-end tests cover Go client ↔ Go server over both transports, plus
cross-language interop (Go client ↔ TypeScript server and TypeScript client ↔
Go server) over WebSocket:

```sh
cd ts && npm ci && cd ..
just e2e
```

## Future work

- **WebTransport over HTTP/2**
  ([draft-ietf-webtrans-http2](https://datatracker.ietf.org/doc/draft-ietf-webtrans-http2/)):
  maps the same WebTransport API onto HTTP/2 streams over TCP, which could
  eventually cover the middleboxes and UDP-blocked networks that an
  end-to-end HTTP/3 path can't cross. Worth tracking, but a long way out:
  - No browser implements it — browser WebTransport is HTTP/3-only. When
    Chromium engineers discussed the HTTP/2 binding, the stated motivation
    was proxy-to-backend communication, with use in Chrome "further away."
    On a UDP-blocked network a browser WebTransport handshake simply fails
    today, so the WebSocket fallback stays necessary regardless.
  - Browsers haven't even converged on the HTTP/3 binding: as of April 2026
    Safari and the IETF are on draft-15 while Chrome and Firefox still
    speak draft-02. HTTP/2 support is realistically well behind that.
  - Server-side it barely exists:
    [erlang-webtransport](https://github.com/benoitc/erlang-webtransport)
    implements both bindings (HTTP/3 draft-15, and HTTP/2 draft-14 via
    RFC 9297 capsules), and the draft's authors are from Meta and Apple,
    whose stacks track it — but webtransport-go, which this project builds
    on, is HTTP/3-only, as are most other libraries.
  - Even once shipped, it's a weaker transport by design: no unreliable
    delivery (datagrams are retransmitted regardless of the application's
    preference) and no stream independence (HTTP/2 head-of-line blocking).
    Session pooling does work — each session is its own HTTP/2 stream. The
    web API's `requireUnreliable` option exists precisely so an application
    can refuse this fallback when real datagrams matter.

## Legal

Apache-2.0. Derived from [connectrpc/connect-go](https://github.com/connectrpc/connect-go) and [connectrpc/connect-es](https://github.com/connectrpc/connect-es) (see `NOTICE`). Not an official ConnectRPC project.
