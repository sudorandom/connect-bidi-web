# connect-bidi-web

Full bidirectional streaming for [ConnectRPC](https://connectrpc.com) in the browser, over WebSockets and WebTransport.

Browsers can't do full bidi streaming with the plain Connect protocol because fetch can't stream request bodies in every browser and network path. This project adds pluggable transports that carry the Connect envelope protocol over:

- **WebSocket** — one shared connection, any number of concurrent RPCs multiplexed by a stream ID on every frame (an option gives each streaming RPC its own connection instead, avoiding head-of-line blocking). Works everywhere, including through Cloudflare Workers.
- **WebTransport** — one HTTP/3 session, one bidirectional stream per RPC. QUIC streams make multiplexing the transport's job: no stream IDs needed, no head-of-line blocking. Baseline in evergreen browsers (Chrome 97+, Firefox 114+, Safari 26.4+), but not available on Cloudflare Workers.

A composite transport keeps unary RPCs on plain HTTP (caching, observability, proxies) and routes streaming RPCs over the bidi transport.

## Packages

| Package | What it is |
|---|---|
| [`github.com/sudorandom/connect-bidi-web/connectwebsocket`](https://pkg.go.dev/github.com/sudorandom/connect-bidi-web/connectwebsocket) | Go client transport + `http.Handler` server |
| [`github.com/sudorandom/connect-bidi-web/connectwebtransport`](https://pkg.go.dev/github.com/sudorandom/connect-bidi-web/connectwebtransport) | Go client transport + WebTransport session handler |
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
http.Handle("/websocket", connectwebsocket.NewHandler(server))
```

### Go client

```go
transport := connectwebsocket.NewTransport("wss://api.example.com/websocket")
client := elizav1connect.NewElizaServiceClient(connect.NewClient(transport))
```

### Browser client (TypeScript)

```ts
import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import {
  createCompositeTransport,
  createConnectWebSocketTransport,
} from "@sudorandom/connect-bidi-web";

const transport = createCompositeTransport(
  createConnectTransport({ baseUrl: "https://api.example.com" }), // unary
  createConnectWebSocketTransport({ baseUrl: "https://api.example.com" }), // streams
);
const client = createClient(ElizaService, transport);
```

## Wire protocol

Frames are Connect-style envelopes: a flag byte and a big-endian u32 payload length.
`0x00` data, `0x01` compressed data, `0x02` end-stream (Connect `EndStreamResponse` JSON), `0x06` headers (JSON metadata, includes `:path`), `0x07` reset (WebSocket only; aborts one stream). On WebSocket, every frame is additionally prefixed with a 4-byte big-endian stream ID so concurrent RPCs can share the connection; WebTransport needs neither stream IDs nor resets, because each RPC has its own QUIC stream. Compression is negotiated with `connect-content-encoding`/`connect-accept-encoding` metadata. See the per-package READMEs for details.

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
