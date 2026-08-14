# Cloudflare Workers demo

Serves the connect-bidi-web demo site and the Eliza service from a single
Worker:

- **Static site**: the built `demo/web/dist` bundle via Workers Assets.
- **Unary + server-streaming RPCs**: standard Connect protocol on the `fetch`
  handler (`@connectrpc/connect` universal fetch helpers).
- **Bidi streaming**: the WebSocket transport via `@sudorandom/connect-bidi-cloudflare`
  (`WebSocketPair` — GA on Workers, no beta required).

WebTransport is **not** available on Cloudflare Workers; the demo UI
feature-detects and offers WebSocket only when served from the Worker.

## Deploy

```sh
just demo-worker               # local: http://localhost:8787 (builds the site first)
```

Or manually:

```sh
cd ts && npm ci && npm run build              # build the workspace packages
cd ../demo/web && npm install && npm run build # build the site bundle
cd ../worker
npm install
npm run dev                    # local: http://localhost:8787
npm run deploy                 # deploy to your Cloudflare account
```

### Workers Builds (Git integration)

The worker depends on `file:` links into `ts/packages/`, and every `dist/`
is gitignored, so this package's `build` script builds the sibling packages
(the ts/ workspace and the demo/web site bundle) before wrangler bundles
the worker:

| Setting        | Value |
|----------------|-------|
| Root directory | `demo/worker` |
| Build command  | `npm run build` |
| Deploy command | `npx wrangler deploy` (default) |

**Branch control matters.** Workers Builds triggers on every push to any
branch of the connected repo, and without branch control it runs the same
deploy command for all of them — a PR branch (or release-please's bot
branch) would deploy straight to production. Under the worker's
*Settings → Builds → Branch control*, set:

| Setting | Value |
|---------|-------|
| Production branch | `main` |
| Non-production branch builds | enabled |
| Non-production deploy command | `npx wrangler versions upload` (default) |

Non-production branches then upload a *preview version* instead of
deploying: each gets its own `workers.dev` preview URL (enabled by
`preview_urls` in `wrangler.jsonc`, independent of `workers_dev: false`)
and a PR comment, while production traffic stays on the custom domain.

The static assets need no dashboard configuration; `wrangler.jsonc` already
points at `../web/dist`.

## Deferred: native gRPC on Workers (private beta)

Cloudflare's [gRPC support for Workers](https://blog.cloudflare.com/grpc-workers/)
adds a `connect(socket)` TCP handler and, for full-duplex gRPC, socket
forwarding into Containers. That path is in **private beta** (signup required)
and complements — rather than replaces — the WebSocket transport used here:

- gRPC-web-style unary/server-streaming already works on Workers today via
  Connect over fetch (no beta needed), which this demo uses.
- True bidi gRPC on Workers requires Containers + TCP forwarding. Once beta
  access lands, the plan is to add a variant entry point that serves the same
  Eliza service over native gRPC through the `connect(socket)` handler, so the
  demo can compare WebSocket bidi vs gRPC-over-TCP bidi side by side.

Until then, this Worker intentionally sticks to GA features.
