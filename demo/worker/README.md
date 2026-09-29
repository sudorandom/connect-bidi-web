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

Deploys are driven from the Cloudflare dashboard, not from GitHub Actions —
nothing in `.github/workflows/` touches Cloudflare. Workers Builds watches
the repo, and the branch decides whether a push becomes production or a
preview.

Set it up under *Workers & Pages → `connect-bidi-web` → Settings → Builds*.

**1. Connect the repository.** Authorize the Cloudflare GitHub app and pick
this repo. Then set the build configuration:

| Setting        | Value |
|----------------|-------|
| Root directory | `demo/worker` |
| Build command  | `npm run build` |
| Deploy command | `npx wrangler deploy` (default) |

The worker depends on `file:` links into `ts/packages/`, and every `dist/`
is gitignored, so this package's `build` script builds the sibling packages
(the ts/ workspace and the demo/web site bundle) before wrangler bundles
the worker. That is why the build command is `npm run build` and not just
wrangler.

**2. Turn on preview builds for every other branch.** Under *Branch
control*:

| Setting | Value |
|---------|-------|
| Production branch | `main` |
| Non-production branch builds | **enabled** |
| Non-production deploy command | `npx wrangler versions upload` (default) |

That is the whole preview setup, and it is not per-branch: *every* branch
that isn't `main` is covered, so a new branch needs no configuration. This
matters in both directions — without branch control, Workers Builds runs
the same deploy command for every branch, and a feature branch (or
release-please's bot branch) would deploy straight to production.

**Why `versions upload` is the safe command.** It uploads a new *version*
and returns a preview URL, but does not shift production traffic and does
not touch the `routes` in `wrangler.jsonc`. Production stays on the custom
domain until a `main` build runs `wrangler deploy`.

Each preview gets its own `workers.dev` URL — enabled by `preview_urls` in
`wrangler.jsonc`, independent of `workers_dev: false` — reported in the
build log and, for pushes belonging to a pull request, as a PR comment.

The static assets need no dashboard configuration; `wrangler.jsonc` already
points at `../web/dist`.

**Two things to watch.** Workers Builds does not read `mise.toml`, so it
picks its own Node version; the npm packages declare `node >=20.19`, and if
a build fails on the toolchain, pin it with a `NODE_VERSION` build variable
or a `.node-version` file. And if you narrow *Build watch paths*, include
`ts/**` and `demo/web/**` alongside `demo/worker/**` — the worker bundles
all three, so watching only `demo/worker` would skip rebuilds when the
library or the site changes.

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
  demo can measure WebSocket bidi against gRPC-over-TCP bidi.

Until then, this Worker intentionally sticks to GA features.
