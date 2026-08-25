# Dev tools (go, node, buf, golangci-lint, protoc-gen-go, just) are managed by
# mise; run `mise install` once to provision them. See mise.toml.

# Generate, build, test, and lint (default)
default: generate build test lint

# Build all packages
build:
    go build ./...

# Run unit tests. GODEBUG=http2xconnect=1 enables RFC 8441 extended CONNECT
# in Go's HTTP/2 stack (off by default), which the WebSocket-over-HTTP/2
# tests need; those tests skip themselves when it's absent.
test: build
    GODEBUG=http2xconnect=1 go test -race -cover ./connectfallback/... ./connectwebsocket/... ./connectwebtransport/... ./internal/bidiprotocol/... ./internal/connectprotocol/...

# Run end-to-end tests: Go client <-> Go server over WebSocket and
# WebTransport, plus cross-language interop (Go <-> TypeScript) in both
# directions. Requires `npm ci` in ts/ first.
e2e: build
    npm --prefix ts run build
    go test -race -count=1 ./internal/e2e/...
    npm --prefix ts run e2e

# Run benchmarks: per-package micro-benchmarks, the cross-transport
# comparison (speed + wire bytes per op; see internal/bench), and the
# TypeScript draft comparison. Requires `npm ci` in ts/ first.
bench: build
    go test -bench=. -benchmem -run=NONE ./connectwebsocket/... ./connectwebtransport/...
    go test -bench=. -benchtime=200x -run=NONE ./internal/bench
    npm --prefix ts run bench -w packages/e2e

# Regenerate the benchmark data the demo site renders (see the Benchmarks
# section of the page). Runs the Go cross-transport benchmarks and the
# TypeScript draft comparison, writing JSON into demo/web/src/, which is
# checked in so the site build never has to run benchmarks.
bench-data: build
    GODEBUG=http2xconnect=1 go test -bench=. -benchtime=200x -run=NONE ./internal/bench | go run ./internal/bench/cmd/benchjson > demo/web/src/bench-go.json
    npm --prefix ts run bench -w packages/e2e -- --out ../../../demo/web/src/bench-ts.json

# Build the demo site bundle, including the TypeScript API reference at
# /docs/ (TypeDoc). Order matters: the site build wipes demo/web/dist.
demo-build:
    npm --prefix demo/web install
    npm --prefix demo/web run build
    just docs

# Generate the TypeScript API reference into demo/web/dist/docs. TypeDoc
# lives in its own small package (ts/docs) because it pins its own
# TypeScript version, separate from the workspace's.
docs:
    npm --prefix ts install
    npm --prefix ts/docs install
    npm --prefix ts/docs run docs

# Run the Go demo server (Connect HTTP + WebSocket + WebTransport) at
# https://localhost:4433. GODEBUG=http2xconnect=1 enables RFC 8441 extended
# CONNECT, so browsers bootstrap WebSockets (all drafts) over HTTP/2 — one
# h2 stream per WebSocket on the page's existing connection — instead of an
# HTTP/1.1 upgrade. Certs are created with mkcert on first run;
# `mkcert -install` (once, prompts for your password) makes the browser
# trust them — WebTransport rejects untrusted certs outright, with no
# click-through interstitial like the HTTPS page gets.
demo: demo-build
    cd demo/go && ([ -f localhost.pem ] || (mkcert -install && mkcert localhost))
    cd demo/go && GODEBUG=http2xconnect=1 go run .

# Like `demo`, but without extended CONNECT: browsers bootstrap every
# WebSocket with a classic HTTP/1.1 upgrade, for comparing the bootstraps.
demo-h1: demo-build
    cd demo/go && ([ -f localhost.pem ] || (mkcert -install && mkcert localhost))
    cd demo/go && go run .

# Run the Cloudflare Workers demo locally with wrangler (WebSocket only,
# no WebTransport) at http://localhost:8787
demo-worker: demo-build
    npm --prefix demo/worker install
    npm --prefix demo/worker run dev

# Lint Go and protobuf
lint:
    go vet ./...
    golangci-lint run ./...
    buf lint
    buf format --diff --exit-code

# Format Go and protobuf
format:
    golangci-lint fmt
    buf format -w

# Regenerate code from protos
generate:
    buf generate

# Upgrade dependencies except the pinned connect-go v2 pseudo-version
upgrade:
    go get -u -t ./...
    go mod tidy -v
