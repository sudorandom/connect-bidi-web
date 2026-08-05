# Dev tools (go, node, buf, golangci-lint, protoc-gen-go, just) are managed by
# mise; run `mise install` once to provision them. See mise.toml.

# Generate, build, test, and lint (default)
default: generate build test lint

# Build all packages
build:
    go build ./...

# Run unit tests
test: build
    go test -race -cover ./connectwebsocket/... ./connectwebtransport/... ./internal/bidiprotocol/... ./internal/connectprotocol/...

# Run end-to-end tests: Go client <-> Go server over WebSocket and
# WebTransport, plus cross-language interop (Go <-> TypeScript) in both
# directions. Requires `npm ci` in ts/ first.
e2e: build
    go test -race -count=1 ./internal/e2e/...
    npm --prefix ts run build
    npm --prefix ts run e2e

# Run benchmarks
bench: build
    go test -bench=. -benchmem -run=NONE ./connectwebsocket/... ./connectwebtransport/...

# Build the demo site bundle
demo-build:
    npm --prefix demo/web install
    npm --prefix demo/web run build

# Run the Go demo server (Connect HTTP + WebSocket + WebTransport) at
# https://localhost:4433. Certs are created with mkcert on first run;
# `mkcert -install` (once, prompts for your password) makes the browser
# trust them — WebTransport rejects untrusted certs outright, with no
# click-through interstitial like the HTTPS page gets.
demo: demo-build
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
