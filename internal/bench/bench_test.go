// Copyright 2021-2026 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package bench compares the bidi transports — WebSocket drafts 1, 3, and
// 4, and WebTransport — on speed and wire-size efficiency. Every
// benchmark reports the standard ns/op plus rxB/op and txB/op: bytes read
// and written on the server's socket per operation, counted below the
// transport (TCP payload bytes for the WebSocket drafts, UDP datagram bytes
// — including QUIC and TLS overhead — for WebTransport).
//
// Run with: go test -bench . -benchtime 100x ./internal/bench
package bench_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	mathrand "math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	connect "connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft1"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft3"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft4"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft5"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft7"
	"github.com/sudorandom/connect-bidi-web/connectwebtransport"
	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	pingv1connect "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
	"github.com/sudorandom/connect-bidi-web/internal/testcert"
	"golang.org/x/net/http2"
)

// -- Wire byte counting -------------------------------------------------------

// wireCounter counts bytes crossing the server's socket: rx is what the
// client sent, tx is what the server sent.
type wireCounter struct {
	rx atomic.Int64
	tx atomic.Int64
}

func (c *wireCounter) reset() {
	c.rx.Store(0)
	c.tx.Store(0)
}

type countingConn struct {
	net.Conn
	counter *wireCounter
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.counter.rx.Add(int64(n))
	return n, err
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.counter.tx.Add(int64(n))
	return n, err
}

type countingListener struct {
	net.Listener
	counter *wireCounter
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: conn, counter: l.counter}, nil
}

type countingPacketConn struct {
	net.PacketConn
	counter *wireCounter
}

func (c *countingPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(p)
	c.counter.rx.Add(int64(n))
	return n, addr, err
}

func (c *countingPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	n, err := c.PacketConn.WriteTo(p, addr)
	c.counter.tx.Add(int64(n))
	return n, err
}

// -- Ping service -------------------------------------------------------------

type benchPingServer struct {
	pingv1connect.UnimplementedPingServiceHandler
}

func (benchPingServer) Ping(_ context.Context, req *pingv1.PingRequest) (*pingv1.PingResponse, error) {
	return &pingv1.PingResponse{Number: req.GetNumber(), Text: req.GetText()}, nil
}

func (benchPingServer) CumSum(_ context.Context, stream pingv1connect.PingServiceCumSumServerStream) error {
	var sum int64
	for {
		req, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		sum += req.GetNumber()
		if err := stream.Send(&pingv1.CumSumResponse{Sum: sum, Text: req.GetText()}); err != nil {
			return err
		}
	}
}

func newConnectServer() *connect.Server {
	srv := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(srv, benchPingServer{})
	return srv
}

// -- Per-transport setup ------------------------------------------------------

// startWebSocketServer serves handler on a plain (no TLS) TCP listener whose
// bytes are counted, and returns the ws:// URL.
func startWebSocketServer(b *testing.B, handler http.Handler) (string, *wireCounter) {
	b.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	counter := &wireCounter{}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		_ = server.Serve(&countingListener{Listener: listener, counter: counter})
	}()
	b.Cleanup(func() { _ = server.Close() })
	return "ws://" + listener.Addr().String(), counter
}

// -- WebSocket over HTTP/2 (RFC 8441 extended CONNECT) ------------------------
//
// The HTTP/1.1 rows above run on a plaintext socket, so their byte counts
// are TCP payload. Extended CONNECT needs HTTP/2, and Go's HTTP/2 server
// only negotiates over TLS here, so these rows count *ciphertext*: TLS
// record overhead is included, exactly as the WebTransport rows include
// QUIC + TLS. They are therefore comparable to each other, not to the
// HTTP/1.1 rows.
//
// What they are for is the comparison the HTTP/1.1 rows cannot make: this
// path negotiates permessage-deflate in the CONNECT exchange (RFC 8441 §5
// keeps Sec-WebSocket-Extensions), so draft 4 compresses here as well as
// over HTTP/1.1. It did not always: these rows caught the gap, which is
// why they exist.

// requireExtendedConnect skips a benchmark unless the process was started
// with extended CONNECT enabled. Go reads this GODEBUG once at init, so it
// cannot be set per benchmark; the justfile sets it for the whole run.
func requireExtendedConnect(b *testing.B) {
	b.Helper()
	if !strings.Contains(os.Getenv("GODEBUG"), "http2xconnect=1") {
		b.Skip("extended CONNECT disabled; run with GODEBUG=http2xconnect=1")
	}
}

// startWebSocketH2Server serves handler over HTTP/2 (TLS, ALPN) on a
// listener whose bytes are counted, and returns the https:// URL.
func startWebSocketH2Server(b *testing.B, handler http.Handler) (string, *wireCounter) {
	b.Helper()
	counter := &wireCounter{}
	server := httptest.NewUnstartedServer(handler)
	// Wrap before StartTLS, which layers its own TLS listener on top: the
	// counter therefore sees encrypted bytes, which is what actually
	// crosses the wire.
	server.Listener = &countingListener{Listener: server.Listener, counter: counter}
	server.EnableHTTP2 = true
	server.StartTLS()
	b.Cleanup(server.Close)
	return server.URL, counter
}

// newH2Transport builds the HTTP/2 client the extended CONNECT dial needs.
// A plain *http.Client cannot send the :protocol pseudo-header.
func newH2Transport(b *testing.B) *http2.Transport {
	b.Helper()
	h2 := &http2.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{http2.NextProtoTLS},
		},
	}
	b.Cleanup(h2.CloseIdleConnections)
	return h2
}

// closeTransport releases the shared CONNECT stream at the end of a
// benchmark. Unlike a hijacked HTTP/1.1 upgrade, that stream is an active
// request which httptest.Server.Close waits on.
func closeTransport(b *testing.B, transport connect.Transport) {
	b.Helper()
	b.Cleanup(func() {
		if closer, ok := transport.(io.Closer); ok {
			_ = closer.Close()
		}
	})
}

func setupDraft3H2() func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
	return func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
		b.Helper()
		requireExtendedConnect(b)
		url, counter := startWebSocketH2Server(b, draft3.NewHandler(newConnectServer()))
		transport := draft3.NewH2Transport(url, newH2Transport(b))
		closeTransport(b, transport)
		return pingv1connect.NewPingServiceClient(connect.NewClient(transport)), counter
	}
}

func setupDraft4H2(compression bool, json bool) func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
	return func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
		b.Helper()
		requireExtendedConnect(b)
		var serverOpts []draft4.Option
		var clientOpts []draft4.Option
		if !compression {
			serverOpts = append(serverOpts, draft4.WithoutCompression())
			clientOpts = append(clientOpts, draft4.WithoutCompression())
		}
		if json {
			clientOpts = append(clientOpts, draft4.WithSendCodec(connect.CodecNameJSON))
		}
		url, counter := startWebSocketH2Server(b, draft4.NewHandler(newConnectServer(), serverOpts...))
		transport := draft4.NewH2Transport(url, newH2Transport(b), clientOpts...)
		closeTransport(b, transport)
		return pingv1connect.NewPingServiceClient(connect.NewClient(transport)), counter
	}
}

// setupDraft1 measures the shape the other drafts are variations on: one
// WebSocket per streaming RPC, with every message wrapped in a standard
// 5-byte Connect envelope. Against draft 5 the difference is exactly those
// five bytes per message; against drafts 3 and 4 it is a handshake per RPC
// instead of a stream ID per frame. The unary rows are plain Connect over
// HTTP, because draft 1 never upgrades for them — which is why the client
// is a composite transport rather than a WebSocket one.
func setupDraft1(compression bool, json bool) func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
	return func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
		b.Helper()
		var opts []draft1.Option
		if !compression {
			opts = append(opts, draft1.WithoutCompression())
		}
		if json {
			opts = append(opts, draft1.WithSendCodec(connect.CodecNameJSON))
		}
		// The deployment shape draft 1 proposes: the Connect procedure URLs
		// answer POST with ordinary Connect and GET+Upgrade with draft 1.
		connectServer := newConnectServer()
		mux := http.NewServeMux()
		connecthttp.Mount(mux, connectServer)
		url, counter := startWebSocketServer(b, draft1.Intercept(mux, connectServer, opts...))
		httpURL := "http" + strings.TrimPrefix(url, "ws")
		transport := connectwebsocket.NewCompositeTransport(
			connecthttp.NewTransport(http.DefaultClient, httpURL),
			draft1.NewTransport(url, opts...),
		)
		return pingv1connect.NewPingServiceClient(connect.NewClient(transport)), counter
	}
}

// setupDraft1H2 is the bootstrap draft 1 wants: a new WebSocket is a new
// HTTP/2 stream on a connection that is already open, which is what makes
// one-socket-per-RPC affordable.
func setupDraft1H2(compression bool, json bool) func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
	return func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
		b.Helper()
		requireExtendedConnect(b)
		var opts []draft1.Option
		if !compression {
			opts = append(opts, draft1.WithoutCompression())
		}
		if json {
			opts = append(opts, draft1.WithSendCodec(connect.CodecNameJSON))
		}
		connectServer := newConnectServer()
		mux := http.NewServeMux()
		connecthttp.Mount(mux, connectServer)
		url, counter := startWebSocketH2Server(b, draft1.Intercept(mux, connectServer, opts...))
		h2 := newH2Transport(b)
		transport := connectwebsocket.NewCompositeTransport(
			connecthttp.NewTransport(&http.Client{Transport: h2}, url),
			draft1.NewH2Transport(url, h2, opts...),
		)
		return pingv1connect.NewPingServiceClient(connect.NewClient(transport)), counter
	}
}

func setupDraft3(compression bool, json bool) func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
	return func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
		b.Helper()
		var clientOpts []draft3.Option
		if !compression {
			// Offer only the identity subprotocol, so nothing is compressed.
			clientOpts = append(clientOpts, draft3.WithoutCompression())
		}
		if json {
			clientOpts = append(clientOpts, draft3.WithSendCodec(connect.CodecNameJSON))
		}
		url, counter := startWebSocketServer(b, draft3.NewHandler(newConnectServer()))
		transport := draft3.NewTransport(url, clientOpts...)
		return pingv1connect.NewPingServiceClient(connect.NewClient(transport)), counter
	}
}

// setupDraft4 measures the cost of draft 4's ASCII frame head. The proto
// codec keeps the payloads identical to the other drafts, so the wire-byte
// difference is the head alone; setting json exercises the all-text
// configuration draft 4 is designed around, where the payload grows too.
func setupDraft4(compression bool, json bool) func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
	return func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
		b.Helper()
		var serverOpts []draft4.Option
		var clientOpts []draft4.Option
		if !compression {
			serverOpts = append(serverOpts, draft4.WithoutCompression())
			clientOpts = append(clientOpts, draft4.WithoutCompression())
		}
		if json {
			clientOpts = append(clientOpts, draft4.WithSendCodec(connect.CodecNameJSON))
		}
		url, counter := startWebSocketServer(b, draft4.NewHandler(newConnectServer(), serverOpts...))
		transport := draft4.NewTransport(url, clientOpts...)
		return pingv1connect.NewPingServiceClient(connect.NewClient(transport)), counter
	}
}

// setupDraft5 measures a protocol with no per-message overhead at all,
// which it pays for per *connection* instead: one WebSocket per streaming
// RPC, so the bidi case includes a full handshake amortized over its 100
// roundtrips. The unary rows are the other half of the design — draft 5
// never upgrades for them, so they measure ordinary Connect over HTTP.
func setupDraft5(compression bool, json bool) func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
	return func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
		b.Helper()
		var opts []draft5.Option
		if !compression {
			opts = append(opts, draft5.WithoutCompression())
		}
		if json {
			opts = append(opts, draft5.WithProtoJSON())
		}
		mux := http.NewServeMux()
		draft5.Mount(mux, newConnectServer(), opts...)
		url, counter := startWebSocketServer(b, mux)
		transport := draft5.NewTransport(http.DefaultClient, url, opts...)
		return pingv1connect.NewPingServiceClient(connect.NewClient(transport)), counter
	}
}

// setupDraft5H2 is the bootstrap draft 5 is designed around: a new
// WebSocket is a new HTTP/2 stream on a connection that is already open,
// which is what makes one-socket-per-RPC affordable.
func setupDraft5H2(compression bool, json bool) func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
	return func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
		b.Helper()
		requireExtendedConnect(b)
		var opts []draft5.Option
		if !compression {
			opts = append(opts, draft5.WithoutCompression())
		}
		if json {
			opts = append(opts, draft5.WithProtoJSON())
		}
		mux := http.NewServeMux()
		draft5.Mount(mux, newConnectServer(), opts...)
		url, counter := startWebSocketH2Server(b, mux)
		h2 := newH2Transport(b)
		opts = append(opts, draft5.WithH2Bootstrap(h2))
		transport := draft5.NewTransport(&http.Client{Transport: h2}, url, opts...)
		return pingv1connect.NewPingServiceClient(connect.NewClient(transport)), counter
	}
}

// setupDraft7 measures the Connect-over-WebSocket specification: draft
// 5's connection model — one WebSocket per RPC, so the bidi rows include
// a handshake amortized over 100 roundtrips — plus a one-byte marker on
// every message. Like draft 5 it keeps unary on HTTP by default, so the
// unary rows measure ordinary Connect over HTTP.
func setupDraft7(compression bool, json bool) func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
	return func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
		b.Helper()
		var opts []draft7.Option
		if !compression {
			opts = append(opts, draft7.WithoutCompression())
		}
		if json {
			opts = append(opts, draft7.WithProtoJSON())
		}
		mux := http.NewServeMux()
		draft7.Mount(mux, newConnectServer(), opts...)
		url, counter := startWebSocketServer(b, mux)
		transport := draft7.NewTransport(http.DefaultClient, url, opts...)
		return pingv1connect.NewPingServiceClient(connect.NewClient(transport)), counter
	}
}

// setupDraft7H2 is the bootstrap the specification does not adopt and the
// implementation serves anyway: a new WebSocket is a new HTTP/2 stream on
// a connection that is already open.
func setupDraft7H2(compression bool, json bool) func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
	return func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
		b.Helper()
		requireExtendedConnect(b)
		var opts []draft7.Option
		if !compression {
			opts = append(opts, draft7.WithoutCompression())
		}
		if json {
			opts = append(opts, draft7.WithProtoJSON())
		}
		mux := http.NewServeMux()
		draft7.Mount(mux, newConnectServer(), opts...)
		url, counter := startWebSocketH2Server(b, mux)
		h2 := newH2Transport(b)
		opts = append(opts, draft7.WithH2Bootstrap(h2))
		transport := draft7.NewTransport(&http.Client{Transport: h2}, url, opts...)
		return pingv1connect.NewPingServiceClient(connect.NewClient(transport)), counter
	}
}

func setupWebTransport(clientOpts ...connectwebtransport.Option) func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
	return func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
		return setupWebTransportClient(b, clientOpts...)
	}
}

func setupWebTransportClient(b *testing.B, clientOpts ...connectwebtransport.Option) (pingv1connect.PingServiceClient, *wireCounter) {
	b.Helper()
	cert, err := testcert.GenerateSelfSignedCert()
	if err != nil {
		b.Fatalf("generate cert: %v", err)
	}
	handler := connectwebtransport.NewHandler(newConnectServer())
	wtServer := &webtransport.Server{
		H3: &http3.Server{
			TLSConfig: &tls.Config{
				Certificates: []tls.Certificate{cert},
				NextProtos:   []string{http3.NextProtoH3},
			},
		},
	}
	mux := http.NewServeMux()
	mux.Handle("/webtransport", handler.UpgradeHandler(wtServer))
	wtServer.H3.Handler = mux
	b.Cleanup(func() { _ = wtServer.Close() })

	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen packet: %v", err)
	}
	b.Cleanup(func() { _ = packetConn.Close() })
	counter := &wireCounter{}
	go func() {
		_ = wtServer.Serve(&countingPacketConn{PacketConn: packetConn, counter: counter})
	}()

	dialer := &webtransport.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{http3.NextProtoH3},
		},
	}
	b.Cleanup(func() { _ = dialer.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dialResp, session, err := dialer.Dial(ctx, "https://"+packetConn.LocalAddr().String()+"/webtransport", nil)
	if err != nil {
		b.Fatalf("webtransport dial: %v", err)
	}
	if dialResp != nil && dialResp.Body != nil {
		b.Cleanup(func() { _ = dialResp.Body.Close() })
	}
	b.Cleanup(func() { _ = session.CloseWithError(0, "") })

	transport := connectwebtransport.NewTransport(session, clientOpts...)
	return pingv1connect.NewPingServiceClient(connect.NewClient(transport)), counter
}

// -- Payloads -----------------------------------------------------------------

const largeSize = 16 * 1024

// repetitiveText compresses extremely well; randomText (base64 of random
// bytes) barely compresses. Both are valid UTF-8, as proto strings require.
func makePayloads() (repetitive, random string) {
	sentence := "all work and no play makes jack a dull boy. "
	repetitive = strings.Repeat(sentence, largeSize/len(sentence)+1)[:largeSize]
	rng := mathrand.New(mathrand.NewSource(1))
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	raw := make([]byte, largeSize)
	for i := range raw {
		raw[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return repetitive, string(raw)
}

// -- Benchmarks ---------------------------------------------------------------

func BenchmarkTransports(b *testing.B) {
	repetitive, random := makePayloads()

	// Case names are "<transport>[/h2]/<codec>/<compression>", so a row says
	// on its face which encoding produced the payload — the byte counts mean
	// nothing without it, and the Go and TypeScript suites do not default to
	// the same codec.
	//
	// Every draft covers the full codec x compression grid on the HTTP/1.1
	// bootstrap. Measuring JSON only uncompressed would libel it: JSON
	// compresses far better than protobuf, so what the codec "costs" is a
	// different number with deflate on. The h2 rows deliberately do not
	// repeat the grid — they are there to test the bootstrap, not the
	// codec.
	//
	// Each transport runs in two compression variants: identity (none
	// anywhere) and compressed (that transport's own mechanism in both
	// directions — per-message gzip for WebTransport,
	// subprotocol DEFLATE for draft 3, permessage-deflate for draft 4).
	cases := []struct {
		name  string
		setup func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter)
	}{
		// Draft 1: one WebSocket per streaming RPC, every message a
		// standard 5-byte Connect envelope. Compare with draft 5, which is
		// the same connection model with those five bytes removed, and the
		// bidi rows show what a handshake per RPC costs when amortized over
		// 100 roundtrips. The unary rows are plain Connect HTTP, because
		// draft 1 never upgrades for them.
		{name: "ws-draft1/proto/identity", setup: setupDraft1(false, false)},
		{name: "ws-draft1/proto/deflate", setup: setupDraft1(true, false)},
		{name: "ws-draft1/json/identity", setup: setupDraft1(false, true)},
		{name: "ws-draft1/json/deflate", setup: setupDraft1(true, true)},
		// Draft 3: compression is the protocol's own — negotiated by
		// subprotocol, applied per frame as raw DEFLATE above the 512-byte
		// threshold.
		{name: "ws-draft3/proto/identity", setup: setupDraft3(false, false)},
		{name: "ws-draft3/proto/deflate", setup: setupDraft3(true, false)},
		{name: "ws-draft3/json/identity", setup: setupDraft3(false, true)},
		{name: "ws-draft3/json/deflate", setup: setupDraft3(true, true)},
		// Draft 4: an ASCII frame head instead of five packed bytes, with
		// compression back in permessage-deflate's hands. The json case is
		// the all-text configuration the draft is designed around, and pays
		// for legibility in the payload as well as the head.
		{name: "ws-draft4/proto/identity", setup: setupDraft4(false, false)},
		{name: "ws-draft4/proto/deflate", setup: setupDraft4(true, false)},
		{name: "ws-draft4/json/identity", setup: setupDraft4(false, true)},
		{name: "ws-draft4/json/deflate", setup: setupDraft4(true, true)},
		// Draft 5: no framing at all, and no multiplexing — one WebSocket
		// per streaming RPC. Its per-message overhead is zero, so the bidi
		// rows show what a handshake per RPC costs when amortized over 100
		// roundtrips; the unary rows are plain Connect HTTP, because draft
		// 5 never upgrades for them.
		{name: "ws-draft5/proto/identity", setup: setupDraft5(false, false)},
		{name: "ws-draft5/proto/deflate", setup: setupDraft5(true, false)},
		{name: "ws-draft5/json/identity", setup: setupDraft5(false, true)},
		{name: "ws-draft5/json/deflate", setup: setupDraft5(true, true)},
		// Draft 7: the specification. Draft 5's connection model with a
		// one-byte marker per message, so the bidi rows should land one
		// byte per message above draft 5's.
		{name: "ws-draft7/proto/identity", setup: setupDraft7(false, false)},
		{name: "ws-draft7/proto/deflate", setup: setupDraft7(true, false)},
		{name: "ws-draft7/json/identity", setup: setupDraft7(false, true)},
		{name: "ws-draft7/json/deflate", setup: setupDraft7(true, true)},
		// The same drafts over the HTTP/2 extended CONNECT bootstrap, each
		// configured like its HTTP/1.1 namesake above: a row called
		// deflate compresses, a row called json does not. Byte counts
		// include TLS, so compare these rows with each other rather than
		// with the HTTP/1.1 rows above.
		{name: "ws-draft1/h2/proto/identity", setup: setupDraft1H2(false, false)},
		{name: "ws-draft1/h2/proto/deflate", setup: setupDraft1H2(true, false)},
		{name: "ws-draft3/h2/proto/deflate", setup: setupDraft3H2()},
		{name: "ws-draft4/h2/proto/identity", setup: setupDraft4H2(false, false)},
		{name: "ws-draft4/h2/proto/deflate", setup: setupDraft4H2(true, false)},
		{name: "ws-draft4/h2/json/identity", setup: setupDraft4H2(false, true)},
		{name: "ws-draft5/h2/proto/identity", setup: setupDraft5H2(false, false)},
		{name: "ws-draft5/h2/proto/deflate", setup: setupDraft5H2(true, false)},
		{name: "ws-draft7/h2/proto/identity", setup: setupDraft7H2(false, false)},
		{name: "ws-draft7/h2/proto/deflate", setup: setupDraft7H2(true, false)},
		// WebTransport wire bytes include QUIC and TLS overhead, unlike the
		// plaintext TCP the WebSocket drafts run on here.
		{name: "webtransport/proto/identity", setup: setupWebTransport(
			connectwebtransport.WithAcceptCompression(),
		)},
		{name: "webtransport/proto/gzip", setup: setupWebTransport(
			connectwebtransport.WithSendCompressor(connect.CompressionNameGzip),
		)},
	}

	for _, benchCase := range cases {
		b.Run(benchCase.name, func(b *testing.B) {
			client, counter := benchCase.setup(b)
			ctx := context.Background()

			unary := func(text string) func(b *testing.B) {
				return func(b *testing.B) {
					req := &pingv1.PingRequest{Number: 42, Text: text}
					if _, err := client.Ping(ctx, req); err != nil {
						b.Fatalf("warmup ping: %v", err)
					}
					counter.reset()
					b.ResetTimer()
					for range b.N {
						if _, err := client.Ping(ctx, req); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					reportWireBytes(b, counter, b.N)
				}
			}

			b.Run("unary_small", unary(""))
			b.Run("unary_16KiB_repetitive", unary(repetitive))
			b.Run("unary_16KiB_random", unary(random))

			// Large payloads *on a stream*. The unary rows above cannot
			// stand in for this: draft 5 dispatches unary over plain HTTP,
			// so its per-message compression never appears there — the
			// numbers made deflate look like it was doing nothing.
			bidiPayload := func(text string) func(b *testing.B) {
				return func(b *testing.B) {
					runStream := func() error {
						stream, err := client.CumSum(ctx)
						if err != nil {
							return err
						}
						for range 4 {
							if err := stream.Send(&pingv1.CumSumRequest{Number: 1, Text: text}); err != nil {
								return err
							}
							if _, err := stream.Receive(); err != nil {
								return err
							}
						}
						if err := stream.CloseSend(); err != nil {
							return err
						}
						if _, err := stream.Receive(); !errors.Is(err, io.EOF) {
							return err
						}
						return stream.Close()
					}
					if err := runStream(); err != nil {
						b.Fatalf("warmup: %v", err)
					}
					counter.reset()
					b.ResetTimer()
					for range b.N {
						if err := runStream(); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					reportWireBytes(b, counter, b.N)
				}
			}
			b.Run("bidi_16KiB_repetitive", bidiPayload(repetitive))

			b.Run("bidi_100_roundtrips", func(b *testing.B) {
				runBidi := func() error {
					stream, err := client.CumSum(ctx)
					if err != nil {
						return err
					}
					for i := int64(1); i <= 100; i++ {
						if err := stream.Send(&pingv1.CumSumRequest{Number: i}); err != nil {
							return err
						}
						if _, err := stream.Receive(); err != nil {
							return err
						}
					}
					if err := stream.CloseSend(); err != nil {
						return err
					}
					if _, err := stream.Receive(); !errors.Is(err, io.EOF) {
						return err
					}
					return stream.Close()
				}
				if err := runBidi(); err != nil {
					b.Fatalf("warmup bidi: %v", err)
				}
				counter.reset()
				b.ResetTimer()
				for range b.N {
					if err := runBidi(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*100), "ns/roundtrip")
				reportWireBytes(b, counter, b.N)
			})
		})
	}
}

func reportWireBytes(b *testing.B, counter *wireCounter, n int) {
	b.Helper()
	b.ReportMetric(float64(counter.rx.Load())/float64(n), "rxB/op")
	b.ReportMetric(float64(counter.tx.Load())/float64(n), "txB/op")
}
