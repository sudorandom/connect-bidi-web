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

// Package bench compares the bidi transports — WebSocket draft 1, WebSocket
// draft 2, and WebTransport — on speed and wire-size efficiency. Every
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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft1"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft2"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft3"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft4"
	"github.com/sudorandom/connect-bidi-web/connectwebtransport"
	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	pingv1connect "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
	"github.com/sudorandom/connect-bidi-web/internal/testcert"
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
		if err := stream.Send(&pingv1.CumSumResponse{Sum: sum}); err != nil {
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

func setupDraft1(clientOpts ...draft1.Option) func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
	return func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
		b.Helper()
		url, counter := startWebSocketServer(b, draft1.NewHandler(newConnectServer()))
		transport := draft1.NewTransport(url, clientOpts...)
		return pingv1connect.NewPingServiceClient(connect.NewClient(transport)), counter
	}
}

func setupDraft2(compression bool) func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
	return func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
		b.Helper()
		var serverOpts []draft2.Option
		var clientOpts []draft2.Option
		if !compression {
			serverOpts = append(serverOpts, draft2.WithoutCompression())
			clientOpts = append(clientOpts, draft2.WithoutCompression())
		}
		url, counter := startWebSocketServer(b, draft2.NewHandler(newConnectServer(), serverOpts...))
		transport := draft2.NewTransport(url, clientOpts...)
		return pingv1connect.NewPingServiceClient(connect.NewClient(transport)), counter
	}
}

func setupDraft3(compression bool) func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
	return func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter) {
		b.Helper()
		var clientOpts []draft3.Option
		if !compression {
			// Offer only the identity subprotocol, so nothing is compressed.
			clientOpts = append(clientOpts, draft3.WithoutCompression())
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

	// Each transport runs in two variants: identity (no compression
	// anywhere) and compressed (that transport's compression mechanism in
	// both directions — per-message gzip for draft 1 and WebTransport,
	// permessage-deflate for draft 2).
	cases := []struct {
		name  string
		setup func(b *testing.B) (pingv1connect.PingServiceClient, *wireCounter)
	}{
		{name: "ws-draft1/identity", setup: setupDraft1(
			draft1.WithAcceptCompression(),
		)},
		{name: "ws-draft1/gzip", setup: setupDraft1(
			draft1.WithSendCompressor(connect.CompressionNameGzip),
		)},
		{name: "ws-draft2/identity", setup: setupDraft2(false)},
		{name: "ws-draft2/deflate", setup: setupDraft2(true)},
		// Draft 3: compression is the protocol's own — negotiated by
		// subprotocol, applied per frame as raw DEFLATE above the 512-byte
		// threshold.
		{name: "ws-draft3/identity", setup: setupDraft3(false)},
		{name: "ws-draft3/deflate", setup: setupDraft3(true)},
		// Draft 4: an ASCII frame head instead of five packed bytes, with
		// compression back in permessage-deflate's hands. The json case is
		// the all-text configuration the draft is designed around, and pays
		// for legibility in the payload as well as the head.
		{name: "ws-draft4/identity", setup: setupDraft4(false, false)},
		{name: "ws-draft4/deflate", setup: setupDraft4(true, false)},
		{name: "ws-draft4/json", setup: setupDraft4(false, true)},
		// WebTransport wire bytes include QUIC and TLS overhead, unlike the
		// plaintext TCP the WebSocket drafts run on here.
		{name: "webtransport/identity", setup: setupWebTransport(
			connectwebtransport.WithAcceptCompression(),
		)},
		{name: "webtransport/gzip", setup: setupWebTransport(
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
