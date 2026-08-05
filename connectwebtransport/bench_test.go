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

package connectwebtransport_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"github.com/sudorandom/connect-bidi-web/connectwebtransport"
	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	pingv1connect "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
)

type benchPingServer struct {
	pingv1connect.UnimplementedPingServiceHandler
}

func (benchPingServer) Ping(_ context.Context, req *pingv1.PingRequest) (*pingv1.PingResponse, error) {
	return &pingv1.PingResponse{Number: req.GetNumber(), Text: req.GetText()}, nil
}

func (benchPingServer) Sum(_ context.Context, stream pingv1connect.PingServiceSumServerStream) (*pingv1.SumResponse, error) {
	var total int64
	for {
		req, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		total += req.GetNumber()
	}
	return &pingv1.SumResponse{Sum: total}, nil
}

func (benchPingServer) CountUp(_ context.Context, req *pingv1.CountUpRequest, stream pingv1connect.PingServiceCountUpServerStream) error {
	for i := int64(1); i <= req.GetNumber(); i++ {
		if err := stream.Send(&pingv1.CountUpResponse{Number: i}); err != nil {
			return err
		}
	}
	return nil
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

func generateBenchCert() (tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"Acme Bench"}},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(time.Hour * 24),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}
	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	return tls.X509KeyPair(certPEM, keyPEM)
}

func BenchmarkWebTransport(b *testing.B) {
	cert, err := generateBenchCert()
	if err != nil {
		b.Fatalf("failed to generate cert: %v", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{http3.NextProtoH3},
	}

	wtServer := &webtransport.Server{
		H3: &http3.Server{
			TLSConfig: tlsConfig,
		},
	}
	b.Cleanup(func() { wtServer.Close() })

	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, benchPingServer{})

	webtransportHandler := connectwebtransport.NewHandler(connectServer)

	mux := http.NewServeMux()
	mux.HandleFunc("/webtransport", func(w http.ResponseWriter, r *http.Request) {
		session, err := wtServer.Upgrade(w, r)
		if err != nil {
			return
		}
		webtransportHandler.HandleSession(r.Context(), session)
	})
	wtServer.H3.Handler = mux

	var listenConfig net.ListenConfig
	packetConn, err := listenConfig.ListenPacket(b.Context(), "udp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen packet: %v", err)
	}
	b.Cleanup(func() { packetConn.Close() })

	addr := packetConn.LocalAddr().String()

	go func() {
		_ = wtServer.Serve(packetConn)
	}()

	time.Sleep(100 * time.Millisecond)

	wtClient := &webtransport.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{http3.NextProtoH3},
		},
	}
	b.Cleanup(func() { _ = wtClient.Close() })

	_, session, err := wtClient.Dial(b.Context(), "https://"+addr+"/webtransport", nil) //nolint:bodyclose // webtransport-go owns the handshake response
	if err != nil {
		b.Fatalf("failed to dial webtransport: %v", err)
	}
	b.Cleanup(func() { _ = session.CloseWithError(0, "") })

	wtTransport := connectwebtransport.NewTransport(session)
	client := pingv1connect.NewPingServiceClient(connect.NewClient(wtTransport))
	compressedTransport := connectwebtransport.NewTransport(
		session,
		connectwebtransport.WithSendCompressor(connect.CompressionNameGzip),
	)
	compressedClient := pingv1connect.NewPingServiceClient(connect.NewClient(compressedTransport))

	ctx := b.Context()
	twoMiB := strings.Repeat("a", 2*1024*1024)

	b.Run("unary_small", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				res, err := client.Ping(ctx, &pingv1.PingRequest{Number: 42})
				if err != nil {
					b.Error(err)
				} else if res.GetNumber() != 42 {
					b.Errorf("expected 42, got %d", res.GetNumber())
				}
			}
		})
	})

	b.Run("unary_big", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := compressedClient.Ping(ctx, &pingv1.PingRequest{Text: twoMiB}); err != nil {
					b.Error(err)
				}
			}
		})
	})

	b.Run("server_stream", func(b *testing.B) {
		for _, benchmark := range []struct {
			name     string
			messages int64
		}{
			{name: "messages_1", messages: 1},
			{name: "messages_1000", messages: 1000},
		} {
			b.Run(benchmark.name, func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						stream, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: benchmark.messages})
						if err != nil {
							b.Error(err)
							return
						}
						var received int64
						for {
							_, err := stream.Receive()
							if errors.Is(err, io.EOF) {
								break
							}
							if err != nil {
								b.Error(err)
								break
							}
							received++
						}
						if received != benchmark.messages {
							b.Errorf("expected %d messages, got %d", benchmark.messages, received)
						}
						_ = stream.Close()
					}
				})
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*float64(benchmark.messages)), "ns/message")
			})
		}
	})

	b.Run("bidi_stream", func(b *testing.B) {
		for _, benchmark := range []struct {
			name       string
			roundTrips int64
		}{
			{name: "round_trips_1", roundTrips: 1},
			{name: "round_trips_1000", roundTrips: 1000},
		} {
			b.Run(benchmark.name, func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						stream, err := client.CumSum(ctx)
						if err != nil {
							b.Error(err)
							return
						}
						for i := int64(1); i <= benchmark.roundTrips; i++ {
							if err := stream.Send(&pingv1.CumSumRequest{Number: i}); err != nil {
								b.Error(err)
								break
							}
							if _, err := stream.Receive(); err != nil {
								b.Error(err)
								break
							}
						}
						_ = stream.CloseSend()
						_ = stream.Close()
					}
				})
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*float64(benchmark.roundTrips)), "ns/roundtrip")
			})
		}
	})
}
