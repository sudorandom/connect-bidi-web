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

package connectwebsocket_test

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket"
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

func BenchmarkWebSocket(b *testing.B) {
	logOutput := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() { log.SetOutput(logOutput) })

	srv := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(srv, benchPingServer{})
	wsHandler := connectwebsocket.NewHandler(srv, connectwebsocket.WithAcceptOptions(&websocket.AcceptOptions{
		InsecureSkipVerify: true,
	}))

	server := httptest.NewServer(wsHandler)
	b.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	ctx := b.Context()
	wsTransport := connectwebsocket.NewTransport(wsURL)
	client := pingv1connect.NewPingServiceClient(connect.NewClient(wsTransport))
	compressedTransport := connectwebsocket.NewTransport(
		wsURL,
		connectwebsocket.WithSendCompressor(connect.CompressionNameGzip),
	)
	compressedClient := pingv1connect.NewPingServiceClient(connect.NewClient(compressedTransport))

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
							continue
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
							continue
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
