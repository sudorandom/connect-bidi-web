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
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"github.com/sudorandom/connect-bidi-web/connectwebtransport"
	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	pingv1connect "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func generateSelfSignedCert() (tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Acme Co"},
		},
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

type testPingServer struct {
	respHeaders  map[string]string
	respTrailers map[string]string
}

func (s testPingServer) Ping(ctx context.Context, req *pingv1.PingRequest) (*pingv1.PingResponse, error) {
	if info, ok := connect.CallInfoForServerContext(ctx); ok {
		for k, v := range s.respHeaders {
			info.ResponseHeader().Set(k, v)
		}
		for k, v := range s.respTrailers {
			info.ResponseTrailer().Set(k, v)
		}
	}
	return &pingv1.PingResponse{Number: req.GetNumber(), Text: req.GetText()}, nil
}

func (testPingServer) Fail(_ context.Context, _ *pingv1.FailRequest) (*pingv1.FailResponse, error) {
	detail, err := connectproto.NewErrorDetail(wrapperspb.String("detail"))
	if err != nil {
		return nil, err
	}
	return nil, connect.NewError(connect.CodeUnimplemented, "Fail").WithDetail(detail)
}

func (testPingServer) Sum(_ context.Context, stream pingv1connect.PingServiceSumServerStream) (*pingv1.SumResponse, error) {
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

func (testPingServer) CountUp(_ context.Context, req *pingv1.CountUpRequest, stream pingv1connect.PingServiceCountUpServerStream) error {
	for i := int64(1); i <= req.GetNumber(); i++ {
		if err := stream.Send(&pingv1.CountUpResponse{Number: i}); err != nil {
			return err
		}
	}
	return nil
}

func (testPingServer) CumSum(_ context.Context, stream pingv1connect.PingServiceCumSumServerStream) error {
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

func TestWebTransportRoundTrip(t *testing.T) {
	t.Parallel()
	cert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
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
	t.Cleanup(func() { wtServer.Close() })

	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, testPingServer{
		respHeaders:  map[string]string{"X-Test-Header": "header-val"},
		respTrailers: map[string]string{"X-Test-Trailer": "trailer-val"},
	})

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
	packetConn, err := listenConfig.ListenPacket(t.Context(), "udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen packet: %v", err)
	}
	t.Cleanup(func() { packetConn.Close() })

	addr := packetConn.LocalAddr().String()

	go func() {
		_ = wtServer.Serve(packetConn)
	}()

	// Wait for server to start serving
	time.Sleep(100 * time.Millisecond)

	wtClient := &webtransport.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{http3.NextProtoH3},
		},
	}
	defer wtClient.Close()

	dialResp, session, err := wtClient.Dial(t.Context(), "https://"+addr+"/webtransport", nil)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	if dialResp != nil && dialResp.Body != nil {
		t.Cleanup(func() {
			dialResp.Body.Close()
		})
	}
	t.Cleanup(func() {
		_ = session.CloseWithError(0, "")
	})

	wtTransport := connectwebtransport.NewTransport(session)
	client := pingv1connect.NewPingServiceClient(connect.NewClient(wtTransport))

	// 1. Unary call
	t.Run("Unary", func(t *testing.T) {
		t.Parallel()
		ctx, callInfo := connect.NewClientContext(t.Context())
		resp, err := client.Ping(ctx, &pingv1.PingRequest{Number: 42, Text: "hello"})
		if err != nil {
			t.Fatalf("Ping failed: %v", err)
		}
		if resp.GetNumber() != 42 || resp.GetText() != "hello" {
			t.Errorf("unexpected response: %+v", resp)
		}

		if got := callInfo.ResponseHeader().Get("X-Test-Header"); got != "header-val" {
			t.Errorf("expected X-Test-Header 'header-val', got %q", got)
		}
		if got := callInfo.ResponseTrailer().Get("X-Test-Trailer"); got != "trailer-val" {
			t.Errorf("expected X-Test-Trailer 'trailer-val', got %q", got)
		}
	})

	// 2. Server streaming call
	t.Run("ServerStreaming", func(t *testing.T) {
		t.Parallel()
		stream, err := client.CountUp(t.Context(), &pingv1.CountUpRequest{Number: 3})
		if err != nil {
			t.Fatalf("CountUp failed: %v", err)
		}
		defer stream.Close()

		var numbers []int64
		for {
			res, err := stream.Receive()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("Receive failed: %v", err)
			}
			numbers = append(numbers, res.GetNumber())
		}
		if len(numbers) != 3 || numbers[0] != 1 || numbers[1] != 2 || numbers[2] != 3 {
			t.Errorf("unexpected numbers: %v", numbers)
		}
	})

	// 3. Bidirectional streaming call
	t.Run("BidiStreaming", func(t *testing.T) {
		t.Parallel()
		bidiStream, err := client.CumSum(t.Context())
		if err != nil {
			t.Fatalf("CumSum failed: %v", err)
		}
		defer bidiStream.Close()

		err = bidiStream.Send(&pingv1.CumSumRequest{Number: 10})
		if err != nil {
			t.Fatalf("Send failed: %v", err)
		}
		res, err := bidiStream.Receive()
		if err != nil {
			t.Fatalf("Receive failed: %v", err)
		}
		if res.GetSum() != 10 {
			t.Errorf("unexpected sum: %d", res.GetSum())
		}

		err = bidiStream.Send(&pingv1.CumSumRequest{Number: 20})
		if err != nil {
			t.Fatalf("Send failed: %v", err)
		}
		res, err = bidiStream.Receive()
		if err != nil {
			t.Fatalf("Receive failed: %v", err)
		}
		if res.GetSum() != 30 {
			t.Errorf("unexpected sum: %d", res.GetSum())
		}
	})

	t.Run("ErrorDetails", func(t *testing.T) {
		t.Parallel()
		_, err := client.Fail(t.Context(), &pingv1.FailRequest{})
		var connectErr *connect.Error
		if !errors.As(err, &connectErr) {
			t.Fatalf("Fail error = %v, want *connect.Error", err)
		}
		if got, want := connectErr.Code(), connect.CodeUnimplemented; got != want {
			t.Errorf("error code = %v, want %v", got, want)
		}
		details := connectErr.Details()
		if len(details) != 1 {
			t.Fatalf("error detail count = %d, want 1", len(details))
		}
		message, err := connectproto.UnmarshalErrorDetail(details[0])
		if err != nil {
			t.Fatalf("unmarshal error detail: %v", err)
		}
		if got, want := message.(*wrapperspb.StringValue).GetValue(), "detail"; got != want {
			t.Errorf("error detail = %q, want %q", got, want)
		}
	})
}
