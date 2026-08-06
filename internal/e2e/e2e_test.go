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

package e2e_test

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
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket"
	"github.com/sudorandom/connect-bidi-web/connectwebtransport"
	elizav1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/eliza/v1"
	"github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/eliza/v1/elizav1connect"
)

// elizaServer is a minimal ElizaService implementation that echoes the
// caller's input back, so tests can assert on response contents regardless
// of transport.
type elizaServer struct{}

func (elizaServer) Say(_ context.Context, req *elizav1.SayRequest) (*elizav1.SayResponse, error) {
	return &elizav1.SayResponse{Sentence: "Eliza says: " + req.GetSentence()}, nil
}

func (elizaServer) Converse(_ context.Context, stream elizav1connect.ElizaServiceConverseServerStream) error {
	for {
		req, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&elizav1.ConverseResponse{Sentence: "Eliza hears: " + req.GetSentence()}); err != nil {
			return err
		}
	}
}

func (elizaServer) Introduce(_ context.Context, req *elizav1.IntroduceRequest, stream elizav1connect.ElizaServiceIntroduceServerStream) error {
	sentences := []string{
		"Hello, " + req.GetName() + ". I am Eliza.",
		"How are you feeling today?",
	}
	for _, s := range sentences {
		if err := stream.Send(&elizav1.IntroduceResponse{Sentence: s}); err != nil {
			return err
		}
	}
	return nil
}

// exercise runs the full RPC matrix (unary, server streaming, bidi) against
// an ElizaService client and asserts the server echoed our input back.
func exercise(ctx context.Context, t *testing.T, client elizav1connect.ElizaServiceClient) {
	t.Helper()

	// Unary over the bidi transport.
	sayRes, err := client.Say(ctx, &elizav1.SayRequest{Sentence: "unary hello"})
	if err != nil {
		t.Fatalf("Say: %v", err)
	}
	if !strings.Contains(sayRes.GetSentence(), "unary hello") {
		t.Fatalf("Say response %q does not echo request", sayRes.GetSentence())
	}

	// Server streaming.
	intro, err := client.Introduce(ctx, &elizav1.IntroduceRequest{Name: "e2e"})
	if err != nil {
		t.Fatalf("Introduce: %v", err)
	}
	count := 0
	for {
		res, err := intro.Receive()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Introduce recv: %v", err)
		}
		if res.GetSentence() == "" {
			t.Fatal("Introduce returned an empty sentence")
		}
		count++
	}
	if err := intro.Close(); err != nil {
		t.Fatalf("Introduce close: %v", err)
	}
	if count == 0 {
		t.Fatal("Introduce returned no messages")
	}

	// Full bidi.
	conv, err := client.Converse(ctx)
	if err != nil {
		t.Fatalf("Converse: %v", err)
	}
	for i := 1; i <= 3; i++ {
		sent := fmt.Sprintf("bidi msg %d", i)
		if err := conv.Send(&elizav1.ConverseRequest{Sentence: sent}); err != nil {
			t.Fatalf("Converse send: %v", err)
		}
		res, err := conv.Receive()
		if err != nil {
			t.Fatalf("Converse recv: %v", err)
		}
		if !strings.Contains(res.GetSentence(), sent) {
			t.Fatalf("Converse response %q does not echo %q", res.GetSentence(), sent)
		}
	}
	if err := conv.CloseSend(); err != nil {
		t.Fatalf("Converse close send: %v", err)
	}
	if _, err := conv.Receive(); !errors.Is(err, io.EOF) {
		t.Fatalf("Converse: expected EOF after close send, got %v", err)
	}
	if err := conv.Close(); err != nil {
		t.Fatalf("Converse close: %v", err)
	}
}

// startGoServer starts an in-process copy of the demo server stack: one
// Connect server exposed over TLS on both a WebSocket endpoint (TCP) and a
// WebTransport endpoint (UDP/HTTP3), on real localhost sockets.
func startGoServer(t *testing.T) (wsURL, wtURL string) {
	t.Helper()

	connectServer := connect.NewServer()
	elizav1connect.RegisterElizaServiceHandler(connectServer, elizaServer{})
	websocketHandler := connectwebsocket.NewHandler(connectServer)
	webtransportHandler := connectwebtransport.NewHandler(connectServer)

	mux := http.NewServeMux()
	mux.Handle("/websocket", websocketHandler)

	// WebSocket over TLS on TCP.
	httpServer := httptest.NewTLSServer(mux)
	t.Cleanup(httpServer.Close)
	wsURL = strings.Replace(httpServer.URL, "https://", "wss://", 1) + "/websocket"

	// WebTransport over HTTP/3 on UDP.
	cert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	wtServer := &webtransport.Server{
		H3: &http3.Server{
			Handler: mux,
			TLSConfig: &tls.Config{
				Certificates: []tls.Certificate{cert},
				NextProtos:   []string{http3.NextProtoH3},
			},
		},
	}
	t.Cleanup(func() { wtServer.Close() })
	mux.Handle("/webtransport", webtransportHandler.UpgradeHandler(wtServer))

	var listenConfig net.ListenConfig
	packetConn, err := listenConfig.ListenPacket(t.Context(), "udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen packet: %v", err)
	}
	t.Cleanup(func() { packetConn.Close() })
	go func() {
		_ = wtServer.Serve(packetConn)
	}()
	wtURL = "https://" + packetConn.LocalAddr().String() + "/webtransport"

	// Wait for the WebTransport server to start serving.
	time.Sleep(100 * time.Millisecond)
	return wsURL, wtURL
}

func TestGoClientGoServerWebSocket(t *testing.T) {
	t.Parallel()
	wsURL, _ := startGoServer(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	transport := connectwebsocket.NewTransport(
		wsURL,
		connectwebsocket.WithDialOptions(&websocket.DialOptions{
			HTTPClient: &http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
				},
			},
		}),
	)
	client := elizav1connect.NewElizaServiceClient(connect.NewClient(transport))
	exercise(ctx, t, client)
}

func TestGoClientGoServerWebTransport(t *testing.T) {
	t.Parallel()
	_, wtURL := startGoServer(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	dialer := &webtransport.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{http3.NextProtoH3},
		},
	}
	t.Cleanup(func() { _ = dialer.Close() })
	dialResp, session, err := dialer.Dial(ctx, wtURL, nil)
	if err != nil {
		t.Fatalf("webtransport dial: %v", err)
	}
	if dialResp != nil && dialResp.Body != nil {
		t.Cleanup(func() { dialResp.Body.Close() })
	}
	t.Cleanup(func() { _ = session.CloseWithError(0, "") })

	transport := connectwebtransport.NewTransport(session)
	client := elizav1connect.NewElizaServiceClient(connect.NewClient(transport))
	exercise(ctx, t, client)
}

// TestGoClientNodeServerInterop runs the Go WebSocket client against the
// TypeScript server (@sudorandom/connect-bidi-node) via the fixture in
// ts/packages/node/interop/server.ts.
func TestGoClientNodeServerInterop(t *testing.T) {
	t.Parallel()

	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("failed to resolve repo root: %v", err)
	}
	nodeBin, err := exec.LookPath("node")
	if err != nil {
		skipOrFail(t, "node not found in PATH")
	}
	nodeDir := filepath.Join(repoRoot, "ts", "packages", "node")
	if _, err := os.Stat(filepath.Join(repoRoot, "ts", "node_modules")); err != nil {
		skipOrFail(t, "ts/node_modules not installed; run `npm ci` in ts/ first")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, nodeBin, "--import", "tsx", "interop/server.ts")
	cmd.Dir = nodeDir
	cmd.Env = append(os.Environ(), "PORT=0")
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("failed to open stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start interop server: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
	})

	wsURL, err := awaitReady(stdout)
	if err != nil {
		t.Fatalf("interop server did not become ready: %v", err)
	}

	transport := connectwebsocket.NewTransport(wsURL)
	client := elizav1connect.NewElizaServiceClient(connect.NewClient(transport))
	exercise(ctx, t, client)
}

// awaitReady reads lines from the fixture server's stdout until it prints
// "READY <url>".
func awaitReady(r io.Reader) (string, error) {
	buf := make([]byte, 4096)
	var out strings.Builder
	for {
		n, err := r.Read(buf)
		out.Write(buf[:n])
		for line := range strings.Lines(out.String()) {
			if url, ok := strings.CutPrefix(strings.TrimSpace(line), "READY "); ok {
				return url, nil
			}
		}
		if err != nil {
			return "", fmt.Errorf("no READY line in output %q: %w", out.String(), err)
		}
	}
}

// skipOrFail skips the calling test unless E2E_REQUIRE_INTEROP is set, in
// which case a missing prerequisite is a failure (used in CI, where the
// interop environment must be present).
func skipOrFail(t *testing.T, msg string) {
	t.Helper()
	if os.Getenv("E2E_REQUIRE_INTEROP") != "" {
		t.Fatalf("%s (E2E_REQUIRE_INTEROP is set)", msg)
	}
	t.Skip(msg)
}

func generateSelfSignedCert() (tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
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
