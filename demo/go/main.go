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

// The demo server serves the connect-bidi-web demo site and the Eliza demo
// service over three transports at once: standard Connect over HTTP/1.1 and
// HTTP/2, WebSocket, and WebTransport (HTTP/3).
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	connect "connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"github.com/coder/websocket"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket"
	"github.com/sudorandom/connect-bidi-web/connectwebtransport"
	elizav1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/eliza/v1"
	"github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/eliza/v1/elizav1connect"
)

type elizaServer struct {
	elizav1connect.UnimplementedElizaServiceHandler
}

func (elizaServer) Say(_ context.Context, req *elizav1.SayRequest) (*elizav1.SayResponse, error) {
	log.Printf("[Go Eliza] Say: %s", req.Sentence)
	return &elizav1.SayResponse{
		Sentence: "Go Eliza says: " + req.Sentence,
	}, nil
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
		log.Printf("[Go Eliza] Converse: %s", req.Sentence)
		if err := stream.Send(&elizav1.ConverseResponse{
			Sentence: "Go Eliza says: " + req.Sentence,
		}); err != nil {
			return err
		}
	}
}

func (elizaServer) Introduce(ctx context.Context, req *elizav1.IntroduceRequest, stream elizav1connect.ElizaServiceIntroduceServerStream) error {
	protocol := "unknown transport"
	if callInfo, ok := connect.CallInfoForServerContext(ctx); ok {
		protocol = callInfo.Protocol
	}
	log.Printf("[Go Eliza] Introduce over %s: %s", protocol, req.Name)
	sentences := []string{
		"Hi " + req.Name + ", I'm Eliza over " + protocol + "!",
		"This RPC is running on the Go backend using " + protocol + ".",
		"How can I help you today?",
	}
	for _, s := range sentences {
		if err := stream.Send(&elizav1.IntroduceResponse{Sentence: s}); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func main() {
	certFile := flag.String("cert", "localhost.pem", "TLS certificate file (create with mkcert localhost)")
	keyFile := flag.String("key", "localhost-key.pem", "TLS key file")
	staticDir := flag.String("static", "../web/dist", "directory with the built demo site")
	addr := flag.String("addr", ":4433", "address to listen on (TCP for HTTP/2, UDP for HTTP/3)")
	flag.Parse()

	cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		log.Fatalf("failed to load keypair: %v", err)
	}

	// 1. Build the Connect server and the per-transport handlers.
	connectServer := connect.NewServer()
	elizav1connect.RegisterElizaServiceHandler(connectServer, elizaServer{})
	webtransportHandler := connectwebtransport.NewHandler(connectServer)
	websocketHandler := connectwebsocket.NewHandler(connectServer, connectwebsocket.WithAcceptOptions(&websocket.AcceptOptions{
		InsecureSkipVerify: true,
	}))

	// 2. One mux serves Connect over HTTP, the WebSocket endpoint, and the
	// static demo site.
	mux := http.NewServeMux()
	connecthttp.Mount(mux, connectServer)
	mux.Handle("/websocket", websocketHandler)
	// The demo UI probes this endpoint to decide whether to offer the
	// WebTransport option; this server terminates HTTP/3, so it does.
	mux.HandleFunc("/capabilities.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"webtransport":true}`))
	})
	mux.Handle("/", http.FileServer(http.Dir(*staticDir)))

	corsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Allow-Methods", "*")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		mux.ServeHTTP(w, r)
	})

	// 3. WebTransport (HTTP/3) server on UDP.
	wtServer := &webtransport.Server{
		H3: &http3.Server{
			Addr:    *addr,
			Handler: corsHandler,
			TLSConfig: &tls.Config{
				Certificates: []tls.Certificate{cert},
				NextProtos:   []string{http3.NextProtoH3},
			},
			EnableDatagrams: true,
		},
		CheckOrigin: func(*http.Request) bool { return true },
	}
	webtransport.ConfigureHTTP3Server(wtServer.H3)

	mux.Handle("/webtransport", webtransportHandler.UpgradeHandler(wtServer))

	go func() {
		log.Printf("UDP WebTransport/H3 server listening on https://localhost%s/webtransport", *addr)
		udpAddr, err := net.ResolveUDPAddr("udp", *addr)
		if err != nil {
			log.Fatalf("failed to resolve UDP: %v", err)
		}
		conn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			log.Fatalf("failed to listen UDP: %v", err)
		}
		if err := wtServer.Serve(conn); err != nil {
			log.Fatalf("WebTransport server failed: %v", err)
		}
	}()

	// 4. HTTP/1.1 + HTTP/2 server on TCP, advertising HTTP/3 via Alt-Svc.
	withH3Headers := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			rw.Header().Add("Alt-Svc", `h3=":4433"; ma=2592000`)
			next.ServeHTTP(rw, r)
		})
	}

	log.Printf("TCP HTTP/2 frontend server listening on https://localhost%s", *addr)
	server := &http.Server{
		Addr:              *addr,
		Handler:           withH3Headers(corsHandler),
		ReadHeaderTimeout: 10 * time.Second,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h2", "http/1.1"},
		},
	}
	if err := server.ListenAndServeTLS("", ""); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}
