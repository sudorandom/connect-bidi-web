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

// Interop fixture for the TypeScript e2e tests in ts/packages/web/e2e.
// Serves ElizaService over plain HTTP (standard Connect protocol) and over a
// plain ws:// WebSocket (no TLS) using every draft. Prints
// "READY ws://<host>:<port>/websocket-draft3" on stdout once listening.
//
// Drafts 3 and 4 get a path each. Draft 5 has none: it mounts on the
// Connect procedure URLs themselves, which is why draft5.Mount stands in
// for connecthttp.Mount here rather than being registered alongside it —
// the same handler serves POST as ordinary Connect and GET+Upgrade as
// draft 5. Drafts 1 and 7 would want those URLs too, so on this one server
// each takes a prefix instead; the procedure is the path's last two
// segments either way, and draft 7's specification provides for exactly
// this deployment shape.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft1"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft3"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft4"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft5"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft7"
	elizav1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/eliza/v1"
	"github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/eliza/v1/elizav1connect"
)

// elizaServer echoes the caller's input back so tests can assert on response
// contents.
type elizaServer struct{}

func (elizaServer) Say(_ context.Context, req *elizav1.SayRequest) (*elizav1.SayResponse, error) {
	return &elizav1.SayResponse{Sentence: "Go Eliza says: " + req.GetSentence()}, nil
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
		if err := stream.Send(&elizav1.ConverseResponse{Sentence: "Go Eliza hears: " + req.GetSentence()}); err != nil {
			return err
		}
	}
}

func (elizaServer) Introduce(_ context.Context, req *elizav1.IntroduceRequest, stream elizav1connect.ElizaServiceIntroduceServerStream) error {
	sentences := []string{
		"Hello, " + req.GetName() + ". I am Go Eliza.",
		"How are you feeling today?",
	}
	for _, s := range sentences {
		if err := stream.Send(&elizav1.IntroduceResponse{Sentence: s}); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "address to listen on; port 0 picks a free port")
	flag.Parse()

	connectServer := connect.NewServer()
	elizav1connect.RegisterElizaServiceHandler(connectServer, elizaServer{})

	mux := http.NewServeMux()
	draft5.Mount(mux, connectServer)
	draft1.Mount(mux, connectServer, draft1.DefaultPathPrefix)
	draft7.MountWebSocket(mux, connectServer, draft7.WithPathPrefix("/websocket-draft7"))
	mux.Handle("/websocket-draft3", draft3.NewHandler(connectServer))
	mux.Handle("/websocket-draft4", draft4.NewHandler(connectServer))

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	fmt.Printf("READY ws://%s/websocket-draft3\n", listener.Addr())

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	if err := server.Serve(listener); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
