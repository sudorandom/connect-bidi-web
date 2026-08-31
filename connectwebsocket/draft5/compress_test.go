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

package draft5_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft5"
	elizav1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/eliza/v1"
	"github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/eliza/v1/elizav1connect"
)

// permessage-deflate on the streaming path is not covered by
// internal/bench, and cannot be: its 16 KiB workloads are unary, and draft
// 5 never puts unary on a socket. The ping service's streaming methods
// carry nothing but numbers, so there is no large streaming payload to
// measure there either.
//
// Eliza's Converse does carry a string, so these tests use it to check the
// thing the benchmark cannot: that a big message on a draft 5 stream
// actually crosses the wire compressed.

type echoEliza struct {
	elizav1connect.UnimplementedElizaServiceHandler
}

func (echoEliza) Converse(_ context.Context, stream elizav1connect.ElizaServiceConverseServerStream) error {
	for {
		req, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&elizav1.ConverseResponse{Sentence: req.GetSentence()}); err != nil {
			return err
		}
	}
}

// countingListener counts every byte crossing the server's socket, so a
// test can see what actually went out rather than what the API reported.
type countingListener struct {
	net.Listener
	rx, tx atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: conn, listener: l}, nil
}

type countingConn struct {
	net.Conn
	listener *countingListener
}

func (c *countingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.listener.rx.Add(int64(n))
	return n, err
}

func (c *countingConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.listener.tx.Add(int64(n))
	return n, err
}

// echoOverStream runs one Converse round trip with a 16 KiB repetitive
// sentence and returns the bytes the server socket carried each way.
func echoOverStream(t *testing.T, opts ...draft5.Option) (rx, tx int64) {
	t.Helper()
	connectServer := connect.NewServer()
	elizav1connect.RegisterElizaServiceHandler(connectServer, echoEliza{})

	mount := append([]draft5.Option{
		draft5.WithWebSocketAcceptOptions(&websocket.AcceptOptions{InsecureSkipVerify: true}),
	}, opts...)
	mux := http.NewServeMux()
	draft5.Mount(mux, connectServer, mount...)

	server := httptest.NewUnstartedServer(mux)
	counter := &countingListener{Listener: server.Listener}
	server.Listener = counter
	server.Start()
	t.Cleanup(server.Close)

	client := elizav1connect.NewElizaServiceClient(
		connect.NewClient(draft5.NewTransport(server.Client(), server.URL, opts...)),
	)

	sentence := strings.Repeat("all work and no play makes jack a dull boy. ", 400)
	stream, err := client.Converse(context.Background())
	if err != nil {
		t.Fatalf("Converse: %v", err)
	}
	if err := stream.Send(&elizav1.ConverseRequest{Sentence: sentence}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp, err := stream.Receive()
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if resp.GetSentence() != sentence {
		t.Fatalf("the payload did not round-trip: got %d bytes, want %d",
			len(resp.GetSentence()), len(sentence))
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if _, err := stream.Receive(); !errors.Is(err, io.EOF) {
		t.Fatalf("Receive = %v, want io.EOF", err)
	}
	_ = stream.Close()

	return counter.rx.Load(), counter.tx.Load()
}

// TestStreamingCompression checks that permessage-deflate actually
// compresses a large message on a draft 5 stream, in *both* directions —
// the client's upload as well as the server's response.
func TestStreamingCompression(t *testing.T) {
	t.Parallel()
	const payload = 43 * 400 // the repeated sentence, ~17 KB

	onRx, onTx := echoOverStream(t)
	offRx, offTx := echoOverStream(t, draft5.WithoutCompression())

	t.Logf("compression on:  rx=%d tx=%d", onRx, onTx)
	t.Logf("compression off: rx=%d tx=%d", offRx, offTx)

	if offRx < payload || offTx < payload {
		t.Fatalf("uncompressed run carried rx=%d tx=%d, expected at least %d each way",
			offRx, offTx, payload)
	}
	// Highly repetitive text compresses by orders of magnitude; anything
	// close to the raw size means deflate did not run.
	if onRx > offRx/4 {
		t.Errorf("client->server was not compressed: %d bytes with deflate vs %d without", onRx, offRx)
	}
	if onTx > offTx/4 {
		t.Errorf("server->client was not compressed: %d bytes with deflate vs %d without", onTx, offTx)
	}
}
