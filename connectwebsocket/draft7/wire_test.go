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

package draft7_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/connectwebsocket/draft7"
	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	"google.golang.org/protobuf/proto"
)

// These tests speak the protocol by hand, so the wire shape is pinned
// independently of the client that produces it:
//
//	→ [text]   M{...}
//	→ [binary] B<request message>
//	→ [text]   C
//	← [text]   M{...}
//	← [binary] B<response message>
//	← [text]   S{...}

// wireConn is a hand-rolled draft 7 client.
type wireConn struct {
	tb   testing.TB
	conn *websocket.Conn
	ctx  context.Context //nolint:containedctx // test helper, scoped to one test
}

func dialWire(tb testing.TB, serverURL, procedure, subprotocol string) *wireConn {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	tb.Cleanup(cancel)

	wsURL := "ws" + strings.TrimPrefix(serverURL, "http") + procedure
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{ //nolint:bodyclose // coder/websocket closes the handshake response body itself
		Subprotocols: []string{subprotocol},
	})
	if err != nil {
		tb.Fatalf("dial %s: %v", wsURL, err)
	}
	if got := conn.Subprotocol(); got != subprotocol {
		tb.Fatalf("negotiated subprotocol = %q, want %q", got, subprotocol)
	}
	conn.SetReadLimit(-1)
	tb.Cleanup(func() { _ = conn.CloseNow() })
	return &wireConn{tb: tb, conn: conn, ctx: ctx}
}

func dialProto(tb testing.TB, serverURL, procedure string) *wireConn {
	tb.Helper()
	return dialWire(tb, serverURL, procedure, "connectrpc.1+proto")
}

func (w *wireConn) writeText(marker byte, payload string) {
	w.tb.Helper()
	w.write(websocket.MessageText, append([]byte{marker}, payload...))
}

func (w *wireConn) writeBinary(marker byte, payload []byte) {
	w.tb.Helper()
	w.write(websocket.MessageBinary, append([]byte{marker}, payload...))
}

func (w *wireConn) writeProto(marker byte, msg proto.Message) {
	w.tb.Helper()
	data, err := proto.Marshal(msg)
	if err != nil {
		w.tb.Fatalf("marshal message: %v", err)
	}
	w.writeBinary(marker, data)
}

func (w *wireConn) write(msgType websocket.MessageType, data []byte) {
	w.tb.Helper()
	if err := w.conn.Write(w.ctx, msgType, data); err != nil {
		w.tb.Fatalf("write: %v", err)
	}
}

// read returns the next message split into frame type, marker, payload.
func (w *wireConn) read() (websocket.MessageType, byte, []byte) {
	w.tb.Helper()
	msgType, data, err := w.conn.Read(w.ctx)
	if err != nil {
		w.tb.Fatalf("read: %v", err)
	}
	if len(data) == 0 {
		w.tb.Fatal("read an empty message: no marker")
	}
	return msgType, data[0], data[1:]
}

// expectMetadata reads the leading-metadata message and returns it parsed.
func (w *wireConn) expectMetadata() map[string][]string {
	w.tb.Helper()
	msgType, marker, payload := w.read()
	if marker != 'M' {
		w.tb.Fatalf("expected M, got marker %q with payload %q", marker, payload)
	}
	if msgType != websocket.MessageText {
		w.tb.Fatalf("M arrived as %v, want text", msgType)
	}
	var metadata map[string][]string
	if err := json.Unmarshal(payload, &metadata); err != nil {
		w.tb.Fatalf("unmarshal M payload %q: %v", payload, err)
	}
	return metadata
}

// expectProtoBody reads one B message, which under the Protobuf codec must
// be binary.
func (w *wireConn) expectProtoBody(dst proto.Message) {
	w.tb.Helper()
	msgType, marker, payload := w.read()
	if marker != 'B' {
		w.tb.Fatalf("expected B, got marker %q with payload %q", marker, payload)
	}
	if msgType != websocket.MessageBinary {
		w.tb.Fatalf("Protobuf body arrived as %v, want binary", msgType)
	}
	if err := proto.Unmarshal(payload, dst); err != nil {
		w.tb.Fatalf("unmarshal body: %v", err)
	}
}

// expectEndStream reads the S message and returns it parsed.
func (w *wireConn) expectEndStream() map[string]json.RawMessage {
	w.tb.Helper()
	msgType, marker, payload := w.read()
	if marker != 'S' {
		w.tb.Fatalf("expected S, got marker %q with payload %q", marker, payload)
	}
	if msgType != websocket.MessageText {
		w.tb.Fatalf("S arrived as %v, want text", msgType)
	}
	var endStream map[string]json.RawMessage
	if err := json.Unmarshal(payload, &endStream); err != nil {
		w.tb.Fatalf("unmarshal S payload %q: %v", payload, err)
	}
	return endStream
}

// expectError reads M then S and asserts the S carries an error with the
// given code — the shape every RPC-level failure takes on the wire.
func (w *wireConn) expectError(code string) {
	w.tb.Helper()
	w.expectMetadata()
	endStream := w.expectEndStream()
	rpcError, ok := endStream["error"]
	if !ok {
		w.tb.Fatalf("S carried no error: %v", endStream)
	}
	if !strings.Contains(string(rpcError), `"`+code+`"`) {
		w.tb.Errorf("error = %s, want code %q", rpcError, code)
	}
}

// TestWireSequence walks one bidi RPC message by message, in both codecs.
func TestWireSequence(t *testing.T) {
	t.Parallel()
	_, _, server := newTestServer(t)

	t.Run("Proto", func(t *testing.T) {
		t.Parallel()
		wire := dialProto(t, server.URL, "/connectbidi.ping.v1.PingService/CumSum")
		wire.writeText('M', `{"x-custom":["v"]}`)
		wire.writeProto('B', &pingv1.CumSumRequest{Number: 3})

		metadata := wire.expectMetadata()
		if got := metadata["x-test-header"]; len(got) != 1 || got[0] != "header-val" {
			t.Errorf("response metadata = %v, want x-test-header", metadata)
		}
		var resp pingv1.CumSumResponse
		wire.expectProtoBody(&resp)
		if resp.GetSum() != 3 {
			t.Errorf("sum = %d, want 3", resp.GetSum())
		}

		wire.writeText('C', "")
		endStream := wire.expectEndStream()
		if _, hasError := endStream["error"]; hasError {
			t.Errorf("S carried an error: %s", endStream["error"])
		}
		if !strings.Contains(string(endStream["metadata"]), "trailer-val") {
			t.Errorf("S metadata = %s, want the response trailer", endStream["metadata"])
		}
	})

	t.Run("JSON", func(t *testing.T) {
		t.Parallel()
		wire := dialWire(t, server.URL, "/connectbidi.ping.v1.PingService/CumSum", "connectrpc.1+json")
		wire.writeText('M', `{}`)
		wire.writeText('B', `{"number":"5"}`)

		wire.expectMetadata()
		msgType, marker, payload := wire.read()
		if marker != 'B' || msgType != websocket.MessageText {
			t.Fatalf("JSON body arrived as marker %q, %v; want B, text", marker, msgType)
		}
		if !strings.Contains(string(payload), `"5"`) {
			t.Errorf("body = %s, want a sum of 5", payload)
		}
		wire.writeText('C', "")
		wire.expectEndStream()
	})

	t.Run("BaseTokenIsJSON", func(t *testing.T) {
		t.Parallel()
		wire := dialWire(t, server.URL, "/connectbidi.ping.v1.PingService/CumSum", "connectrpc.1")
		wire.writeText('M', `{}`)
		wire.writeText('B', `{"number":"2"}`)
		wire.expectMetadata()
		msgType, marker, _ := wire.read()
		if marker != 'B' || msgType != websocket.MessageText {
			t.Fatalf("body under the base token arrived as marker %q, %v; want B, text", marker, msgType)
		}
	})
}

// TestWireUnary is the specification's worked example: a unary RPC over
// the socket is M, B, C one way and M, B, S the other.
func TestWireUnary(t *testing.T) {
	t.Parallel()
	_, _, server := newTestServer(t)
	wire := dialProto(t, server.URL, "/connectbidi.ping.v1.PingService/Ping")
	wire.writeText('M', `{}`)
	wire.writeProto('B', &pingv1.PingRequest{Number: 7})
	wire.writeText('C', "")

	wire.expectMetadata()
	var resp pingv1.PingResponse
	wire.expectProtoBody(&resp)
	if resp.GetNumber() != 7 {
		t.Errorf("number = %d, want 7", resp.GetNumber())
	}
	wire.expectEndStream()
}

// TestWireClientEndStreamWithBody checks that C may carry the final
// request message instead of a bare marker.
func TestWireClientEndStreamWithBody(t *testing.T) {
	t.Parallel()
	_, _, server := newTestServer(t)
	wire := dialProto(t, server.URL, "/connectbidi.ping.v1.PingService/Sum")
	wire.writeText('M', `{}`)
	wire.writeProto('B', &pingv1.SumRequest{Number: 1})
	wire.writeProto('C', &pingv1.SumRequest{Number: 2})

	wire.expectMetadata()
	var resp pingv1.SumResponse
	wire.expectProtoBody(&resp)
	if got, want := resp.GetSum(), int64(3*1000+2); got != want {
		t.Errorf("sum = %d, want %d: the body on C must count", got, want)
	}
	wire.expectEndStream()
}

// TestWireEmptyBodyIsBinary pins the rule for a message that encodes to
// zero bytes: a bare B is the empty Protobuf message, and it is binary.
func TestWireEmptyBodyIsBinary(t *testing.T) {
	t.Parallel()
	_, _, server := newTestServer(t)
	wire := dialProto(t, server.URL, "/connectbidi.ping.v1.PingService/CumSum")
	wire.writeText('M', `{}`)
	wire.writeBinary('B', nil)

	wire.expectMetadata()
	msgType, marker, payload := wire.read()
	if marker != 'B' || len(payload) != 0 || msgType != websocket.MessageBinary {
		t.Errorf("empty response arrived as marker %q, %d bytes, %v; want a bare binary B", marker, len(payload), msgType)
	}
	wire.writeText('C', "")
	wire.expectEndStream()
}

// TestWireProtocolErrors sends each violation the specification names and
// checks that the server ends the stream with M and an S carrying the
// error, rather than skipping the message and carrying on.
func TestWireProtocolErrors(t *testing.T) {
	t.Parallel()
	_, _, server := newTestServer(t)

	tests := []struct {
		name string
		send func(w *wireConn)
		code string
	}{
		{
			name: "BodyBeforeMetadata",
			send: func(w *wireConn) { w.writeProto('B', &pingv1.CumSumRequest{Number: 1}) },
			code: "invalid_argument",
		},
		{
			name: "MetadataAsBinary",
			send: func(w *wireConn) { w.writeBinary('M', []byte(`{}`)) },
			code: "invalid_argument",
		},
		{
			name: "MetadataWithoutPayload",
			send: func(w *wireConn) { w.writeText('M', "") },
			code: "invalid_argument",
		},
		{
			name: "MetadataNotAnObject",
			send: func(w *wireConn) { w.writeText('M', `["a"]`) },
			code: "invalid_argument",
		},
		{
			name: "MetadataBareStringValue",
			send: func(w *wireConn) { w.writeText('M', `{"k":"v"}`) },
			code: "invalid_argument",
		},
		{
			name: "MetadataDuplicateKeyByCase",
			send: func(w *wireConn) { w.writeText('M', `{"K":["a"],"k":["b"]}`) },
			code: "invalid_argument",
		},
		{
			name: "MetadataReservedKey",
			send: func(w *wireConn) { w.writeText('M', `{"x-forwarded-for":["1.2.3.4"]}`) },
			code: "invalid_argument",
		},
		{
			name: "MetadataBadBase64",
			send: func(w *wireConn) { w.writeText('M', `{"k-bin":["***"]}`) },
			code: "invalid_argument",
		},
		{
			name: "SecondMetadata",
			send: func(w *wireConn) {
				w.writeText('M', `{}`)
				w.writeText('M', `{}`)
			},
			code: "invalid_argument",
		},
		{
			name: "UnknownMarker",
			send: func(w *wireConn) {
				w.writeText('M', `{}`)
				w.writeText('X', "")
			},
			code: "invalid_argument",
		},
		{
			name: "HighBitMarker",
			send: func(w *wireConn) {
				w.writeText('M', `{}`)
				w.writeBinary(0x80, []byte("anything"))
			},
			code: "invalid_argument",
		},
		{
			name: "ServerMarkerFromClient",
			send: func(w *wireConn) {
				w.writeText('M', `{}`)
				w.writeText('S', `{}`)
			},
			code: "invalid_argument",
		},
		{
			name: "EmptyMessage",
			send: func(w *wireConn) {
				w.writeText('M', `{}`)
				w.write(websocket.MessageBinary, nil)
			},
			code: "invalid_argument",
		},
		{
			name: "TextBodyUnderProto",
			send: func(w *wireConn) {
				w.writeText('M', `{}`)
				w.writeText('B', `{"number":"1"}`)
			},
			code: "invalid_argument",
		},
		{
			name: "BinaryClientEndStreamWithoutBody",
			send: func(w *wireConn) {
				w.writeText('M', `{}`)
				w.writeBinary('C', nil)
			},
			code: "invalid_argument",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			wire := dialProto(t, server.URL, "/connectbidi.ping.v1.PingService/CumSum")
			test.send(wire)
			wire.expectError(test.code)
		})
	}

	t.Run("EmptyTextBodyUnderJSON", func(t *testing.T) {
		t.Parallel()
		wire := dialWire(t, server.URL, "/connectbidi.ping.v1.PingService/CumSum", "connectrpc.1+json")
		wire.writeText('M', `{}`)
		wire.writeText('B', "")
		wire.expectError("invalid_argument")
	})

	t.Run("BinaryBodyUnderJSON", func(t *testing.T) {
		t.Parallel()
		wire := dialWire(t, server.URL, "/connectbidi.ping.v1.PingService/CumSum", "connectrpc.1+json")
		wire.writeText('M', `{}`)
		wire.writeBinary('B', []byte{0x08, 0x01})
		wire.expectError("invalid_argument")
	})

	t.Run("MessageAfterClientEndStream", func(t *testing.T) {
		t.Parallel()
		// The violation has to reach the server while the RPC is still
		// running. CountUp is made to block, so the read pump always sees
		// the extra message in flight.
		impl, _, blocking := newTestServer(t)
		impl.countUpBlocks = true
		wire := dialProto(t, blocking.URL, "/connectbidi.ping.v1.PingService/CountUp")
		wire.writeText('M', `{}`)
		wire.writeProto('C', &pingv1.CountUpRequest{Number: 1})

		wire.expectMetadata()
		var resp pingv1.CountUpResponse
		wire.expectProtoBody(&resp)

		wire.writeProto('B', &pingv1.CountUpRequest{Number: 2})
		endStream := wire.expectEndStream()
		if !strings.Contains(string(endStream["error"]), "invalid_argument") {
			t.Errorf("a message after C was accepted: %v", endStream)
		}
	})

	t.Run("OversizedMessage", func(t *testing.T) {
		t.Parallel()
		_, _, limited := newTestServer(t, draft7WithReadMaxBytes(64))
		wire := dialProto(t, limited.URL, "/connectbidi.ping.v1.PingService/CumSum")
		wire.writeText('M', `{}`)
		wire.writeProto('B', &pingv1.CumSumRequest{Text: strings.Repeat("a", 4096)})
		wire.expectError("resource_exhausted")
	})
}

// TestWireInvalidTimeout checks that a malformed connect-timeout-ms is
// reported on the socket, not by refusing the handshake: the upgrade
// succeeded, so the failure arrives as M then S.
func TestWireInvalidTimeout(t *testing.T) {
	t.Parallel()
	_, _, server := newTestServer(t)
	for _, timeout := range []string{"soon", "12345678901", "1.5"} {
		t.Run(timeout, func(t *testing.T) {
			t.Parallel()
			wire := dialProto(t, server.URL, "/connectbidi.ping.v1.PingService/CumSum?connect-timeout-ms="+timeout)
			wire.expectError("invalid_argument")
		})
	}
}

// TestWireDeadlineWhileWaitingForMetadata checks that a client which
// upgrades and then goes silent is still answered: the deadline expires,
// and the server sends M and S rather than dropping the connection.
func TestWireDeadlineWhileWaitingForMetadata(t *testing.T) {
	t.Parallel()
	_, _, server := newTestServer(t)
	wire := dialProto(t, server.URL, "/connectbidi.ping.v1.PingService/CumSum?connect-timeout-ms=200")
	wire.expectError("deadline_exceeded")
}

// TestWireCloseWithoutClientEndStream checks that a server does not need a
// C to complete an RPC it has already ended with S: the client may simply
// close.
func TestWireCloseWithoutClientEndStream(t *testing.T) {
	t.Parallel()
	_, _, server := newTestServer(t)
	wire := dialProto(t, server.URL, "/connectbidi.ping.v1.PingService/CountUp")
	wire.writeText('M', `{}`)
	wire.writeProto('C', &pingv1.CountUpRequest{Number: 2})
	wire.expectMetadata()
	var resp pingv1.CountUpResponse
	wire.expectProtoBody(&resp)
	wire.expectProtoBody(&resp)
	wire.expectEndStream()
	// The server closes 1000 after S; reading past it reports the close.
	_, _, err := wire.conn.Read(wire.ctx)
	if got := websocket.CloseStatus(err); got != websocket.StatusNormalClosure && !errors.Is(err, io.EOF) {
		t.Errorf("after S the connection ended with %v, want close 1000", err)
	}
}

// draft7WithReadMaxBytes is the read-limit option, named for the wire
// tests' readability.
func draft7WithReadMaxBytes(n int) draft7.Option { return draft7.WithReadMaxBytes(n) }
