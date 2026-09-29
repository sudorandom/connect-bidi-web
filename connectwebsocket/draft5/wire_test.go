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
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/internal/connectprotocol"
	pingv1 "github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1"
	"google.golang.org/protobuf/proto"
)

// These tests speak the protocol by hand, so the wire shape is pinned
// independently of the client that produces it. Draft 5 is small enough
// that the whole of it fits in one exchange:
//
//	→ [text]   {"metadata":{...}}
//	→ [binary] <request message>
//	→ [text]   (empty)
//	← [text]   {"metadata":{...}}
//	← [binary] <response message>
//	← [text]   (empty)
//	← [text]   {"metadata":{...}}

const wireSubprotocol = "connect.bidi.d5"

// wireConn is a hand-rolled draft 5 client.
type wireConn struct {
	tb   testing.TB
	conn *websocket.Conn
	ctx  context.Context //nolint:containedctx // test helper, scoped to one test
}

func dialWire(tb testing.TB, serverURL, procedure string) *wireConn {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	tb.Cleanup(cancel)

	wsURL := "ws" + strings.TrimPrefix(serverURL, "http") + procedure
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{ //nolint:bodyclose // coder/websocket closes the handshake response body itself
		Subprotocols: []string{wireSubprotocol},
	})
	if err != nil {
		tb.Fatalf("dial %s: %v", wsURL, err)
	}
	if got := conn.Subprotocol(); got != wireSubprotocol {
		tb.Fatalf("negotiated subprotocol = %q, want %q", got, wireSubprotocol)
	}
	tb.Cleanup(func() { _ = conn.CloseNow() })
	return &wireConn{tb: tb, conn: conn, ctx: ctx}
}

// writeHeaders sends the mandatory first message.
func (w *wireConn) writeHeaders(header http.Header) {
	w.tb.Helper()
	data, err := connectprotocol.MarshalHeaders(header)
	if err != nil {
		w.tb.Fatalf("marshal headers: %v", err)
	}
	w.write(websocket.MessageText, data)
}

func (w *wireConn) writeMessage(msg proto.Message) {
	w.tb.Helper()
	data, err := proto.Marshal(msg)
	if err != nil {
		w.tb.Fatalf("marshal message: %v", err)
	}
	w.write(websocket.MessageBinary, data)
}

// writeSeparator half-closes the request direction.
func (w *wireConn) writeSeparator() {
	w.tb.Helper()
	w.write(websocket.MessageText, nil)
}

func (w *wireConn) write(msgType websocket.MessageType, data []byte) {
	w.tb.Helper()
	if err := w.conn.Write(w.ctx, msgType, data); err != nil {
		w.tb.Fatalf("write: %v", err)
	}
}

func (w *wireConn) read() (websocket.MessageType, []byte) {
	w.tb.Helper()
	msgType, data, err := w.conn.Read(w.ctx)
	if err != nil {
		w.tb.Fatalf("read: %v", err)
	}
	return msgType, data
}

// expectHeaders reads the response headers message and returns its metadata.
func (w *wireConn) expectHeaders() http.Header {
	w.tb.Helper()
	msgType, data := w.read()
	if msgType != websocket.MessageText {
		w.tb.Fatalf("headers message arrived as %v, want text", msgType)
	}
	if len(data) == 0 {
		w.tb.Fatal("expected a headers message, got the separator")
	}
	header, err := connectprotocol.UnmarshalHeaders(data)
	if err != nil {
		w.tb.Fatalf("unmarshal headers %q: %v", data, err)
	}
	return header
}

// expectMessage reads one response data message, checking the opcode rule
// as it goes: a data message travels as text exactly when its payload is
// non-empty and valid UTF-8, and as binary otherwise. Pinning it here means
// every test that reads a message also tests the rule.
func (w *wireConn) expectMessage(dst proto.Message) {
	w.tb.Helper()
	msgType, data := w.read()
	wantText := len(data) > 0 && utf8.Valid(data)
	want := websocket.MessageBinary
	if wantText {
		want = websocket.MessageText
	}
	if msgType != want {
		w.tb.Fatalf("data message of %d bytes (valid UTF-8: %v) arrived as %v, want %v",
			len(data), wantText, msgType, want)
	}
	if err := proto.Unmarshal(data, dst); err != nil {
		w.tb.Fatalf("unmarshal message: %v", err)
	}
}

// expectSeparator reads the empty text message ending the data phase.
func (w *wireConn) expectSeparator() {
	w.tb.Helper()
	msgType, data := w.read()
	if msgType != websocket.MessageText {
		w.tb.Fatalf("separator arrived as %v, want text", msgType)
	}
	if len(data) != 0 {
		w.tb.Fatalf("expected the separator, got a %d-byte message: %q", len(data), data)
	}
}

// expectEndStream reads the end-stream message and returns it parsed.
func (w *wireConn) expectEndStream() map[string]json.RawMessage {
	w.tb.Helper()
	msgType, data := w.read()
	if msgType != websocket.MessageText {
		w.tb.Fatalf("end-stream message arrived as %v, want text", msgType)
	}
	var endStream map[string]json.RawMessage
	if err := json.Unmarshal(data, &endStream); err != nil {
		w.tb.Fatalf("unmarshal end-stream %q: %v", data, err)
	}
	return endStream
}

func protoHeaders() http.Header {
	return http.Header{
		"Content-Type":             []string{"application/connect+proto"},
		"Connect-Protocol-Version": []string{"1"},
	}
}

// TestWireSequence walks one bidi RPC message by message.
func TestWireSequence(t *testing.T) {
	t.Parallel()
	_, _, server := newTestServer(t)
	wire := dialWire(t, server.URL, "/connectbidi.ping.v1.PingService/CumSum")

	wire.writeHeaders(protoHeaders())
	wire.writeMessage(&pingv1.CumSumRequest{Number: 3})

	header := wire.expectHeaders()
	if got, want := header.Get("Content-Type"), "application/connect+proto"; got != want {
		t.Errorf("response content-type = %q, want %q", got, want)
	}
	if got := header.Get("X-Test-Header"); got != "header-val" {
		t.Errorf("response header = %q, want header-val", got)
	}

	var resp pingv1.CumSumResponse
	wire.expectMessage(&resp)
	if resp.GetSum() != 3 {
		t.Errorf("sum = %d, want 3", resp.GetSum())
	}

	// Half-close, then the server finishes: separator, then end-stream.
	wire.writeSeparator()
	wire.expectSeparator()

	endStream := wire.expectEndStream()
	if _, hasError := endStream["error"]; hasError {
		t.Errorf("end-stream carried an error: %s", endStream["error"])
	}
	metadata, ok := endStream["metadata"]
	if !ok {
		t.Fatal("end-stream carried no metadata")
	}
	if !strings.Contains(string(metadata), "trailer-val") {
		t.Errorf("end-stream metadata = %s, want the response trailer", metadata)
	}
}

// TestWireEmptyMessageIsNotSeparator sends zero-length binary messages,
// which are legitimate empty protobuf messages, and checks they are not
// read as half-closes.
func TestWireEmptyMessageIsNotSeparator(t *testing.T) {
	t.Parallel()
	_, _, server := newTestServer(t)
	wire := dialWire(t, server.URL, "/connectbidi.ping.v1.PingService/Sum")

	wire.writeHeaders(protoHeaders())
	for range 3 {
		wire.writeMessage(&pingv1.SumRequest{})
	}
	wire.writeSeparator()

	wire.expectHeaders()
	var resp pingv1.SumResponse
	wire.expectMessage(&resp)
	if got, want := resp.GetSum(), int64(3); got != want {
		t.Errorf("server counted %d request messages, want %d", got, want)
	}
	wire.expectSeparator()
	wire.expectEndStream()
}

// TestWireErrorsArriveOnTheSocket checks that RPC-level failures are
// reported in the end-stream message rather than by refusing the handshake:
// a browser cannot read the status of a failed WebSocket handshake.
func TestWireErrorsArriveOnTheSocket(t *testing.T) {
	t.Parallel()
	_, _, server := newTestServer(t)

	tests := []struct {
		name   string
		header http.Header
		want   string
	}{
		{
			name:   "MissingContentType",
			header: http.Header{"Connect-Protocol-Version": []string{"1"}},
			want:   "invalid_argument",
		},
		{
			name:   "UnknownContentType",
			header: http.Header{"Content-Type": []string{"application/octet-stream"}},
			want:   "invalid_argument",
		},
		{
			name:   "UnknownCodec",
			header: http.Header{"Content-Type": []string{"application/connect+yaml"}},
			want:   "invalid_argument",
		},
		{
			name: "InvalidTimeout",
			header: http.Header{
				"Content-Type":       []string{"application/connect+proto"},
				"Connect-Timeout-Ms": []string{"soon"},
			},
			want: "invalid_argument",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			wire := dialWire(t, server.URL, "/connectbidi.ping.v1.PingService/CumSum")
			wire.writeHeaders(test.header)

			// The upgrade succeeded, so the failure has to arrive as a
			// well-formed response: headers, separator, end-stream.
			wire.expectHeaders()
			wire.expectSeparator()
			endStream := wire.expectEndStream()
			rpcError, ok := endStream["error"]
			if !ok {
				t.Fatalf("end-stream carried no error: %v", endStream)
			}
			if !strings.Contains(string(rpcError), test.want) {
				t.Errorf("error = %s, want code %q", rpcError, test.want)
			}
		})
	}
}

// TestWireRejectsMisorderedMessages checks the two ways a client can get
// the sequence wrong.
func TestWireRejectsMisorderedMessages(t *testing.T) {
	t.Parallel()
	_, _, server := newTestServer(t)

	t.Run("DataBeforeHeaders", func(t *testing.T) {
		t.Parallel()
		wire := dialWire(t, server.URL, "/connectbidi.ping.v1.PingService/CumSum")
		wire.writeMessage(&pingv1.CumSumRequest{Number: 1})

		wire.expectHeaders()
		wire.expectSeparator()
		endStream := wire.expectEndStream()
		if _, ok := endStream["error"]; !ok {
			t.Errorf("a data message before the headers was accepted: %v", endStream)
		}
	})

	t.Run("DataAfterSeparator", func(t *testing.T) {
		t.Parallel()
		// The violation has to reach the server while the RPC is still
		// running, or there is nothing left to fail: a stray message that
		// arrives after the handler has already returned cannot retract a
		// response that is on its way out. CountUp is made to block here, so
		// the server's read pump always sees the extra message in flight.
		impl, _, blocking := newTestServer(t)
		impl.countUpBlocks = true
		wire := dialWire(t, blocking.URL, "/connectbidi.ping.v1.PingService/CountUp")
		wire.writeHeaders(protoHeaders())
		wire.writeMessage(&pingv1.CountUpRequest{Number: 1})
		wire.writeSeparator()

		// Wait for the handler's first response message before violating
		// the protocol: by then it is blocked with the stream open, so the
		// read pump is certain to see the stray message in flight.
		wire.expectHeaders()
		var resp pingv1.CountUpResponse
		wire.expectMessage(&resp)

		wire.writeMessage(&pingv1.CountUpRequest{Number: 2})
		wire.expectSeparator()
		endStream := wire.expectEndStream()
		if _, ok := endStream["error"]; !ok {
			t.Errorf("a data message after the separator was accepted: %v", endStream)
		}
	})
}

// TestWireJSONCodec checks that the JSON codec is selected by content type,
// and that its data messages travel as text: JSON is never empty, so it can
// never be confused with the separator, and sending it as text is what
// makes the whole conversation readable in a browser's Network tab.
func TestWireJSONCodec(t *testing.T) {
	t.Parallel()
	_, _, server := newTestServer(t)
	wire := dialWire(t, server.URL, "/connectbidi.ping.v1.PingService/CumSum")

	wire.writeHeaders(http.Header{
		"Content-Type":             []string{"application/connect+json"},
		"Connect-Protocol-Version": []string{"1"},
	})
	wire.write(websocket.MessageText, []byte(`{"number":"5"}`))

	header := wire.expectHeaders()
	if got, want := header.Get("Content-Type"), "application/connect+json"; got != want {
		t.Errorf("response content-type = %q, want %q", got, want)
	}
	msgType, data := wire.read()
	if msgType != websocket.MessageText {
		t.Errorf("JSON data message arrived as %v, want text", msgType)
	}
	if !strings.Contains(string(data), "5") {
		t.Errorf("response = %q, want a sum of 5", data)
	}
}

// TestWireEmptyResponseIsBinary pins the invariant the whole opcode rule
// exists to protect: a response message that encodes to zero bytes must go
// out as binary, because an empty *text* message is the separator. A
// cumulative sum of zero is exactly that message.
func TestWireEmptyResponseIsBinary(t *testing.T) {
	t.Parallel()
	_, _, server := newTestServer(t)
	wire := dialWire(t, server.URL, "/connectbidi.ping.v1.PingService/CumSum")

	wire.writeHeaders(protoHeaders())
	wire.writeMessage(&pingv1.CumSumRequest{Number: 0})

	wire.expectHeaders()
	msgType, data := wire.read()
	if len(data) != 0 {
		t.Fatalf("expected a zero-byte response message, got %d bytes", len(data))
	}
	if msgType != websocket.MessageBinary {
		t.Errorf("empty response message arrived as %v, want binary — an empty text message is the separator", msgType)
	}

	wire.writeSeparator()
	wire.expectSeparator()
	wire.expectEndStream()
}
