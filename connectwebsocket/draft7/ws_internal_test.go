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

package draft7

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/coder/websocket"
	"github.com/sudorandom/connect-bidi-web/internal/gen/connectbidi/ping/v1/pingv1connect"
)

// withoutCodec is a test-only option that unregisters a codec, for
// exercising a server that serves only some of the subprotocol tokens.
type withoutCodec string

func (o withoutCodec) apply(opts *options) { delete(opts.codecs, string(o)) }

// TestUnsupportedCodecIs415 checks the handshake status that separates
// "not speaking this protocol" from "speaking it with a codec this server
// lacks": a recognized token whose codec is unsupported is a 415, and the
// server skips past it to a later token it can serve.
func TestUnsupportedCodecIs415(t *testing.T) {
	t.Parallel()
	connectServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(connectServer, pingv1connect.UnimplementedPingServiceHandler{})
	mux := http.NewServeMux()
	Mount(mux, connectServer, withoutCodec(connect.CodecNameJSON))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, wsURL+"/connectbidi.ping.v1.PingService/CumSum", //nolint:bodyclose
		&websocket.DialOptions{Subprotocols: []string{subprotocolJSON, subprotocolBase}})
	if err == nil {
		_ = conn.CloseNow()
		t.Fatal("a JSON-only offer was accepted by a Protobuf-only server")
	}
	if resp == nil || resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %v, want 415", resp)
	}

	// Offering both lets the server skip to the one it serves.
	conn, _, err = websocket.Dial(ctx, wsURL+"/connectbidi.ping.v1.PingService/CumSum", //nolint:bodyclose
		&websocket.DialOptions{Subprotocols: []string{subprotocolJSON, subprotocolProto}})
	if err != nil {
		t.Fatalf("offering json then proto was refused: %v", err)
	}
	if got := conn.Subprotocol(); got != subprotocolProto {
		t.Errorf("selected %q, want %s", got, subprotocolProto)
	}
	_ = conn.CloseNow()
}

// TestSelectSubprotocol pins the selection rule on its own: first offered
// token that is both recognized and served, and whether anything was
// recognized at all.
func TestSelectSubprotocol(t *testing.T) {
	t.Parallel()
	all := defaultCodecs()
	protoOnly := map[string]connect.Codec{connect.CodecNameProto: all[connect.CodecNameProto]}

	tests := []struct {
		name           string
		offered        []string
		codecs         map[string]connect.Codec
		wantToken      string
		wantRecognized bool
	}{
		{"base means json", []string{"connectrpc.1"}, all, "connectrpc.1", true},
		{"first preference wins", []string{"connectrpc.1+json", "connectrpc.1+proto"}, all, "connectrpc.1+json", true},
		{"skips an unserved codec", []string{"connectrpc.1+json", "connectrpc.1+proto"}, protoOnly, "connectrpc.1+proto", true},
		{"recognized but unserved", []string{"connectrpc.1"}, protoOnly, "", true},
		{"nothing recognized", []string{"connect.bidi.d5", "graphql-ws"}, all, "", false},
		{"unknown tokens are skipped", []string{"graphql-ws", "connectrpc.1+proto"}, all, "connectrpc.1+proto", true},
		{"nothing offered", nil, all, "", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			token, recognized := selectSubprotocol(test.offered, test.codecs)
			if token != test.wantToken || recognized != test.wantRecognized {
				t.Errorf("selectSubprotocol(%v) = %q, %v; want %q, %v",
					test.offered, token, recognized, test.wantToken, test.wantRecognized)
			}
		})
	}
}
