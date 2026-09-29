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

package draft3

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/sudorandom/connect-bidi-web/internal/connectprotocol"
)

// handleRPC serves a single RPC carried by conn: it reads the request
// headers frame, dispatches the procedure on server, and finishes the
// stream with an end-stream frame.
func handleRPC(
	ctx context.Context,
	conn frameConn,
	server *connect.Server,
	opts protocolOptions,
	remoteAddr string,
) {
	frameType, payload, err := conn.ReadFrame()
	if err != nil {
		slog.DebugContext(ctx, "websocket server: failed to read initial frame", "error", err)
		return
	}
	if frameType != frameTypeHeaders {
		slog.DebugContext(ctx, "websocket server: unexpected initial frame type", "type", frameType)
		return
	}

	headers, err := connectprotocol.UnmarshalHeaders(payload)
	if err != nil {
		slog.DebugContext(ctx, "websocket server: failed to unmarshal request headers", "error", err)
		return
	}

	procedure := ""
	if vs := headers[":path"]; len(vs) > 0 {
		procedure = vs[0]
	}
	if procedure == "" {
		slog.DebugContext(ctx, "websocket server: missing :path header")
		return
	}

	if timeoutStr := headers.Get("Connect-Timeout-Ms"); timeoutStr != "" {
		if ms, err := strconv.ParseInt(timeoutStr, 10, 64); err == nil && ms > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
			defer cancel()
		}
	}

	codec := negotiateCodec(headers.Get("Content-Type"), opts.Codecs)

	callInfo := &connect.CallInfo{
		PeerAddr: remoteAddr,
		Protocol: protocolName,
		Codec:    codec.Name(),
	}
	for k, vs := range headers {
		if !strings.HasPrefix(k, ":") {
			callInfo.RequestHeader().SetValues(k, vs)
		}
	}

	stream := &serverStream{
		conn:     conn,
		callInfo: callInfo,
		opts:     opts,
		codec:    codec,
		ctx:      ctx,
	}

	callErr := server.Call(ctx, procedure, callInfo, stream)

	// The headers frame must precede the end-stream frame, even if the
	// handler never sent a message.
	_ = stream.SendHeaders()

	var endErr error
	if callErr != nil {
		endErr = connectprotocol.ErrorForWire(callErr)
	}
	trailers := make(http.Header)
	maps.Insert(trailers, callInfo.ResponseTrailer().All())
	endPayload, marshalErr := connectprotocol.MarshalEndStream(endErr, trailers)
	if marshalErr != nil {
		endPayload = nil
	}
	_ = conn.WriteFrame(frameTypeEndStream, endPayload)
}

// negotiateCodec picks a codec from the request content type, defaulting to
// proto when the content type is missing or unknown.
func negotiateCodec(contentType string, codecs map[string]connect.Codec) connect.Codec {
	codecName := connect.CodecNameProto
	if strings.Contains(contentType, "json") {
		codecName = connect.CodecNameJSON
	}
	if codec, ok := codecs[codecName]; ok {
		return codec
	}
	for _, codec := range codecs {
		return codec
	}
	return nil
}

// serverStream implements connect.Stream for a single server-side RPC.
type serverStream struct {
	conn     frameConn
	callInfo *connect.CallInfo
	opts     protocolOptions
	codec    connect.Codec
	ctx      context.Context

	sendHeadersOnce sync.Once
	sendHeadersErr  error
}

// Receive reads the next request message.
func (ss *serverStream) Receive(msg any) error {
	frameType, payload, err := ss.conn.ReadFrame()
	if err != nil {
		return err
	}

	if frameType == frameTypeHeaders {
		return connect.Errorf(connect.CodeInternal, "protocol error: unexpected headers frame in request body")
	}

	if frameType == frameTypeEndStream {
		return io.EOF
	}

	if frameType != frameTypeData {
		return connect.Errorf(connect.CodeInternal, "protocol error: unknown frame type: 0x%x", frameType)
	}

	if ss.opts.ReadMaxBytes > 0 && len(payload) > ss.opts.ReadMaxBytes {
		return connect.Errorf(connect.CodeResourceExhausted, "message size %d exceeds read limit %d", len(payload), ss.opts.ReadMaxBytes)
	}

	if err := ss.codec.UnmarshalRead(ss.ctx, bytes.NewReader(payload), msg); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to unmarshal request: %v", err)
	}
	return nil
}

// SendHeaders writes the response headers frame exactly once.
func (ss *serverStream) SendHeaders() error {
	ss.sendHeadersOnce.Do(func() {
		ss.sendHeadersErr = ss.sendHeaders()
	})
	return ss.sendHeadersErr
}

func (ss *serverStream) sendHeaders() error {
	header := make(http.Header)
	if ss.callInfo.ResponseHeader() != nil {
		maps.Insert(header, ss.callInfo.ResponseHeader().All())
	}
	header.Set("Content-Type", "application/connect+"+ss.codec.Name())

	data, err := connectprotocol.MarshalHeaders(header)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal headers: %v", err)
	}

	return ss.conn.WriteFrame(frameTypeHeaders, data)
}

// Send marshals and writes a response message.
func (ss *serverStream) Send(msg any) error {
	if err := ss.SendHeaders(); err != nil {
		return err
	}

	var buf bytes.Buffer
	if err := ss.codec.MarshalWrite(ss.ctx, &buf, msg); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal response: %v", err)
	}
	payload := buf.Bytes()

	if ss.opts.SendMaxBytes > 0 && len(payload) > ss.opts.SendMaxBytes {
		return connect.Errorf(connect.CodeResourceExhausted, "message size %d exceeds send limit %d", len(payload), ss.opts.SendMaxBytes)
	}

	return ss.conn.WriteFrame(frameTypeData, payload)
}
