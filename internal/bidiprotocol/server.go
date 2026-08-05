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

package bidiprotocol

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

// HandleRPC serves a single RPC carried by conn: it reads the request headers
// envelope, dispatches the procedure on server, and finishes the stream with
// an end-stream envelope. The protocol string is surfaced through
// connect.CallInfo (for example "websocket" or "webtransport").
func HandleRPC(
	ctx context.Context,
	conn Conn,
	server *connect.Server,
	opts Options,
	protocol string,
	remoteAddr string,
) {
	flag, payload, err := conn.ReadEnvelope()
	if err != nil {
		slog.DebugContext(ctx, "bidi server: failed to read initial envelope", "error", err)
		return
	}
	if flag != FlagEnvelopeHeaders {
		slog.DebugContext(ctx, "bidi server: unexpected initial envelope flag", "flag", flag)
		return
	}

	headers, err := UnmarshalHeaders(payload)
	if err != nil {
		slog.DebugContext(ctx, "bidi server: failed to unmarshal request headers", "error", err)
		return
	}

	procedure := ""
	if vs := headers[":path"]; len(vs) > 0 {
		procedure = vs[0]
	}
	if procedure == "" {
		slog.DebugContext(ctx, "bidi server: missing :path header")
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
		Protocol: protocol,
		Codec:    codec.Name(),
	}
	for k, vs := range headers {
		if !strings.HasPrefix(k, ":") {
			callInfo.RequestHeader().SetValues(k, vs)
		}
	}

	stream := &ServerStream{
		conn:     conn,
		callInfo: callInfo,
		opts:     opts,
		codec:    codec,
		ctx:      ctx,
	}

	callErr := server.Call(ctx, procedure, callInfo, stream)

	// The headers envelope must precede the end-stream envelope, even if the
	// handler never sent a message.
	_ = stream.SendHeaders()

	var endErr *connect.Error
	if callErr != nil {
		endErr = ErrorForWire(callErr)
	}
	trailers := make(http.Header)
	maps.Insert(trailers, callInfo.ResponseTrailer().All())
	endPayload, marshalErr := MarshalEndStream(endErr, trailers)
	if marshalErr != nil {
		endPayload = nil
	}
	_ = conn.WriteEnvelope(FlagEnvelopeEndStream, endPayload)
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

// ServerStream implements connect.Stream for a single server-side RPC.
type ServerStream struct {
	conn     Conn
	callInfo *connect.CallInfo
	opts     Options
	codec    connect.Codec
	ctx      context.Context

	sendHeadersOnce sync.Once
	sendHeadersErr  error

	responseCompressor connect.Compressor
}

// Receive reads the next request message.
func (ss *ServerStream) Receive(msg any) error {
	flag, payload, err := ss.conn.ReadEnvelope()
	if err != nil {
		// A transport-level half-close (io.EOF from a stream FIN) ends the
		// request stream just like an explicit end-stream envelope.
		return err
	}

	if flag == FlagEnvelopeHeaders {
		return connect.Errorf(connect.CodeInternal, "protocol error: unexpected headers envelope in request body")
	}

	if flag == FlagEnvelopeEndStream {
		return io.EOF
	}

	if flag != FlagEnvelopeData && flag != FlagEnvelopeCompressed {
		return connect.Errorf(connect.CodeInternal, "protocol error: unknown frame flag: 0x%x", flag)
	}

	if ss.opts.ReadMaxBytes > 0 && len(payload) > ss.opts.ReadMaxBytes {
		return connect.Errorf(connect.CodeResourceExhausted, "message size %d exceeds read limit %d", len(payload), ss.opts.ReadMaxBytes)
	}

	if flag == FlagEnvelopeCompressed {
		encoding := ss.callInfo.RequestHeader().Get("Connect-Content-Encoding")
		compressor := ss.opts.Compressors[encoding]
		if compressor == nil {
			return connect.Errorf(connect.CodeInternal, "protocol error: compressed message without Connect-Content-Encoding")
		}
		decompressed, err := decompress(compressor, payload)
		if err != nil {
			return err
		}
		payload = decompressed
	}

	if err := ss.codec.UnmarshalRead(ss.ctx, bytes.NewReader(payload), msg); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to unmarshal request: %v", err)
	}
	return nil
}

// SendHeaders writes the response headers envelope exactly once.
func (ss *ServerStream) SendHeaders() error {
	ss.sendHeadersOnce.Do(func() {
		ss.sendHeadersErr = ss.sendHeaders()
	})
	return ss.sendHeadersErr
}

func (ss *ServerStream) sendHeaders() error {
	header := make(http.Header)
	if ss.callInfo.ResponseHeader() != nil {
		maps.Insert(header, ss.callInfo.ResponseHeader().All())
	}
	header.Set("Content-Type", "application/connect+"+ss.codec.Name())

	encoding, compressor := connectprotocol.NegotiateCompression(
		ss.callInfo.RequestHeader().Get("Connect-Accept-Encoding"),
		ss.opts.Compressors,
	)
	if compressor != nil {
		ss.responseCompressor = compressor
		header.Set("Connect-Content-Encoding", encoding)
	}
	if ss.opts.AcceptCompression != "" {
		header.Set("Connect-Accept-Encoding", ss.opts.AcceptCompression)
	}

	data, err := MarshalHeaders(header)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal headers: %v", err)
	}

	return ss.conn.WriteEnvelope(FlagEnvelopeHeaders, data)
}

// Send marshals and writes a response message.
func (ss *ServerStream) Send(msg any) error {
	if err := ss.SendHeaders(); err != nil {
		return err
	}

	var buf bytes.Buffer
	if err := ss.codec.MarshalWrite(ss.ctx, &buf, msg); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal response: %v", err)
	}
	payload := buf.Bytes()

	flag := FlagEnvelopeData
	if ss.responseCompressor != nil {
		compressed, err := compress(ss.responseCompressor, payload)
		if err != nil {
			return err
		}
		payload = compressed
		flag = FlagEnvelopeCompressed
	}

	if ss.opts.SendMaxBytes > 0 && len(payload) > ss.opts.SendMaxBytes {
		return connect.Errorf(connect.CodeResourceExhausted, "message size %d exceeds send limit %d", len(payload), ss.opts.SendMaxBytes)
	}

	return ss.conn.WriteEnvelope(flag, payload)
}
