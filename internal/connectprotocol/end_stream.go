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

// Package connectprotocol contains wire types shared by Connect transports.
package connectprotocol

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
)

const (
	// FlagEnvelopeCompressed marks a compressed message envelope.
	FlagEnvelopeCompressed uint8 = 0x01
	// FlagEnvelopeEndStream marks a Connect EndStreamResponse envelope.
	FlagEnvelopeEndStream uint8 = 0x02
)

// WireDetail adapts a connect.ErrorDetail to the Connect protocol's JSON
// error-detail object.
type WireDetail connect.ErrorDetail

// MarshalJSON implements json.Marshaler using the Connect wire format.
func (d *WireDetail) MarshalJSON() ([]byte, error) {
	wire := struct {
		Type  string          `json:"type"`
		Value string          `json:"value"`
		Debug json.RawMessage `json:"debug,omitempty"`
	}{
		Type:  d.Type,
		Value: base64.RawStdEncoding.EncodeToString(d.Value),
	}
	if json.Valid(d.Debug) {
		wire.Debug = json.RawMessage(d.Debug)
	} else if msg, err := connectproto.ErrorDetailToAny((*connect.ErrorDetail)(d)).UnmarshalNew(); err == nil {
		var buffer bytes.Buffer
		var codec connectproto.JSONCodec
		if err := codec.MarshalWrite(context.Background(), &buffer, msg); err == nil {
			wire.Debug = buffer.Bytes()
		}
	}
	return json.Marshal(wire)
}

// UnmarshalJSON implements json.Unmarshaler using the Connect wire format.
func (d *WireDetail) UnmarshalJSON(data []byte) error {
	var wire struct {
		Type  string          `json:"type"`
		Value string          `json:"value"`
		Debug json.RawMessage `json:"debug,omitempty"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	value, err := connect.DecodeBinaryHeader(wire.Value)
	if err != nil {
		return fmt.Errorf("decode base64: %w", err)
	}
	*d = WireDetail{
		Type:  wire.Type,
		Value: value,
		Debug: wire.Debug,
	}
	return nil
}

// WireError is the Connect protocol's JSON error object.
type WireError struct {
	Code    connect.Code  `json:"code"`
	Message string        `json:"message,omitempty"`
	Details []*WireDetail `json:"details,omitempty"`
}

// NewWireError converts an error to its Connect wire representation.
func NewWireError(err error) *WireError {
	if err == nil {
		return nil
	}
	var connectErr *connect.Error
	if errors.As(err, &connectErr) && connectErr == nil {
		return nil
	}
	wire := &WireError{
		Code:    connect.CodeUnknown,
		Message: err.Error(),
	}
	if connectErr != nil {
		wire.Code = connectErr.Code()
		wire.Message = connectErr.Message()
		if details := connectErr.Details(); len(details) > 0 {
			wire.Details = make([]*WireDetail, len(details))
			for i, detail := range details {
				wire.Details[i] = (*WireDetail)(detail)
			}
		}
	}
	return wire
}

// AsError converts a wire error to a remote connect.Error.
func (e *WireError) AsError() *connect.Error {
	if e == nil {
		return nil
	}
	if e.Code < connect.CodeCanceled || e.Code > connect.CodeUnauthenticated {
		e.Code = connect.CodeUnknown
	}
	err := connect.NewError(e.Code, e.Message).WithRemote()
	for _, detail := range e.Details {
		err = err.WithDetail((*connect.ErrorDetail)(detail))
	}
	return err
}

// UnmarshalJSON implements json.Unmarshaler using the Connect wire format.
func (e *WireError) UnmarshalJSON(data []byte) error {
	var wire struct {
		Code    string        `json:"code"`
		Message string        `json:"message"`
		Details []*WireDetail `json:"details"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	e.Message = wire.Message
	e.Details = wire.Details
	// Leave Code unset if the peer sent an unrecognized value.
	_ = e.Code.UnmarshalText([]byte(wire.Code))
	return nil
}

// EndStreamMessage is the Connect protocol's JSON EndStreamResponse.
type EndStreamMessage struct {
	Error   *WireError  `json:"error,omitempty"`
	Trailer http.Header `json:"metadata,omitempty"`
}

// MarshalEndStream serializes a Connect EndStreamResponse.
func MarshalEndStream(err error, trailer http.Header) ([]byte, error) {
	end := EndStreamMessage{Trailer: trailer}
	if err != nil {
		end.Error = NewWireError(err)
	}
	return json.Marshal(&end)
}

// UnmarshalEndStream parses a Connect EndStreamResponse.
func UnmarshalEndStream(data []byte) (*connect.Error, http.Header, error) {
	var end EndStreamMessage
	if err := json.Unmarshal(data, &end); err != nil {
		return nil, nil, err
	}
	return end.Error.AsError(), end.Trailer, nil
}
