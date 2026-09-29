package nrpc

import (
	"encoding/json"
	"fmt"
)

// Codec serializes and deserializes values for the wire protocol and payloads.
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
}

// JSONCodec is the default JSON implementation.
type JSONCodec struct{}

// NewJSONCodec returns a JSON codec.
func NewJSONCodec() Codec {
	return JSONCodec{}
}

type wireRequest struct {
	ID      string            `json:"id"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
}

type wireResponse struct {
	ID      string            `json:"id"`
	Status  Status            `json:"status"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
	Error   *Error            `json:"error,omitempty"`
}

func (JSONCodec) Encode(v any) ([]byte, error) {
	switch x := v.(type) {
	case Request:
		return json.Marshal(wireRequest{
			ID:      x.ID,
			Headers: x.Headers,
			Body:    rawOrNil(x.Body),
		})
	case *Request:
		if x == nil {
			return json.Marshal(wireRequest{})
		}
		return json.Marshal(wireRequest{
			ID:      x.ID,
			Headers: x.Headers,
			Body:    rawOrNil(x.Body),
		})
	case Response:
		return json.Marshal(wireResponse{
			ID:      x.ID,
			Status:  x.Status,
			Headers: x.Headers,
			Body:    rawOrNil(x.Body),
			Error:   x.Error,
		})
	case *Response:
		if x == nil {
			return json.Marshal(wireResponse{})
		}
		return json.Marshal(wireResponse{
			ID:      x.ID,
			Status:  x.Status,
			Headers: x.Headers,
			Body:    rawOrNil(x.Body),
			Error:   x.Error,
		})
	default:
		return json.Marshal(v)
	}
}

func (JSONCodec) Decode(data []byte, v any) error {
	switch x := v.(type) {
	case *Request:
		var w wireRequest
		if err := json.Unmarshal(data, &w); err != nil {
			return err
		}
		x.ID = w.ID
		x.Headers = w.Headers
		x.Body = []byte(w.Body)
		return nil
	case *Response:
		var w wireResponse
		if err := json.Unmarshal(data, &w); err != nil {
			return err
		}
		x.ID = w.ID
		x.Status = w.Status
		x.Headers = w.Headers
		x.Body = []byte(w.Body)
		x.Error = w.Error
		if x.Error != nil && x.Error.Status == StatusOK && x.Status != StatusOK {
			x.Error.Status = x.Status
		}
		return nil
	default:
		if err := json.Unmarshal(data, v); err != nil {
			return fmt.Errorf("json decode: %w", err)
		}
		return nil
	}
}

func rawOrNil(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	return json.RawMessage(b)
}
