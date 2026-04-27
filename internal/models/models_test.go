package models

import (
	"encoding/json"
	"testing"
)

func TestAPIResponseJSON(t *testing.T) {
	resp := APIResponse{
		ID:      "test-id-123",
		Payload: json.RawMessage(`{"key":"value"}`),
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal error = %v", err)
	}

	var decoded APIResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error = %v", err)
	}

	if decoded.ID != resp.ID {
		t.Errorf("ID = %q, want %q", decoded.ID, resp.ID)
	}
	if string(decoded.Payload) != string(resp.Payload) {
		t.Errorf("Payload = %s, want %s", decoded.Payload, resp.Payload)
	}
}

func TestAPIResponseEmptyPayload(t *testing.T) {
	resp := APIResponse{ID: "test-id"}
	if resp.Payload != nil {
		t.Errorf("Payload = %v, want nil", resp.Payload)
	}
}
