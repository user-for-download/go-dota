package worker

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/user-for-download/go-dota/internal/models"
)

func TestExtractIDFromPayload(t *testing.T) {
	tests := []struct {
		name      string
		payload   string
		wantID    string
		wantEmpty bool
	}{
		{
			name:      "top-level id as string",
			payload:   `{"id":"match-123","payload":{}}`,
			wantID:    "match-123",
			wantEmpty: false,
		},
		{
			name:      "top-level match_id as number",
			payload:   `{"match_id":789012,"payload":{}}`,
			wantID:    "789012",
			wantEmpty: false,
		},
		{
			name:      "no id fields",
			payload:   `{"foo":"bar","baz":123}`,
			wantID:    "",
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := []byte(tt.payload)
			var apiResp models.APIResponse
			if err := json.Unmarshal(data, &apiResp); err != nil {
				t.Fatalf("unmarshal payload failed: %v", err)
			}

			if apiResp.ID == "" {
				var raw map[string]json.RawMessage
				if err := json.Unmarshal(data, &raw); err == nil {
					if idBytes, ok := raw["id"]; ok {
						var idStr string
						if json.Unmarshal(idBytes, &idStr) == nil && idStr != "" {
							apiResp.ID = idStr
						}
					}
					if apiResp.ID == "" {
						if midBytes, ok := raw["match_id"]; ok {
							var mid float64
							if json.Unmarshal(midBytes, &mid) == nil {
								apiResp.ID = strconv.FormatInt(int64(mid), 10)
							}
						}
					}
				}
			}

			if tt.wantEmpty {
				if apiResp.ID != "" {
					t.Errorf("expected empty ID, got %q", apiResp.ID)
				}
			} else {
				if apiResp.ID != tt.wantID {
					t.Errorf("ID = %q, want %q", apiResp.ID, tt.wantID)
				}
			}
		})
	}
}

func TestAPIResponsePayload(t *testing.T) {
	payload := json.RawMessage(`{"key":"value"}`)
	resp := models.APIResponse{
		ID:      "test-123",
		Payload: payload,
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal error = %v", err)
	}

	var decoded models.APIResponse
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

func TestFetchTaskFields(t *testing.T) {
	task := models.FetchTask{MatchID: "12345", URL: "https://example.com/data"}
	if task.MatchID != "12345" {
		t.Errorf("MatchID = %q, want %q", task.MatchID, "12345")
	}
	if task.URL != "https://example.com/data" {
		t.Errorf("URL = %q, want %q", task.URL, "https://example.com/data")
	}
}

func TestEmptyPayloadParsing(t *testing.T) {
	payload := []byte(`{}`)
	var apiResp models.APIResponse
	if err := json.Unmarshal(payload, &apiResp); err != nil {
		t.Fatalf("unmarshal empty object failed: %v", err)
	}
	if apiResp.ID != "" {
		t.Errorf("expected empty ID for empty payload, got %q", apiResp.ID)
	}
}

func TestMalformedJSON(t *testing.T) {
	payload := []byte(`{invalid json}`)
	var apiResp models.APIResponse
	err := json.Unmarshal(payload, &apiResp)
	if err == nil {
		t.Error("expected error for malformed JSON, got nil")
	}
}
