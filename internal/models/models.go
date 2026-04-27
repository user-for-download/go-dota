package models

import "encoding/json"

type FetchTask struct {
	MatchID string `json:"match_id"`
	URL     string `json:"url"`
}

type APIResponse struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}
