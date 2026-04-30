package models

type FetchStreamTask struct {
	MatchID string `json:"match_id"`
	URL     string `json:"url"`
}

type ParseStreamTask struct {
	TaskID  string `json:"task_id"`
	MatchID string `json:"match_id,omitempty"`
}

type StreamMessage struct {
	ID          string
	Task        interface{}
	RetryCount  int
}