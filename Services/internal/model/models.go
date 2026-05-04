package model

import "time"

type RunRequest struct {
	RunID     string `json:"run_id,omitempty"`
	VUs       int    `json:"vus"`
	Script    string `json:"script"`
	RunnerURL string `json:"runner_url,omitempty"`
}

type StreamChunk struct {
	RunID   string
	Message string
	Stream  string
}

type Run struct {
	RunID          string    `json:"run_id"`
	LogFileContent []byte    `json:"-"`
	Status         string    `json:"status"`
	StartTime      time.Time `json:"start_time"`
	EndTime        time.Time `json:"end_time,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type Metric struct {
	RunID     string    `json:"run_id"`
	Name      string    `json:"name"`
	Value     string    `json:"value"`
	Stream    string    `json:"stream"`
	Raw       string    `json:"raw"`
	CreatedAt time.Time `json:"created_at"`
}
