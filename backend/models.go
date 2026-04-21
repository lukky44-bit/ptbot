package main

import "time"

type RunRequest struct {
	RunID     string `json:"run_id,omitempty"`
	VUs       int    `json:"vus"`
	Script    string `json:"script"`
	RunnerURL string `json:"runner_url,omitempty"`
}

type StreamMessage struct {
	RunID   string
	Message string
	Stream  string
}

type Run struct {
	RunID          string    `bson:"run_id" json:"run_id"`
	LogFileContent []byte    `bson:"log_file_content,omitempty" json:"-"`
	Status         string    `bson:"status" json:"status"`
	StartTime      time.Time `bson:"start_time" json:"start_time"`
	EndTime        time.Time `bson:"end_time,omitempty" json:"end_time,omitempty"`
	CreatedAt      time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt      time.Time `bson:"updated_at" json:"updated_at"`
}

type Metric struct {
	RunID     string    `bson:"run_id" json:"run_id"`
	Name      string    `bson:"name" json:"name"`
	Value     string    `bson:"value" json:"value"`
	Stream    string    `bson:"stream" json:"stream"`
	Raw       string    `bson:"raw" json:"raw"`
	CreatedAt time.Time `bson:"created_at" json:"created_at"`
}