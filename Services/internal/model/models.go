// Package model defines data structures used throughout the load testing system.
package model

import "time"

// RunRequest represents a load test request from the client.
type RunRequest struct {
	RunID     string `json:"run_id,omitempty"`     // Optional unique identifier for the test run
	VUs       int    `json:"vus"`                  // Number of virtual users
	Script    string `json:"script"`               // K6 test script
	RunnerURL string `json:"runner_url,omitempty"` // Optional custom runner service URL
}

// StreamChunk represents a chunk of output from the test runner.
type StreamChunk struct {
	RunID   string // Identifier of the test run
	Message string // The actual output message
	Stream  string // Source stream (stdout or stderr)
}

// Run represents a test run record in the database.
type Run struct {
	RunID          string    `json:"run_id"`             // Unique identifier for the test run
	LogFileContent []byte    `json:"-"`                  // Binary content of the log file
	Status         string    `json:"status"`             // Current status (created, running, completed, failed)
	StartTime      time.Time `json:"start_time"`         // When the test started
	EndTime        time.Time `json:"end_time,omitempty"` // When the test ended
	CreatedAt      time.Time `json:"created_at"`         // Record creation timestamp
	UpdatedAt      time.Time `json:"updated_at"`         // Last update timestamp
}

// Metric represents a single performance metric collected from a test run.
type Metric struct {
	RunID     string    `json:"run_id"`     // Identifier of the test run
	Name      string    `json:"name"`       // Metric name
	Value     string    `json:"value"`      // Metric value as string
	Stream    string    `json:"stream"`     // Source of the metric (stdout, stderr, or prometheus)
	Raw       string    `json:"raw"`        // Raw unparsed metric data
	CreatedAt time.Time `json:"created_at"` // When the metric was recorded
}
