// Package app defines Temporal activities and workflows for orchestrating load test execution.
// Activities are the individual work units that get executed as part of a workflow.
package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"backend/internal/db"
	"backend/internal/model"
	"backend/internal/util"

	"go.temporal.io/sdk/activity"
)

// Regular expressions for parsing K6 test output and extracting metrics.
var (
	summaryMetricLine   = regexp.MustCompile(`^(?:[✓✗]\s*)?([a-zA-Z_][a-zA-Z0-9_]*)\.{2,}:\s*(.+)$`)
	thresholdHeaderLine = regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)\s*$`)
	thresholdRuleLine   = regexp.MustCompile(`^[✓✗]\s*'([^']+)'\s+(.+)$`)
)

// ActivityCreateRun creates a new test run record in the database.
func ActivityCreateRun(ctx context.Context, runID string, vus int, script string) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Creating run record", "runID", runID, "vus", vus)

	if err := db.CreateRun(ctx, runID, vus, script); err != nil {
		logger.Error("Failed to create run record", "runID", runID, "error", err)
		return err
	}

	logger.Info("Run record created", "runID", runID)
	return nil
}

// ActivityCreateLogFile creates the log file for capturing test output.
func ActivityCreateLogFile(ctx context.Context, runID string) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Creating log file for run", "runID", runID)

	fileName := util.SanitizeRunID(runID)
	if fileName == "" {
		fileName = "run_unknown"
	}

	if err := os.MkdirAll(util.ResultsDir(), 0755); err != nil {
		logger.Error("Failed to create results directory", "error", err)
		return err
	}

	filePath := filepath.Join(util.ResultsDir(), fmt.Sprintf("%s.txt", fileName))
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		logger.Error("Failed to create log file", "filePath", filePath, "error", err)
		return err
	}
	defer file.Close()

	logger.Info("Log file created successfully", "filePath", filePath)
	return nil
}

// ActivityCallRunner resolves the runner service URL from configuration or environment.
func ActivityCallRunner(ctx context.Context, req model.RunRequest) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Resolving runner URL", "runID", req.RunID, "vus", req.VUs)

	runnerURL := req.RunnerURL
	if runnerURL == "" {
		runnerURL = os.Getenv("RUNNER_URL")
		if runnerURL == "" {
			runnerURL = "http://localhost:8080/run-test"
		}
	}

	logger.Info("Runner URL resolved", "url", runnerURL)
	return runnerURL, nil
}

// ActivityProcessStream calls the runner service, captures streaming output, and writes it to a log file.
// It also polls Prometheus for metrics in parallel.
func ActivityProcessStream(ctx context.Context, req model.RunRequest, runnerURL string) ([]model.StreamChunk, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Processing stream from runner", "runID", req.RunID)

	metricsStop := make(chan struct{})
	defer close(metricsStop)

	fileName := util.SanitizeRunID(req.RunID)
	if fileName == "" {
		fileName = "run_unknown"
	}

	if err := os.MkdirAll(util.ResultsDir(), 0755); err != nil {
		logger.Error("Failed to ensure results directory", "error", err)
		return nil, err
	}

	filePath := filepath.Join(util.ResultsDir(), fmt.Sprintf("%s.txt", fileName))
	logFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		logger.Error("Failed to open log file for streaming writes", "filePath", filePath, "error", err)
		return nil, err
	}
	defer logFile.Close()

	runnerPayload := struct {
		RunID  string `json:"run_id"`
		VUs    int    `json:"vus"`
		Script string `json:"script"`
	}{
		RunID:  req.RunID,
		VUs:    req.VUs,
		Script: req.Script,
	}

	payload, err := json.Marshal(runnerPayload)
	if err != nil {
		logger.Error("Failed to marshal payload", "error", err)
		return nil, err
	}

	runnerReq, err := http.NewRequestWithContext(ctx, http.MethodPost, runnerURL, bytes.NewReader(payload))
	if err != nil {
		logger.Error("Failed to create request", "error", err)
		return nil, err
	}
	runnerReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Minute}
	resp, err := client.Do(runnerReq)
	if err != nil {
		logger.Error("Failed to call runner", "error", err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		errMsg := fmt.Sprintf("runner returned status %d", resp.StatusCode)
		logger.Error(errMsg, "body", string(body))
		return nil, fmt.Errorf("%s", errMsg)
	}

	go pollPrometheusMetrics(ctx, req.RunID, metricsStop)

	var chunks []model.StreamChunk
	reader := bufio.NewReader(resp.Body)

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stream processing cancelled")
			return chunks, ctx.Err()
		default:
		}

		line, err := reader.ReadString('\n')
		if line != "" {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "data:") {
				data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
				if data != "" {
					stream := "stdout"
					if strings.Contains(data, "[STDERR]") || strings.Contains(data, "TEST FAILED") {
						stream = "stderr"
					}

					if _, writeErr := logFile.WriteString(fmt.Sprintf("[%s] [%s] %s\n", req.RunID, stream, data)); writeErr != nil {
						logger.Error("Failed to append stream chunk to log file", "filePath", filePath, "error", writeErr)
						return chunks, writeErr
					}
					if syncErr := logFile.Sync(); syncErr != nil {
						logger.Error("Failed to sync log file", "filePath", filePath, "error", syncErr)
						return chunks, syncErr
					}

					chunk := model.StreamChunk{
						RunID:   req.RunID,
						Message: data,
						Stream:  stream,
					}
					chunks = append(chunks, chunk)
				}
			}
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			logger.Error("Error reading stream", "error", err)
			break
		}
	}

	logger.Info("Stream processing completed", "chunksReceived", len(chunks))
	return chunks, nil
}

// pollPrometheusMetrics periodically queries Prometheus for metrics and saves them to the database.
func pollPrometheusMetrics(ctx context.Context, runID string, stop <-chan struct{}) {
	logger := activity.GetLogger(ctx)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			metrics, err := db.FetchPrometheusMetrics(ctx, runID)
			if err != nil {
				logger.Warn("Failed to fetch Prometheus metrics", "runID", runID, "error", err)
				continue
			}

			if len(metrics) > 0 {
				if err := db.SaveMetricsBatch(ctx, metrics); err != nil {
					logger.Warn("Failed to save batch of Prometheus metrics", "count", len(metrics), "error", err)
				}
			}
		}
	}
}

// ActivityWriteToLogFile is a placeholder activity as stream chunks are already written in real-time.
func ActivityWriteToLogFile(ctx context.Context, runID string, chunks []model.StreamChunk) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Writing chunks to log file", "runID", runID, "chunkCount", len(chunks))
	logger.Info("Skipping duplicate batch write because stream is already appended in real time", "runID", runID)
	return nil
}

// ActivityExtractMetrics parses the log file and extracts metrics from K6 test output.
func ActivityExtractMetrics(ctx context.Context, runID string) ([]model.Metric, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Extracting metrics from log file", "runID", runID)

	fileName := util.SanitizeRunID(runID)
	if fileName == "" {
		fileName = "run_unknown"
	}

	filePath := filepath.Join(util.ResultsDir(), fmt.Sprintf("%s.txt", fileName))
	content, err := os.ReadFile(filePath)
	if err != nil {
		logger.Error("Failed to read log file", "filePath", filePath, "error", err)
		return nil, err
	}

	lines := strings.Split(string(content), "\n")
	var metrics []model.Metric
	currentThresholdHeader := ""

	for _, logLine := range lines {
		if strings.TrimSpace(logLine) == "" {
			continue
		}

		parts := strings.SplitN(logLine, "] ", 3)
		if len(parts) < 3 {
			continue
		}

		message := parts[2]
		stream := "stdout"
		if strings.Contains(logLine, "[stderr]") {
			stream = "stderr"
		}

		clean := strings.TrimSpace(message)
		clean = strings.TrimPrefix(clean, "[STDOUT] ")
		clean = strings.TrimPrefix(clean, "[STDERR] ")
		clean = strings.TrimSpace(clean)
		if clean == "" {
			continue
		}

		if strings.HasPrefix(clean, "{") {
			if metric, ok := parseMetricMessageFromLine(clean, runID, stream); ok {
				metrics = append(metrics, metric)
				continue
			}
		}

		if m := summaryMetricLine.FindStringSubmatch(clean); len(m) == 3 {
			metrics = append(metrics, model.Metric{
				RunID:     runID,
				Name:      strings.TrimSpace(m[1]),
				Value:     strings.TrimSpace(m[2]),
				Stream:    stream,
				Raw:       clean,
				CreatedAt: time.Now(),
			})
			continue
		}

		if m := thresholdHeaderLine.FindStringSubmatch(clean); len(m) == 2 {
			header := strings.TrimSpace(m[1])
			switch header {
			case "THRESHOLDS", "HTTP", "EXECUTION", "NETWORK", "TOTAL", "RESULTS":
				// ignore section headers
			default:
				currentThresholdHeader = header
			}
			continue
		}

		if m := thresholdRuleLine.FindStringSubmatch(clean); len(m) == 3 {
			name := "threshold"
			if currentThresholdHeader != "" {
				name = "threshold_" + currentThresholdHeader + "_" + strings.TrimSpace(m[1])
			}

			metrics = append(metrics, model.Metric{
				RunID:     runID,
				Name:      name,
				Value:     strings.TrimSpace(m[2]),
				Stream:    stream,
				Raw:       clean,
				CreatedAt: time.Now(),
			})
		}
	}

	logger.Info("Extracted metrics", "count", len(metrics), "runID", runID)
	return metrics, nil
}

// parseMetricMessageFromLine attempts to parse a JSON metric message from a log line.
func parseMetricMessageFromLine(message string, runID string, stream string) (model.Metric, bool) {
	raw := strings.TrimSpace(message)
	if raw == "" {
		return model.Metric{}, false
	}

	clean := strings.TrimPrefix(raw, "[STDOUT] ")
	clean = strings.TrimPrefix(clean, "[STDERR] ")
	clean = strings.TrimSpace(clean)
	if clean == "" {
		return model.Metric{}, false
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
		if typ, _ := parsed["type"].(string); typ == "metric" {
			name, _ := parsed["name"].(string)
			value := ""
			if v, ok := parsed["value"]; ok {
				value = toString(v)
			}
			return model.Metric{
				RunID:     runID,
				Name:      name,
				Value:     value,
				Stream:    stream,
				Raw:       raw,
				CreatedAt: time.Now(),
			}, true
		}
	}

	return model.Metric{}, false
}

// ActivitySaveMetricsToDb saves extracted metrics to the database as both individual records and a summary.
func ActivitySaveMetricsToDb(ctx context.Context, metrics []model.Metric) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Saving metrics to database", "count", len(metrics))

	summaryPayload := make(map[string]interface{})
	for _, metric := range metrics {
		summaryPayload[metric.Name] = metric.Value
	}

	if len(summaryPayload) > 0 {
		runID := metrics[0].RunID
		if err := db.SaveSummaryMetric(ctx, runID, summaryPayload); err != nil {
			logger.Warn("Failed to save summary metrics payload", "error", err)
		}
	}

	logger.Info("All metrics saved to database")
	return nil
}

// ActivitySaveRunLogFile moves the log file from disk to the database for long-term storage.
func ActivitySaveRunLogFile(ctx context.Context, runID string) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Saving run log file to database", "runID", runID)

	if err := db.SaveRunLogFile(ctx, runID); err != nil {
		logger.Error("Failed to save run log file", "runID", runID, "error", err)
		return err
	}

	logger.Info("Log file saved to database and removed from local disk", "runID", runID)
	return nil
}

// ActivityCleanupLogFile removes the log file from disk (used on workflow failure to prevent orphan files).
func ActivityCleanupLogFile(ctx context.Context, runID string) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Cleaning up log file on workflow failure", "runID", runID)

	fileName := util.SanitizeRunID(runID)
	if fileName == "" {
		fileName = "run_unknown"
	}

	filePath := filepath.Join(util.ResultsDir(), fmt.Sprintf("%s.txt", fileName))
	if err := os.Remove(filePath); err != nil {
		if os.IsNotExist(err) {
			logger.Info("Log file already missing", "filePath", filePath)
			return nil
		}
		logger.Warn("Failed to cleanup log file", "filePath", filePath, "error", err)
		return nil // Don't fail the workflow on cleanup errors
	}

	logger.Info("Log file cleaned up successfully", "filePath", filePath)
	return nil
}

// ActivityUpdateRunStatus updates the test run status in the database.
func ActivityUpdateRunStatus(ctx context.Context, runID string, status string) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Updating run status", "runID", runID, "status", status)

	if err := db.UpdateRunStatus(ctx, runID, status); err != nil {
		logger.Error("Failed to update run status", "runID", runID, "status", status, "error", err)
		return err
	}

	logger.Info("Run status updated", "runID", runID, "status", status)
	return nil
}

// toString converts any value to its string representation.
func toString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}
