// Package db provides database connection pooling, schema management, and data persistence operations.
// It handles all interactions with the PostgreSQL/TimescaleDB database.
package db

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"backend/internal/model"
	"backend/internal/util"

	"github.com/jackc/pgx/v5/pgxpool"
)

// pool is the global database connection pool.
var pool *pgxpool.Pool

// InitDB initializes the database connection pool with connection string from environment variables.
// It retries the connection with exponential backoff and ensures the schema is initialized.
func InitDB(ctx context.Context) error {
	if pool != nil {
		return nil
	}

	dbHost := os.Getenv("DB_HOST")
	if dbHost == "" {
		dbHost = "localhost"
	}

	dbPort := os.Getenv("DB_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}

	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		dbName = "loadtest"
	}

	dbUser := os.Getenv("DB_USER")
	if dbUser == "" {
		dbUser = "postgres"
	}

	dbPassword := os.Getenv("DB_PASSWORD")
	if dbPassword == "" {
		dbPassword = "postgres"
	}

	connString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		dbUser, dbPassword, dbHost, dbPort, dbName)

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return fmt.Errorf("failed to parse connection string: %w", err)
	}

	config.MaxConns = 25
	config.MinConns = 5

	connPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}

	var pingErr error
	for attempt := 1; attempt <= 15; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		pingErr = connPool.Ping(pingCtx)
		cancel()
		if pingErr == nil {
			pool = connPool
			if err := ensureSchema(ctx); err != nil {
				connPool.Close()
				return fmt.Errorf("failed to ensure database schema: %w", err)
			}
			return nil
		}

		if attempt < 15 {
			time.Sleep(2 * time.Second)
		}
	}

	connPool.Close()
	return fmt.Errorf("failed to ping database after retries: %w", pingErr)
}

// ensureSchema creates all necessary database tables, indexes, and Timescale hypertables.
// It is idempotent and safe to call multiple times.
func ensureSchema(ctx context.Context) error {
	statements := []string{
		`CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE`,
		`CREATE TABLE IF NOT EXISTS test_runs (
			id TEXT PRIMARY KEY,
			vus INTEGER NOT NULL,
			script TEXT NOT NULL,
			status VARCHAR(50) NOT NULL DEFAULT 'created',
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			ended_at TIMESTAMP WITH TIME ZONE,
			started_at TIMESTAMP WITH TIME ZONE
		)`,
		`CREATE TABLE IF NOT EXISTS realtime_metrics (
			run_id TEXT NOT NULL REFERENCES test_runs(id) ON DELETE CASCADE,
			name VARCHAR(255) NOT NULL,
			value DOUBLE PRECISION NOT NULL,
			ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			stream VARCHAR(50) DEFAULT 'stdout',
			raw TEXT
		)`,
		`SELECT create_hypertable('realtime_metrics', 'ts', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE)`,
		`ALTER TABLE realtime_metrics SET (timescaledb.compress = true)`,
		`ALTER TABLE realtime_metrics SET (timescaledb.compress_segmentby = 'run_id,name')`,
		`CREATE TABLE IF NOT EXISTS test_summaries (
			id BIGSERIAL PRIMARY KEY,
			run_id TEXT NOT NULL UNIQUE REFERENCES test_runs(id) ON DELETE CASCADE,
			metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS test_logs (
			id BIGSERIAL PRIMARY KEY,
			run_id TEXT NOT NULL UNIQUE REFERENCES test_runs(id) ON DELETE CASCADE,
			content TEXT,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		)`,
	}

	for _, stmt := range statements {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}

	indexStatements := []string{
		`CREATE INDEX IF NOT EXISTS idx_test_runs_created_at ON test_runs(created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_test_runs_status ON test_runs(status)`,
		`CREATE INDEX IF NOT EXISTS idx_realtime_metrics_run_id ON realtime_metrics(run_id)`,
		`CREATE INDEX IF NOT EXISTS idx_realtime_metrics_run_id_ts ON realtime_metrics(run_id, ts DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_realtime_metrics_name ON realtime_metrics(name)`,
		`CREATE INDEX IF NOT EXISTS idx_test_summaries_run_id ON test_summaries(run_id)`,
		`CREATE INDEX IF NOT EXISTS idx_test_logs_run_id ON test_logs(run_id)`,
	}

	for _, stmt := range indexStatements {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the database connection pool.
func Close() {
	if pool != nil {
		pool.Close()
	}
}

// Ping checks if the database connection is healthy.
func Ping(ctx context.Context) error {
	if pool == nil {
		return fmt.Errorf("database not initialized")
	}
	return pool.Ping(ctx)
}

// SaveMetric saves a single metric to the realtime_metrics table.
func SaveMetric(ctx context.Context, metric model.Metric) error {
	query := `
		INSERT INTO realtime_metrics (run_id, name, value, ts, stream, raw)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	var floatValue float64
	if metric.Value != "" {
		parts := strings.Fields(metric.Value)
		if len(parts) > 0 {
			value := strings.TrimSuffix(parts[0], "ms")
			value = strings.TrimSuffix(value, "s")
			if val, err := strconv.ParseFloat(value, 64); err == nil {
				floatValue = val
			}
		}
	}

	_, err := pool.Exec(ctx,
		query,
		metric.RunID,
		metric.Name,
		floatValue,
		time.Now(),
		metric.Stream,
		metric.Raw,
	)
	if err != nil {
		return fmt.Errorf("save metric: %w", err)
	}
	return nil
}

// SaveMetricsBatch saves multiple metrics to the realtime_metrics table in a single batch INSERT.
func SaveMetricsBatch(ctx context.Context, metrics []model.Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	// Build the INSERT statement with multiple VALUES
	query := `INSERT INTO realtime_metrics (run_id, name, value, ts, stream, raw) VALUES `
	args := make([]interface{}, 0, len(metrics)*6)

	for i, metric := range metrics {
		if i > 0 {
			query += ", "
		}

		// Parse metric value
		var floatValue float64
		if metric.Value != "" {
			parts := strings.Fields(metric.Value)
			if len(parts) > 0 {
				value := strings.TrimSuffix(parts[0], "ms")
				value = strings.TrimSuffix(value, "s")
				if val, err := strconv.ParseFloat(value, 64); err == nil {
					floatValue = val
				}
			}
		}

		// Add to VALUES clause
		query += fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)", i*6+1, i*6+2, i*6+3, i*6+4, i*6+5, i*6+6)
		args = append(args, metric.RunID, metric.Name, floatValue, time.Now(), metric.Stream, metric.Raw)
	}

	_, err := pool.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("batch save metrics: %w", err)
	}
	return nil
}

// CreateRun creates a new test run record in the database.
func CreateRun(ctx context.Context, runID string, vus int, script string) error {
	query := `
		INSERT INTO test_runs (id, vus, script, status, created_at, updated_at, started_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO UPDATE
		SET status = EXCLUDED.status, updated_at = $6
	`

	now := time.Now()
	_, err := pool.Exec(ctx, query,
		runID,
		vus,
		script,
		"started",
		now,
		now,
		now,
	)
	if err != nil {
		return fmt.Errorf("create run: %w", err)
	}
	return nil
}

// SaveRunLogFile reads the log file from disk and saves it to the database, then deletes the local file.
func SaveRunLogFile(ctx context.Context, runID string) error {
	fileName := util.SanitizeRunID(runID)
	if fileName == "" {
		fileName = "run_unknown"
	}

	filePath := filepath.Join(util.ResultsDir(), fmt.Sprintf("%s.txt", fileName))
	content, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	query := `
		INSERT INTO test_logs (run_id, content, created_at, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (run_id) DO UPDATE
		SET content = EXCLUDED.content, updated_at = $4
	`

	now := time.Now()
	_, err = pool.Exec(ctx, query,
		runID,
		string(content),
		now,
		now,
	)
	if err != nil {
		return err
	}

	if err := os.Remove(filePath); err != nil {
		return err
	}

	return nil
}

// UpdateRunStatus updates the status of a test run and sets the ended_at timestamp.
func UpdateRunStatus(ctx context.Context, runID, status string) error {
	query := `
		UPDATE test_runs
		SET status = $1, updated_at = $2, ended_at = $3
		WHERE id = $4
	`

	now := time.Now()
	_, err := pool.Exec(ctx, query, status, now, now, runID)
	if err != nil {
		return fmt.Errorf("update run status: %w", err)
	}
	return nil
}

// SaveSummaryMetric saves a JSON summary of all metrics for a test run.
func SaveSummaryMetric(ctx context.Context, runID string, metrics map[string]interface{}) error {
	payload, err := json.Marshal(metrics)
	if err != nil {
		return err
	}

	query := `
		INSERT INTO test_summaries (run_id, metrics, created_at, updated_at)
		VALUES ($1, $2::jsonb, $3, $4)
		ON CONFLICT (run_id) DO UPDATE
		SET metrics = EXCLUDED.metrics, updated_at = EXCLUDED.updated_at
	`

	now := time.Now()
	_, err = pool.Exec(ctx, query, runID, string(payload), now, now)
	if err != nil {
		return fmt.Errorf("save summary metric: %w", err)
	}
	return nil
}

// FetchPrometheusMetrics queries Prometheus for metrics associated with a specific test run.
func FetchPrometheusMetrics(ctx context.Context, runID string) ([]model.Metric, error) {
	promURL := os.Getenv("PROMETHEUS_URL")
	if promURL == "" {
		promURL = "http://localhost:9090"
	}

	query := fmt.Sprintf(`{test_run_id="%s"}`, runID)
	endpoint := fmt.Sprintf("%s/api/v1/query?query=%s", promURL, url.QueryEscape(query))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("prometheus query failed with status %d", resp.StatusCode)
	}

	var payload struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	if payload.Status != "success" {
		return nil, fmt.Errorf("prometheus query returned status %s", payload.Status)
	}

	metrics := make([]model.Metric, 0, len(payload.Data.Result))
	for _, item := range payload.Data.Result {
		metricName := item.Metric["__name__"]
		if metricName == "" {
			metricName = "unknown_metric"
		}

		if len(item.Value) < 2 {
			continue
		}

		tsFloat, _ := item.Value[0].(float64)
		valueStr, _ := item.Value[1].(string)
		valueNum, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			continue
		}

		sec, frac := math.Modf(tsFloat)
		sampleTime := time.Unix(int64(sec), int64(frac*1e9))
		raw, _ := json.Marshal(item)

		metrics = append(metrics, model.Metric{
			RunID:     runID,
			Name:      metricName,
			Value:     strconv.FormatFloat(valueNum, 'f', -1, 64),
			Stream:    "prometheus",
			Raw:       string(raw),
			CreatedAt: sampleTime,
		})
	}

	return metrics, nil
}
