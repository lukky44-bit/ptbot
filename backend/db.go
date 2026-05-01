package main

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

	"github.com/jackc/pgx/v5/pgxpool"
)

var dbPool *pgxpool.Pool

func initDB() {
	// Build PostgreSQL connection string
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse connection string: %v", err))
	}

	config.MaxConns = 25
	config.MinConns = 5

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		panic(fmt.Sprintf("Failed to create connection pool: %v", err))
	}

	if err := pool.Ping(ctx); err != nil {
		panic(fmt.Sprintf("Failed to ping database: %v", err))
	}

	dbPool = pool
}

func saveMetric(ctx context.Context, metric Metric) error {
	query := `
		INSERT INTO realtime_metrics (run_id, name, value, ts, stream, raw)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	// Parse value to float64
	var floatValue float64
	if metric.Value != "" {
		// Try to extract numeric value from the string
		parts := strings.Fields(metric.Value)
		if len(parts) > 0 {
			if val, err := strconv.ParseFloat(strings.TrimSuffix(parts[0], "ms"), 64); err == nil {
				floatValue = val
			}
		}
	}

	_, err := dbPool.Exec(ctx,
		query,
		metric.RunID,
		metric.Name,
		floatValue,
		time.Now(),
		metric.Stream,
		metric.Raw,
	)
	if err != nil {
		fmt.Printf("saveMetric error: %v\n", err)
	}
	return err
}

func createRun(ctx context.Context, runID string, vus int, script string) error {
	query := `
		INSERT INTO test_runs (id, vus, script, status, created_at, updated_at, started_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO UPDATE
		SET status = EXCLUDED.status, updated_at = $6
	`

	now := time.Now()
	_, err := dbPool.Exec(ctx, query,
		runID,
		vus,
		script,
		"started",
		now,
		now,
		now,
	)
	if err != nil {
		fmt.Printf("createRun error: %v\n", err)
	}
	return err
}

func saveRunLogFile(ctx context.Context, runID string) error {
	fileName := sanitizeRunID(runID)
	if fileName == "" {
		fileName = "run_unknown"
	}

	filePath := filepath.Join("results", fmt.Sprintf("%s.txt", fileName))
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
	_, err = dbPool.Exec(ctx, query,
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

func updateRunStatus(ctx context.Context, runID, status string) error {
	query := `
		UPDATE test_runs
		SET status = $1, updated_at = $2, ended_at = $3
		WHERE id = $4
	`

	now := time.Now()
	_, err := dbPool.Exec(ctx, query, status, now, now, runID)
	if err != nil {
		fmt.Printf("updateRunStatus error: %v\n", err)
	}
	return err
}

func saveSummaryMetric(ctx context.Context, runID string, metrics map[string]interface{}) error {
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
	_, err = dbPool.Exec(ctx, query, runID, string(payload), now, now)
	if err != nil {
		fmt.Printf("saveSummaryMetric error: %v\n", err)
	}
	return err
}

func fetchPrometheusMetrics(ctx context.Context, runID string) ([]Metric, error) {
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

	metrics := make([]Metric, 0, len(payload.Data.Result))
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

		metrics = append(metrics, Metric{
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
