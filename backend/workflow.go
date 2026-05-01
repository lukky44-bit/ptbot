package main

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// LoadTestWorkflow orchestrates the entire load test execution pipeline
// It coordinates all activities with proper error handling, retries, and timeouts
func LoadTestWorkflow(ctx workflow.Context, req RunRequest) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("LoadTestWorkflow started", "runID", req.RunID, "vus", req.VUs)

	// Define activity options with timeouts
	// Note: Retries are handled by Temporal's default retry policy
	options := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 30,
	}

	// Apply options to context for all activities
	ctx = workflow.WithActivityOptions(ctx, options)

	// ============================================
	// Step 1: Create Run Record in Database
	// ============================================
	logger.Info("Step 1: Creating run record", "runID", req.RunID)
	err := workflow.ExecuteActivity(ctx, ActivityCreateRun, req.RunID, req.VUs, req.Script).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to create run record", "error", err)
		return "", err
	}

	err = workflow.ExecuteActivity(ctx, ActivityUpdateRunStatus, req.RunID, "initializing").Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to update initial run status", "error", err)
		return "", err
	}

	// ============================================
	// Step 2: Create Log File
	// ============================================
	logger.Info("Step 2: Creating log file", "runID", req.RunID)
	err = workflow.ExecuteActivity(ctx, ActivityCreateLogFile, req.RunID).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to create log file", "error", err)
		_ = workflow.ExecuteActivity(ctx, ActivityUpdateRunStatus, req.RunID, "failed_log_creation").Get(ctx, nil)
		return "", err
	}

	// ============================================
	// Step 3: Process Stream from Runner
	// ============================================
	logger.Info("Step 3: Processing stream from runner", "runID", req.RunID)

	// First, call runner to establish connection
	var runnerURL string
	err = workflow.ExecuteActivity(ctx, ActivityCallRunner, req).Get(ctx, &runnerURL)
	if err != nil {
		logger.Error("Failed to call runner", "error", err)
		_ = workflow.ExecuteActivity(ctx, ActivityUpdateRunStatus, req.RunID, "failed_runner_connection").Get(ctx, nil)
		return "", err
	}

	// Process the stream
	var chunks []StreamChunk
	err = workflow.ExecuteActivity(ctx, ActivityProcessStream, req, runnerURL).Get(ctx, &chunks)
	if err != nil {
		logger.Error("Failed to process stream", "error", err)
		_ = workflow.ExecuteActivity(ctx, ActivityUpdateRunStatus, req.RunID, "failed_stream_processing").Get(ctx, nil)
		return "", err
	}

	logger.Info("Stream processed successfully", "runID", req.RunID, "chunkCount", len(chunks))

	// ============================================
	// Step 4: Extract Metrics from Log File
	// ============================================
	logger.Info("Step 4: Extracting metrics from log file", "runID", req.RunID)
	var metrics []Metric
	err = workflow.ExecuteActivity(ctx, ActivityExtractMetrics, req.RunID).Get(ctx, &metrics)
	if err != nil {
		logger.Error("Failed to extract metrics", "error", err)
		_ = workflow.ExecuteActivity(ctx, ActivityUpdateRunStatus, req.RunID, "failed_metric_extraction").Get(ctx, nil)
		return "", err
	}

	logger.Info("Metrics extracted successfully", "runID", req.RunID, "metricCount", len(metrics))

	// ============================================
	// Step 5: Save Metrics to Database
	// ============================================
	logger.Info("Step 5: Saving metrics to database", "runID", req.RunID)
	err = workflow.ExecuteActivity(ctx, ActivitySaveMetricsToDb, metrics).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to save metrics to database", "error", err)
		_ = workflow.ExecuteActivity(ctx, ActivityUpdateRunStatus, req.RunID, "failed_metrics_save").Get(ctx, nil)
		return "", err
	}

	// ============================================
	// Step 6: Save Log File Content to Database
	// ============================================
	logger.Info("Step 6: Saving log file to database", "runID", req.RunID)
	err = workflow.ExecuteActivity(ctx, ActivitySaveRunLogFile, req.RunID).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to save log file to database", "error", err)
		_ = workflow.ExecuteActivity(ctx, ActivityUpdateRunStatus, req.RunID, "failed_log_save").Get(ctx, nil)
		return "", err
	}

	// ============================================
	// Step 7: Update Final Status
	// ============================================
	logger.Info("Step 7: Updating final status", "runID", req.RunID)
	err = workflow.ExecuteActivity(ctx, ActivityUpdateRunStatus, req.RunID, "completed").Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to update final status", "error", err)
		return "", err
	}

	logger.Info("LoadTestWorkflow completed successfully", "runID", req.RunID)
	return req.RunID, nil
}