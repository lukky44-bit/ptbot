// Package app defines the load test workflow orchestration logic.
package app

import (
	"time"

	"backend/internal/model"

	"go.temporal.io/sdk/workflow"
)

// LoadTestWorkflow orchestrates a complete load test execution through a series of steps:
// 1. Create run record in database
// 2. Initialize log file
// 3. Execute K6 test via runner service and capture output
// 4. Extract metrics from test output
// 5. Save metrics to database
// 6. Archive logs to database
// 7. Update final status
func LoadTestWorkflow(ctx workflow.Context, req model.RunRequest) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("LoadTestWorkflow started", "runID", req.RunID, "vus", req.VUs)

	options := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 30,
	}
	ctx = workflow.WithActivityOptions(ctx, options)

	logger.Info("Step 1: Creating run record", "runID", req.RunID)
	if err := workflow.ExecuteActivity(ctx, ActivityCreateRun, req.RunID, req.VUs, req.Script).Get(ctx, nil); err != nil {
		logger.Error("Failed to create run record", "error", err)
		return "", err
	}

	if err := workflow.ExecuteActivity(ctx, ActivityUpdateRunStatus, req.RunID, "initializing").Get(ctx, nil); err != nil {
		logger.Error("Failed to update initial run status", "error", err)
		return "", err
	}

	logger.Info("Step 2: Creating log file", "runID", req.RunID)
	if err := workflow.ExecuteActivity(ctx, ActivityCreateLogFile, req.RunID).Get(ctx, nil); err != nil {
		logger.Error("Failed to create log file", "error", err)
		_ = workflow.ExecuteActivity(ctx, ActivityUpdateRunStatus, req.RunID, "failed_log_creation").Get(ctx, nil)
		_ = workflow.ExecuteActivity(ctx, ActivityCleanupLogFile, req.RunID).Get(ctx, nil)
		return "", err
	}

	logger.Info("Step 3: Processing stream from runner", "runID", req.RunID)
	var runnerURL string
	if err := workflow.ExecuteActivity(ctx, ActivityCallRunner, req).Get(ctx, &runnerURL); err != nil {
		logger.Error("Failed to call runner", "error", err)
		_ = workflow.ExecuteActivity(ctx, ActivityUpdateRunStatus, req.RunID, "failed_runner_connection").Get(ctx, nil)
		_ = workflow.ExecuteActivity(ctx, ActivityCleanupLogFile, req.RunID).Get(ctx, nil)
		return "", err
	}

	var chunkCount int
	if err := workflow.ExecuteActivity(ctx, ActivityProcessStream, req, runnerURL).Get(ctx, &chunkCount); err != nil {
		logger.Error("Failed to process stream", "error", err)
		_ = workflow.ExecuteActivity(ctx, ActivityUpdateRunStatus, req.RunID, "failed_stream_processing").Get(ctx, nil)
		_ = workflow.ExecuteActivity(ctx, ActivityCleanupLogFile, req.RunID).Get(ctx, nil)
		return "", err
	}

	logger.Info("Stream processed successfully", "runID", req.RunID, "chunkCount", chunkCount)

	logger.Info("Step 4: Extracting metrics from log file", "runID", req.RunID)
	var metrics []model.Metric
	if err := workflow.ExecuteActivity(ctx, ActivityExtractMetrics, req.RunID).Get(ctx, &metrics); err != nil {
		logger.Error("Failed to extract metrics", "error", err)
		_ = workflow.ExecuteActivity(ctx, ActivityUpdateRunStatus, req.RunID, "failed_metric_extraction").Get(ctx, nil)
		_ = workflow.ExecuteActivity(ctx, ActivityCleanupLogFile, req.RunID).Get(ctx, nil)
		return "", err
	}

	logger.Info("Metrics extracted successfully", "runID", req.RunID, "metricCount", len(metrics))

	logger.Info("Step 5: Saving metrics to database", "runID", req.RunID)
	if err := workflow.ExecuteActivity(ctx, ActivitySaveMetricsToDb, metrics).Get(ctx, nil); err != nil {
		logger.Error("Failed to save metrics to database", "error", err)
		_ = workflow.ExecuteActivity(ctx, ActivityUpdateRunStatus, req.RunID, "failed_metrics_save").Get(ctx, nil)
		_ = workflow.ExecuteActivity(ctx, ActivityCleanupLogFile, req.RunID).Get(ctx, nil)
		return "", err
	}

	logger.Info("Step 6: Saving log file to database", "runID", req.RunID)
	if err := workflow.ExecuteActivity(ctx, ActivitySaveRunLogFile, req.RunID).Get(ctx, nil); err != nil {
		logger.Error("Failed to save log file to database", "error", err)
		_ = workflow.ExecuteActivity(ctx, ActivityUpdateRunStatus, req.RunID, "failed_log_save").Get(ctx, nil)
		_ = workflow.ExecuteActivity(ctx, ActivityCleanupLogFile, req.RunID).Get(ctx, nil)
		return "", err
	}

	logger.Info("Step 7: Updating final status", "runID", req.RunID)
	if err := workflow.ExecuteActivity(ctx, ActivityUpdateRunStatus, req.RunID, "completed").Get(ctx, nil); err != nil {
		logger.Error("Failed to update final status", "error", err)
		return "", err
	}

	logger.Info("LoadTestWorkflow completed successfully", "runID", req.RunID)
	return req.RunID, nil
}
