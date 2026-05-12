// Package worker manages the Temporal worker initialization, workflow registration, and execution.
package worker

import (
	"context"
	"fmt"
	"os"

	"backend/internal/app"
	"backend/internal/model"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

var (
	// TemporalClient is the global Temporal client for workflow execution.
	TemporalClient client.Client
	// temporalWorker polls for and executes Temporal tasks.
	temporalWorker worker.Worker
)

const (
	// TemporalTaskQueue is the task queue name for load test workflows.
	TemporalTaskQueue = "k6-loadtest-queue"
)

// InitTemporalWorker initializes a Temporal client and worker, registering all workflows and activities.
func InitTemporalWorker() error {
	var err error

	hostPort := os.Getenv("TEMPORAL_SERVICE_ADDRESS")
	if hostPort == "" {
		hostPort = "localhost:7233"
	}

	TemporalClient, err = client.Dial(client.Options{HostPort: hostPort})
	if err != nil {
		return fmt.Errorf("failed to create Temporal client: %w", err)
	}

	fmt.Println("Connected to Temporal server at", hostPort)

	temporalWorker = worker.New(TemporalClient, TemporalTaskQueue, worker.Options{})

	temporalWorker.RegisterWorkflow(app.LoadTestWorkflow)

	temporalWorker.RegisterActivity(app.ActivityCreateRun)
	temporalWorker.RegisterActivity(app.ActivityCreateLogFile)
	temporalWorker.RegisterActivity(app.ActivityCallRunner)
	temporalWorker.RegisterActivity(app.ActivityProcessStream)
	temporalWorker.RegisterActivity(app.ActivityExtractMetrics)
	temporalWorker.RegisterActivity(app.ActivitySaveMetricsToDb)
	temporalWorker.RegisterActivity(app.ActivitySaveRunLogFile)
	temporalWorker.RegisterActivity(app.ActivityCleanupLogFile)
	temporalWorker.RegisterActivity(app.ActivityUpdateRunStatus)

	fmt.Println("Temporal worker initialized and activities registered")
	return nil
}

// StartTemporalWorker starts the Temporal worker to begin polling for tasks.
func StartTemporalWorker() error {
	err := temporalWorker.Start()
	if err != nil {
		return fmt.Errorf("failed to start Temporal worker: %w", err)
	}

	fmt.Println("Temporal worker started successfully")
	return nil
}

// StopTemporalWorker gracefully stops the Temporal worker and closes the client connection.
func StopTemporalWorker() {
	if temporalWorker != nil {
		temporalWorker.Stop()
	}
	if TemporalClient != nil {
		TemporalClient.Close()
	}
	fmt.Println("Temporal worker stopped")
}

// StartLoadTestWorkflow starts a new load test workflow and returns its workflow ID.
func StartLoadTestWorkflow(req model.RunRequest) (string, error) {
	if TemporalClient == nil {
		return "", fmt.Errorf("temporal client not initialized")
	}

	options := client.StartWorkflowOptions{
		ID:        req.RunID,
		TaskQueue: TemporalTaskQueue,
	}

	we, err := TemporalClient.ExecuteWorkflow(context.Background(), options, app.LoadTestWorkflow, req)
	if err != nil {
		return "", fmt.Errorf("failed to start workflow: %w", err)
	}

	fmt.Printf("Workflow started with ID: %s\n", we.GetID())
	return we.GetID(), nil
}

// DescribeWorkflowExecution retrieves detailed information about a workflow execution.
func DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	if TemporalClient == nil {
		return nil, fmt.Errorf("temporal client not initialized")
	}
	return TemporalClient.DescribeWorkflowExecution(ctx, workflowID, runID)
}
