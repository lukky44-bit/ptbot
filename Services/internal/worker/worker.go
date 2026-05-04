package worker

import (
	"context"
	"fmt"
	"log"
	"os"

	"backend/internal/app"
	"backend/internal/model"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

var TemporalClient client.Client
var temporalWorker worker.Worker

const (
	TemporalTaskQueue = "k6-loadtest-queue"
)

func InitTemporalWorker() error {
	var err error

	hostPort := os.Getenv("TEMPORAL_SERVICE_ADDRESS")
	if hostPort == "" {
		hostPort = "temporal:7233"
	}

	TemporalClient, err = client.Dial(client.Options{HostPort: hostPort})
	if err != nil {
		log.Fatalf("Failed to create Temporal client: %v", err)
		return err
	}

	fmt.Println("Connected to Temporal server at", hostPort)

	temporalWorker = worker.New(TemporalClient, TemporalTaskQueue, worker.Options{})

	temporalWorker.RegisterWorkflow(app.LoadTestWorkflow)

	temporalWorker.RegisterActivity(app.ActivityCreateRun)
	temporalWorker.RegisterActivity(app.ActivityCreateLogFile)
	temporalWorker.RegisterActivity(app.ActivityCallRunner)
	temporalWorker.RegisterActivity(app.ActivityProcessStream)
	temporalWorker.RegisterActivity(app.ActivityWriteToLogFile)
	temporalWorker.RegisterActivity(app.ActivityExtractMetrics)
	temporalWorker.RegisterActivity(app.ActivitySaveMetricsToDb)
	temporalWorker.RegisterActivity(app.ActivitySaveRunLogFile)
	temporalWorker.RegisterActivity(app.ActivityUpdateRunStatus)

	fmt.Println("Temporal worker initialized and activities registered")
	return nil
}

func StartTemporalWorker() error {
	err := temporalWorker.Start()
	if err != nil {
		log.Fatalf("Failed to start Temporal worker: %v", err)
		return err
	}

	fmt.Println("Temporal worker started successfully")
	return nil
}

func StopTemporalWorker() {
	temporalWorker.Stop()
	if TemporalClient != nil {
		TemporalClient.Close()
	}
	fmt.Println("Temporal worker stopped")
}

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
		log.Printf("Failed to start workflow: %v", err)
		return "", err
	}

	fmt.Printf("Workflow started with ID: %s\n", we.GetID())
	return we.GetID(), nil
}

func DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	if TemporalClient == nil {
		return nil, fmt.Errorf("temporal client not initialized")
	}
	return TemporalClient.DescribeWorkflowExecution(ctx, workflowID, runID)
}
