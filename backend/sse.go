package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// handleRunTestWithWorkflow is the new HTTP handler that uses Temporal workflow
func handleRunTestWithWorkflow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req RunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Script == "" || req.VUs <= 0 {
		http.Error(w, "invalid script or vus", http.StatusBadRequest)
		return
	}

	if req.RunID == "" {
		req.RunID = fmt.Sprintf("run_%d", time.Now().UnixNano())
	}

	// Start the Temporal workflow
	workflowID, err := StartLoadTestWorkflow(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to start workflow: %v", err), http.StatusInternalServerError)
		return
	}

	// Create a polling checker to stream the progress
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "data: Workflow started with ID: %s\n\n", workflowID)
	flusher.Flush()

	// Poll for workflow completion
	ctx := context.Background()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	maxAttempts := 900 // 30 minutes with 2-second intervals
	attempts := 0

	for range ticker.C {
		attempts++

		// Check workflow status
		we, err := temporalClient.DescribeWorkflowExecution(ctx, workflowID, "")
		if err != nil {
			fmt.Fprintf(w, "data: [ERROR] Failed to check workflow status: %v\n\n", err)
			flusher.Flush()
			break
		}

		status := we.WorkflowExecutionInfo.Status.String()
		fmt.Fprintf(w, "data: Workflow status: %s\n\n", status)
		flusher.Flush()

		// Check if workflow is done
		if status != "Running" {
			if status == "COMPLETED" {
				fmt.Fprintf(w, "data: Workflow completed successfully\n\n")
				flusher.Flush()
			} else if status == "FAILED" {
				fmt.Fprintf(w, "data: [ERROR] Workflow failed\n\n")
				flusher.Flush()
			} else {
				fmt.Fprintf(w, "data: Workflow ended with status: %s\n\n", status)
				flusher.Flush()
			}
			break
		}

		if attempts >= maxAttempts {
			fmt.Fprintf(w, "data: [ERROR] Workflow polling timeout\n\n")
			flusher.Flush()
			break
		}
	}

	fmt.Fprintf(w, "data: BACKEND COMPLETED\n\n")
	flusher.Flush()
}