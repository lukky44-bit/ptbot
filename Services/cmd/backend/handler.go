package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"backend/internal/model"
	"backend/internal/worker"
)

func ServeRunTestWithWorkflow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req model.RunRequest
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

	workflowID, err := worker.StartLoadTestWorkflow(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to start workflow: %v", err), http.StatusInternalServerError)
		return
	}

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

	ctx := context.Background()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		we, err := worker.DescribeWorkflowExecution(ctx, workflowID, "")
		if err != nil {
			fmt.Fprintf(w, "data: [ERROR] Failed to check workflow status: %v\n\n", err)
			flusher.Flush()
			break
		}

		status := we.WorkflowExecutionInfo.Status.String()
		fmt.Fprintf(w, "data: Workflow status: %s\n\n", status)
		flusher.Flush()

		if status != "Running" {
			if status == "COMPLETED" {
				fmt.Fprintf(w, "data: Workflow completed successfully\n\n")
			} else if status == "FAILED" {
				fmt.Fprintf(w, "data: [ERROR] Workflow failed\n\n")
			} else {
				fmt.Fprintf(w, "data: Workflow ended with status: %s\n\n", status)
			}
			flusher.Flush()
			break
		}
	}

	fmt.Fprintf(w, "data: BACKEND COMPLETED\n\n")
	flusher.Flush()
}
