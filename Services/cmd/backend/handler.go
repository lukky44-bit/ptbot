// Package main provides HTTP handlers for the backend server.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"backend/internal/model"
	"backend/internal/worker"
)

// ServeRunTestWithWorkflow handles POST requests to start a load test workflow.
// It validates the request, starts a Temporal workflow, and streams workflow status updates via SSE.
func ServeRunTestWithWorkflow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req model.RunRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20)) // 1MB max body
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
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

	if _, err := fmt.Fprintf(w, "data: Workflow started with ID: %s\n\n", workflowID); err != nil {
		return
	}
	flusher.Flush()

	ctx := context.Background()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}

		we, err := worker.DescribeWorkflowExecution(ctx, workflowID, "")
		if err != nil {
			if _, writeErr := fmt.Fprintf(w, "data: [ERROR] Failed to check workflow status: %v\n\n", err); writeErr != nil {
				return
			}
			flusher.Flush()
			break
		}

		status := we.WorkflowExecutionInfo.Status.String()
		if _, err := fmt.Fprintf(w, "data: Workflow status: %s\n\n", status); err != nil {
			return
		}
		flusher.Flush()

		if status != "Running" {
			if status == "COMPLETED" {
				if _, err := fmt.Fprintf(w, "data: Workflow completed successfully\n\n"); err != nil {
					return
				}
			} else if status == "FAILED" {
				if _, err := fmt.Fprintf(w, "data: [ERROR] Workflow failed\n\n"); err != nil {
					return
				}
			} else {
				if _, err := fmt.Fprintf(w, "data: Workflow ended with status: %s\n\n", status); err != nil {
					return
				}
			}
			flusher.Flush()
			break
		}
	}

	if _, err := fmt.Fprintf(w, "data: BACKEND COMPLETED\n\n"); err != nil {
		return
	}
	flusher.Flush()
}
