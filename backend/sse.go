package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func handleRunTest(logChan chan StreamMessage, metricChan chan StreamMessage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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

		if err := createRunLogFile(req.RunID); err != nil {
			http.Error(w, fmt.Sprintf("failed to create run log file: %v", err), http.StatusInternalServerError)
			return
		}

		if err := createRun(req.RunID); err != nil {
			http.Error(w, fmt.Sprintf("failed to create run metadata: %v", err), http.StatusInternalServerError)
			return
		}

		runnerURL := req.RunnerURL
		if runnerURL == "" {
			runnerURL = os.Getenv("RUNNER_URL")
			if runnerURL == "" {
				runnerURL = "http://localhost:8080/run-test"
			}
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		runnerPayload := struct {
			VUs    int    `json:"vus"`
			Script string `json:"script"`
		}{
			VUs:    req.VUs,
			Script: req.Script,
		}

		payload, err := json.Marshal(runnerPayload)
		if err != nil {
			http.Error(w, "failed to build runner payload", http.StatusInternalServerError)
			return
		}

		runnerReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, runnerURL, bytes.NewReader(payload))
		if err != nil {
			http.Error(w, "failed to create runner request", http.StatusInternalServerError)
			return
		}
		runnerReq.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(runnerReq)
		if err != nil {
			http.Error(w, "failed to call runner api", http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(resp.Body)
			http.Error(w, fmt.Sprintf("runner api failed: %s - %s", resp.Status, strings.TrimSpace(string(body))), http.StatusBadGateway)
			return
		}

		reader := bufio.NewReader(resp.Body)
		for {
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

						msg := StreamMessage{RunID: req.RunID, Message: data, Stream: stream}
						logChan <- msg
						metricChan <- msg

						fmt.Fprintf(w, "data: %s\n\n", data)
						flusher.Flush()
					}
				}
			}

			if err != nil {
				if err == io.EOF {
					break
				}
				fmt.Fprintf(w, "data: [ERROR] %s\n\n", err.Error())
				flusher.Flush()
				break
			}
		}


		if err := saveRunLogFile(req.RunID); err != nil {
			fmt.Fprintf(w, "data: [ERROR] failed to save log file: %s\n\n", err.Error())
			flusher.Flush()
			updateRunStatus(req.RunID, "completed_with_log_error")
			return
		}

		updateRunStatus(req.RunID, "completed")

		fmt.Fprintf(w, "data: BACKEND COMPLETED\n\n")
		flusher.Flush()
	}
}