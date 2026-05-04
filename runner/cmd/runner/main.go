// Package main provides the K6 runner service that executes load test scripts.
// It manages virtual user capacity and streams test output via HTTP Server-Sent Events.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sync"
)

// TestRequest represents a K6 test execution request.
type TestRequest struct {
	RunID  string `json:"run_id"` // Unique identifier for the test run
	VUs    int    `json:"vus"`    // Number of virtual users to simulate
	Script string `json:"script"` // K6 test script to execute
}

var (
	// currentVUs tracks the number of virtual users currently in use.
	currentVUs = 0
	// maxVUs is the maximum number of virtual users this runner can support.
	maxVUs = 10000
	// mu synchronizes access to currentVUs.
	mu sync.Mutex
)

// handler processes incoming test execution requests.
// It validates the request, manages virtual user allocation, executes K6, and streams results.
func handler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var req TestRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.RunID == "" || req.Script == "" || req.VUs <= 0 {
		http.Error(w, "Invalid run_id, script or VUs", http.StatusBadRequest)
		return
	}

	mu.Lock()
	if currentVUs+req.VUs > maxVUs {
		mu.Unlock()
		http.Error(w, "container limit reached", http.StatusTooManyRequests)
		return
	}
	currentVUs += req.VUs
	fmt.Println("Allocated VUs:", req.VUs, "| Current VUs:", currentVUs)
	mu.Unlock()

	defer func() {
		mu.Lock()
		currentVUs -= req.VUs
		fmt.Println("Released VUs:", req.VUs, "| Current VUs:", currentVUs)
		mu.Unlock()
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	cmd := exec.CommandContext(context.Background(), "k6", "run",
		"--tag", "test_run_id="+req.RunID,
		"-o", "experimental-prometheus-rw=http://prometheus:9090/api/v1/write",
		"-",
	)
	cmd.Env = append(os.Environ(),
		"K6_PROMETHEUS_RW_SERVER_URL=http://prometheus:9090/api/v1/write",
		"K6_PROMETHEUS_RW_PUSH_INTERVAL=2s",
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		http.Error(w, "Error getting stdout", http.StatusInternalServerError)
		return
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		http.Error(w, "Error getting stderr", http.StatusInternalServerError)
		return
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		http.Error(w, "Error getting stdin", http.StatusInternalServerError)
		return
	}

	go func() {
		defer stdin.Close()
		stdin.Write([]byte(req.Script))
	}()

	if err := cmd.Start(); err != nil {
		http.Error(w, "Failed to start k6", http.StatusInternalServerError)
		return
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf(`{"type":"stdout","message":"%s"}\n`, line)
			fmt.Fprintf(w, "data: [STDOUT] %s\n\n", line)
			flusher.Flush()
		}
		if err := scanner.Err(); err != nil {
			fmt.Fprintf(w, "data: [ERROR] stdout read error: %v\n\n", err)
			flusher.Flush()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderr)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf(`{"type":"stderr","message":"%s"}\n`, line)
			fmt.Fprintf(w, "data: [STDERR] %s\n\n", line)
			flusher.Flush()
		}
		if err := scanner.Err(); err != nil {
			fmt.Fprintf(w, "data: [ERROR] stderr read error: %v\n\n", err)
			flusher.Flush()
		}
	}()

	if err := cmd.Wait(); err != nil {
		fmt.Fprintf(w, "data: [ERROR] k6 exited with error: %v\n\n", err)
		flusher.Flush()
	}

	wg.Wait()

	fmt.Println("TEST COMPLETED")
	fmt.Fprintf(w, "data: TEST COMPLETED\n\n")
	flusher.Flush()
}

// main starts the runner HTTP server with test execution and health check endpoints.
func main() {
	http.HandleFunc("/run-test", handler)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	fmt.Println("Runner server running on port 8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("Server error:", err)
	}
}
