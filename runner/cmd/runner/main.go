package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sync"
)

type TestRequest struct {
	RunID  string `json:"run_id"`
	VUs    int    `json:"vus"`
	Script string `json:"script"`
}

var (
	currentVUs = 0
	maxVUs     = 10000
	mu         sync.Mutex
)

func handler(w http.ResponseWriter, r *http.Request) {
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

	cmd := exec.Command("k6", "run",
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
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf(`{"type":"stdout","message":"%s"}\n`, line)
			fmt.Fprintf(w, "data: [STDOUT] %s\n\n", line)
			flusher.Flush()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf(`{"type":"stderr","message":"%s"}\n`, line)
			fmt.Fprintf(w, "data: [STDERR] %s\n\n", line)
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

func main() {
	http.HandleFunc("/run-test", handler)

	fmt.Println("Runner server running on port 8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("Server error:", err)
	}
}
