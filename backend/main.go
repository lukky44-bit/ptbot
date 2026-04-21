package main

import (
	"fmt"
	"net/http"
	"os"
)

func main() {
	fmt.Println("Starting Backend2...")

	// Init DB
	initDB()

	// Channels
	logChan := make(chan StreamMessage)
	metricChan := make(chan StreamMessage)

	// Start workers
	go startFileWriter(logChan)
	go startMetricParser(metricChan)

	http.HandleFunc("/run-test", handleRunTest(logChan, metricChan))

	port := os.Getenv("BACKEND_PORT")
	if port == "" {
		port = "8081"
	}

	fmt.Println("Backend running on port", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Println("Server error:", err)
	}

	fmt.Println("Backend stopped")
}