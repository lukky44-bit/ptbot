package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type ServiceKind string

const (
	ServiceDB   ServiceKind = "db"
	ServiceHTTP ServiceKind = "http"
	ServiceTCP  ServiceKind = "tcp"
)

// Service represents a critical service with its health check URL
type Service struct {
	Name      string
	URL       string
	Kind      ServiceKind
	Timeout   time.Duration
	Retries   int
	RetryWait time.Duration
}

var services = []Service{
	{Name: "PostgreSQL", URL: "localhost:5432", Kind: ServiceDB, Timeout: 5 * time.Second, Retries: 5, RetryWait: 2 * time.Second},
	{Name: "Temporal Server", URL: "localhost:7233", Kind: ServiceTCP, Timeout: 5 * time.Second, Retries: 5, RetryWait: 2 * time.Second},
	{Name: "Prometheus", URL: "http://localhost:9090/-/healthy", Kind: ServiceHTTP, Timeout: 5 * time.Second, Retries: 3, RetryWait: 1 * time.Second},
	{Name: "Runner Service", URL: "localhost:8080", Kind: ServiceTCP, Timeout: 5 * time.Second, Retries: 3, RetryWait: 1 * time.Second},
}

var (
	healthCheckMutex sync.Mutex
	shutdownOnce     sync.Once
	shutdownChan     = make(chan struct{})
)

// checkServiceHealth checks if a service is available
func checkServiceHealth(service Service) bool {
	switch service.Kind {
	case ServiceDB:
		ctx, cancel := context.WithTimeout(context.Background(), service.Timeout)
		defer cancel()
		err := dbPool.Ping(ctx)
		return err == nil
	case ServiceTCP:
		conn, err := net.DialTimeout("tcp", service.URL, service.Timeout)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	case ServiceHTTP:
	client := &http.Client{Timeout: service.Timeout}
	resp, err := client.Get(service.URL)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
	default:
		return false
	}
}

// waitForServices waits for all critical services to be available
func waitForServices() error {
	fmt.Println("Checking critical services...")

	for _, service := range services {
		fmt.Printf("Checking %s (%s)...\n", service.Name, service.URL)

		for attempt := 1; attempt <= service.Retries; attempt++ {
			if checkServiceHealth(service) {
				fmt.Printf("✓ %s is healthy\n", service.Name)
				break
			}

			if attempt < service.Retries {
				fmt.Printf("✗ %s not available, retrying in %v (attempt %d/%d)...\n",
					service.Name, service.RetryWait, attempt, service.Retries)
				time.Sleep(service.RetryWait)
			} else {
				return fmt.Errorf("%s failed to become healthy after %d attempts", service.Name, service.Retries)
			}
		}
	}

	fmt.Println("✓ All critical services are healthy!")
	return nil
}

// monitorServices continuously monitors service health and shuts down if any fails
func monitorServices() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-shutdownChan:
			fmt.Println("Service monitor shutting down")
			return
		case <-ticker.C:
			healthCheckMutex.Lock()
			for _, service := range services {
				if !checkServiceHealth(service) {
					fmt.Printf("✗ CRITICAL: %s is no longer available. Shutting down backend...\n", service.Name)
					healthCheckMutex.Unlock()
					shutdownBackend()
					return
				}
			}
			healthCheckMutex.Unlock()
		}
	}
}

// shutdownBackend gracefully shuts down the backend
func shutdownBackend() {
	shutdownOnce.Do(func() {
		fmt.Println("Initiating graceful shutdown...")
		close(shutdownChan)
		time.Sleep(1 * time.Second)
		StopTemporalWorker()
		fmt.Println("Backend stopped")
		os.Exit(1)
	})
}

func main() {
	fmt.Println("Starting Backend with Temporal Workflow Engine...")

	// Init DB
	initDB()

	// Check if all critical services are available before proceeding
	if err := waitForServices(); err != nil {
		fmt.Printf("✗ Failed to start: %v\n", err)
		os.Exit(1)
	}

	// Initialize Temporal worker
	if err := InitTemporalWorker(); err != nil {
		fmt.Println("Failed to initialize Temporal worker:", err)
		os.Exit(1)
	}

	// Start Temporal worker in background
	if err := StartTemporalWorker(); err != nil {
		fmt.Println("Failed to start Temporal worker:", err)
		os.Exit(1)
	}

	// Start monitoring services in background
	go monitorServices()

	// Graceful shutdown handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start HTTP server in goroutine
	go func() {
		http.HandleFunc("/run-test", handleRunTestWithWorkflow)

		port := os.Getenv("BACKEND_PORT")
		if port == "" {
			port = "8081"
		}

		fmt.Println("Backend running on port", port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			fmt.Println("Server error:", err)
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	fmt.Println("Shutting down...")
	close(shutdownChan)
	StopTemporalWorker()
	fmt.Println("Backend stopped")
}