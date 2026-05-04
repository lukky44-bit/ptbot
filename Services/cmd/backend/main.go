// Package main provides the backend HTTP server for the load testing orchestration system.
// It implements service health checks, graceful shutdown, and SSE-based workflow status streaming.
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

	"backend/internal/db"
	"backend/internal/worker"
)

// ServiceKind defines the type of service to check (database, HTTP, or TCP).
type ServiceKind string

const (
	ServiceDB   ServiceKind = "db"
	ServiceHTTP ServiceKind = "http"
	ServiceTCP  ServiceKind = "tcp"
)

// Service represents a critical service with health check configuration.
type Service struct {
	Name      string        // Human-readable service name
	URL       string        // Service endpoint URL or address
	Kind      ServiceKind   // Type of health check (DB, HTTP, or TCP)
	Timeout   time.Duration // Timeout for individual health check attempts
	Retries   int           // Number of retry attempts before failure
	RetryWait time.Duration // Wait duration between retry attempts
}

var services = []Service{
	{Name: "PostgreSQL", URL: envOrDefault("DB_HOST", "localhost") + ":" + envOrDefault("DB_PORT", "5432"), Kind: ServiceDB, Timeout: 5 * time.Second, Retries: 5, RetryWait: 2 * time.Second},
	{Name: "Temporal Server", URL: envOrDefault("TEMPORAL_SERVICE_ADDRESS", "localhost:7233"), Kind: ServiceTCP, Timeout: 5 * time.Second, Retries: 5, RetryWait: 2 * time.Second},
	{Name: "Prometheus", URL: envOrDefault("PROMETHEUS_HEALTH_URL", "http://localhost:9090/-/healthy"), Kind: ServiceHTTP, Timeout: 5 * time.Second, Retries: 3, RetryWait: 1 * time.Second},
	{Name: "Runner Service", URL: envOrDefault("RUNNER_HEALTH_ADDR", "localhost:8080"), Kind: ServiceTCP, Timeout: 5 * time.Second, Retries: 3, RetryWait: 1 * time.Second},
}

var (
	shutdownOnce sync.Once
	shutdownChan = make(chan struct{})
)

// envOrDefault retrieves an environment variable or returns a fallback value.
func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// checkServiceHealth verifies the health status of a service based on its kind.
// Returns true if the service is healthy, false otherwise.
func checkServiceHealth(service Service) bool {
	switch service.Kind {
	case ServiceDB:
		ctx, cancel := context.WithTimeout(context.Background(), service.Timeout)
		defer cancel()
		return db.Ping(ctx) == nil
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

// waitForServices performs health checks on all critical services with retry logic.
// Returns an error if any service fails to become healthy within the retry limit.
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

// monitorServices continuously monitors service health and triggers graceful shutdown
// if any critical service becomes unavailable.
func monitorServices() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-shutdownChan:
			fmt.Println("Service monitor shutting down")
			return
		case <-ticker.C:
			for _, service := range services {
				if !checkServiceHealth(service) {
					fmt.Printf("✗ CRITICAL: %s is no longer available. Shutting down backend...\n", service.Name)
					shutdownBackend(1)
					return
				}
			}
		}
	}
}

// shutdownBackend performs graceful shutdown of the backend server.
// Ensures cleanup happens only once using sync.Once pattern.
func shutdownBackend(exitCode int) {
	shutdownOnce.Do(func() {
		fmt.Println("Initiating graceful shutdown...")
		close(shutdownChan)
		worker.StopTemporalWorker()
		db.Close()
		fmt.Println("Backend stopped")
		os.Exit(exitCode)
	})
}

// main initializes the backend server with database and Temporal worker,
// performs service health checks, and starts the HTTP server.
func main() {
	fmt.Println("Starting Backend with Temporal Workflow Engine...")

	if err := db.InitDB(context.Background()); err != nil {
		fmt.Printf("Failed to initialize database: %v\n", err)
		os.Exit(1)
	}

	if err := waitForServices(); err != nil {
		fmt.Printf("✗ Failed to start: %v\n", err)
		os.Exit(1)
	}

	if err := worker.InitTemporalWorker(); err != nil {
		fmt.Println("Failed to initialize Temporal worker:", err)
		os.Exit(1)
	}

	if err := worker.StartTemporalWorker(); err != nil {
		fmt.Println("Failed to start Temporal worker:", err)
		os.Exit(1)
	}

	go monitorServices()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		http.HandleFunc("/run-test", ServeRunTestWithWorkflow)
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
		})

		port := os.Getenv("BACKEND_PORT")
		if port == "" {
			port = "8081"
		}

		fmt.Println("Backend running on port", port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			fmt.Println("Server error:", err)
		}
	}()

	<-sigChan
	fmt.Println("Shutting down...")
	shutdownBackend(0)
}
