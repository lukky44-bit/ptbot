package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type runLogDoc struct {
	RunID          string `bson:"run_id"`
	LogFileContent []byte `bson:"log_file_content"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run ./cmd/log_file_extractor <run_id>")
		os.Exit(1)
	}

	runID := strings.TrimSpace(os.Args[1])
	if runID == "" {
		fmt.Println("run_id is required")
		os.Exit(1)
	}

	collection, err := initExtractorDB()
	if err != nil {
		fmt.Println("DB init error:", err)
		os.Exit(1)
	}

	outPath, err := restoreLogFileFromMongo(collection, runID)
	if err != nil {
		fmt.Println("Restore error:", err)
		os.Exit(1)
	}

	fmt.Println("Restored file:", outPath)
}

func initExtractorDB() (*mongo.Collection, error) {
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://localhost:27017"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	return client.Database("loadtest").Collection("metrics"), nil
}

func restoreLogFileFromMongo(collection *mongo.Collection, runID string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var doc runLogDoc
	err := collection.FindOne(ctx, bson.M{"run_id": runID}).Decode(&doc)
	if err != nil {
		return "", fmt.Errorf("failed to fetch run from mongo: %w", err)
	}

	if len(doc.LogFileContent) == 0 {
		return "", fmt.Errorf("log_file_content is empty for run_id=%s", runID)
	}

	if err := os.MkdirAll("results", 0755); err != nil {
		return "", fmt.Errorf("failed to create results folder: %w", err)
	}

	fileName := sanitizeRunID(runID)
	if fileName == "" {
		fileName = "run_unknown"
	}

	outPath := filepath.Join("results", fmt.Sprintf("%s_restored.txt", fileName))
	if err := os.WriteFile(outPath, doc.LogFileContent, 0644); err != nil {
		return "", fmt.Errorf("failed to write restored file: %w", err)
	}

	return outPath, nil
}

func sanitizeRunID(runID string) string {
	if runID == "" {
		return ""
	}

	var b strings.Builder
	for _, r := range runID {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteRune('_')
	}

	return b.String()
}
