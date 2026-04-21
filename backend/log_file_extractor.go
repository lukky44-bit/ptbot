package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

type runLogDoc struct {
    RunID          string `bson:"run_id"`
    LogFileContent []byte `bson:"log_file_content"`
}

// restoreLogFileFromMongo fetches log_file_content from MongoDB
// and writes it to results/<runID>_restored.txt
func restoreLogFileFromMongo(runID string) (string, error) {
    runID = strings.TrimSpace(runID)
    if runID == "" {
        return "", fmt.Errorf("runID is required")
    }

    if collection == nil {
        initDB()
    }

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