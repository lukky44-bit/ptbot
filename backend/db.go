package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var collection *mongo.Collection

func initDB() {
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://localhost:27017"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		panic(err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		panic(err)
	}

	collection = client.Database("loadtest").Collection("metrics")
}

func saveMetric(metric Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"run_id": metric.RunID}
	update := bson.M{
		"$setOnInsert": bson.M{
			"run_id":     metric.RunID,
			"created_at": time.Now(),
		},
		"$set": bson.M{
			"updated_at": time.Now(),
		},
		"$push": bson.M{
			"metrics": metric,
		},
	}

	opts := options.Update().SetUpsert(true)

	if _, err := collection.UpdateOne(ctx, filter, update, opts); err != nil {
		fmt.Println("saveMetric error:", err)
	}
}

func createRun(runID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"run_id": runID}
	update := bson.M{
		"$setOnInsert": bson.M{
			"run_id":     runID,
			"created_at": time.Now(),
			"start_time": time.Now(),
			"metrics":    bson.A{},
		},
		"$set": bson.M{
			"status":     "started",
			"updated_at":  time.Now(),
		},
	}

	opts := options.Update().SetUpsert(true)
	_, err := collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		fmt.Println("createRun error:", err)
	}
	return err
}

func saveRunLogFile(runID string) error {
	fileName := sanitizeRunID(runID)
	if fileName == "" {
		fileName = "run_unknown"
	}

	filePath := filepath.Join("results", fmt.Sprintf("%s.txt", fileName))
	content, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"run_id": runID}
	update := bson.M{
		"$set": bson.M{
			"log_file_content": content,
			"updated_at":       time.Now(),
		},
	}

	if _, err := collection.UpdateOne(ctx, filter, update); err != nil {
		return err
	}

	if err := os.Remove(filePath); err != nil {
		return err
	}

	return nil
}

func updateRunStatus(runID, status string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"run_id": runID}
	update := bson.M{
		"$set": bson.M{
			"status":     status,
			"end_time":   time.Now(),
			"updated_at": time.Now(),
		},
	}

	_, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		fmt.Println("updateRunStatus error:", err)
	}
	return err
}