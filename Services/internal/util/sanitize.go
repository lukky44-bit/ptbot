// Package util provides utility functions for file path management and string sanitization.
package util

import (
	"os"
	"strings"
	"unicode"
)

// SanitizeRunID converts a run ID into a filesystem-safe filename by replacing invalid characters.
func SanitizeRunID(runID string) string {
	if runID == "" {
		return ""
	}

	var b strings.Builder
	for _, r := range runID {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteRune('_')
	}

	return b.String()
}

// ResultsDir returns the configured results directory from the RESULTS_DIR environment variable,
// or defaults to "results" if not set.
func ResultsDir() string {
	if dir := os.Getenv("RESULTS_DIR"); dir != "" {
		return dir
	}
	return "results"
}
