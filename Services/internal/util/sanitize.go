package util

import (
	"strings"
	"unicode"
)

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
