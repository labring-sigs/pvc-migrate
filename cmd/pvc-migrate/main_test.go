package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestCommandExitCode(t *testing.T) {
	if code := commandExitCode(context.Background(), func() int { return 130 }, nil); code != 0 {
		t.Fatalf("success exit code=%d", code)
	}
	validationErr := domain.NewError(domain.ErrorValidation, "test", "invalid")
	if code := commandExitCode(context.Background(), func() int { return 130 }, validationErr); code != 2 {
		t.Fatalf("validation exit code=%d", code)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if code := commandExitCode(ctx, func() int { return 130 }, nil); code != 130 {
		t.Fatalf("canceled success exit code=%d", code)
	}
}

func TestWriteCommandErrorUsesRequestedLogFormat(t *testing.T) {
	var jsonOutput bytes.Buffer
	writeCommandError(&jsonOutput, []string{"--log-format=json"}, errors.New("planned failure"))
	var entry map[string]string
	if err := json.Unmarshal(jsonOutput.Bytes(), &entry); err != nil {
		t.Fatalf("JSON error=%v output=%s", err, jsonOutput.String())
	}
	if entry["level"] != "ERROR" || entry["error"] != "planned failure" {
		t.Fatalf("entry=%v", entry)
	}
	var textOutput bytes.Buffer
	writeCommandError(&textOutput, nil, errors.New("planned failure"))
	if !strings.Contains(textOutput.String(), "error: planned failure") {
		t.Fatalf("text output=%q", textOutput.String())
	}
}
