package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/cli"
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

func TestWriteCommandErrorUsesFinalLogFormat(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		structured bool
	}{
		{
			name: "text final value",
			args: []string{"--log-format", "json", "--log-format", "text", "version"},
		},
		{
			name:       "json final value",
			args:       []string{"--log-format", "text", "--log-format", "json", "version"},
			structured: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stderr bytes.Buffer
			command := cli.NewRoot(cli.Options{Version: "test", ErrOut: &stderr, Out: io.Discard})
			command.SetArgs(test.args)
			if err := command.Execute(); err != nil {
				t.Fatal(err)
			}
			cli.WriteCommandError(command.ErrOrStderr(), errors.New("planned failure"))
			if !test.structured {
				if output := stderr.String(); output != "error: planned failure\n" {
					t.Fatalf("text output=%q", output)
				}
				return
			}
			var entry map[string]any
			if err := json.Unmarshal(stderr.Bytes(), &entry); err != nil {
				t.Fatalf("JSON error=%v output=%s", err, stderr.String())
			}
			if entry["level"] != "ERROR" || entry["msg"] != "command failed" || entry["error"] != "planned failure" {
				t.Fatalf("entry=%v", entry)
			}
		})
	}
}
