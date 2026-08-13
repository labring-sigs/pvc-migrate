package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestLogOutputWriterKeepsJSONLogsAndGuidanceParseable(t *testing.T) {
	var output bytes.Buffer
	structured := true
	writer := newLogOutputWriter(&output, func() bool { return structured })
	if _, err := fmt.Fprintln(writer, `{"msg":"existing"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(writer, "follow-up guidance"); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines=%q", output.String())
	}
	for index, line := range lines {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("line %d is not JSON: %v\n%s", index, err, output.String())
		}
	}
	if lines[0] != `{"msg":"existing"}` || !strings.Contains(lines[1], `"msg":"follow-up guidance"`) {
		t.Fatalf("output=%q", output.String())
	}
	structured = false
	if _, err := fmt.Fprintln(writer, "text guidance"); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(output.String(), "text guidance\n") {
		t.Fatalf("text output=%q", output.String())
	}
}

func TestRootUsesStructuredStderrForJSONLogFormat(t *testing.T) {
	var stdout, stderr bytes.Buffer
	command := NewRoot(Options{Version: "test", Out: &stdout, ErrOut: &stderr})
	command.SetArgs([]string{"--log-format", "json", "version"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(command.ErrOrStderr(), "dry-run completed"); err != nil {
		t.Fatal(err)
	}
	var entry map[string]any
	if err := json.Unmarshal(stderr.Bytes(), &entry); err != nil {
		t.Fatalf("stderr is not JSON: %v\n%s", err, stderr.String())
	}
	if entry["msg"] != "dry-run completed" {
		t.Fatalf("entry=%v", entry)
	}
}
