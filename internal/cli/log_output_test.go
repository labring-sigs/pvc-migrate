package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
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
	command.SetArgs([]string{"--log-format", "json", "--color", "always", "version"})
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

func TestColorizeTextLogsByLevelComponentAndTool(t *testing.T) {
	input := []byte("time=2026-08-14T00:00:00Z level=ERROR msg=failed component=migration\n[tool app/pv-migrate-copy rsync] checksum mismatch\n")
	output := string(colorizeLogText(input))
	for _, want := range []string{
		"\x1b[1;31mERROR\x1b[0m",
		"component=\x1b[",
		"\x1b[",
		"checksum mismatch",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("colored output lacks %q: %q", want, output)
		}
	}
	if !strings.HasSuffix(output, "\n") {
		t.Fatalf("colored output lost newline: %q", output)
	}
	plain := regexp.MustCompile(`\x1b\[[0-9;]*m`).ReplaceAllString(output, "")
	if plain != string(input) {
		t.Fatalf("colorization changed log content: got %q want %q", plain, input)
	}
	if componentColor("controller") == componentColor("planner") {
		t.Fatal("known components share a color")
	}
}

func TestColorOutputWriterModes(t *testing.T) {
	for _, test := range []struct {
		mode    string
		colored bool
	}{
		{mode: colorAlways, colored: true},
		{mode: colorAuto, colored: false},
		{mode: colorNever, colored: false},
	} {
		t.Run(test.mode, func(t *testing.T) {
			var output bytes.Buffer
			writer := newColorOutputWriter(&output, func() bool { return colorEnabled(test.mode, &output) })
			if _, err := writer.Write([]byte("level=INFO msg=ready\n")); err != nil {
				t.Fatal(err)
			}
			got := output.String()
			if strings.Contains(got, "\x1b[") != test.colored {
				t.Fatalf("mode=%s output=%q", test.mode, got)
			}
		})
	}
}
