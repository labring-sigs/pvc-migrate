package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"
)

// logOutputWriter keeps stderr machine-readable when JSON logging is selected.
// slog already writes JSON records, while command guidance and Cobra errors use
// regular text writes. Converting only the latter produces one JSON Lines
// stream without changing the text-mode operator experience.
type logOutputWriter struct {
	target     io.Writer
	structured func() bool
	json       slog.Handler
	mu         sync.Mutex
}

func newLogOutputWriter(target io.Writer, structured func() bool) io.Writer {
	if target == nil {
		target = io.Discard
	}

	return &logOutputWriter{
		target:     target,
		structured: structured,
		json:       slog.NewJSONHandler(target, localLogHandlerOptions(nil)),
	}
}

// WriteCommandError keeps top-level command failures in the active stderr
// format after Cobra has applied persistent flags.
func WriteCommandError(w io.Writer, err error) {
	if writer, ok := w.(*logOutputWriter); ok {
		writer.writeCommandError(err)
		return
	}

	_, _ = fmt.Fprintf(w, "error: %v\n", err)
}

func (w *logOutputWriter) writeCommandError(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.structured != nil && w.structured() {
		record := slog.NewRecord(time.Now(), slog.LevelError, "command failed", 0)
		record.AddAttrs(slog.String("error", err.Error()))
		_ = w.json.Handle(context.Background(), record)
		return
	}

	_, _ = fmt.Fprintf(w.target, "error: %v\n", err)
}

func (w *logOutputWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.structured == nil || !w.structured() {
		return w.target.Write(data)
	}

	for line := range bytes.SplitSeq(data, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		if json.Valid(line) {
			if _, err := w.target.Write(append(line, '\n')); err != nil {
				return 0, err
			}
			continue
		}

		if err := w.json.Handle(
			context.Background(),
			slog.NewRecord(time.Now(), slog.LevelInfo, string(line), 0),
		); err != nil {
			return 0, err
		}
	}

	return len(data), nil
}

func localLogHandlerOptions(level slog.Leveler) *slog.HandlerOptions {
	location := time.Local

	return &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if len(groups) == 0 && attr.Key == slog.TimeKey && attr.Value.Kind() == slog.KindTime {
				attr.Value = slog.TimeValue(attr.Value.Time().In(location))
			}

			return attr
		},
	}
}
