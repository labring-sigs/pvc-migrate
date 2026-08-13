package cli

import (
	"bytes"
	"context"
	"encoding/json"
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
	mu         sync.Mutex
}

func newLogOutputWriter(target io.Writer, structured func() bool) io.Writer {
	if target == nil {
		target = io.Discard
	}
	return &logOutputWriter{target: target, structured: structured}
}

func (w *logOutputWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.structured == nil || !w.structured() {
		return w.target.Write(data)
	}
	for _, line := range bytes.Split(data, []byte{'\n'}) {
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
		if err := slog.NewJSONHandler(w.target, nil).Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, string(line), 0)); err != nil {
			return 0, err
		}
	}
	return len(data), nil
}
