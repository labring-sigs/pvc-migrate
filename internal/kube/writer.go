package kube

import (
	"io"
	"sync"
)

// SynchronizedWriter serializes writes made by independent workflow workers.
// Kubernetes clients and slog handlers are concurrency-safe, while an
// arbitrary io.Writer (for example bytes.Buffer or a file wrapper) is not.
// The wrapper gives long-lived services one ownership boundary for output
// without imposing a concurrency contract on their callers.
type SynchronizedWriter struct {
	mu     sync.Mutex
	target io.Writer
}

func NewSynchronizedWriter(target io.Writer) io.Writer {
	if target == nil {
		target = io.Discard
	}

	return &SynchronizedWriter{target: target}
}

func (w *SynchronizedWriter) Write(p []byte) (int, error) {
	if w == nil || w.target == nil {
		return len(p), nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	return w.target.Write(p)
}
