package cli

import (
	"context"
	"errors"
	"log/slog"
)

// kubernetesLogHandler drops the expected error emitted when a Kubernetes
// reflector is stopped by context cancellation.
type kubernetesLogHandler struct {
	next slog.Handler
}

func (h *kubernetesLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *kubernetesLogHandler) Handle(ctx context.Context, record slog.Record) error {
	if isExpectedWatchCancellation(record) {
		return nil
	}
	return h.next.Handle(ctx, record)
}

func (h *kubernetesLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &kubernetesLogHandler{next: h.next.WithAttrs(attrs)}
}

func (h *kubernetesLogHandler) WithGroup(name string) slog.Handler {
	return &kubernetesLogHandler{next: h.next.WithGroup(name)}
}

func isExpectedWatchCancellation(record slog.Record) bool {
	if record.Message != "Failed to watch" {
		return false
	}

	var canceled bool
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key != "err" {
			return true
		}

		if err, ok := attr.Value.Any().(error); ok {
			canceled = errors.Is(err, context.Canceled)
		}

		return false
	})

	return canceled
}
