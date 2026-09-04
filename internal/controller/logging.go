package controller

import (
	"context"
	"log/slog"
	"strings"
)

// NewControllerLogger adapts dependency logs to the controller's structured
// output contract. pv-migrate currently prefixes some messages with terminal
// emoji; those glyphs are useful in an interactive CLI and noisy in controller
// logs, where the record message and attributes are the operator interface.
func NewControllerLogger(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	return slog.New(&controllerLogHandler{next: logger.Handler()})
}

type controllerLogHandler struct {
	next slog.Handler
}

func (h *controllerLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h != nil && h.next != nil && h.next.Enabled(ctx, level)
}

func (h *controllerLogHandler) Handle(ctx context.Context, record slog.Record) error {
	if h == nil || h.next == nil {
		return nil
	}

	record.Message = normalizeControllerLogMessage(record.Message)

	return h.next.Handle(ctx, record)
}

func (h *controllerLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if h == nil || h.next == nil {
		return slog.New(slog.DiscardHandler).Handler()
	}

	return &controllerLogHandler{next: h.next.WithAttrs(attrs)}
}

func (h *controllerLogHandler) WithGroup(name string) slog.Handler {
	if h == nil || h.next == nil {
		return slog.New(slog.DiscardHandler).Handler()
	}

	return &controllerLogHandler{next: h.next.WithGroup(name)}
}

var dependencyLogPrefixes = []string{
	"\U0001f504 ",
	"\U0001f681 ",
	"\U0001f4e6 ",
	"\U0001f516 ",
	"\u23f3 ",
	"\u2705 ",
	"\u2728 ",
	"\U0001f4e5 ",
	"\U0001f9f9 ",
	"\U0001f536 ",
	"\U0001f3c3 ",
	"\U0001f3c1 ",
	"\u2755 ",
	"\u274c ",
}

func normalizeControllerLogMessage(message string) string {
	for _, prefix := range dependencyLogPrefixes {
		if trimmed, ok := strings.CutPrefix(message, prefix); ok {
			return strings.TrimSpace(trimmed)
		}
	}

	return message
}
