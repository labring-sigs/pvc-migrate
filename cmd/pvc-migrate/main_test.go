package main

import (
	"context"
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
