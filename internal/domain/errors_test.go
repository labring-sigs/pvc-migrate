package domain_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	. "github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestErrorFormattingWrappingAndClassification(t *testing.T) {
	cause := errors.New("connection reset")
	wrapped := WrapError(ErrorKubernetes, "load PVC", "API request failed", cause)

	if got, want := wrapped.Error(), "load PVC: API request failed"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}

	if !errors.Is(wrapped, cause) {
		t.Fatal("wrapped error does not expose its cause")
	}

	var typed *Error
	if !errors.As(wrapped, &typed) {
		t.Fatalf("errors.As(%T) failed", wrapped)
	}

	if typed.Category != ErrorKubernetes || typed.Op != "load PVC" ||
		typed.Message != "API request failed" {
		t.Fatalf("typed error = %#v", typed)
	}

	if got := CategoryOf(fmtWrapped(wrapped)); got != ErrorKubernetes {
		t.Fatalf("CategoryOf(nested) = %q, want %q", got, ErrorKubernetes)
	}

	withoutOperation := NewError(ErrorValidation, "", "invalid input")
	if got := withoutOperation.Error(); got != "invalid input" {
		t.Fatalf("Error() without operation = %q", got)
	}
}

func TestCategoryAndExitCodeDefaults(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		category ErrorCategory
		exitCode int
	}{
		{name: "nil", category: ErrorInternal, exitCode: 1},
		{name: "plain", err: errors.New("plain"), category: ErrorInternal, exitCode: 1},
		{
			name:     "validation",
			err:      NewError(ErrorValidation, "op", "message"),
			category: ErrorValidation,
			exitCode: 2,
		},
		{
			name:     "precondition",
			err:      NewError(ErrorPrecondition, "op", "message"),
			category: ErrorPrecondition,
			exitCode: 3,
		},
		{
			name:     "conflict",
			err:      NewError(ErrorConflict, "op", "message"),
			category: ErrorConflict,
			exitCode: 4,
		},
		{
			name:     "kubernetes",
			err:      NewError(ErrorKubernetes, "op", "message"),
			category: ErrorKubernetes,
			exitCode: 5,
		},
		{name: "copy", err: NewError(ErrorCopy, "op", "message"), category: ErrorCopy, exitCode: 6},
		{
			name:     "timeout",
			err:      NewError(ErrorTimeout, "op", "message"),
			category: ErrorTimeout,
			exitCode: 7,
		},
		{
			name:     "unknown category",
			err:      NewError(ErrorCategory("future"), "op", "message"),
			category: ErrorCategory("future"),
			exitCode: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CategoryOf(tc.err); got != tc.category {
				t.Fatalf("CategoryOf() = %q, want %q", got, tc.category)
			}

			if got := ExitCode(tc.err); got != tc.exitCode {
				t.Fatalf("ExitCode() = %d, want %d", got, tc.exitCode)
			}
		})
	}
}

func fmtWrapped(err error) error {
	return fmt.Errorf("outer: %w", err)
}

func TestWrapErrorMessageDoesNotExposeCause(t *testing.T) {
	err := WrapError(ErrorInternal, "persist", "session write failed", errors.New("secret token"))
	if strings.Contains(err.Error(), "secret token") {
		t.Fatalf("public message exposed cause: %q", err.Error())
	}
}
