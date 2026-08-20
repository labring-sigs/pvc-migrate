package domain

import (
	"errors"
	"fmt"
)

// ErrorCategory is stable CLI and API-facing error classification.
type ErrorCategory string

const (
	ErrorValidation   ErrorCategory = "validation"
	ErrorPrecondition ErrorCategory = "precondition"
	ErrorConflict     ErrorCategory = "conflict"
	ErrorKubernetes   ErrorCategory = "kubernetes"
	ErrorCopy         ErrorCategory = "copy"
	ErrorTimeout      ErrorCategory = "timeout"
	ErrorInternal     ErrorCategory = "internal"
)

// Error is an operation error with a stable category and optional wrapped cause.
type Error struct {
	Category ErrorCategory `json:"category"            yaml:"category"`
	Op       string        `json:"operation,omitempty" yaml:"operation,omitempty"`
	Message  string        `json:"message"             yaml:"message"`
	Cause    error         `json:"-"                   yaml:"-"`
}

func (e *Error) Error() string {
	if e.Op == "" {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Op, e.Message)
}

func (e *Error) Unwrap() error { return e.Cause }

func NewError(category ErrorCategory, op, message string) error {
	return &Error{Category: category, Op: op, Message: message}
}

func WrapError(category ErrorCategory, op, message string, cause error) error {
	return &Error{Category: category, Op: op, Message: message, Cause: cause}
}

func CategoryOf(err error) ErrorCategory {
	if typed, ok := errors.AsType[*Error](err); ok {
		return typed.Category
	}

	return ErrorInternal
}

func ExitCode(err error) int {
	switch CategoryOf(err) {
	case ErrorValidation:
		return 2
	case ErrorPrecondition:
		return 3
	case ErrorConflict:
		return 4
	case ErrorKubernetes:
		return 5
	case ErrorCopy:
		return 6
	case ErrorTimeout:
		return 7
	default:
		return 1
	}
}
