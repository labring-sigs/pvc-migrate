package app

import (
	"context"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestIdentityEntryPointsValidateBeforeLocking(t *testing.T) {
	rename := &RenameExecutor{}
	move := &MoveExecutor{}

	tests := []struct {
		name string
		run  func(context.Context) error
	}{
		{"missing rename", func(ctx context.Context) error { return rename.Run(ctx, nil) }},
		{"missing move", func(ctx context.Context) error { return move.Run(ctx, nil) }},
		{
			"empty rename",
			func(ctx context.Context) error { return rename.Run(ctx, &v1alpha1.Rename{}) },
		},
		{
			"empty move",
			func(ctx context.Context) error { return move.Run(ctx, &v1alpha1.Move{}) },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(t.Context()); domain.CategoryOf(err) != domain.ErrorValidation {
				t.Fatalf("expected validation before lock or storage access, got %v", err)
			}
		})
	}
}
