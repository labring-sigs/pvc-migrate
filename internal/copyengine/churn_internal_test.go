package copyengine

import (
	"context"
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
)

func TestOperationIDIsStableAndValid(t *testing.T) {
	request := CopyRequest{
		AttemptIdentity: AttemptIdentity{
			SessionID: "migration-123",
			Source:    v1alpha1.ObjectReference{Namespace: "app", Name: "data"},
			Mode:      ModeFinal,
			Attempt:   2,
		},
	}
	first := OperationID(request.AttemptIdentity)

	second := OperationID(request.AttemptIdentity)
	if first != second {
		t.Fatalf("operation IDs differ: %q != %q", first, second)
	}

	if len(first) > pvmigrate.MaxIDLength {
		t.Fatalf("operation ID length %d exceeds %d", len(first), pvmigrate.MaxIDLength)
	}
}

func TestStrategyValidation(t *testing.T) {
	for _, value := range []string{"mount", "clusterip", "loadbalancer", "nodeport", "local"} {
		if _, err := StrategyValueForTest(value); err != nil {
			t.Fatalf("strategy %q: %v", value, err)
		}
	}

	if _, err := StrategyValueForTest("exec"); domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("invalid strategy category=%q error=%v", domain.CategoryOf(err), err)
	}
}

func TestCopyToleratesLiveSourceChurnOnlyWhenRequested(t *testing.T) {
	churnErr := errors.New(
		"pod ns/job failed: the data mover exited with code 23: " +
			"rsync documents this exit code as: Partial transfer due to error",
	)

	tests := []struct {
		name    string
		tolerat bool
		wantErr bool
	}{
		{name: "warm with tolerance reports success", tolerat: true},
		{name: "without tolerance the failure surfaces", tolerat: false, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &PVMigrate{}
			p.run = func(ctx context.Context, m pvmigrate.Migration) error {
				return churnErr
			}

			err := p.Copy(context.Background(), CopyRequest{
				AttemptIdentity: AttemptIdentity{
					Mode: ModeWarm,
					Source: v1alpha1.ObjectReference{
						Kind: "PersistentVolumeClaim", Namespace: "source", Name: "a", UID: "a",
					},
				},
				Destination: CopyDestination{
					Reference: v1alpha1.ObjectReference{
						Kind: "PersistentVolumeClaim", Namespace: "dest", Name: "b", UID: "b",
					},
				},
				Policy: CopyPolicy{TolerateLiveSourceChurn: tc.tolerat},
			}, nil)

			if tc.wantErr && err == nil {
				t.Fatal("expected the churn failure to surface")
			}

			if !tc.wantErr && err != nil {
				t.Fatalf("churn must be tolerated: %v", err)
			}
		})
	}
}
