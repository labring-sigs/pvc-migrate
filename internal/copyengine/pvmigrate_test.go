package copyengine_test

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	. "github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
)

func TestOperationIDIsStableAndValid(t *testing.T) {
	request := Request{
		SessionID: "migration-123",
		Source:    v1alpha1.ObjectReference{Namespace: "app", Name: "data"},
		Mode:      ModeFinal,
		Attempt:   2,
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
