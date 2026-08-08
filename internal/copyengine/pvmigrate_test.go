package copyengine

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
)

func TestOperationIDIsStableAndValid(t *testing.T) {
	request := Request{
		SessionID: "migration-123",
		Source:    domain.ObjectReference{Namespace: "app", Name: "data"},
		Mode:      ModeFinal,
		Attempt:   2,
	}
	first := operationID(request)
	second := operationID(request)
	if first != second {
		t.Fatalf("operation IDs differ: %q != %q", first, second)
	}
	if len(first) > pvmigrate.MaxIDLength {
		t.Fatalf("operation ID length %d exceeds %d", len(first), pvmigrate.MaxIDLength)
	}
}

func TestStrategyValidation(t *testing.T) {
	for _, value := range []string{"mount", "clusterip", "loadbalancer", "nodeport", "local"} {
		if _, err := strategyValue(value); err != nil {
			t.Fatalf("strategy %q: %v", value, err)
		}
	}
	if _, err := strategyValue("exec"); domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("invalid strategy category=%q error=%v", domain.CategoryOf(err), err)
	}
}
