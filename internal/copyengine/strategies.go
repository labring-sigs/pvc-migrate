package copyengine

import (
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// ResolveStrategies expands auto into a concrete fallback order and owns the
// returned slice, including when the caller supplied an explicit strategy list.
func ResolveStrategies(sourceNamespace, destinationNamespace string, strategies []string) []string {
	if len(strategies) == 0 || (len(strategies) == 1 && strategies[0] == domain.StrategyAuto) {
		if sourceNamespace == destinationNamespace {
			return []string{domain.StrategyMount, domain.StrategyClusterIP}
		}

		return []string{domain.StrategyClusterIP, domain.StrategyLocal}
	}

	return slices.Clone(strategies)
}

func ValidateStrategies(strategies []string) error {
	for _, strategy := range strategies {
		if _, err := strategyValue(strategy); err != nil {
			return err
		}
	}

	return nil
}
