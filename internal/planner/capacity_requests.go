package planner

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"k8s.io/apimachinery/pkg/api/resource"
)

func supportsDestinationCapacity(operation domain.Operation) bool {
	switch operation {
	case domain.OperationReserve,
		domain.OperationCopy,
		domain.OperationMigrate,
		domain.OperationMigratePod:
		return true
	default:
		return false
	}
}

// resolveDestinationCapacities broadcasts one bare value or maps named values
// using source PVC names. Named entries use the form pvc-name=capacity.
func resolveDestinationCapacities(values, pvcNames []string) ([]string, error) {
	volumeCount := len(pvcNames)
	if len(values) == 0 {
		return make([]string, volumeCount), nil
	}

	result := make([]string, volumeCount)
	if len(values) == 1 {
		if _, _, ok := strings.Cut(strings.TrimSpace(values[0]), "="); ok {
			return resolveNamedCapacity(values, pvcNames, result)
		}

		for index := range result {
			result[index] = strings.TrimSpace(values[0])
		}

		return result, nil
	}

	return resolveNamedCapacity(values, pvcNames, result)
}

func resolveNamedCapacity(values, pvcNames, result []string) ([]string, error) {
	known := make(map[string]int, len(pvcNames))
	for index, name := range pvcNames {
		known[name] = index
	}

	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		key, value, ok := strings.Cut(strings.TrimSpace(raw), "=")

		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if !ok || key == "" || value == "" {
			return nil, fmt.Errorf("named destination capacity %q must use pvc-name=capacity", raw)
		}

		index, exists := known[key]
		if !exists {
			return nil, fmt.Errorf("destination capacity references unknown source PVC %q", key)
		}

		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf(
				"destination capacity for source PVC %q is specified more than once",
				key,
			)
		}

		seen[key] = struct{}{}
		result[index] = value
	}

	if len(seen) != len(known) {
		missing := make([]string, 0, len(known)-len(seen))
		for name := range known {
			if _, exists := seen[name]; !exists {
				missing = append(missing, name)
			}
		}

		sort.Strings(missing)

		return nil, fmt.Errorf(
			"destination capacity is missing source PVC mapping(s): %s",
			strings.Join(missing, ","),
		)
	}

	return result, nil
}

func validateDestinationCapacityValue(value string) error {
	if _, mapped, ok := strings.Cut(strings.TrimSpace(value), "="); ok {
		value = strings.TrimSpace(mapped)
	}

	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return err
	}

	if quantity.Sign() <= 0 {
		return fmt.Errorf("capacity %s must be positive", quantity.String())
	}

	return nil
}

func resolveDestinationPVCs(values, pvcNames []string) ([]string, error) {
	result := make([]string, len(pvcNames))
	if len(values) == 0 {
		return result, nil
	}

	if len(values) == 1 && !strings.Contains(values[0], "=") {
		if len(pvcNames) != 1 {
			return nil, errors.New(
				"a bare --destination-pvc value is valid only for one source PVC; use source-pvc-name=destination-pvc-name mappings for multiple PVCs",
			)
		}

		result[0] = strings.TrimSpace(values[0])

		return result, nil
	}

	known := make(map[string]int, len(pvcNames))
	for index, name := range pvcNames {
		known[name] = index
	}

	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		key, value, ok := strings.Cut(strings.TrimSpace(raw), "=")

		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if !ok || key == "" || value == "" {
			return nil, fmt.Errorf(
				"named destination PVC %q must use source-pvc-name=destination-pvc-name",
				raw,
			)
		}

		index, exists := known[key]
		if !exists {
			return nil, fmt.Errorf("destination PVC references unknown source PVC %q", key)
		}

		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf(
				"destination PVC for source PVC %q is specified more than once",
				key,
			)
		}

		seen[key] = struct{}{}
		result[index] = value
	}

	if len(seen) != len(known) {
		missing := make([]string, 0, len(known)-len(seen))
		for name := range known {
			if _, exists := seen[name]; !exists {
				missing = append(missing, name)
			}
		}

		sort.Strings(missing)

		return nil, fmt.Errorf(
			"destination PVC is missing source PVC mapping(s): %s",
			strings.Join(missing, ","),
		)
	}

	return result, nil
}
