package planner

import (
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func resolveTransferScopes(sourceValues, destinationValues, pvcNames []string) ([]*domain.TransferScope, error) {
	sources, sourceSet, err := resolveTransferPathValues("source", sourceValues, pvcNames)
	if err != nil {
		return nil, err
	}
	destinations, destinationSet, err := resolveTransferPathValues("destination", destinationValues, pvcNames)
	if err != nil {
		return nil, err
	}
	scopes := make([]*domain.TransferScope, len(pvcNames))
	for index := range pvcNames {
		if !sourceSet[index] && !destinationSet[index] {
			continue
		}
		scope, scopeErr := domain.NewTransferScope(sources[index], destinations[index])
		if scopeErr != nil {
			return nil, fmt.Errorf("transfer paths for source PVC %q are invalid: %w", pvcNames[index], scopeErr)
		}
		scopes[index] = scope
	}
	return scopes, nil
}

func resolveTransferPathValues(side string, values, pvcNames []string) ([]string, []bool, error) {
	paths := make([]string, len(pvcNames))
	set := make([]bool, len(pvcNames))
	for index := range paths {
		paths[index] = domain.VolumeRootPath
	}
	if len(values) == 0 {
		return paths, set, nil
	}
	known := make(map[string]int, len(pvcNames))
	for index, name := range pvcNames {
		known[name] = index
	}
	for _, raw := range values {
		entry := strings.TrimSpace(raw)
		key, value, mapped := strings.Cut(entry, "=")
		if !mapped {
			if len(pvcNames) != 1 {
				return nil, nil, fmt.Errorf("a bare --%s-path value is valid only for one source PVC; use source-pvc-name=relative-path mappings for multiple PVCs", side)
			}
			key, value = pvcNames[0], entry
		}
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if key == "" || value == "" {
			return nil, nil, fmt.Errorf("--%s-path %q must use source-pvc-name=relative-path; use . for the PVC root", side, raw)
		}
		index, exists := known[key]
		if !exists {
			return nil, nil, fmt.Errorf("--%s-path references unknown source PVC %q", side, key)
		}
		if set[index] {
			return nil, nil, fmt.Errorf("--%s-path for source PVC %q is specified more than once", side, key)
		}
		normalized, normalizeErr := domain.NormalizeTransferPath(value)
		if normalizeErr != nil {
			return nil, nil, fmt.Errorf("--%s-path for source PVC %q is invalid: %w", side, key, normalizeErr)
		}
		paths[index] = normalized
		set[index] = true
	}
	return paths, set, nil
}

func transferScopePlanMessage(namespace, pvcName string, scope *domain.TransferScope) string {
	source := "the full source volume"
	if domain.SourceTransferPath(scope) != domain.VolumeRootPath {
		source = fmt.Sprintf("source directory %q", scope.SourcePath)
	}
	message := fmt.Sprintf("PVC %s/%s copies %s into destination directory %q", namespace, pvcName, source, domain.DestinationTransferPath(scope))
	if domain.SourceTransferPath(scope) != domain.VolumeRootPath {
		message += "; content outside the selected source directory is excluded"
	}
	if domain.DestinationTransferPath(scope) != domain.VolumeRootPath {
		message += "; destination paths outside the selected directory are not populated by this transfer"
	}
	return message
}
