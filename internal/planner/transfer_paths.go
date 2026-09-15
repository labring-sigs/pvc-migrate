package planner

import (
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func recordTransferScopeChecks(
	result checkRecorder,
	namespace string,
	volumes []v1alpha1.VolumeSpec,
	severity domain.CheckSeverity,
) {
	for _, volume := range volumes {
		if volume.TransferScope == nil {
			continue
		}

		result.AddCheck(domain.Check{
			Name:     domain.CheckNameTransferScope,
			Passed:   true,
			Severity: severity,
			Message: transferScopePlanMessage(
				namespace,
				volume.SourcePVC.Name,
				volume.TransferScope,
			),
		})
	}
}

func transferScopePlanMessage(namespace, pvcName string, scope *v1alpha1.TransferScope) string {
	source := "the full source volume"
	if domain.SourceTransferPath(scope) != domain.VolumeRootPath {
		source = fmt.Sprintf("source directory %q", scope.SourcePath)
	}

	message := fmt.Sprintf(
		"PVC %s/%s copies %s into destination directory %q",
		namespace,
		pvcName,
		source,
		domain.DestinationTransferPath(scope),
	)
	if domain.SourceTransferPath(scope) != domain.VolumeRootPath {
		message += "; content outside the selected source directory is excluded"
	}

	if domain.DestinationTransferPath(scope) != domain.VolumeRootPath {
		message += "; destination paths outside the selected directory are not populated by this transfer"
	}

	return message
}
