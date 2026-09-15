package planner

import (
	"context"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func validateTransferReferences(
	volumes []v1alpha1.VolumeRequest,
	resolved []v1alpha1.VolumeSpec,
) error {
	for _, requested := range volumes {
		found := false
		for _, resolved := range resolved {
			if resolved.SourcePVC.Name != requested.SourcePVC.Name {
				continue
			}

			found = true

			if err := checkReference(&requested.SourcePVC, resolved.SourcePVC); err != nil {
				return err
			}

			if err := checkReference(requested.SourcePV, resolved.SourcePV); err != nil {
				return err
			}

			if err := checkReference(
				requested.DestinationPVC,
				resolved.DestinationPVC,
			); err != nil {
				return err
			}
		}

		if !found {
			return domain.NewError(
				domain.ErrorPrecondition,
				"plan workflow",
				"requested PVC is not part of the selected workload: "+requested.SourcePVC.Name,
			)
		}
	}

	return nil
}

func applyWorkflowVolumes(
	state *planState,
	volumes []v1alpha1.VolumeRequest,
	defaults v1alpha1.TransferOptions,
) error {
	requests := make(map[string]v1alpha1.VolumeRequest, len(volumes))
	for _, request := range volumes {
		name := request.SourcePVC.Name
		if _, exists := requests[name]; exists {
			return domain.NewError(
				domain.ErrorValidation,
				"plan workflow",
				"duplicate source PVC: "+name,
			)
		}

		requests[name] = request
	}

	for index, name := range state.pvcNames {
		request := requests[name]
		delete(requests, name)

		capacity := defaults.DestinationCapacity
		if request.Capacity != "" {
			capacity = request.Capacity
		}

		state.requestedCapacities[index] = capacity
		if request.DestinationPVC != nil {
			state.destinationPVCs[index] = request.DestinationPVC.Name
		}

		sourcePath, destinationPath := defaults.SourcePath, defaults.DestinationPath
		if request.TransferScope != nil {
			sourcePath = request.TransferScope.SourcePath
			destinationPath = request.TransferScope.DestinationPath
		}

		if sourcePath != "" || destinationPath != "" {
			scope, err := domain.NewTransferScope(sourcePath, destinationPath)
			if err != nil {
				return domain.WrapError(
					domain.ErrorValidation,
					"plan workflow",
					"invalid transfer paths for "+name,
					err,
				)
			}

			state.transferScopes[index] = scope
		}
	}

	for name := range requests {
		return domain.NewError(
			domain.ErrorValidation,
			"plan workflow",
			"PVC override is not part of the selected workload: "+name,
		)
	}

	return nil
}

func checkReference(
	request *v1alpha1.LocalResourceReference,
	resolved v1alpha1.LocalResourceReference,
) error {
	if request == nil {
		return nil
	}

	if request.Name != resolved.Name || (request.UID != "" && request.UID != resolved.UID) ||
		(request.ResourceVersion != "" && request.ResourceVersion != resolved.ResourceVersion) ||
		(request.Kind != "" && request.Kind != resolved.Kind) ||
		(request.APIVersion != "" && request.APIVersion != resolved.APIVersion) {
		return domain.NewError(
			domain.ErrorConflict,
			"plan workflow",
			fmt.Sprintf(
				"%s %s does not match the requested identity constraints",
				resolved.Kind,
				resolved.Name,
			),
		)
	}

	return nil
}

func (p *Planner) newTransferPlanState(
	options planOptions,
	transfer v1alpha1.TransferOptions,
	volumes []v1alpha1.VolumeRequest,
) planState {
	options.Volumes = volumes
	options.TransferOptions = transfer

	state := newPlanState(p, options)
	p.validateStorageInputs(state.plan, state.options)

	return state
}

func (p *Planner) completeTransferPlan(
	ctx context.Context,
	state *planState,
	volumes []v1alpha1.VolumeRequest,
) (*domain.TransferPlan, error) {
	p.finalizePlan(ctx, state)

	if state.plan.Ready {
		if err := validateTransferReferences(
			volumes,
			state.volumeSpecs,
		); err != nil {
			return nil, err
		}
	}

	return state.plan, nil
}

func requestedPVCNames(volumes []v1alpha1.VolumeRequest) []string {
	names := make([]string, 0, len(volumes))
	for _, volume := range volumes {
		names = append(names, volume.SourcePVC.Name)
	}

	return uniqueInOrder(names)
}
