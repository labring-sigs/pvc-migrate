package planner

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PlanRestore records either an existing destination identity or an explicitly
// requested PVC creation. The requested spec remains unchanged.
func (p *Planner) PlanRestore(ctx context.Context, object *v1alpha1.Restore, image string) error {
	if object == nil || object.Name == "" || object.Namespace == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"plan restore",
			"workflow name and namespace are required",
		)
	}

	if object.DeletionTimestamp != nil || object.Status.Plan != nil ||
		object.Status.DestinationPVC != nil || object.Status.DestinationPV != nil ||
		!repositoryCanPlan(object.Status.WorkflowStatus) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"plan restore",
			"only an unplanned workflow can be planned",
		)
	}

	request := object.Spec
	if request.DestinationPVC.Name == "" || request.Name == "" || request.RepositoryRef.Name == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"plan restore",
			"destination PVC, backup name and repository reference are required",
		)
	}

	plan := &v1alpha1.RestorePlan{
		DestinationPVC:          request.DestinationPVC,
		Path:                    request.Path,
		Name:                    request.Name,
		RepositoryRef:           request.RepositoryRef,
		CreatePVC:               request.CreatePVC,
		DestinationStorageClass: request.DestinationStorageClass,
		DestinationAccessMode:   request.DestinationAccessMode,
		DestinationCapacity:     request.DestinationCapacity,
		AllowMounted:            request.AllowMounted,
		TargetNode:              request.TargetNode,
		DeleteExtraneous:        request.DeleteExtraneous,
		ToolImage:               image,
	}
	if plan.CreatePVC && plan.DestinationAccessMode == "" {
		plan.DestinationAccessMode = string(corev1.ReadWriteOnce)
	}

	pvc, err := p.client.CoreV1().
		PersistentVolumeClaims(object.Namespace).
		Get(ctx, request.DestinationPVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) && request.CreatePVC && request.DestinationPVC.UID == "" &&
		request.DestinationPVC.ResourceVersion == "" {
		object.Status.Plan = plan
		return nil
	}

	if err != nil {
		return err
	}

	if pvc.DeletionTimestamp != nil || pvc.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"plan restore",
			"destination PVC must have a stable identity and not be deleting",
		)
	}

	if err := checkReference(
		&request.DestinationPVC,
		localPlanningReference(kube.PVCReference(pvc)),
	); err != nil {
		return err
	}

	plan.DestinationPVC = localPlanningReference(kube.PVCReference(pvc))
	object.Status.Plan = plan

	return nil
}
