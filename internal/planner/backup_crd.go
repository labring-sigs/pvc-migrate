package planner

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PlanBackup resolves the source identity directly into the Backup status.
// Callers must persist the plan before starting the data-plane operation.
func (p *Planner) PlanBackup(ctx context.Context, object *v1alpha1.Backup, image string) error {
	if object == nil || object.Name == "" || object.Namespace == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"plan backup",
			"workflow name and namespace are required",
		)
	}

	if object.DeletionTimestamp != nil || object.Status.Plan != nil ||
		len(
			object.Status.OpenEBSLVMSharedMounts,
		) != 0 || !repositoryCanPlan(object.Status.WorkflowStatus) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"plan backup",
			"only an unplanned workflow can be planned",
		)
	}

	request := object.Spec
	if request.SourcePVC.Name == "" || request.Name == "" || request.RepositoryRef.Name == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"plan backup",
			"source PVC, backup name and repository reference are required",
		)
	}

	if request.OpenEBSLVMEnableShared && !request.Online {
		return domain.NewError(
			domain.ErrorValidation,
			"plan backup",
			"shared mounts require online backup",
		)
	}

	pvc, err := p.client.CoreV1().
		PersistentVolumeClaims(object.Namespace).
		Get(ctx, request.SourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound ||
		pvc.Spec.VolumeName == "" ||
		pvc.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"plan backup",
			"source PVC must be bound with a stable identity and not deleting",
		)
	}

	pv, err := p.client.CoreV1().
		PersistentVolumes().
		Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if err := checkReference(
		&request.SourcePVC,
		localPlanningReference(kube.PVCReference(pvc)),
	); err != nil {
		return err
	}

	if err := checkReference(
		request.SourcePV,
		localPlanningReference(kube.PVReference(pv)),
	); err != nil {
		return err
	}

	if pv.DeletionTimestamp != nil || pv.UID == "" || pv.Spec.ClaimRef == nil ||
		pv.Spec.ClaimRef.UID != pvc.UID || pv.Spec.ClaimRef.Namespace != pvc.Namespace || pv.Spec.ClaimRef.Name != pvc.Name {
		return domain.NewError(
			domain.ErrorConflict,
			"plan backup",
			"source PV is not bound to the selected PVC identity",
		)
	}

	object.Status.Plan = &v1alpha1.BackupPlan{
		SourcePVC: localPlanningReference(kube.PVCReference(pvc)),
		SourcePV:  localPlanningReference(kube.PVReference(pv)),
		Path:      request.Path, Name: request.Name, RepositoryRef: request.RepositoryRef,
		Online: request.Online, OpenEBSLVMEnableShared: request.OpenEBSLVMEnableShared,
		ToolImage: image,
	}

	return nil
}

func repositoryCanPlan(status v1alpha1.WorkflowStatus) bool {
	return status.Phase == "" || status.Phase == domain.PhasePlanned ||
		(status.Phase == domain.PhaseFailed && status.ResumeFrom == domain.PhasePlanned)
}
