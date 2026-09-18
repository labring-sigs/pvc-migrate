package backup

import (
	"context"
	"slices"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func validateRestoreObject(object *v1alpha1.Restore) error {
	if object == nil || object.Name == "" || object.Namespace == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"restore",
			"workflow name and namespace are required",
		)
	}

	if object.DeletionTimestamp == nil {
		if object.Status.ObservedGeneration != 0 &&
			object.Status.ObservedGeneration != object.Generation {
			return domain.NewError(
				domain.ErrorConflict,
				"restore",
				"workflow spec changed after planning",
			)
		}

		if err := validateRestoreSpecPlan(object.Spec, object.Status.Plan); err != nil {
			return err
		}
	}

	if err := validateRepositoryStatus(
		object.Status.WorkflowStatus,
		object.Status.Plan != nil,
	); err != nil {
		return err
	}

	return validateRestoreCheckpoints(object.Namespace, object.Status)
}

func validateRestoreSpecPlan(spec v1alpha1.RestoreSpec, plan *v1alpha1.RestorePlan) error {
	if spec.DestinationPVC.Name == "" || spec.RepositoryRef.Name == "" || spec.Name == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"restore",
			"destination PVC, repository and backup name are required",
		)
	}

	if err := domain.ValidateUnusedStoragePolicy(spec.UnusedStoragePolicy); err != nil {
		return err
	}

	if plan == nil {
		return nil
	}

	mode := spec.DestinationAccessMode
	if spec.CreatePVC && mode == "" {
		mode = string(corev1.ReadWriteOnce)
	}

	if plan.DestinationPVC.Name != spec.DestinationPVC.Name ||
		(spec.DestinationPVC.UID != "" && spec.DestinationPVC.UID != plan.DestinationPVC.UID) ||
		plan.RepositoryRef != spec.RepositoryRef || plan.Name != spec.Name || plan.Path != spec.Path ||
		plan.CreatePVC != spec.CreatePVC || plan.DestinationStorageClass != spec.DestinationStorageClass ||
		plan.DestinationAccessMode != mode || plan.DestinationCapacity != spec.DestinationCapacity ||
		plan.AllowMounted != spec.AllowMounted || plan.TargetNode != spec.TargetNode ||
		plan.DeleteExtraneous != spec.DeleteExtraneous || plan.UnusedStoragePolicy != spec.UnusedStoragePolicy {
		return domain.NewError(
			domain.ErrorConflict,
			"restore",
			"execution plan differs from the requested restore",
		)
	}

	return nil
}

func validateRestoreCheckpoints(namespace string, status v1alpha1.RestoreStatus) error {
	plan := status.Plan
	if plan == nil {
		if status.Repository != nil || status.DestinationPVC != nil || status.DestinationPV != nil {
			return domain.NewError(
				domain.ErrorValidation,
				"restore",
				"resource checkpoints require an execution plan",
			)
		}

		return nil
	}

	if plan.DestinationPVC.Name == "" || (!plan.CreatePVC && plan.DestinationPVC.UID == "") ||
		plan.RepositoryRef.Name == "" ||
		plan.Name == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"restore",
			"execution plan requires destination identity and repository location",
		)
	}

	if ref := status.DestinationPVC; ref != nil {
		if ref.Namespace != namespace || ref.Name != plan.DestinationPVC.Name || ref.UID == "" ||
			(plan.DestinationPVC.UID != "" && ref.UID != plan.DestinationPVC.UID) {
			return domain.NewError(
				domain.ErrorConflict,
				"restore",
				"destination checkpoint differs from the planned PVC",
			)
		}
	}

	if ref := status.DestinationPV; ref != nil {
		if status.DestinationPVC == nil || ref.Name == "" || ref.UID == "" || ref.Namespace != "" {
			return domain.NewError(
				domain.ErrorValidation,
				"restore",
				"PV checkpoint requires a PVC checkpoint and cluster-scoped identity",
			)
		}
	}

	phase := status.Phase
	if phase == domain.PhaseFailed {
		phase = status.ResumeFrom
	}

	if (phase == domain.PhaseWarmCopied || phase == domain.PhaseCompleted) &&
		(status.DestinationPVC == nil || status.DestinationPV == nil) {
		return domain.NewError(
			domain.ErrorValidation,
			"restore",
			"completed transfer requires destination PVC and PV checkpoints",
		)
	}

	return nil
}

func (r *RestoreExecutor) Validate(ctx context.Context, object *v1alpha1.Restore) error {
	if err := validateRestoreObject(object); err != nil {
		return err
	}

	if object.Status.Plan == nil || object.Status.Phase == domain.PhaseCompleted ||
		object.Status.Phase == domain.PhaseAborted {
		return nil
	}

	if object.Status.Phase == domain.PhaseAborting ||
		(object.Status.Phase == domain.PhaseFailed && object.Status.ResumeFrom == domain.PhaseAborting) {
		return r.ValidateAbort(ctx, object)
	}

	if object.Status.Phase == domain.PhaseWarmCopied ||
		(object.Status.Phase == domain.PhaseFailed && object.Status.ResumeFrom == domain.PhaseWarmCopied) {
		return r.validateTransferredDestination(ctx, object)
	}

	_, _, err := r.validatePlan(ctx, object)

	return err
}

// Prepare captures repository and existing destination identities together with
// the plan. The caller commits this status in a single transaction.
func (r *RestoreExecutor) Prepare(ctx context.Context, object *v1alpha1.Restore) error {
	if err := validateRestoreObject(object); err != nil {
		return err
	}

	if object.Status.Plan == nil || object.Status.Phase != domain.PhasePlanned {
		return domain.NewError(
			domain.ErrorPrecondition,
			"plan restore",
			"identity binding requires a newly planned restore",
		)
	}

	info, binding, err := r.validatePlan(ctx, object)
	if err != nil {
		return err
	}

	object.Status.Repository = binding.DeepCopy()
	if info != nil {
		ref := kube.PVCReference(info.PVC)

		object.Status.DestinationPVC = &ref
		if info.PV != nil {
			pv := kube.PVReference(info.PV)
			object.Status.DestinationPV = &pv
		}
	}

	return nil
}

func (r *RestoreExecutor) validatePlan(
	ctx context.Context,
	object *v1alpha1.Restore,
) (*PVCInfo, *v1alpha1.BackupRepositoryBindingStatus, error) {
	plan := r.executionPlan(object)

	repository, binding, err := resolveTransferRepository(
		ctx,
		r.config.Repository,
		object.Namespace,
		plan.RepositoryRef.Name,
		plan.Name,
	)
	if err != nil {
		return nil, nil, err
	}

	if err := validateRepositoryMatch(binding, object.Status.Repository); err != nil {
		return nil, nil, err
	}

	manifest, err := readRestoreManifest(ctx, repository, plan.Path)
	if err != nil {
		return nil, nil, err
	}

	info, err := r.validateDestination(
		ctx,
		object.Namespace,
		object.Name,
		plan,
		object.Status.DestinationPV,
		repository.Config(),
		*manifest,
	)

	return info, binding, err
}

func (r *RestoreExecutor) ValidateRepositoryPlan(
	ctx context.Context,
	object *v1alpha1.Restore,
	repository S3RepositoryStore,
) error {
	if err := validateRestoreObject(object); err != nil {
		return err
	}

	if object.Status.Plan == nil {
		return domain.NewError(domain.ErrorPrecondition, "restore", "execution plan is required")
	}

	plan := r.executionPlan(object)

	manifest, err := readRestoreManifest(ctx, repository, plan.Path)
	if err != nil {
		return err
	}

	_, err = r.validateDestination(
		ctx,
		object.Namespace,
		object.Name,
		plan,
		object.Status.DestinationPV,
		repository.Config(),
		*manifest,
	)

	return err
}

// ValidateDestinationPlan checks local placement and consumers without reading
// credentials. The controller subsequently verifies the published inventory
// and its authoritative capacity before persisting the execution plan.
func (r *RestoreExecutor) ValidateDestinationPlan(
	ctx context.Context,
	object *v1alpha1.Restore,
	config objectstore.Config,
) error {
	if err := validateRestoreObject(object); err != nil {
		return err
	}

	if object.Status.Plan == nil {
		return domain.NewError(domain.ErrorPrecondition, "restore", "execution plan is required")
	}

	plan := r.executionPlan(object)
	if _, err := normalizeObjectTransferPath(plan.Path); err != nil {
		return err
	}

	if _, err := kube.NormalizeToolImage(plan.ToolImage); err != nil {
		return err
	}

	capacity, err := deferredRestoreCapacity(plan.DestinationCapacity)
	if err != nil {
		return err
	}

	if err := checkNamespaceAdmissionPolicies(
		ctx,
		r.client,
		object.Namespace,
		"restore",
		objectTransferToolResourceEstimate(),
	); err != nil {
		return err
	}

	pvc, err := r.client.CoreV1().
		PersistentVolumeClaims(object.Namespace).
		Get(ctx, plan.DestinationPVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) && plan.CreatePVC && plan.DestinationPVC.UID == "" {
		if capacity.Sign() > 0 {
			return r.validateCreationAdmission(ctx, object.Namespace, plan, capacity)
		}

		if plan.DestinationStorageClass == "" {
			return domain.NewError(
				domain.ErrorValidation,
				"restore",
				"PVC creation requires a destination storage class",
			)
		}

		if _, err := parseRestoreAccessMode(plan.DestinationAccessMode); err != nil {
			return err
		}

		return kube.ValidateStorageClassPlacement(
			ctx,
			r.client,
			plan.DestinationStorageClass,
			plan.TargetNode,
		)
	}

	if err != nil {
		return err
	}

	if pvc.UID == "" || pvc.UID != plan.DestinationPVC.UID || pvc.DeletionTimestamp != nil {
		return domain.NewError(
			domain.ErrorConflict,
			"restore",
			"destination PVC changed after planning",
		)
	}

	if plan.CreatePVC {
		mode, err := parseRestoreAccessMode(plan.DestinationAccessMode)
		if err != nil {
			return err
		}

		if err := validateRestoreCreatedPVC(
			pvc,
			object.Name,
			config,
			capacity,
			mode,
			plan.DestinationStorageClass,
		); err != nil {
			return err
		}

		if pvc.Status.Phase == corev1.ClaimPending {
			return kube.ValidateStorageClassPlacement(
				ctx,
				r.client,
				plan.DestinationStorageClass,
				plan.TargetNode,
			)
		}
	}

	_, err = r.validateBoundDestination(
		ctx,
		kube.PVCReference(pvc),
		object.Status.DestinationPV,
		capacity,
		plan.AllowMounted,
		plan.TargetNode,
	)

	return err
}

func (r *RestoreExecutor) validateDestination(
	ctx context.Context,
	namespace, id string,
	plan v1alpha1.RestorePlan,
	expectedPV *v1alpha1.ObjectReference,
	config objectstore.Config,
	manifest objectstore.Manifest,
) (*PVCInfo, error) {
	path, err := normalizeObjectTransferPath(plan.Path)
	if err != nil {
		return nil, err
	}

	if path != plan.Path || manifest.Path != plan.Path {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"restore",
			"execution plan requires a normalized path matching the backup",
		)
	}

	if _, err := kube.NormalizeToolImage(plan.ToolImage); err != nil {
		return nil, err
	}

	capacity, err := restoreDestinationCapacity(manifest, plan.DestinationCapacity)
	if err != nil {
		return nil, err
	}

	if manifest.VolumeMode != string(corev1.PersistentVolumeFilesystem) {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"restore",
			"restore requires a Filesystem backup",
		)
	}

	if err := checkNamespaceAdmissionPolicies(
		ctx,
		r.client,
		namespace,
		"restore",
		objectTransferToolResourceEstimate(),
	); err != nil {
		return nil, err
	}

	pvc, err := r.client.CoreV1().
		PersistentVolumeClaims(namespace).
		Get(ctx, plan.DestinationPVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if plan.DestinationPVC.UID != "" || expectedPV != nil {
			return nil, domain.NewError(
				domain.ErrorConflict,
				"restore",
				"recorded destination PVC disappeared; it cannot be recreated",
			)
		}

		if !plan.CreatePVC {
			return nil, err
		}

		return nil, r.validateCreationAdmission(ctx, namespace, plan, capacity)
	}

	if err != nil {
		return nil, err
	}

	if pvc.UID == "" || pvc.DeletionTimestamp != nil ||
		(plan.DestinationPVC.UID != "" && pvc.UID != plan.DestinationPVC.UID) {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"restore",
			"destination PVC identity changed or is deleting",
		)
	}

	if plan.CreatePVC {
		mode, err := parseRestoreAccessMode(plan.DestinationAccessMode)
		if err != nil {
			return nil, err
		}

		if err := validateRestoreCreatedPVC(
			pvc,
			id,
			config,
			capacity,
			mode,
			plan.DestinationStorageClass,
		); err != nil {
			return nil, err
		}

		if (pvc.Status.Phase == "" || pvc.Status.Phase == corev1.ClaimPending) &&
			expectedPV == nil {
			if err := kube.ValidateStorageClassPlacement(
				ctx,
				r.client,
				plan.DestinationStorageClass,
				plan.TargetNode,
			); err != nil {
				return nil, err
			}

			return &PVCInfo{PVC: pvc}, nil
		}
	}

	return r.validateBoundDestination(
		ctx,
		kube.PVCReference(pvc),
		expectedPV,
		capacity,
		plan.AllowMounted,
		plan.TargetNode,
	)
}

func (r *RestoreExecutor) validateBoundDestination(
	ctx context.Context,
	expectedPVC v1alpha1.ObjectReference,
	expectedPV *v1alpha1.ObjectReference,
	capacity resource.Quantity,
	allowMounted bool,
	targetNode string,
) (*PVCInfo, error) {
	info, err := inspectRestorePVC(
		ctx,
		r.client,
		expectedPVC.Namespace,
		expectedPVC.Name,
		allowMounted,
	)
	if err != nil {
		return nil, err
	}

	if info.PVC.UID != expectedPVC.UID ||
		(expectedPV != nil && (expectedPV.UID != info.PV.UID || expectedPV.Name != info.PV.Name)) {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"restore",
			"destination identity changed after planning",
		)
	}

	if info.Capacity.Cmp(capacity) < 0 || info.Mode != corev1.PersistentVolumeFilesystem {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"restore",
			"destination PVC must be Filesystem with sufficient restore capacity",
		)
	}

	node, err := preflightRestoreToolNode(ctx, r.client, targetNode, info)
	if err != nil {
		return nil, err
	}

	consumer, err := rwoConsumerNode(info, restoreSchedulingPhase)
	if err != nil {
		return nil, err
	}

	if _, err := selectRestoreToolNode(targetNode, consumer, node); err != nil {
		return nil, err
	}

	return info, nil
}

func (r *RestoreExecutor) validateCreationAdmission(
	ctx context.Context,
	namespace string,
	plan v1alpha1.RestorePlan,
	capacity resource.Quantity,
) error {
	if plan.DestinationStorageClass == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"restore",
			"PVC creation requires a destination storage class",
		)
	}

	if _, err := parseRestoreAccessMode(plan.DestinationAccessMode); err != nil {
		return err
	}

	if err := kube.ValidateStorageClassPlacement(
		ctx,
		r.client,
		plan.DestinationStorageClass,
		plan.TargetNode,
	); err != nil {
		return err
	}

	report, err := kube.CheckPVCAdmissionPolicies(
		ctx,
		r.client,
		[]kube.PVCAdmissionChange{
			{
				Namespace:             namespace,
				Name:                  plan.DestinationPVC.Name,
				RequestedStorage:      capacity,
				RequestedStorageClass: plan.DestinationStorageClass,
			},
		},
	)
	if err != nil {
		return err
	}

	violations := slices.Concat(report.QuotaViolations, report.LimitRangeViolations)
	if len(violations) != 0 {
		return domain.NewError(domain.ErrorPrecondition, "restore", strings.Join(violations, "; "))
	}

	return nil
}

func (r *RestoreExecutor) validateTransferredDestination(
	ctx context.Context,
	object *v1alpha1.Restore,
) error {
	if err := r.toolsStopped(ctx, object); err != nil {
		return err
	}

	pvc, pv := object.Status.DestinationPVC, object.Status.DestinationPV
	if pvc == nil || pv == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"restore",
			"transferred destination identities are missing",
		)
	}

	currentPVC, currentPV, err := verifyPVCIdentity(
		ctx,
		r.client,
		pvc.Namespace,
		pvc.Name,
		string(pvc.UID),
		string(pv.UID),
	)
	if err != nil {
		return err
	}

	if currentPV.Name != pv.Name || currentPVC.DeletionTimestamp != nil ||
		currentPV.DeletionTimestamp != nil {
		return domain.NewError(
			domain.ErrorConflict,
			"restore",
			"transferred destination changed or is being deleted",
		)
	}

	return nil
}
