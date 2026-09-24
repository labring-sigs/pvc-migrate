package planner

import (
	"context"
	"fmt"
	"maps"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

type pvcRebindPlan struct {
	domain.PVCIdentityReport
	identity v1alpha1.PVCIdentityFields
}

func newPVCIdentityReport(
	kind, id, storageNamespace, sourceNamespace, destinationNamespace string,
) domain.PVCIdentityReport {
	return domain.PVCIdentityReport{
		PlanSummary: domain.PlanSummary{
			APIVersion: domain.SessionAPIVersion,
			Kind:       kind,
			SessionID:  id,

			SessionNamespace: storageNamespace,
			Ready:            true,
		}, SourceNamespace: sourceNamespace,

		DestinationNamespace: destinationNamespace,

		TemporaryUsage: domain.ResourceEstimate{
			ByStorageClass:     map[string]string{},
			PVCsByStorageClass: map[string]int{},
		},
	}
}

func (p *Planner) planPVCRebind(
	ctx context.Context,
	report domain.PVCIdentityReport,
	sourcePVC, destinationPVC string,
) *pvcRebindPlan {
	plan := &pvcRebindPlan{PVCIdentityReport: report}
	if !validatePVCRebindInputs(&plan.PVCIdentityReport, sourcePVC, destinationPVC) {
		return plan
	}

	if !plan.Ready {
		return plan
	}

	p.logInfo(
		"loading PVC identity cluster inventory",
		"session",
		plan.SessionID,
		"source",
		plan.SourceNamespace+"/"+sourcePVC,
		"destination",
		plan.DestinationNamespace+"/"+destinationPVC,
	)

	var (
		pvc               *corev1.PersistentVolumeClaim
		pvcErr            error
		existing          *corev1.PersistentVolumeClaim
		destinationPVCErr error
		pods              *corev1.PodList
		podListErr        error
	)
	parallel.ForLimit(3, 3, func(index int) {
		switch index {
		case 0:
			pvc, pvcErr = p.client.CoreV1().
				PersistentVolumeClaims(plan.SourceNamespace).
				Get(ctx, sourcePVC, metav1.GetOptions{})
		case 1:
			existing, destinationPVCErr = p.client.CoreV1().
				PersistentVolumeClaims(plan.DestinationNamespace).
				Get(ctx, destinationPVC, metav1.GetOptions{})
		case 2:
			pods, podListErr = p.client.CoreV1().
				Pods(plan.SourceNamespace).
				List(ctx, metav1.ListOptions{})
		}
	})

	if !p.validateRenameInventory(
		&plan.PlanSummary,
		plan.SourceNamespace,
		plan.DestinationNamespace,
		destinationPVC,
		pvc,
		pvcErr,
		existing,
		destinationPVCErr,
		pods,
		podListErr,
	) {
		return plan
	}

	storageClass := ""
	if pvc.Spec.StorageClassName != nil {
		storageClass = *pvc.Spec.StorageClassName
	}

	var (
		pv    *corev1.PersistentVolume
		pvErr error
		sc    *storagev1.StorageClass
		scErr error
	)
	parallel.ForLimit(2, 2, func(index int) {
		if index == 0 {
			pv, pvErr = p.client.CoreV1().
				PersistentVolumes().
				Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
			return
		}

		if storageClass != "" {
			sc, scErr = p.client.StorageV1().
				StorageClasses().
				Get(ctx, storageClass, metav1.GetOptions{})
		}
	})

	if pvErr != nil {
		plan.AddCheck(failed(domain.CheckNameSourcePV, fmt.Sprintf("read source PV: %v", pvErr)))
		return plan
	}

	if pv == nil || pv.Name == "" {
		plan.AddCheck(failed(domain.CheckNameSourcePV, "read source PV returned an empty object"))
		return plan
	}

	if pv.DeletionTimestamp != nil {
		plan.AddCheck(failed(
			domain.CheckNameSourcePV,
			fmt.Sprintf(
				"PV %s is terminating (deletion requested at %s); wait for the deletion to settle before rebinding",
				pv.Name,
				pv.DeletionTimestamp.UTC().Format(time.RFC3339),
			),
		))

		return plan
	}

	if scErr != nil {
		plan.AddCheck(failed(
			domain.CheckNameSourceStorageClass,
			fmt.Sprintf("read source StorageClass %s: %v", storageClass, scErr),
		))

		return plan
	}

	if !sourceBindingMatches(pvc, pv) {
		plan.AddCheck(
			failed(
				domain.CheckNameSourceBinding,
				fmt.Sprintf(
					"PV %s claimRef does not match PVC %s/%s UID %s",
					pv.Name,
					pvc.Namespace,
					pvc.Name,
					pvc.UID,
				),
			),
		)
	}

	p.checkSessionOwnership(ctx, plan, plan.SessionNamespace, pvc, pv)
	capacity := pv.Spec.Capacity[corev1.ResourceStorage]
	bindingMode := storagev1.VolumeBindingImmediate

	provisioner := ""
	if sc != nil {
		provisioner = sc.Provisioner
		if sc.VolumeBindingMode != nil {
			bindingMode = *sc.VolumeBindingMode
		}
	}

	mode := corev1.PersistentVolumeFilesystem
	if pvc.Spec.VolumeMode != nil {
		mode = *pvc.Spec.VolumeMode
	}

	destinationRef := v1alpha1.ObjectReference{
		APIVersion: domain.CoreAPIVersion,
		Kind:       domain.KindPersistentVolumeClaim,
		Namespace:  plan.DestinationNamespace,
		Name:       destinationPVC,
	}
	plan.identity = v1alpha1.PVCIdentityFields{
		SourcePVC:      localPlanningReference(kube.PVCReference(pvc)),
		SourcePV:       localPlanningReference(kube.PVReference(pv)),
		DestinationPVC: localPlanningReference(destinationRef),
		SourceTemplate: v1alpha1.PVCSourceTemplate{
			Spec: *pvc.Spec.DeepCopy(),
			Metadata: v1alpha1.PVCMetadata{
				Labels:          maps.Clone(pvc.Labels),
				Annotations:     kube.PVCAnnotationsForRecreation(pvc.Annotations),
				OwnerReferences: pvc.DeepCopy().OwnerReferences,
			},
			ReclaimPolicy: pv.Spec.PersistentVolumeReclaimPolicy,
		},
	}
	plan.Volumes = []domain.PlannedVolume{{
		SourcePVC:      kube.PVCReference(pvc),
		SourcePV:       kube.PVReference(pv),
		DestinationPVC: destinationRef,
		Capacity:       capacity.String(),
		SourceCapacity: capacity.String(),
		AccessModes:    append([]corev1.PersistentVolumeAccessMode(nil), pvc.Spec.AccessModes...),
		VolumeMode:     mode,
		StorageClass:   storageClass,
		BindingMode:    bindingMode,
		CSIProvisioner: provisioner,
	}}
	requestedCapacity := capacity.String()

	requestedPVCs := 1
	if plan.SourceNamespace == plan.DestinationNamespace {
		requestedCapacity = "0"
		requestedPVCs = 0
	}

	plan.TemporaryUsage = domain.ResourceEstimate{
		StorageRequests:    requestedCapacity,
		PVCs:               requestedPVCs,
		ByStorageClass:     map[string]string{storageClass: requestedCapacity},
		PVCsByStorageClass: map[string]int{storageClass: requestedPVCs},
	}
	if plan.SessionNamespace == plan.DestinationNamespace {
		if !p.controllerSubmission {
			plan.TemporaryUsage.ConfigMaps = 1
		}

		plan.TemporaryUsage.Leases = 1
	}

	p.logInfo(
		"validating PVC identity cluster policies",
		"session",
		plan.SessionID,
		"sourceNamespace",
		plan.SourceNamespace,
		"destinationNamespace",
		plan.DestinationNamespace,
	)

	p.checkIdentityPlanPolicies(ctx, &plan.PVCIdentityReport)

	return plan
}

func (p *Planner) checkIdentityPlanPolicies(
	ctx context.Context,
	plan *domain.PVCIdentityReport,
) {
	tasks := []planCheckTask{
		func(result checkRecorder) {
			p.checkNamespaceResourcePolicies(
				ctx,
				result,
				plan.DestinationNamespace,
				plan.Volumes,
				plan.TemporaryUsage,
			)
		},
		func(result checkRecorder) {
			p.checkRenameRBAC(
				ctx,
				result,
				plan.SourceNamespace,
				plan.DestinationNamespace,
				plan.SessionNamespace,
			)
		},
	}
	if plan.SessionNamespace != plan.DestinationNamespace {
		tasks = append(tasks, func(result checkRecorder) {
			configMaps := 1
			if p.controllerSubmission {
				configMaps = 0
			}

			p.checkNamespaceResourcePolicies(
				ctx,
				result,
				plan.SessionNamespace,
				nil,
				domain.ResourceEstimate{
					StorageRequests:    "0",
					ConfigMaps:         configMaps,
					Leases:             1,
					ByStorageClass:     map[string]string{},
					PVCsByStorageClass: map[string]int{},
				},
			)
		})
	}

	runPlanCheckTasks(plan, tasks)
}

func (p *Planner) validateRenameInventory(
	plan *domain.PlanSummary,
	sourceNamespace, destinationNamespace, destinationPVC string,
	pvc *corev1.PersistentVolumeClaim,
	pvcErr error,
	existing *corev1.PersistentVolumeClaim,
	destinationPVCErr error,
	pods *corev1.PodList,
	podListErr error,
) bool {
	if pvcErr != nil {
		plan.AddCheck(failed(domain.CheckNameSourcePVC, fmt.Sprintf("read source PVC: %v", pvcErr)))
		return false
	}

	if pvc == nil || pvc.Name == "" {
		plan.AddCheck(failed(domain.CheckNameSourcePVC, "read source PVC returned an empty object"))
		return false
	}

	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		plan.AddCheck(failed(domain.CheckNameSourcePVC, "source PVC must be Bound"))
		return false
	}

	// A requested deletion only waits on protection finalizers while a
	// consumer keeps mounting the claim; rebinding a terminating claim would
	// race the deletion or wait on it forever. Terminating storage is never a
	// rebind source.
	if pvc.DeletionTimestamp != nil {
		plan.AddCheck(failed(
			domain.CheckNameSourcePVC,
			fmt.Sprintf(
				"PVC %s/%s is terminating (deletion requested at %s); wait for the deletion to settle before rebinding",
				pvc.Namespace,
				pvc.Name,
				pvc.DeletionTimestamp.UTC().Format(time.RFC3339),
			),
		))

		return false
	}

	p.checkPVCFinalizers(plan, pvc)

	switch {
	case destinationPVCErr == nil && existing == nil:
		plan.AddCheck(
			failed(domain.CheckNameDestinationPVC, "read destination PVC returned an empty object"),
		)
	case destinationPVCErr == nil:
		plan.AddCheck(
			failed(
				domain.CheckNameDestinationPVC,
				fmt.Sprintf(
					"destination PVC %s/%s already exists with UID %s",
					existing.Namespace,
					existing.Name,
					existing.UID,
				),
			),
		)
	case !apierrors.IsNotFound(destinationPVCErr):
		plan.AddCheck(
			failed(
				domain.CheckNameDestinationPVC,
				fmt.Sprintf("read destination PVC: %v", destinationPVCErr),
			),
		)
	default:
		plan.AddCheck(
			passed(
				domain.CheckNameDestinationPVC,
				fmt.Sprintf(
					"destination identity %s/%s is available",
					destinationNamespace,
					destinationPVC,
				),
			),
		)
	}

	if podListErr == nil && pods == nil {
		podListErr = fmt.Errorf("list Pods in %s returned an empty object", sourceNamespace)
	}

	var podItems []corev1.Pod
	if pods != nil {
		podItems = pods.Items
	}

	consumers, listed := collectPVCConsumers(
		plan,
		pvc,
		podItems,
		podListErr,
		kube.PodPreventsSafePVCDeletion,
	)
	if listed {
		checkIdentityConsumers(plan, pvc, consumers)
	}

	if len(pvc.OwnerReferences) > 0 {
		plan.AddCheck(
			failed(
				domain.CheckNamePVCOwnership,
				"PVC identity changes require a PVC without ownerReferences because its controller may recreate the source name",
			),
		)
	}

	return true
}

func validatePVCRebindInputs(
	plan *domain.PVCIdentityReport,
	sourcePVC, destinationPVC string,
) bool {
	if sourcePVC == "" || destinationPVC == "" {
		plan.AddCheck(
			failed(domain.CheckNameIdentity, "source and destination PVC names are required"),
		)
		return false
	}

	for _, field := range []struct{ name, value string }{
		{name: "session ID", value: plan.SessionID},
		{name: "source namespace", value: plan.SourceNamespace},
		{name: "destination namespace", value: plan.DestinationNamespace},
		{name: "session namespace", value: plan.SessionNamespace},
		{name: "source PVC", value: sourcePVC},
		{name: "destination PVC", value: destinationPVC},
	} {
		if problems := validation.IsDNS1123Subdomain(field.value); len(problems) > 0 {
			plan.AddCheck(
				failed(
					domain.CheckNameIdentity,
					fmt.Sprintf("%s %q is invalid: %v", field.name, field.value, problems),
				),
			)
		}
	}

	if plan.SourceNamespace == plan.DestinationNamespace && sourcePVC == destinationPVC {
		plan.AddCheck(
			failed(domain.CheckNameRename, "source and destination PVC identities must differ"),
		)
	}

	return true
}
