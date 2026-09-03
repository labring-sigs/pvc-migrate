package planner

import (
	"context"
	"fmt"
	"maps"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// pvcIdentityPlanOptions is the shared planning kernel for the independent
// rename and move entry points. It is private so callers cannot accidentally
// build a mixed identity operation by selecting an Operation field.
type pvcIdentityPlanOptions struct {
	Operation            domain.Operation
	SessionID            string
	SourceNamespace      string
	SourcePVC            string
	DestinationNamespace string
	DestinationPVC       string
	SessionNamespace     string
}

// RenamePlanOptions is the dedicated same-namespace PVC rename contract.
type RenamePlanOptions struct {
	SessionID        string
	SourceNamespace  string
	SourcePVC        string
	DestinationPVC   string
	SessionNamespace string
}

// MovePlanOptions is the dedicated cluster-scoped PVC identity contract.
type MovePlanOptions struct {
	SessionID            string
	SourceNamespace      string
	SourcePVC            string
	DestinationNamespace string
	DestinationPVC       string
	SessionNamespace     string
}

func (p *Planner) PlanRenamePVC(
	ctx context.Context,
	options RenamePlanOptions,
) (*domain.MigrationPlan, error) {
	return p.planPVCIdentity(ctx, pvcIdentityPlanOptions{
		Operation: domain.OperationRename, SessionID: options.SessionID,
		SourceNamespace: options.SourceNamespace, SourcePVC: options.SourcePVC,
		DestinationNamespace: options.SourceNamespace, DestinationPVC: options.DestinationPVC,
		SessionNamespace: options.SessionNamespace,
	})
}

func (p *Planner) PlanMovePVC(
	ctx context.Context,
	options MovePlanOptions,
) (*domain.MigrationPlan, error) {
	return p.planPVCIdentity(ctx, pvcIdentityPlanOptions{
		Operation: domain.OperationMove, SessionID: options.SessionID,
		SourceNamespace: options.SourceNamespace, SourcePVC: options.SourcePVC,
		DestinationNamespace: options.DestinationNamespace, DestinationPVC: options.DestinationPVC,
		SessionNamespace: options.SessionNamespace,
	})
}

func (p *Planner) planPVCIdentity(
	ctx context.Context,
	options pvcIdentityPlanOptions,
) (*domain.MigrationPlan, error) {
	options, destinationNamespaceProvided := normalizeRenameOptions(options)

	if options.Operation == domain.OperationMove && options.DestinationPVC == "" {
		options.DestinationPVC = options.SourcePVC
	}

	p.logInfo(
		"PVC identity planning started",
		"operation",
		options.Operation,
		"session",
		options.SessionID,
		"source",
		options.SourceNamespace+"/"+options.SourcePVC,
		"destination",
		options.DestinationNamespace+"/"+options.DestinationPVC,
	)

	kind := "RenamePlan"
	if options.Operation == domain.OperationMove {
		kind = "MovePlan"
	}

	plan := &domain.MigrationPlan{
		APIVersion:           domain.SessionAPIVersion,
		Kind:                 kind,
		SessionID:            options.SessionID,
		SourceNamespace:      options.SourceNamespace,
		TemporaryNamespace:   options.DestinationNamespace,
		DestinationNamespace: options.DestinationNamespace,
		SessionNamespace:     options.SessionNamespace,
		Ready:                true,
		TemporaryUsage: domain.ResourceEstimate{
			ByStorageClass:     map[string]string{},
			PVCsByStorageClass: map[string]int{},
		},
		RollbackRetention: domain.ResourceEstimate{
			ByStorageClass:     map[string]string{},
			PVCsByStorageClass: map[string]int{},
		},
		Workload: domain.WorkloadSpec{Adapter: domain.WorkloadNone},
	}
	if !validateRenameInputs(plan, options, destinationNamespaceProvided) {
		return plan, nil
	}

	if !options.Operation.RebindsPVC() {
		plan.AddCheck(
			failed(
				domain.CheckNameOperation,
				fmt.Sprintf("unsupported PVC identity operation %q", options.Operation),
			),
		)

		return plan, nil
	}

	if !plan.Ready {
		return plan, nil
	}

	p.logInfo(
		"loading PVC identity cluster inventory",
		"session",
		options.SessionID,
		"source",
		options.SourceNamespace+"/"+options.SourcePVC,
		"destination",
		options.DestinationNamespace+"/"+options.DestinationPVC,
	)

	var (
		destinationNamespaceErr error
		pvc                     *corev1.PersistentVolumeClaim
		pvcErr                  error
		existing                *corev1.PersistentVolumeClaim
		destinationPVCErr       error
		pods                    *corev1.PodList
		podListErr              error
	)
	parallel.ForLimit(4, 4, func(index int) {
		switch index {
		case 0:
			pvc, pvcErr = p.client.CoreV1().
				PersistentVolumeClaims(options.SourceNamespace).
				Get(ctx, options.SourcePVC, metav1.GetOptions{})
		case 1:
			existing, destinationPVCErr = p.client.CoreV1().
				PersistentVolumeClaims(options.DestinationNamespace).
				Get(ctx, options.DestinationPVC, metav1.GetOptions{})
		case 2:
			pods, podListErr = p.client.CoreV1().
				Pods(options.SourceNamespace).
				List(ctx, metav1.ListOptions{})
		case 3:
			if options.Operation == domain.OperationMove {
				_, destinationNamespaceErr = p.client.CoreV1().
					Namespaces().
					Get(ctx, options.DestinationNamespace, metav1.GetOptions{})
			}
		}
	})

	if !p.validateRenameInventory(
		plan,
		options,
		destinationNamespaceErr,
		pvc,
		pvcErr,
		existing,
		destinationPVCErr,
		pods,
		podListErr,
	) {
		return plan, nil
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
		return plan, nil
	}

	if pv == nil || pv.Name == "" {
		plan.AddCheck(failed(domain.CheckNameSourcePV, "read source PV returned an empty object"))
		return plan, nil
	}

	if scErr != nil {
		plan.AddCheck(failed(
			domain.CheckNameSourceStorageClass,
			fmt.Sprintf("read source StorageClass %s: %v", storageClass, scErr),
		))

		return plan, nil
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

	p.checkSessionOwnership(ctx, plan, options.SessionNamespace, pvc, pv)
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

	destinationRef := domain.ObjectReference{
		APIVersion: domain.CoreAPIVersion,
		Kind:       domain.KindPersistentVolumeClaim,
		Namespace:  options.DestinationNamespace,
		Name:       options.DestinationPVC,
	}
	volume := domain.VolumeSpec{
		SourcePVC:           kube.PVCReference(pvc),
		SourcePV:            kube.PVReference(pv),
		SourceReclaimPolicy: pv.Spec.PersistentVolumeReclaimPolicy,
		SourcePVCSpec:       *pvc.Spec.DeepCopy(),
		SourcePVCMetadata: domain.PVCMetadata{
			Labels:          maps.Clone(pvc.Labels),
			Annotations:     kube.PVCAnnotationsForRecreation(pvc.Annotations),
			OwnerReferences: append([]metav1.OwnerReference(nil), pvc.OwnerReferences...),
		},
		DestinationPVC: destinationRef,
		Capacity:       capacity.String(),
		SourceCapacity: capacity.String(),
		StorageClass:   storageClass,
		AccessModes:    append([]corev1.PersistentVolumeAccessMode(nil), pvc.Spec.AccessModes...),
		VolumeMode:     mode,
	}
	plan.Volumes = []domain.PlannedVolume{{
		SourcePVC:      volume.SourcePVC,
		SourcePV:       volume.SourcePV,
		DestinationPVC: destinationRef,
		Capacity:       volume.Capacity,
		SourceCapacity: volume.SourceCapacity,
		AccessModes:    volume.AccessModes,
		VolumeMode:     mode,
		StorageClass:   storageClass,
		BindingMode:    bindingMode,
		CSIProvisioner: provisioner,
	}}
	requestedCapacity := capacity.String()

	requestedPVCs := 1
	if options.SourceNamespace == options.DestinationNamespace {
		requestedCapacity = "0"
		requestedPVCs = 0
	}

	plan.TemporaryUsage = domain.ResourceEstimate{
		StorageRequests:    requestedCapacity,
		PVCs:               requestedPVCs,
		ByStorageClass:     map[string]string{storageClass: requestedCapacity},
		PVCsByStorageClass: map[string]int{storageClass: requestedPVCs},
	}
	if options.SessionNamespace == options.DestinationNamespace {
		plan.TemporaryUsage.ConfigMaps = 1
		plan.TemporaryUsage.Leases = 1
	}

	plan.SessionSpec = domain.NewSessionSpec(options.Operation, domain.SessionCommon{
		SourceNamespace:      options.SourceNamespace,
		TemporaryNamespace:   options.DestinationNamespace,
		DestinationNamespace: options.DestinationNamespace,
		SessionNamespace:     options.SessionNamespace,
		Volumes:              []domain.VolumeSpec{volume},
	}, false, domain.SessionWorkflowOptions{})

	p.logInfo(
		"validating PVC identity cluster policies",
		"session",
		options.SessionID,
		"sourceNamespace",
		options.SourceNamespace,
		"destinationNamespace",
		options.DestinationNamespace,
	)

	tasks := []planCheckTask{
		func(result *domain.MigrationPlan) {
			p.checkNamespaceResourcePolicies(
				ctx,
				result,
				options.DestinationNamespace,
				plan.Volumes,
				plan.TemporaryUsage,
			)
		},
		func(result *domain.MigrationPlan) {
			p.checkRenameRBAC(
				ctx,
				result,
				options.SourceNamespace,
				options.DestinationNamespace,
				options.SessionNamespace,
			)
		},
	}
	if options.SessionNamespace != options.DestinationNamespace {
		tasks = append(tasks, func(result *domain.MigrationPlan) {
			p.checkNamespaceResourcePolicies(
				ctx,
				result,
				options.SessionNamespace,
				nil,
				domain.ResourceEstimate{
					StorageRequests:    "0",
					ConfigMaps:         1,
					Leases:             1,
					ByStorageClass:     map[string]string{},
					PVCsByStorageClass: map[string]int{},
				},
			)
		})
	}

	runPlanCheckTasks(plan, tasks)

	return plan, nil
}

func (p *Planner) validateRenameInventory(
	plan *domain.MigrationPlan,
	options pvcIdentityPlanOptions,
	destinationNamespaceErr error,
	pvc *corev1.PersistentVolumeClaim,
	pvcErr error,
	existing *corev1.PersistentVolumeClaim,
	destinationPVCErr error,
	pods *corev1.PodList,
	podListErr error,
) bool {
	if options.Operation == domain.OperationMove {
		if destinationNamespaceErr != nil {
			plan.AddCheck(
				failed(
					domain.CheckNameDestinationNamespace,
					fmt.Sprintf(
						"read destination namespace %s: %v",
						options.DestinationNamespace,
						destinationNamespaceErr,
					),
				),
			)

			return false
		}

		plan.AddCheck(
			passed(
				domain.CheckNameDestinationNamespace,
				fmt.Sprintf("destination namespace %s exists", options.DestinationNamespace),
			),
		)
	}

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

	p.checkPVCFinalizers(plan, pvc, options.Operation)

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
					options.DestinationNamespace,
					options.DestinationPVC,
				),
			),
		)
	}

	if podListErr == nil && pods == nil {
		podListErr = fmt.Errorf("list Pods in %s returned an empty object", options.SourceNamespace)
	}

	var podItems []corev1.Pod
	if pods != nil {
		podItems = pods.Items
	}

	p.checkPVCReferencesFromPods(
		plan,
		pvc,
		nil,
		domain.WorkloadSpec{},
		domain.OperationRename,
		false,
		podItems,
		podListErr,
	)

	if len(pvc.OwnerReferences) > 0 {
		plan.AddCheck(
			failed(
				domain.CheckNamePVCOwnership,
				"PVC identity changes require a PVC without ownerReferences because its controller may recreate the source name",
			),
		)
	}

	for _, check := range plan.Checks {
		if check.Name == "pvc-consumers" && check.Severity == domain.SeverityWarning {
			plan.Ready = false
			plan.AddCheck(
				failed(
					domain.CheckNameRenameOffline,
					"PVC identity changes require the source PVC to have zero active Pod references",
				),
			)

			break
		}
	}

	return true
}

func normalizeRenameOptions(options pvcIdentityPlanOptions) (pvcIdentityPlanOptions, bool) {
	destinationProvided := options.DestinationNamespace != ""
	if options.Operation == "" {
		options.Operation = domain.OperationRename
	}

	if options.SourceNamespace == "" {
		options.SourceNamespace = "default"
	}

	if options.DestinationNamespace == "" {
		options.DestinationNamespace = options.SourceNamespace
	}

	if options.SessionNamespace == "" {
		options.SessionNamespace = "pvc-migrate-system"
	}

	if options.Operation == domain.OperationMove && options.DestinationPVC == "" {
		options.DestinationPVC = options.SourcePVC
	}

	return options, destinationProvided
}

func validateRenameInputs(
	plan *domain.MigrationPlan,
	options pvcIdentityPlanOptions,
	destinationProvided bool,
) bool {
	if !options.Operation.RebindsPVC() {
		return true
	}

	if options.Operation == domain.OperationMove && !destinationProvided {
		plan.AddCheck(failed(domain.CheckNameMove, "destination namespace is required"))
		return false
	}

	if options.SourcePVC == "" || options.DestinationPVC == "" {
		plan.AddCheck(
			failed(domain.CheckNameIdentity, "source and destination PVC names are required"),
		)
		return false
	}

	for _, field := range []struct{ name, value string }{
		{name: "session ID", value: options.SessionID},
		{name: "source namespace", value: options.SourceNamespace},
		{name: "destination namespace", value: options.DestinationNamespace},
		{name: "session namespace", value: options.SessionNamespace},
		{name: "source PVC", value: options.SourcePVC},
		{name: "destination PVC", value: options.DestinationPVC},
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

	if options.Operation == domain.OperationRename &&
		options.SourceNamespace != options.DestinationNamespace {
		plan.AddCheck(
			failed(
				domain.CheckNameRename,
				"rename requires source and destination PVCs in the same namespace; use move for a cross-namespace identity change",
			),
		)
	}

	if options.SourceNamespace == options.DestinationNamespace &&
		options.SourcePVC == options.DestinationPVC {
		plan.AddCheck(
			failed(domain.CheckNameRename, "source and destination PVC identities must differ"),
		)
	}

	return true
}
