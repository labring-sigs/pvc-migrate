package planner

import (
	"context"
	"fmt"
	"maps"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

type RenameOptions struct {
	Operation            domain.Operation
	SessionID            string
	SourceNamespace      string
	SourcePVC            string
	DestinationNamespace string
	DestinationPVC       string
	SessionNamespace     string
}

func (p *Planner) PlanRename(ctx context.Context, options RenameOptions) (*domain.MigrationPlan, error) {
	if options.Operation == "" {
		options.Operation = domain.OperationRename
	}
	destinationNamespaceProvided := options.DestinationNamespace != ""
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
	p.logInfo("PVC identity planning started", "operation", options.Operation, "session", options.SessionID, "source", options.SourceNamespace+"/"+options.SourcePVC, "destination", options.DestinationNamespace+"/"+options.DestinationPVC)
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
		TemporaryUsage:       domain.ResourceEstimate{ByStorageClass: map[string]string{}, PVCsByStorageClass: map[string]int{}},
		RollbackRetention:    domain.ResourceEstimate{ByStorageClass: map[string]string{}, PVCsByStorageClass: map[string]int{}},
		Workload:             domain.WorkloadSpec{Adapter: domain.WorkloadNone},
	}
	if !options.Operation.RebindsPVC() {
		plan.AddCheck(failed("operation", fmt.Sprintf("unsupported PVC identity operation %q", options.Operation)))
		return plan, nil
	}
	if options.Operation == domain.OperationMove && !destinationNamespaceProvided {
		plan.AddCheck(failed("move", "destination namespace is required"))
		return plan, nil
	}
	if options.SourcePVC == "" || options.DestinationPVC == "" {
		plan.AddCheck(failed("identity", "source and destination PVC names are required"))
		return plan, nil
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "session ID", value: options.SessionID},
		{name: "source namespace", value: options.SourceNamespace},
		{name: "destination namespace", value: options.DestinationNamespace},
		{name: "session namespace", value: options.SessionNamespace},
		{name: "source PVC", value: options.SourcePVC},
		{name: "destination PVC", value: options.DestinationPVC},
	} {
		if problems := validation.IsDNS1123Subdomain(field.value); len(problems) > 0 {
			plan.AddCheck(failed("identity", fmt.Sprintf("%s %q is invalid: %v", field.name, field.value, problems)))
		}
	}
	if !plan.Ready {
		return plan, nil
	}
	if options.Operation == domain.OperationRename && options.SourceNamespace != options.DestinationNamespace {
		plan.AddCheck(failed("rename", "rename requires source and destination PVCs in the same namespace; use move for a cross-namespace identity change"))
		return plan, nil
	}
	if options.Operation == domain.OperationMove && options.SourceNamespace == options.DestinationNamespace {
		plan.AddCheck(failed("move", "move requires a destination namespace different from the source namespace; use rename for an in-namespace identity change"))
		return plan, nil
	}
	if options.SourceNamespace == options.DestinationNamespace && options.SourcePVC == options.DestinationPVC {
		plan.AddCheck(failed("rename", "source and destination PVC identities must differ"))
		return plan, nil
	}
	p.logInfo("loading PVC identity cluster inventory", "session", options.SessionID, "source", options.SourceNamespace+"/"+options.SourcePVC, "destination", options.DestinationNamespace+"/"+options.DestinationPVC)
	var destinationNamespaceErr error
	var pvc *corev1.PersistentVolumeClaim
	var pvcErr error
	var existing *corev1.PersistentVolumeClaim
	var destinationPVCErr error
	var pods *corev1.PodList
	var podListErr error
	parallel.ForLimit(4, 4, func(index int) {
		switch index {
		case 0:
			pvc, pvcErr = p.client.CoreV1().PersistentVolumeClaims(options.SourceNamespace).Get(ctx, options.SourcePVC, metav1.GetOptions{})
		case 1:
			existing, destinationPVCErr = p.client.CoreV1().PersistentVolumeClaims(options.DestinationNamespace).Get(ctx, options.DestinationPVC, metav1.GetOptions{})
		case 2:
			pods, podListErr = p.client.CoreV1().Pods(options.SourceNamespace).List(ctx, metav1.ListOptions{})
		case 3:
			if options.Operation == domain.OperationMove {
				_, destinationNamespaceErr = p.client.CoreV1().Namespaces().Get(ctx, options.DestinationNamespace, metav1.GetOptions{})
			}
		}
	})
	if options.Operation == domain.OperationMove {
		if destinationNamespaceErr != nil {
			plan.AddCheck(failed("destination-namespace", fmt.Sprintf("read destination namespace %s: %v", options.DestinationNamespace, destinationNamespaceErr)))
			return plan, nil
		}
		plan.AddCheck(passed("destination-namespace", fmt.Sprintf("destination namespace %s exists", options.DestinationNamespace)))
	}
	if pvcErr != nil {
		plan.AddCheck(failed("source-pvc", fmt.Sprintf("read source PVC: %v", pvcErr)))
		return plan, nil
	}
	if pvc == nil || pvc.Name == "" {
		plan.AddCheck(failed("source-pvc", "read source PVC returned an empty object"))
		return plan, nil
	}
	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		plan.AddCheck(failed("source-pvc", "source PVC must be Bound"))
		return plan, nil
	}
	p.checkPVCFinalizers(plan, pvc, options.Operation)
	switch {
	case destinationPVCErr == nil && existing == nil:
		plan.AddCheck(failed("destination-pvc", "read destination PVC returned an empty object"))
	case destinationPVCErr == nil:
		plan.AddCheck(failed("destination-pvc", fmt.Sprintf("destination PVC %s/%s already exists with UID %s", existing.Namespace, existing.Name, existing.UID)))
	case !apierrors.IsNotFound(destinationPVCErr):
		plan.AddCheck(failed("destination-pvc", fmt.Sprintf("read destination PVC: %v", destinationPVCErr)))
	default:
		plan.AddCheck(passed("destination-pvc", fmt.Sprintf("destination identity %s/%s is available", options.DestinationNamespace, options.DestinationPVC)))
	}
	var podItems []corev1.Pod
	if podListErr == nil && pods == nil {
		podListErr = fmt.Errorf("list Pods in %s returned an empty object", options.SourceNamespace)
	}
	if pods != nil {
		podItems = pods.Items
	}
	p.checkPVCReferencesFromPods(plan, pvc, nil, domain.OperationRename, false, podItems, podListErr)
	if len(pvc.OwnerReferences) > 0 {
		plan.AddCheck(failed("pvc-ownership", "PVC identity changes require a PVC without ownerReferences because its controller may recreate the source name"))
	}
	for _, check := range plan.Checks {
		if check.Name == "pvc-consumers" && check.Severity == domain.SeverityWarning {
			plan.Ready = false
			plan.AddCheck(failed("rename-offline", "PVC identity changes require the source PVC to have zero active Pod references"))
			break
		}
	}
	storageClass := ""
	if pvc.Spec.StorageClassName != nil {
		storageClass = *pvc.Spec.StorageClassName
	}
	var pv *corev1.PersistentVolume
	var pvErr error
	var sc *storagev1.StorageClass
	parallel.ForLimit(2, 2, func(index int) {
		if index == 0 {
			pv, pvErr = p.client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
			return
		}
		if storageClass != "" {
			sc, _ = p.client.StorageV1().StorageClasses().Get(ctx, storageClass, metav1.GetOptions{})
		}
	})
	if pvErr != nil {
		plan.AddCheck(failed("source-pv", fmt.Sprintf("read source PV: %v", pvErr)))
		return plan, nil
	}
	if pv == nil || pv.Name == "" {
		plan.AddCheck(failed("source-pv", "read source PV returned an empty object"))
		return plan, nil
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
	destinationRef := domain.ObjectReference{APIVersion: domain.CoreAPIVersion, Kind: domain.KindPersistentVolumeClaim, Namespace: options.DestinationNamespace, Name: options.DestinationPVC}
	volume := domain.VolumeSpec{
		SourcePVC:           pvcReference(pvc),
		SourcePV:            pvReference(pv),
		SourceReclaimPolicy: pv.Spec.PersistentVolumeReclaimPolicy,
		SourcePVCSpec:       *pvc.Spec.DeepCopy(),
		SourcePVCMetadata: domain.PVCMetadata{
			Labels:          maps.Clone(pvc.Labels),
			Annotations:     filteredPVCAnnotations(pvc.Annotations),
			OwnerReferences: append([]metav1.OwnerReference(nil), pvc.OwnerReferences...),
		},
		DestinationPVC: destinationRef,
		Capacity:       capacity.String(),
		StorageClass:   storageClass,
		AccessModes:    append([]corev1.PersistentVolumeAccessMode(nil), pvc.Spec.AccessModes...),
		VolumeMode:     mode,
	}
	plan.Volumes = []domain.PlannedVolume{{
		SourcePVC:      volume.SourcePVC,
		SourcePV:       volume.SourcePV,
		DestinationPVC: destinationRef,
		Capacity:       volume.Capacity,
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
		ConfigMaps:         1,
		ByStorageClass:     map[string]string{storageClass: requestedCapacity},
		PVCsByStorageClass: map[string]int{storageClass: requestedPVCs},
	}
	plan.SessionSpec = domain.NewSessionSpec(options.Operation, domain.SessionCommon{
		SourceNamespace:      options.SourceNamespace,
		TemporaryNamespace:   options.DestinationNamespace,
		DestinationNamespace: options.DestinationNamespace,
		SessionNamespace:     options.SessionNamespace,
		Volumes:              []domain.VolumeSpec{volume},
	}, domain.WorkloadSpec{Adapter: domain.WorkloadNone}, false, domain.SessionWorkflowOptions{})
	p.logInfo("validating PVC identity cluster policies", "session", options.SessionID, "sourceNamespace", options.SourceNamespace, "destinationNamespace", options.DestinationNamespace)
	runPlanCheckTasks(plan, []planCheckTask{
		func(result *domain.MigrationPlan) {
			p.checkLimitRanges(ctx, result, options.DestinationNamespace, plan.Volumes)
		},
		func(result *domain.MigrationPlan) {
			p.checkQuotas(ctx, result, options.DestinationNamespace, plan.TemporaryUsage)
		},
		func(result *domain.MigrationPlan) {
			p.checkRenameRBAC(ctx, result, options.SourceNamespace, options.DestinationNamespace, options.SessionNamespace)
		},
	})
	return plan, nil
}
