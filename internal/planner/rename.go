package planner

import (
	"context"
	"fmt"
	"maps"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
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
	if options.Operation == domain.OperationMove {
		if _, err := p.client.CoreV1().Namespaces().Get(ctx, options.DestinationNamespace, metav1.GetOptions{}); err != nil {
			plan.AddCheck(failed("destination-namespace", fmt.Sprintf("read destination namespace %s: %v", options.DestinationNamespace, err)))
			return plan, nil
		}
		plan.AddCheck(passed("destination-namespace", fmt.Sprintf("destination namespace %s exists", options.DestinationNamespace)))
	}
	pvc, err := p.client.CoreV1().PersistentVolumeClaims(options.SourceNamespace).Get(ctx, options.SourcePVC, metav1.GetOptions{})
	if err != nil {
		plan.AddCheck(failed("source-pvc", fmt.Sprintf("read source PVC: %v", err)))
		return plan, nil
	}
	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		plan.AddCheck(failed("source-pvc", "source PVC must be Bound"))
		return plan, nil
	}
	if existing, getErr := p.client.CoreV1().PersistentVolumeClaims(options.DestinationNamespace).Get(ctx, options.DestinationPVC, metav1.GetOptions{}); getErr == nil {
		plan.AddCheck(failed("destination-pvc", fmt.Sprintf("destination PVC %s/%s already exists with UID %s", existing.Namespace, existing.Name, existing.UID)))
	} else if !apierrors.IsNotFound(getErr) {
		plan.AddCheck(failed("destination-pvc", fmt.Sprintf("read destination PVC: %v", getErr)))
	} else {
		plan.AddCheck(passed("destination-pvc", fmt.Sprintf("destination identity %s/%s is available", options.DestinationNamespace, options.DestinationPVC)))
	}
	p.checkPVCReferences(ctx, plan, pvc, nil, domain.OperationRename, false)
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
	pv, err := p.client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		plan.AddCheck(failed("source-pv", fmt.Sprintf("read source PV: %v", err)))
		return plan, nil
	}
	capacity := pv.Spec.Capacity[corev1.ResourceStorage]
	storageClass := ""
	if pvc.Spec.StorageClassName != nil {
		storageClass = *pvc.Spec.StorageClassName
	}
	bindingMode := storagev1.VolumeBindingImmediate
	provisioner := ""
	if storageClass != "" {
		if sc, getErr := p.client.StorageV1().StorageClasses().Get(ctx, storageClass, metav1.GetOptions{}); getErr == nil {
			provisioner = sc.Provisioner
			if sc.VolumeBindingMode != nil {
				bindingMode = *sc.VolumeBindingMode
			}
		}
	}
	mode := corev1.PersistentVolumeFilesystem
	if pvc.Spec.VolumeMode != nil {
		mode = *pvc.Spec.VolumeMode
	}
	destinationRef := domain.ObjectReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: options.DestinationNamespace, Name: options.DestinationPVC}
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
	p.checkLimitRanges(ctx, plan, options.DestinationNamespace, plan.Volumes)
	p.checkQuotas(ctx, plan, options.DestinationNamespace, plan.TemporaryUsage)
	plan.SessionSpec = domain.NewSessionSpec(options.Operation, domain.SessionCommon{
		SourceNamespace:      options.SourceNamespace,
		TemporaryNamespace:   options.DestinationNamespace,
		DestinationNamespace: options.DestinationNamespace,
		SessionNamespace:     options.SessionNamespace,
		Volumes:              []domain.VolumeSpec{volume},
	}, domain.WorkloadSpec{Adapter: domain.WorkloadNone}, false)
	p.checkRenameRBAC(ctx, plan, options.SourceNamespace, options.DestinationNamespace, options.SessionNamespace)
	return plan, nil
}
