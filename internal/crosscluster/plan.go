package crosscluster

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) Plan(ctx context.Context, options Options) (*Plan, error) {
	if err := s.validateClients(); err != nil {
		return nil, err
	}

	if options.SessionID == "" || options.SessionNamespace == "" ||
		options.SourceNamespace == "" || options.DestinationNamespace == "" {
		return nil, errors.New(
			"session ID, session namespace, and both PVC namespaces are required",
		)
	}

	sourceID, err := kube.Identity(ctx, s.source)
	if err != nil {
		return nil, err
	}

	destID, err := kube.Identity(ctx, s.destination)
	if err != nil {
		return nil, err
	}

	plan := newCrossClusterPlan(options, sourceID, destID)

	destinationClass := s.planCrossClusterEnvironment(ctx, plan, options)
	if destinationClass == nil {
		return plan, nil
	}

	inputs, ok := resolveCrossClusterInputs(plan, options)
	if !ok {
		return plan, nil
	}

	s.planCrossClusterVolumes(ctx, plan, options, destinationClass, inputs)

	if len(plan.Volumes) == len(options.SourcePVCs) {
		s.planCrossClusterPolicies(ctx, plan, options, destinationClass)
	}

	return plan, nil
}

type crossClusterPlanInputs struct {
	destinations []string
	capacities   []string
	sourcePaths  []string
	destPaths    []string
}

func newCrossClusterPlan(options Options, sourceID, destinationID kube.ClusterIdentity) *Plan {
	return &Plan{
		APIVersion:           APIVersion,
		Kind:                 Kind,
		SessionID:            options.SessionID,
		SourceCluster:        sourceID,
		DestinationCluster:   destinationID,
		SourceNamespace:      options.SourceNamespace,
		DestinationNamespace: options.DestinationNamespace,
		Strategies:           normalizeStrategies(options.Strategies),
		Ready:                true,
	}
}

func (s *Service) planCrossClusterEnvironment(
	ctx context.Context,
	plan *Plan,
	options Options,
) *storagev1.StorageClass {
	if err := ValidateSessionID(options.SessionID); err != nil {
		plan.AddCheck("session-id", false, err.Error())
	}

	if plan.SourceCluster.ID == plan.DestinationCluster.ID {
		plan.AddCheck(
			"clusters",
			false,
			"source and destination resolve to the same Kubernetes API endpoint; use single-cluster copy",
		)
	}

	if _, err := kube.NormalizeToolImage(options.ToolImage); err != nil {
		plan.AddCheck("tool-image", false, err.Error())
	}

	_, err := s.destination.Kubernetes.CoreV1().Namespaces().Get(
		ctx,
		options.DestinationNamespace,
		metav1.GetOptions{},
	)
	switch {
	case err != nil && !apierrors.IsNotFound(err):
		plan.AddCheck(
			"destination-namespace",
			false,
			fmt.Sprintf("read destination namespace %s: %v", options.DestinationNamespace, err),
		)
	case apierrors.IsNotFound(err):
		plan.AddCheck(
			"destination-namespace",
			true,
			fmt.Sprintf(
				"destination namespace %s will be created by reserve",
				options.DestinationNamespace,
			),
		)
	default:
		plan.AddCheck(
			"destination-namespace",
			true,
			fmt.Sprintf("destination namespace %s exists", options.DestinationNamespace),
		)
	}

	var destinationClass *storagev1.StorageClass
	if options.DestinationStorageClass == "" {
		plan.AddCheck(
			"destination-storage-class",
			false,
			"--destination-storage-class is required for cross-cluster copy",
		)
	} else if storageClass, getErr := s.destination.Kubernetes.StorageV1().StorageClasses().Get(
		ctx,
		options.DestinationStorageClass,
		metav1.GetOptions{},
	); getErr != nil {
		plan.AddCheck(
			"destination-storage-class",
			false,
			fmt.Sprintf(
				"read destination StorageClass %s: %v",
				options.DestinationStorageClass,
				getErr,
			),
		)
	} else {
		destinationClass = storageClass

		plan.AddCheck(
			"destination-storage-class",
			true,
			fmt.Sprintf(
				"destination StorageClass %s is available",
				options.DestinationStorageClass,
			),
		)
	}

	if destinationClass != nil {
		node, selectErr := s.selectTargetNode(ctx, options.TargetNode, destinationClass)
		if selectErr != nil {
			plan.AddCheck("target-node", false, selectErr.Error())
		} else {
			plan.TargetNode = node.Name
			plan.AddCheck(
				"target-node",
				true,
				fmt.Sprintf(
					"destination node %s is Ready, schedulable, and topology-compatible",
					node.Name,
				),
			)
		}
	}

	if err := validateStrategies(plan.Strategies); err != nil {
		plan.AddCheck("strategy", false, err.Error())
	} else {
		plan.AddCheck(
			"strategy",
			true,
			"cross-cluster transfer uses "+strings.Join(plan.Strategies, ", "),
		)
	}

	return destinationClass
}

func resolveCrossClusterInputs(plan *Plan, options Options) (crossClusterPlanInputs, bool) {
	if len(options.SourcePVCs) == 0 {
		plan.AddCheck("source-pvc", false, "at least one --source-pvc is required")
		return crossClusterPlanInputs{}, false
	}

	seen := make(map[string]struct{}, len(options.SourcePVCs))
	for _, source := range options.SourcePVCs {
		if source == "" {
			plan.AddCheck("source-pvc", false, "source PVC names must not be empty")
			return crossClusterPlanInputs{}, false
		}

		if _, exists := seen[source]; exists {
			plan.AddCheck(
				"source-pvc",
				false,
				fmt.Sprintf(
					"source PVC %s was specified more than once; use one explicit mapping per PVC",
					source,
				),
			)

			return crossClusterPlanInputs{}, false
		}

		seen[source] = struct{}{}
	}

	if len(options.SourcePVCs) > 1 && len(options.DestinationPVCs) == 1 &&
		!strings.Contains(options.DestinationPVCs[0], "=") {
		plan.AddCheck(
			"destination-pvc",
			false,
			"multiple source PVCs require explicit source=destination mappings",
		)

		return crossClusterPlanInputs{}, false
	}

	inputs := crossClusterPlanInputs{}

	var err error
	if inputs.destinations, err = resolveNames(
		options.DestinationPVCs,
		options.SourcePVCs,
	); err != nil {
		plan.AddCheck("destination-pvc", false, err.Error())
		return crossClusterPlanInputs{}, false
	}

	if inputs.capacities, err = resolveValues(
		options.DestinationCapacities,
		options.SourcePVCs,
	); err != nil {
		plan.AddCheck("destination-capacity", false, err.Error())
		return crossClusterPlanInputs{}, false
	}

	if inputs.sourcePaths, err = resolvePaths(options.SourcePaths, options.SourcePVCs); err != nil {
		plan.AddCheck("source-path", false, err.Error())
		return crossClusterPlanInputs{}, false
	}

	if inputs.destPaths, err = resolvePaths(
		options.DestinationPaths,
		options.SourcePVCs,
	); err != nil {
		plan.AddCheck("destination-path", false, err.Error())
		return crossClusterPlanInputs{}, false
	}

	return inputs, true
}

func (s *Service) planCrossClusterVolumes(
	ctx context.Context,
	plan *Plan,
	options Options,
	destinationClass *storagev1.StorageClass,
	inputs crossClusterPlanInputs,
) {
	for index, name := range options.SourcePVCs {
		if volume, ok := s.planCrossClusterVolume(
			ctx,
			plan,
			options,
			destinationClass,
			inputs,
			index,
			name,
		); ok {
			plan.Volumes = append(plan.Volumes, volume)
		}
	}

	if len(plan.Volumes) == len(options.SourcePVCs) {
		plan.AddCheck(
			"source-pvcs",
			true,
			fmt.Sprintf("validated %d source PVC(s)", len(plan.Volumes)),
		)
	}
}

func (s *Service) planCrossClusterVolume(
	ctx context.Context,
	plan *Plan,
	options Options,
	destinationClass *storagev1.StorageClass,
	inputs crossClusterPlanInputs,
	index int,
	name string,
) (VolumePlan, bool) {
	sourcePath, err := domain.NormalizeTransferPath(inputs.sourcePaths[index])
	if err != nil {
		plan.AddCheck(
			"source-path",
			false,
			fmt.Sprintf("source path for %s is invalid: %v", name, err),
		)

		return VolumePlan{}, false
	}

	destinationPath, err := domain.NormalizeTransferPath(inputs.destPaths[index])
	if err != nil {
		plan.AddCheck(
			"destination-path",
			false,
			fmt.Sprintf("destination path for %s is invalid: %v", name, err),
		)

		return VolumePlan{}, false
	}

	existing, err := s.destination.Kubernetes.CoreV1().
		PersistentVolumeClaims(options.DestinationNamespace).
		Get(
			ctx,
			inputs.destinations[index],
			metav1.GetOptions{},
		)
	if err == nil {
		plan.AddCheck(
			"destination-pvc",
			false,
			fmt.Sprintf(
				"destination PVC %s/%s already exists with UID %s",
				existing.Namespace,
				existing.Name,
				existing.UID,
			),
		)

		return VolumePlan{}, false
	}

	if !apierrors.IsNotFound(err) {
		plan.AddCheck(
			"destination-pvc",
			false,
			fmt.Sprintf(
				"read destination PVC %s/%s: %v",
				options.DestinationNamespace,
				inputs.destinations[index],
				err,
			),
		)

		return VolumePlan{}, false
	}

	pvc, err := s.source.Kubernetes.CoreV1().
		PersistentVolumeClaims(options.SourceNamespace).
		Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		plan.AddCheck(
			"source-pvc",
			false,
			fmt.Sprintf("read source PVC %s/%s: %v", options.SourceNamespace, name, err),
		)

		return VolumePlan{}, false
	}

	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		plan.AddCheck(
			"source-pvc",
			false,
			fmt.Sprintf("source PVC %s/%s must be Bound", pvc.Namespace, pvc.Name),
		)

		return VolumePlan{}, false
	}

	if !kube.HasWritableAccessMode(pvc.Spec.AccessModes) {
		plan.AddCheck(
			"access-mode",
			false,
			fmt.Sprintf(
				"source PVC %s/%s has no writable access mode for the destination copy",
				pvc.Namespace,
				pvc.Name,
			),
		)

		return VolumePlan{}, false
	}

	if err := kube.ValidateDestinationAccessModes(
		destinationClass.Provisioner,
		pvc.Spec.AccessModes,
	); err != nil {
		plan.AddCheck(
			"destination-access-modes",
			false,
			fmt.Sprintf(
				"destination StorageClass %s cannot provide source PVC %s/%s access modes: %v; choose a StorageClass with matching access-mode support",
				destinationClass.Name,
				pvc.Namespace,
				pvc.Name,
				err,
			),
		)

		return VolumePlan{}, false
	}

	pv, err := s.source.Kubernetes.CoreV1().
		PersistentVolumes().
		Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		plan.AddCheck(
			"source-pv",
			false,
			fmt.Sprintf("read source PV %s: %v", pvc.Spec.VolumeName, err),
		)

		return VolumePlan{}, false
	}

	if pvc.UID == "" || pv.UID == "" || pv.Spec.ClaimRef == nil ||
		pv.Spec.ClaimRef.UID == "" ||
		pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		plan.AddCheck(
			"source-binding",
			false,
			fmt.Sprintf(
				"source PV %s claimRef does not match PVC %s/%s UID %s",
				pv.Name,
				pvc.Namespace,
				pvc.Name,
				pvc.UID,
			),
		)

		return VolumePlan{}, false
	}

	capacity := pv.Spec.Capacity[corev1.ResourceStorage]
	if capacity.Sign() <= 0 {
		plan.AddCheck(
			"capacity",
			false,
			fmt.Sprintf("source PV %s has no positive storage capacity", pv.Name),
		)

		return VolumePlan{}, false
	}

	destinationCapacity, ok := resolveCrossClusterCapacity(
		plan,
		options,
		name,
		capacity,
		inputs.capacities[index],
	)
	if !ok {
		return VolumePlan{}, false
	}

	s.checkCrossClusterConsumers(ctx, plan, options, pvc)

	if pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		plan.AddCheck(
			"volume-mode",
			false,
			fmt.Sprintf("source PVC %s/%s is not a filesystem volume", pvc.Namespace, pvc.Name),
		)

		return VolumePlan{}, false
	}

	return VolumePlan{
		SourceNamespace:      pvc.Namespace,
		SourcePVC:            pvc.Name,
		SourcePVCUID:         pvc.UID,
		SourcePVUID:          pv.UID,
		DestinationNamespace: options.DestinationNamespace,
		DestinationPVC:       inputs.destinations[index],
		SourceCapacity:       capacity.String(),
		Capacity:             destinationCapacity.String(),
		StorageClass:         options.DestinationStorageClass,
		StorageClassUID:      destinationClass.UID,
		SourcePath:           sourcePath,
		DestinationPath:      destinationPath,
	}, true
}

func resolveCrossClusterCapacity(
	plan *Plan,
	options Options,
	name string,
	sourceCapacity resource.Quantity,
	requested string,
) (resource.Quantity, bool) {
	destinationCapacity := sourceCapacity
	if requested == "" {
		return destinationCapacity, true
	}

	parsed, err := resource.ParseQuantity(requested)
	if err != nil || parsed.Sign() <= 0 {
		plan.AddCheck(
			"destination-capacity",
			false,
			fmt.Sprintf("destination capacity for %s is invalid", name),
		)

		return destinationCapacity, false
	}

	if parsed.Cmp(sourceCapacity) < 0 {
		switch {
		case !options.AllowVolumeShrink:
			plan.AddCheck(
				"destination-capacity",
				false,
				fmt.Sprintf(
					"destination capacity for %s is smaller than source; pass --allow-volume-shrink",
					name,
				),
			)
		case !options.SkipSourceUsageCheck:
			plan.AddCheck(
				"source-usage",
				false,
				fmt.Sprintf(
					"cross-cluster shrink for %s has no trusted storage-backend usage reader; independently verify the selected data fits, then pass --skip-source-usage-check",
					name,
				),
			)
		default:
			plan.AddCheck(
				"source-usage",
				true,
				fmt.Sprintf("source usage check for %s was explicitly skipped", name),
			)
		}
	}

	return parsed, true
}

func (s *Service) checkCrossClusterConsumers(
	ctx context.Context,
	plan *Plan,
	options Options,
	pvc *corev1.PersistentVolumeClaim,
) {
	consumers, err := activeConsumers(ctx, s.source.Kubernetes, pvc.Namespace, pvc.Name)
	switch {
	case err != nil:
		plan.AddCheck("source-consumers", false, err.Error())
	case !options.Online && len(consumers) > 0:
		plan.AddCheck(
			"source-consumers",
			false,
			fmt.Sprintf(
				"source PVC %s/%s has active consumers: %s",
				pvc.Namespace,
				pvc.Name,
				strings.Join(consumers, ", "),
			),
		)
	case options.Online && len(consumers) > 0 && !hasSharedAccessMode(pvc.Spec.AccessModes):
		plan.AddCheck(
			"source-consumers",
			false,
			fmt.Sprintf(
				"online cross-cluster copy requires RWX/ROX while PVC %s/%s has active consumers; a second source tool Pod cannot safely mount this access mode",
				pvc.Namespace,
				pvc.Name,
			),
		)
	case options.Online:
		plan.AddCheck(
			"source-consumers",
			true,
			fmt.Sprintf(
				"online copy source PVC %s/%s has a compatible shared access mode",
				pvc.Namespace,
				pvc.Name,
			),
		)
	}
}
