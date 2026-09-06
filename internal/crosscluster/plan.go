package crosscluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
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

	sourceID, destID, err := s.clusterIdentities(ctx)
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
		plan.AddCheck(domain.CheckNameSessionID, false, err.Error())
	}

	if plan.SourceCluster.ID == plan.DestinationCluster.ID {
		plan.AddCheck(
			domain.CheckNameClusters,
			false,
			"source and destination resolve to the same Kubernetes API endpoint; use single-cluster copy",
		)
	}

	if _, err := kube.NormalizeToolImage(options.ToolImage); err != nil {
		plan.AddCheck(domain.CheckNameToolImage, false, err.Error())
	}

	var (
		namespaceErr        error
		sessionNamespaceErr error
		destinationClass    *storagev1.StorageClass
		storageClassErr     error
		environmentReads    sync.WaitGroup
	)
	environmentReads.Go(func() {
		namespaceErr = kube.RequireNamespace(
			ctx,
			s.destination.Kubernetes,
			options.DestinationNamespace,
		)
	})
	environmentReads.Go(func() {
		sessionNamespaceErr = kube.RequireNamespace(
			ctx,
			s.source.Kubernetes,
			options.SessionNamespace,
		)
	})

	if options.DestinationStorageClass != "" {
		environmentReads.Go(func() {
			destinationClass, storageClassErr = s.destination.Kubernetes.StorageV1().
				StorageClasses().Get(ctx, options.DestinationStorageClass, metav1.GetOptions{})
		})
	}

	environmentReads.Wait()

	if sessionNamespaceErr != nil {
		plan.AddCheck(domain.CheckNameNamespace, false, "session "+sessionNamespaceErr.Error())
	}

	if namespaceErr != nil {
		plan.AddCheck(
			domain.CheckNameDestinationNamespace,
			false,
			namespaceErr.Error(),
		)
	} else {
		plan.AddCheck(
			domain.CheckNameDestinationNamespace,
			true,
			fmt.Sprintf("destination namespace %s exists", options.DestinationNamespace),
		)
	}

	switch {
	case options.DestinationStorageClass == "":
		plan.AddCheck(
			domain.CheckNameDestinationStorageClass,
			false,
			"--destination-storage-class is required for cross-cluster copy",
		)
	case storageClassErr != nil:
		plan.AddCheck(
			domain.CheckNameDestinationStorageClass,
			false,
			fmt.Sprintf(
				"read destination StorageClass %s: %v",
				options.DestinationStorageClass,
				storageClassErr,
			),
		)
	default:
		plan.AddCheck(
			domain.CheckNameDestinationStorageClass,
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
			plan.AddCheck(domain.CheckNameTargetNode, false, selectErr.Error())
		} else {
			plan.TargetNode = node.Name
			plan.AddCheck(
				domain.CheckNameTargetNode,
				true,
				fmt.Sprintf(
					"destination node %s is Ready, schedulable, and topology-compatible",
					node.Name,
				),
			)
		}
	}

	if err := validateStrategies(plan.Strategies); err != nil {
		plan.AddCheck(domain.CheckNameStrategy, false, err.Error())
	} else {
		plan.AddCheck(
			domain.CheckNameStrategy,
			true,
			"cross-cluster transfer uses "+strings.Join(plan.Strategies, ", "),
		)
	}

	return destinationClass
}

func resolveCrossClusterInputs(plan *Plan, options Options) (crossClusterPlanInputs, bool) {
	if len(options.SourcePVCs) == 0 {
		plan.AddCheck(domain.CheckNameSourcePVC, false, "at least one --source-pvc is required")
		return crossClusterPlanInputs{}, false
	}

	seen := make(map[string]struct{}, len(options.SourcePVCs))
	for _, source := range options.SourcePVCs {
		if source == "" {
			plan.AddCheck(domain.CheckNameSourcePVC, false, "source PVC names must not be empty")
			return crossClusterPlanInputs{}, false
		}

		if _, exists := seen[source]; exists {
			plan.AddCheck(
				domain.CheckNameSourcePVC,
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
			domain.CheckNameDestinationPVC,
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
		plan.AddCheck(domain.CheckNameDestinationPVC, false, err.Error())
		return crossClusterPlanInputs{}, false
	}

	if inputs.capacities, err = resolveValues(
		options.DestinationCapacities,
		options.SourcePVCs,
	); err != nil {
		plan.AddCheck(domain.CheckNameDestinationCapacity, false, err.Error())
		return crossClusterPlanInputs{}, false
	}

	if inputs.sourcePaths, err = resolvePaths(options.SourcePaths, options.SourcePVCs); err != nil {
		plan.AddCheck(domain.CheckNameSourcePath, false, err.Error())
		return crossClusterPlanInputs{}, false
	}

	if inputs.destPaths, err = resolvePaths(
		options.DestinationPaths,
		options.SourcePVCs,
	); err != nil {
		plan.AddCheck(domain.CheckNameDestinationPath, false, err.Error())
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
	type result struct {
		volume VolumePlan
		checks []Check
		ready  bool
		ok     bool
	}

	results := make([]result, len(options.SourcePVCs))
	parallel.For(len(options.SourcePVCs), func(index int) {
		// planCrossClusterVolume records validation details through AddCheck.
		// Isolate those writes per volume so independent source/target reads can
		// run concurrently, then merge them below in input order.
		volumePlan := *plan
		volumePlan.Checks = nil
		volumePlan.Ready = true

		volume, ok := s.planCrossClusterVolume(
			ctx,
			&volumePlan,
			options,
			destinationClass,
			inputs,
			index,
			options.SourcePVCs[index],
		)
		results[index] = result{
			volume: volume,
			checks: volumePlan.Checks,
			ready:  volumePlan.Ready,
			ok:     ok,
		}
	})

	for _, item := range results {
		plan.Checks = append(plan.Checks, item.checks...)
		if !item.ready {
			plan.Ready = false
		}

		if !item.ok {
			continue
		}

		plan.Volumes = append(plan.Volumes, item.volume)
	}

	if len(plan.Volumes) == len(options.SourcePVCs) {
		plan.AddCheck(
			domain.CheckNameSourcePVCs,
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
			domain.CheckNameSourcePath,
			false,
			fmt.Sprintf("source path for %s is invalid: %v", name, err),
		)

		return VolumePlan{}, false
	}

	destinationPath, err := domain.NormalizeTransferPath(inputs.destPaths[index])
	if err != nil {
		plan.AddCheck(
			domain.CheckNameDestinationPath,
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
			domain.CheckNameDestinationPVC,
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
			domain.CheckNameDestinationPVC,
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
			domain.CheckNameSourcePVC,
			false,
			fmt.Sprintf("read source PVC %s/%s: %v", options.SourceNamespace, name, err),
		)

		return VolumePlan{}, false
	}

	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		plan.AddCheck(
			domain.CheckNameSourcePVC,
			false,
			fmt.Sprintf("source PVC %s/%s must be Bound", pvc.Namespace, pvc.Name),
		)

		return VolumePlan{}, false
	}

	if !kube.HasWritableAccessMode(pvc.Spec.AccessModes) {
		plan.AddCheck(
			domain.CheckNameAccessMode,
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
			domain.CheckNameDestinationAccessModes,
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
			domain.CheckNameSourcePV,
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
			domain.CheckNameSourceBinding,
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
			domain.CheckNameCapacity,
			false,
			fmt.Sprintf("source PV %s has no positive storage capacity", pv.Name),
		)

		return VolumePlan{}, false
	}

	if err := kube.ValidateBoundVolumeCapacity(pvc, pv, nil); err != nil {
		plan.AddCheck(domain.CheckNameCapacity, false, err.Error())

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
			domain.CheckNameVolumeMode,
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
			domain.CheckNameDestinationCapacity,
			false,
			fmt.Sprintf("destination capacity for %s is invalid", name),
		)

		return destinationCapacity, false
	}

	if parsed.Cmp(sourceCapacity) < 0 {
		switch {
		case !options.AllowVolumeShrink:
			plan.AddCheck(
				domain.CheckNameDestinationCapacity,
				false,
				fmt.Sprintf(
					"destination capacity for %s is smaller than source; pass --allow-volume-shrink",
					name,
				),
			)
		case !options.SkipSourceUsageCheck:
			plan.AddCheck(
				domain.CheckNameSourceUsage,
				false,
				fmt.Sprintf(
					"cross-cluster shrink for %s has no trusted storage-backend usage reader; independently verify the selected data fits, then pass --skip-source-usage-check",
					name,
				),
			)
		default:
			plan.AddCheck(
				domain.CheckNameSourceUsage,
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
		plan.AddCheck(domain.CheckNameSourceConsumers, false, err.Error())
	case !options.Online && len(consumers) > 0:
		plan.AddCheck(
			domain.CheckNameSourceConsumers,
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
			domain.CheckNameSourceConsumers,
			false,
			fmt.Sprintf(
				"online cross-cluster copy requires RWX/ROX while PVC %s/%s has active consumers; a second source tool Pod cannot safely mount this access mode",
				pvc.Namespace,
				pvc.Name,
			),
		)
	case options.Online:
		plan.AddCheck(
			domain.CheckNameSourceConsumers,
			true,
			fmt.Sprintf(
				"online copy source PVC %s/%s has a compatible shared access mode",
				pvc.Namespace,
				pvc.Name,
			),
		)
	}
}
