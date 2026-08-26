package app

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) probeToolImage(
	ctx context.Context,
	session *domain.Session,
	targets []kube.ToolProbeTarget,
) ([]kube.ToolImageProbeResult, error) {
	if session == nil || s.config.ToolImageProber == nil {
		return nil, nil
	}

	if len(targets) == 0 {
		return nil, nil
	}

	s.logInfo(
		"tool image validation started",
		"session",
		session.ID,
		"image",
		session.Spec.WorkflowOptions().ToolImage,
		"targets",
		len(targets),
	)

	return s.config.ToolImageProber.Probe(ctx, kube.ToolImageProbeOptions{
		OperationID: session.ID,
		Image:       session.Spec.WorkflowOptions().ToolImage,
		Targets:     targets,
		Timeout:     s.config.HelmTimeout,
		Writer:      s.config.Writer,
		Logger:      s.config.Logger,
	})
}

func reservationToolProbeTargets(session *domain.Session) []kube.ToolProbeTarget {
	if session == nil {
		return nil
	}

	options := session.Spec.WorkflowOptions()
	if options.TargetNode == "" {
		return nil
	}

	return toolProbeTargetsForNamespaces(
		destinationVolumeNamespaces(session),
		options.TargetNode,
		nil,
	)
}

func copyToolProbeTargets(session *domain.Session, mountSourcePVC bool) []kube.ToolProbeTarget {
	if session == nil {
		return nil
	}

	options := session.Spec.WorkflowOptions()
	switch session.Spec.Operation() {
	case domain.OperationCopy, domain.OperationMigrate, domain.OperationMigratePod:
		targets := destinationTransferPathProbeTargets(session, options.TargetNode)
		if options.TargetNode == "" {
			return targets
		}

		targetNamespaces := destinationVolumeNamespaces(session)
		targets = append(
			targets,
			toolProbeTargetsForNamespaces(
				targetNamespaces,
				options.TargetNode,
				[]string{kube.ToolComponentRsync},
			)...,
		)
		needsSSHD := sessionNeedsSourceSSHD(session)

		if slices.Contains(options.Strategies, domain.StrategyLocal) {
			targets = append(
				targets,
				toolProbeTargetsForNamespaces(
					targetNamespaces,
					options.TargetNode,
					[]string{kube.ToolComponentSSHD},
				)...,
			)
		}

		if (!needsSSHD && !mountSourcePVC) || options.SourceNode == "" {
			return targets
		}

		targets = append(
			targets,
			sourceToolProbeTargets(session, options.SourceNode, mountSourcePVC)...,
		)

		return targets
	}

	return nil
}

func sessionNeedsSourceSSHD(session *domain.Session) bool {
	if session == nil {
		return false
	}

	operation := session.Spec.Operation()
	if operation != domain.OperationCopy && operation != domain.OperationMigrate &&
		operation != domain.OperationMigratePod {
		return false
	}

	strategies := session.Spec.WorkflowOptions().Strategies
	if len(strategies) == 0 {
		return true
	}

	for _, strategy := range strategies {
		if strategy != domain.StrategyMount {
			return true
		}
	}

	return false
}

func (s *Service) resolveCopyToolProbeTargets(
	ctx context.Context,
	session *domain.Session,
	mountSourcePVC bool,
) ([]kube.ToolProbeTarget, error) {
	targets := copyToolProbeTargets(session, mountSourcePVC)
	if !sessionNeedsSourceSSHD(session) && !mountSourcePVC {
		return targets, nil
	}

	options := session.Spec.WorkflowOptions()

	sourceTargets, err := s.resolveSourceToolProbeTargets(ctx, session, mountSourcePVC)
	if err != nil {
		return nil, err
	}

	if options.SourceNode == "" {
		targets = append(targets, sourceTargets...)
	}

	if mountSourcePVC {
		if err := s.markSharedOpenEBSLVMProbeMounts(ctx, session, targets); err != nil {
			return nil, err
		}
	}

	return targets, nil
}

func (s *Service) markSharedOpenEBSLVMProbeMounts(
	ctx context.Context,
	session *domain.Session,
	targets []kube.ToolProbeTarget,
) error {
	if session == nil {
		return nil
	}

	probedPVCs := make(map[string]struct{}, len(targets))
	for index := range targets {
		target := &targets[index]
		if target.PVCName == "" || target.SkipPVCMount {
			continue
		}

		probedPVCs[target.Namespace+"/"+target.PVCName] = struct{}{}
	}

	sharedPVCs := make(map[string]bool, len(probedPVCs))
	for volumeIndex := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[volumeIndex]

		key := volume.SourcePVC.Namespace + "/" + volume.SourcePVC.Name
		if _, probed := probedPVCs[key]; !probed {
			continue
		}

		isLVM, shared, err := s.sharedOpenEBSLVMSource(ctx, session, volume)
		if err != nil {
			return err
		}

		if isLVM && !shared {
			active, err := s.sourcePVCIsActive(ctx, volume)
			if err != nil {
				return err
			}

			if active && !session.Spec.WorkflowOptions().OpenEBSLVMEnableShared {
				return activeUnsharedOpenEBSLVMError(session, volume)
			}
		}

		sharedPVCs[key] = shared
	}

	for index := range targets {
		target := &targets[index]
		if target.PVCName != "" && !target.SkipPVCMount &&
			sharedPVCs[target.Namespace+"/"+target.PVCName] {
			target.WritablePVCMount = true
		}
	}

	return nil
}

func activeUnsharedOpenEBSLVMError(session *domain.Session, volume *domain.VolumeSpec) error {
	recovery := "stop all active PVC consumers and retry"
	if session != nil {
		switch session.Spec.Operation() {
		case domain.OperationCopy:
			recovery = "abort this pre-cutover session, clean its retained resources, and rerun the copy without --online"
		case domain.OperationMigrate, domain.OperationMigratePod:
			if session.Spec.WorkflowOptions().OpenEBSLVMEnableShared {
				recovery = "retry the session so temporary shared-mount preparation can use the current active consumer state"
			} else {
				recovery = "abort this pre-cutover session, clean its retained resources, and rerun with --precopy-passes 0 or --openebs-lvm-enable-shared"
			}
		}
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		"warm-copy mount preflight",
		fmt.Sprintf(
			"source PVC %s/%s is active and its OpenEBS LVMVolume does not currently have spec.shared=yes; %s",
			volume.SourcePVC.Namespace,
			volume.SourcePVC.Name,
			recovery,
		),
	)
}

func (s *Service) sharedOpenEBSLVMSource(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
) (bool, bool, error) {
	manager := s.config.OpenEBSLVMSharedVolumeManager
	if manager == nil {
		return false, false, nil
	}

	isLVM, err := s.openEBSLVMSource(ctx, volume)
	if err != nil || !isLVM {
		return false, false, err
	}

	sharedSessionID := ""

	expectedLVMVolume := domain.ObjectReference{}
	if state, found := pendingOpenEBSLVMSharedMount(session, volume); found {
		sharedSessionID = session.ID
		expectedLVMVolume = state.LVMVolume
	}

	shared, err := manager.Shared(ctx, volume.SourcePV, expectedLVMVolume, sharedSessionID)

	return true, shared, err
}

func pendingOpenEBSLVMSharedMount(
	session *domain.Session,
	volume *domain.VolumeSpec,
) (domain.OpenEBSLVMSharedMount, bool) {
	if session == nil || volume == nil {
		return domain.OpenEBSLVMSharedMount{}, false
	}

	for _, state := range session.Status.OpenEBSLVMSharedMounts {
		if state.SourcePV.Name == volume.SourcePV.Name &&
			state.SourcePV.UID == volume.SourcePV.UID {
			return state, true
		}
	}

	return domain.OpenEBSLVMSharedMount{}, false
}

func (s *Service) openEBSLVMSource(ctx context.Context, volume *domain.VolumeSpec) (bool, error) {
	if s == nil || s.client == nil || volume == nil {
		return false, nil
	}

	if volume.SourcePVC.Namespace == "" || volume.SourcePVC.Name == "" ||
		volume.SourcePV.Name == "" {
		return false, domain.NewError(
			domain.ErrorValidation,
			"OpenEBS LVM shared mount",
			"source PVC and PV identities are required",
		)
	}

	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			"OpenEBS LVM shared mount",
			fmt.Sprintf("read source PVC %s/%s", volume.SourcePVC.Namespace, volume.SourcePVC.Name),
			err,
		)
	}

	if pvc.UID != volume.SourcePVC.UID || pvc.Status.Phase != corev1.ClaimBound ||
		pvc.Spec.VolumeName != volume.SourcePV.Name {
		return false, domain.NewError(
			domain.ErrorConflict,
			"OpenEBS LVM shared mount",
			fmt.Sprintf(
				"source PVC %s/%s identity or binding changed",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	pv, err := s.client.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			"OpenEBS LVM shared mount",
			"read source PV "+volume.SourcePV.Name,
			err,
		)
	}

	if pv.UID != volume.SourcePV.UID || pv.Spec.ClaimRef == nil ||
		pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		return false, domain.NewError(
			domain.ErrorConflict,
			"OpenEBS LVM shared mount",
			fmt.Sprintf("source PV %s identity or claimRef changed", volume.SourcePV.Name),
		)
	}

	return pv.Spec.CSI != nil && pv.Spec.CSI.Driver == kube.OpenEBSLVMCSIDriver, nil
}

func (s *Service) ensureConcurrentDestinationMount(
	ctx context.Context,
	session *domain.Session,
	volumeIndex int,
) error {
	if session == nil || !session.Spec.Orchestrated() {
		return nil
	}

	if volumeIndex < 0 || volumeIndex >= len(session.Spec.Volumes) ||
		volumeIndex >= len(session.Status.Volumes) {
		return domain.NewError(
			domain.ErrorInternal,
			"destination shared mount",
			"session volume spec and status are not aligned",
		)
	}

	volume := &session.Spec.Volumes[volumeIndex]
	if !volume.RequiresConcurrentRWOMount() {
		return nil
	}

	expectedPVC := volume.DestinationPVC

	activePVC := session.Status.Volumes[volumeIndex].Activation.ActivePVC
	if activePVC.Namespace != "" || activePVC.Name != "" || activePVC.UID != "" {
		expectedPVC = activePVC
	}

	pv, err := s.client.CoreV1().PersistentVolumes().Get(
		ctx,
		volume.DestinationPV.Name,
		metav1.GetOptions{},
	)
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"destination shared mount",
			"read destination PV "+volume.DestinationPV.Name,
			err,
		)
	}

	if pv.UID != volume.DestinationPV.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"destination shared mount",
			fmt.Sprintf("destination PV %s UID changed", volume.DestinationPV.Name),
		)
	}

	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != kube.OpenEBSLVMCSIDriver {
		return nil
	}

	manager := s.config.OpenEBSLVMSharedVolumeManager
	if manager == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"destination shared mount",
			"OpenEBS LVMVolume manager is required for a multi-consumer RWO destination",
		)
	}

	if !session.Spec.WorkflowOptions().OpenEBSLVMEnableShared {
		return domain.NewError(
			domain.ErrorPrecondition,
			"destination shared mount",
			fmt.Sprintf(
				"destination PV %s is OpenEBS LVM and requires spec.shared=yes for %d concurrent consumers; rerun the migration plan with --openebs-lvm-enable-shared",
				pv.Name,
				volume.ConcurrentConsumers,
			),
		)
	}

	result, err := manager.EnsureShared(
		ctx,
		expectedPVC,
		volume.DestinationPV,
	)
	if err != nil {
		return err
	}

	if result.NeedsChange {
		s.logInfo(
			"OpenEBS LVM destination shared mount configured",
			"destinationPV",
			volume.DestinationPV.Name,
			"resource",
			result.Reference,
		)
	}

	return nil
}

func (s *Service) enableOpenEBSLVMSharedMounts(
	ctx context.Context,
	session *domain.Session,
) (resultErr error) {
	if session == nil || !session.Spec.WorkflowOptions().OpenEBSLVMEnableShared {
		return nil
	}

	manager := s.config.OpenEBSLVMSharedVolumeManager
	if manager == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"OpenEBS LVM shared mount",
			"OpenEBS LVMVolume manager is required when --openebs-lvm-enable-shared is set",
		)
	}
	defer func() {
		if resultErr == nil {
			return
		}

		if err := s.restoreOpenEBSLVMSharedMountsAfterFailure(ctx, session); err != nil {
			resultErr = errors.Join(resultErr, err)
		}
	}()

	type preparedSharedMount struct {
		state     domain.OpenEBSLVMSharedMount
		reference string
	}

	prepared := make([]preparedSharedMount, 0, len(session.Spec.Volumes))
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		isLVM, err := s.openEBSLVMSource(ctx, volume)
		if err != nil {
			return err
		}

		if !isLVM {
			continue
		}

		active, err := s.sourcePVCIsActive(ctx, volume)
		if err != nil {
			return err
		}

		if !active {
			continue
		}

		result, err := manager.PrepareShared(ctx, volume.SourcePV)
		if err != nil {
			return err
		}

		if !result.NeedsChange {
			continue
		}

		state := domain.OpenEBSLVMSharedMount{
			SourcePV:          volume.SourcePV,
			LVMVolume:         result.LVMVolume,
			PreviousShared:    result.PreviousShared,
			PreviousSharedSet: result.PreviousSharedSet,
		}
		prepared = append(prepared, preparedSharedMount{state: state, reference: result.Reference})
	}

	for _, item := range prepared {
		session.Status.OpenEBSLVMSharedMounts = append(
			session.Status.OpenEBSLVMSharedMounts,
			item.state,
		)
		if err := s.persist(ctx, session); err != nil {
			return err
		}

		if err := manager.EnableShared(ctx, session.ID, item.state); err != nil {
			return err
		}

		s.logInfo(
			"OpenEBS LVM shared mount configured",
			"sourcePV",
			item.state.SourcePV.Name,
			"resource",
			item.reference,
			"previousShared",
			item.state.PreviousShared,
		)
	}

	return nil
}

// A temporary shared-volume patch must be reverted even after the operation
// deadline or cancellation fires. Preserve context values such as the session
// lock while giving cleanup its own bounded lifetime.
func (s *Service) restoreOpenEBSLVMSharedMountsAfterFailure(
	ctx context.Context,
	session *domain.Session,
) error {
	if err := sessionFenceError(ctx); err != nil {
		return err
	}

	restoreCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		openEBSLVMSharedMountCleanupTimeout,
	)
	defer cancel()

	return s.restoreOpenEBSLVMSharedMounts(restoreCtx, session)
}

func sessionFenceError(ctx context.Context) error {
	if held, ok := ctx.Value(sessionLockContextKey{}).(heldSessionLock); ok {
		return held.lock.Err()
	}
	return nil
}

func (s *Service) validateOpenEBSLVMSharedMountRestore(
	ctx context.Context,
	session *domain.Session,
) error {
	if session == nil || len(session.Status.OpenEBSLVMSharedMounts) == 0 {
		return nil
	}

	manager := s.config.OpenEBSLVMSharedVolumeManager
	if manager == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"restore OpenEBS LVM shared mount",
			"OpenEBS LVMVolume manager is required to restore session-managed shared mounts",
		)
	}

	for _, state := range session.Status.OpenEBSLVMSharedMounts {
		if err := manager.ValidateRestoreShared(ctx, session.ID, state); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) restoreOpenEBSLVMSharedMounts(
	ctx context.Context,
	session *domain.Session,
) error {
	if session == nil {
		return nil
	}

	if err := sessionFenceError(ctx); err != nil {
		return err
	}

	if err := kube.CleanupSessionToolProbePods(
		ctx,
		s.client,
		session.ID,
		sessionToolProbeNamespaces(session),
	); err != nil {
		return err
	}

	if len(session.Status.OpenEBSLVMSharedMounts) == 0 {
		return nil
	}

	manager := s.config.OpenEBSLVMSharedVolumeManager
	if manager == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"restore OpenEBS LVM shared mount",
			"OpenEBS LVMVolume manager is required to restore session-managed shared mounts",
		)
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	remaining := make([]domain.OpenEBSLVMSharedMount, 0, len(session.Status.OpenEBSLVMSharedMounts))
	for index, state := range session.Status.OpenEBSLVMSharedMounts {
		if err := sessionFenceError(ctx); err != nil {
			return err
		}

		if err := manager.RestoreShared(ctx, session.ID, state); err != nil {
			remaining = append(remaining, session.Status.OpenEBSLVMSharedMounts[index:]...)

			session.Status.OpenEBSLVMSharedMounts = remaining
			if persistErr := s.persist(ctx, session); persistErr != nil {
				return errors.Join(err, persistErr)
			}

			return err
		}

		s.logInfo(
			"OpenEBS LVM shared mount restored",
			"session",
			session.ID,
			"sourcePV",
			state.SourcePV.Name,
			"previousShared",
			state.PreviousShared,
			"previousSharedSet",
			state.PreviousSharedSet,
		)
	}

	if err := sessionFenceError(ctx); err != nil {
		return err
	}

	session.Status.OpenEBSLVMSharedMounts = nil

	return s.persist(ctx, session)
}

func sessionToolProbeNamespaces(session *domain.Session) []string {
	if session == nil {
		return nil
	}

	namespaces := []string{
		session.Spec.SourceNamespace,
		session.Spec.TemporaryNamespace,
		session.Spec.DestinationNamespace,
	}
	for _, volume := range session.Spec.Volumes {
		namespaces = append(namespaces, volume.SourcePVC.Namespace, volume.DestinationPVC.Namespace)
	}

	result := make([]string, 0, len(namespaces))
	for _, namespace := range namespaces {
		if namespace != "" && !slices.Contains(result, namespace) {
			result = append(result, namespace)
		}
	}

	sort.Strings(result)

	return result
}

func (s *Service) sourcePVCIsActive(ctx context.Context, volume *domain.VolumeSpec) (bool, error) {
	if s == nil || s.client == nil || volume == nil {
		return false, nil
	}

	pods, err := s.client.CoreV1().Pods(volume.SourcePVC.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			"OpenEBS LVM shared mount",
			"list Pods in namespace "+volume.SourcePVC.Namespace,
			err,
		)
	}

	for index := range pods.Items {
		if kube.ActivePodUsesPVC(&pods.Items[index], volume.SourcePVC.Name) {
			return true, nil
		}
	}

	return false, nil
}

type sourcePodListResult struct {
	pods []corev1.Pod
	err  error
}

func (s *Service) resolveSourceToolProbeTargets(
	ctx context.Context,
	session *domain.Session,
	mountSourcePVC bool,
) ([]kube.ToolProbeTarget, error) {
	if session == nil {
		return nil, domain.NewError(domain.ErrorValidation, "tool image probe", "session is nil")
	}

	options := session.Spec.WorkflowOptions()
	namespaces := sourceVolumeNamespaces(session)

	podLists := make([]sourcePodListResult, len(namespaces))
	parallel.For(len(namespaces), func(index int) {
		pods, err := s.client.CoreV1().Pods(namespaces[index]).List(ctx, metav1.ListOptions{})
		if err == nil && pods == nil {
			err = fmt.Errorf("list PVC consumers in %s returned an empty object", namespaces[index])
		}

		if pods != nil {
			podLists[index].pods = pods.Items
		}

		podLists[index].err = err
	})

	resolvedNodes, needsTopology, activeCopyNodes, err := resolveConsumerNodes(
		session,
		namespaces,
		podLists,
		options.SourceNode,
	)
	if err != nil {
		return nil, err
	}

	if len(activeCopyNodes) > 1 {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"tool image probe",
			"online copy consumers span multiple source nodes",
		)
	}

	if options.SourceNode != "" {
		return nil, nil
	}

	needsAnyTopology := slices.Contains(needsTopology, true)

	var nodes []corev1.Node
	if needsAnyTopology {
		nodeList, err := s.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, domain.WrapError(
				domain.ErrorKubernetes,
				"tool image probe",
				"list nodes for source PV topology",
				err,
			)
		}

		if nodeList == nil {
			return nil, domain.NewError(
				domain.ErrorKubernetes,
				"tool image probe",
				"list nodes for source PV topology returned an empty object",
			)
		}

		nodes = nodeList.Items

		type pvResult struct {
			pv  *corev1.PersistentVolume
			err error
		}

		pvs := make([]pvResult, len(session.Spec.Volumes))
		parallel.For(len(session.Spec.Volumes), func(index int) {
			if !needsTopology[index] || session.Spec.Volumes[index].SourcePV.Name == "" {
				return
			}

			pvs[index].pv, pvs[index].err = s.client.CoreV1().
				PersistentVolumes().
				Get(ctx, session.Spec.Volumes[index].SourcePV.Name, metav1.GetOptions{})
		})

		for index := range pvs {
			if pvs[index].err != nil {
				return nil, domain.WrapError(
					domain.ErrorKubernetes,
					"tool image probe",
					"read source PV "+session.Spec.Volumes[index].SourcePV.Name,
					pvs[index].err,
				)
			}

			if pvs[index].pv != nil {
				resolvedNodes[index] = kube.PVUniqueNodeName(pvs[index].pv, nodes)
			}
		}
	}

	targets := make([]kube.ToolProbeTarget, 0, len(session.Spec.Volumes))

	var sourceComponents []string
	if sessionNeedsSourceSSHD(session) {
		sourceComponents = []string{kube.ToolComponentSSHD}
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		target := kube.ToolProbeTarget{
			Namespace:    volume.SourcePVC.Namespace,
			NodeName:     resolvedNodes[index],
			PVCName:      volume.SourcePVC.Name,
			SkipPVCMount: resolvedNodes[index] != "" && !mountSourcePVC,
			Components:   slices.Clone(sourceComponents),
		}
		if mountSourcePVC &&
			domain.SourceTransferPath(volume.TransferScope) != domain.VolumeRootPath {
			target.RequiredPath = domain.SourceTransferPath(volume.TransferScope)
		}

		targets = append(targets, target)
	}

	return targets, nil
}

func resolveConsumerNodes(
	session *domain.Session,
	namespaces []string,
	podLists []sourcePodListResult,
	sourceNode string,
) ([]string, []bool, map[string]struct{}, error) {
	resolved := make([]string, len(session.Spec.Volumes))
	needsTopology := make([]bool, len(session.Spec.Volumes))

	activeCopyNodes := map[string]struct{}{}
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		namespaceIndex := slices.Index(namespaces, volume.SourcePVC.Namespace)
		if namespaceIndex < 0 {
			return nil, nil, nil, domain.NewError(
				domain.ErrorInternal,
				"tool image probe",
				fmt.Sprintf("source namespace %s was not inventoried", volume.SourcePVC.Namespace),
			)
		}

		result := podLists[namespaceIndex]
		if result.err != nil {
			return nil, nil, nil, domain.WrapError(
				domain.ErrorKubernetes,
				"tool image probe",
				"list PVC consumers in "+volume.SourcePVC.Namespace,
				result.err,
			)
		}

		node, err := resolveVolumeConsumerNode(session, volume, result.pods)
		if err != nil {
			return nil, nil, nil, err
		}

		resolved[index] = node

		needsTopology[index] = node == ""
		if node != "" && session.Spec.Operation() == domain.OperationCopy {
			activeCopyNodes[node] = struct{}{}
		}

		if sourceNode != "" && node != "" && node != sourceNode {
			return nil, nil, nil, domain.NewError(
				domain.ErrorConflict,
				"tool image probe",
				fmt.Sprintf(
					"PVC %s/%s consumer runs on %s, session source node is %s",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					node,
					sourceNode,
				),
			)
		}
	}

	return resolved, needsTopology, activeCopyNodes, nil
}

func resolveVolumeConsumerNode(
	session *domain.Session,
	volume *domain.VolumeSpec,
	pods []corev1.Pod,
) (string, error) {
	activeCount, scheduledCount := 0, 0

	nodes := map[string]struct{}{}
	for index := range pods {
		pod := &pods[index]
		if !kube.ActivePodUsesPVC(pod, volume.SourcePVC.Name) {
			continue
		}

		activeCount++

		if pod.Spec.NodeName != "" {
			scheduledCount++
			nodes[pod.Spec.NodeName] = struct{}{}
		}
	}

	if err := validateCopyConsumers(session, volume, activeCount, scheduledCount); err != nil {
		return "", err
	}

	if len(nodes) > 1 {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"tool image probe",
			fmt.Sprintf(
				"PVC %s/%s active consumers span multiple nodes",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	for node := range nodes {
		return node, nil
	}

	return "", nil
}

func validateCopyConsumers(
	session *domain.Session,
	volume *domain.VolumeSpec,
	active, scheduled int,
) error {
	if session.Spec.Operation() != domain.OperationCopy || active == 0 {
		return nil
	}

	if !session.Spec.Online() {
		return domain.NewError(
			domain.ErrorPrecondition,
			"tool image probe",
			fmt.Sprintf(
				"offline copy requires PVC %s/%s to have zero active Pod consumers",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if kube.HasAccessMode(volume.AccessModes, corev1.ReadWriteOncePod) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"tool image probe",
			fmt.Sprintf(
				"active RWOP PVC %s/%s cannot be warm-copied",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if kube.HasAccessMode(volume.AccessModes, corev1.ReadWriteOnce) && scheduled != active {
		return domain.NewError(
			domain.ErrorPrecondition,
			"tool image probe",
			fmt.Sprintf(
				"every active consumer of RWO PVC %s/%s must be scheduled before online copy",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	return nil
}

func destinationVolumeNamespaces(session *domain.Session) []string {
	return volumeNamespaces(
		session,
		func(volume domain.VolumeSpec) string { return volume.DestinationPVC.Namespace },
		session.Spec.TemporaryNamespace,
	)
}

func sourceVolumeNamespaces(session *domain.Session) []string {
	return volumeNamespaces(
		session,
		func(volume domain.VolumeSpec) string { return volume.SourcePVC.Namespace },
		session.Spec.SourceNamespace,
	)
}

func volumeNamespaces(
	session *domain.Session,
	namespaceFor func(domain.VolumeSpec) string,
	fallback string,
) []string {
	namespaces := make([]string, 0, len(session.Spec.Volumes))
	for _, volume := range session.Spec.Volumes {
		namespace := namespaceFor(volume)
		if namespace != "" && !slices.Contains(namespaces, namespace) {
			namespaces = append(namespaces, namespace)
		}
	}

	if len(namespaces) == 0 && fallback != "" {
		namespaces = append(namespaces, fallback)
	}

	return namespaces
}

func toolProbeTargetsForNamespaces(
	namespaces []string,
	nodeName string,
	components []string,
) []kube.ToolProbeTarget {
	targets := make([]kube.ToolProbeTarget, 0, len(namespaces))
	for _, namespace := range namespaces {
		targets = append(
			targets,
			kube.ToolProbeTarget{
				Namespace:  namespace,
				NodeName:   nodeName,
				Components: slices.Clone(components),
			},
		)
	}

	return targets
}

func sourceToolProbeTargets(
	session *domain.Session,
	nodeName string,
	mountSourcePVC bool,
) []kube.ToolProbeTarget {
	if session == nil {
		return nil
	}

	targets := make([]kube.ToolProbeTarget, 0, len(session.Spec.Volumes))

	var sourceComponents []string
	if sessionNeedsSourceSSHD(session) {
		sourceComponents = []string{kube.ToolComponentSSHD}
	}

	for _, volume := range session.Spec.Volumes {
		target := kube.ToolProbeTarget{
			Namespace:    volume.SourcePVC.Namespace,
			NodeName:     nodeName,
			PVCName:      volume.SourcePVC.Name,
			SkipPVCMount: !mountSourcePVC,
			Components:   slices.Clone(sourceComponents),
		}
		if mountSourcePVC &&
			domain.SourceTransferPath(volume.TransferScope) != domain.VolumeRootPath {
			target.RequiredPath = domain.SourceTransferPath(volume.TransferScope)
		}

		targets = append(targets, target)
	}

	return targets
}

func destinationTransferPathProbeTargets(
	session *domain.Session,
	nodeName string,
) []kube.ToolProbeTarget {
	if session == nil {
		return nil
	}

	targets := make([]kube.ToolProbeTarget, 0, len(session.Spec.Volumes))
	for _, volume := range session.Spec.Volumes {
		path := domain.DestinationTransferPath(volume.TransferScope)
		if path == domain.VolumeRootPath {
			continue
		}

		targets = append(targets, kube.ToolProbeTarget{
			Namespace:        volume.DestinationPVC.Namespace,
			NodeName:         nodeName,
			PVCName:          volume.DestinationPVC.Name,
			WritablePVCMount: true,
			RequiredPath:     path,
			CreatePath:       true,
			Components:       []string{kube.ToolComponentRsync},
		})
	}

	return targets
}

func (s *Service) ensureSessionNamespaces(
	ctx context.Context,
	plan *domain.MigrationPlan,
	dryRun bool,
) error {
	s.logInfo("ensuring migration namespaces", "session", plan.SessionID, "dryRun", dryRun)

	if err := kube.EnsureNamespace(
		ctx,
		s.client,
		plan.SessionSpec.SessionNamespace,
		plan.SessionID,
		dryRun,
	); err != nil {
		return err
	}

	if plan.SessionSpec.TemporaryNamespace != plan.SessionSpec.SessionNamespace {
		if err := kube.EnsureNamespace(
			ctx,
			s.client,
			plan.SessionSpec.TemporaryNamespace,
			plan.SessionID,
			dryRun,
		); err != nil {
			return err
		}
	}

	ensured := map[string]struct{}{
		plan.SessionSpec.SessionNamespace:   {},
		plan.SessionSpec.TemporaryNamespace: {},
	}
	for _, volume := range plan.SessionSpec.Volumes {
		if _, ok := ensured[volume.DestinationPVC.Namespace]; ok {
			continue
		}

		if err := kube.EnsureNamespace(
			ctx,
			s.client,
			volume.DestinationPVC.Namespace,
			plan.SessionID,
			dryRun,
		); err != nil {
			return err
		}

		ensured[volume.DestinationPVC.Namespace] = struct{}{}
	}

	return nil
}
