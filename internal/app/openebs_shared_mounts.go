package app

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// OpenEBS shared-mount preparation is isolated from copy probing so every
// workflow can reuse the storage compensation without owning its policy.
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

			if active && !session.Spec.OpenEBSLVMSharedMountEnabled() {
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
		case domain.OperationMigratePod:
			if session.Spec.OpenEBSLVMSharedMountEnabled() {
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
	if !isPodMigrationSession(session) {
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

	if !session.Spec.OpenEBSLVMSharedMountEnabled() {
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
	if session == nil || !session.Spec.OpenEBSLVMSharedMountEnabled() {
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
