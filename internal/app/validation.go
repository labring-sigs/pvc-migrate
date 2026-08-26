package app

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func (s *Service) ValidateReservation(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "reservation dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	if err := s.verifyShrinkUsage(ctx, session); err != nil {
		return err
	}

	s.logInfo(
		"reservation preflight started",
		"session",
		session.ID,
		"volumes",
		len(session.Spec.Volumes),
	)

	for index := range session.Spec.Volumes {
		volume := session.Spec.Volumes[index]

		status := session.Status.Volumes[index]
		if err := s.validateReservedVolume(ctx, session, &volume, &status); err != nil {
			return err
		}
	}

	return nil
}

// ValidateWarmCopy performs every read-only check needed before reservation
// or warm-copy mutation. It deliberately leaves source-node inference and
// temporary OpenEBS shared-mount preparation unpersisted.
func (s *Service) ValidateWarmCopy(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "warm copy dry-run", "session is nil")
	}

	if err := validateRetryableSessionFailure(session); err != nil {
		return err
	}

	valid := session.Status.Phase == domain.PhasePlanned ||
		session.Status.Phase == domain.PhaseReserving ||
		session.Status.Phase == domain.PhaseReserved ||
		session.Status.Phase == domain.PhaseWarmCopied ||
		session.Status.Phase == domain.PhaseWarmCopying ||
		(session.Status.Phase == domain.PhaseFailed && (session.Status.ResumeFrom == domain.PhaseReserving || session.Status.ResumeFrom == domain.PhaseWarmCopying))
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"warm copy dry-run",
			fmt.Sprintf("session phase %s cannot warm-copy", session.Status.Phase),
		)
	}

	if err := s.ValidateReservation(ctx, session); err != nil {
		return err
	}

	if err := s.validateCopyConsumersBatch(ctx, session, false); err != nil {
		return err
	}

	if err := s.validateWarmCopyOpenEBSLVM(ctx, session); err != nil {
		return err
	}

	_, err := s.resolveSourceToolProbeTargets(ctx, session, true)

	return err
}

func (s *Service) verifyShrinkUsage(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "source usage check", "session is nil")
	}

	options := session.Spec.WorkflowOptions()
	for _, volume := range session.Spec.Volumes {
		source, sourceErr := resource.ParseQuantity(volume.SourceCapacity)

		destination, destinationErr := resource.ParseQuantity(volume.Capacity)
		if sourceErr != nil || destinationErr != nil || destination.Cmp(source) >= 0 {
			continue
		}

		if options.SkipSourceUsageCheck {
			s.logWarn(
				"source usage check skipped by explicit approval",
				"session",
				session.ID,
				"pvc",
				volume.SourcePVC.Name,
				"destinationCapacity",
				destination.String(),
			)

			continue
		}

		if s.config.VolumeUsageReader == nil {
			return domain.NewError(
				domain.ErrorPrecondition,
				"source usage check",
				fmt.Sprintf(
					"PVC %s/%s has no trusted storage-backend CRD usage reader; pass --skip-source-usage-check only after independently verifying that its data fits destination capacity %s",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					destination.String(),
				),
			)
		}

		usage, err := s.config.VolumeUsageReader.Read(
			ctx,
			kube.VolumeUsageReadOptions{SourcePVC: volume.SourcePVC, SourcePV: volume.SourcePV},
		)
		if err != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"source usage check",
				fmt.Sprintf(
					"PVC %s/%s usage could not be read from its storage backend CRD; pass --skip-source-usage-check only after independently verifying that its data fits",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
				),
				err,
			)
		}

		if usage.UsedBytes < 0 {
			return domain.NewError(
				domain.ErrorPrecondition,
				"source usage check",
				fmt.Sprintf(
					"PVC %s/%s storage backend returned invalid used bytes %d",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					usage.UsedBytes,
				),
			)
		}

		usageSource := strings.TrimSpace(usage.Source)
		if usageSource == "" {
			usageSource = "the storage backend CRD"
		}

		if usage.UsedBytes > destination.Value() {
			if sourcePath := domain.SourceTransferPath(
				volume.TransferScope,
			); sourcePath != domain.VolumeRootPath {
				return domain.NewError(
					domain.ErrorConflict,
					"source usage check",
					fmt.Sprintf(
						"PVC %s/%s whole-volume usage is %d bytes according to %s, above destination capacity %s; this cannot prove that selected source directory %q fits; abort this session and create a new one with a larger destination, or use --skip-source-usage-check only after independently measuring the selected data",
						volume.SourcePVC.Namespace,
						volume.SourcePVC.Name,
						usage.UsedBytes,
						usageSource,
						destination.String(),
						sourcePath,
					),
				)
			}

			return domain.NewError(
				domain.ErrorConflict,
				"source usage check",
				fmt.Sprintf(
					"PVC %s/%s uses %d bytes according to %s, above destination capacity %s; increase --destination-capacity or abort this shrink",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					usage.UsedBytes,
					usageSource,
					destination.String(),
				),
			)
		}
	}

	return nil
}

func (s *Service) validateWarmCopyOpenEBSLVM(ctx context.Context, session *domain.Session) error {
	manager := s.config.OpenEBSLVMSharedVolumeManager
	if manager == nil {
		return nil
	}

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

		if state, pending := pendingOpenEBSLVMSharedMount(session, volume); pending {
			previousShared := strings.TrimSpace(state.PreviousShared)
			if state.PreviousSharedSet && previousShared != "" &&
				!strings.EqualFold(previousShared, "no") &&
				!strings.EqualFold(previousShared, "yes") {
				return domain.NewError(
					domain.ErrorPrecondition,
					"OpenEBS LVM shared mount",
					fmt.Sprintf(
						"LVMVolume %s/%s has unsupported recorded spec.shared value %q",
						state.LVMVolume.Namespace,
						state.LVMVolume.Name,
						state.PreviousShared,
					),
				)
			}

			needsChange := !state.PreviousSharedSet || previousShared == "" ||
				strings.EqualFold(previousShared, "no")
			if needsChange && !session.Spec.WorkflowOptions().OpenEBSLVMEnableShared {
				return activeUnsharedOpenEBSLVMError(session, volume)
			}

			continue
		}

		prepared, err := manager.PrepareShared(ctx, volume.SourcePV)
		if err != nil {
			return err
		}

		if prepared.NeedsChange && !session.Spec.WorkflowOptions().OpenEBSLVMEnableShared {
			return activeUnsharedOpenEBSLVMError(session, volume)
		}
	}

	return nil
}

func (s *Service) validateReservedVolume(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
) error {
	checkVolume := *volume
	checkStatus := *status
	return s.reserver.ReserveVolume(ctx, session, &checkVolume, &checkStatus, true)
}

// ValidateResume performs the read-only checks for the next resumable stage.
// Copy and controller mutations remain behind --dry-run=false.
func (s *Service) ValidateResume(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "resume dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if err := validateRetryableSessionFailure(session); err != nil {
		return err
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	switch session.Spec.Operation() {
	case domain.OperationReserve,
		domain.OperationCopy,
		domain.OperationRename,
		domain.OperationMove:
		return s.validateSingleOperationResume(ctx, session, phase)
	case domain.OperationBackup:
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume session",
			"backup sessions require the backup resume workflow",
		)
	}

	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving, domain.PhaseReserved:
		if session.Status.WarmPassesCompleted < session.Spec.PrecopyPasses() {
			return s.ValidateWarmCopy(ctx, session)
		}
		return s.ValidateReservation(ctx, session)
	case domain.PhaseWarmCopying:
		return s.ValidateWarmCopy(ctx, session)
	case domain.PhaseWarmCopied:
		if session.Status.WarmPassesCompleted < session.Spec.PrecopyPasses() {
			return s.ValidateWarmCopy(ctx, session)
		}
		return s.ValidateReservation(ctx, session)
	case domain.PhasePausing:
		return s.ValidateReservation(ctx, session)
	case domain.PhasePaused, domain.PhaseFinalSyncing:
		return s.ValidateFinalSync(ctx, session)
	case domain.PhaseFinalSynced, domain.PhaseActivating:
		return s.ValidateActivation(ctx, session)
	case domain.PhaseActivated:
		if err := s.controllers.VerifyPaused(ctx, session); err != nil {
			return err
		}
		return s.validateWorkloadResume(ctx, session)
	case domain.PhaseResuming:
		return s.validateWorkloadResume(ctx, session)
	case domain.PhaseRenaming, domain.PhaseMoving:
		return s.validateOfflineVolumes(ctx, session)
	case domain.PhaseRollingBack:
		return s.ValidateRollback(ctx, session)
	case domain.PhaseAborting:
		return s.ValidateAbort(ctx, session)
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return nil
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume dry-run",
			fmt.Sprintf("phase %s cannot be resumed", phase),
		)
	}
}

func (s *Service) validateSingleOperationResume(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
) error {
	if err := validateSingleResumePhase(session.Spec.Operation(), phase); err != nil {
		return err
	}

	switch phase {
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return nil
	case domain.PhaseRollingBack:
		return s.ValidateRollback(ctx, session)
	case domain.PhaseAborting:
		return s.ValidateAbort(ctx, session)
	}

	switch session.Spec.Operation() {
	case domain.OperationReserve:
		switch phase {
		case domain.PhasePlanned, domain.PhaseReserving:
			return s.ValidateReservation(ctx, session)
		case domain.PhaseReserved:
			return nil
		default:
			return domain.NewError(
				domain.ErrorPrecondition,
				"resume dry-run",
				fmt.Sprintf("phase %s cannot be resumed for %s", phase, session.Spec.Operation()),
			)
		}
	case domain.OperationCopy:
		switch phase {
		case domain.PhasePlanned, domain.PhaseReserving:
			return s.ValidateWarmCopy(ctx, session)
		case domain.PhaseReserved, domain.PhaseWarmCopying:
			return s.ValidateWarmCopy(ctx, session)
		case domain.PhaseWarmCopied:
			return nil
		default:
			return domain.NewError(
				domain.ErrorPrecondition,
				"resume dry-run",
				fmt.Sprintf("phase %s cannot be resumed for %s", phase, session.Spec.Operation()),
			)
		}
	case domain.OperationRename, domain.OperationMove:
		expected := domain.PhaseRenaming
		if session.Spec.Operation() == domain.OperationMove {
			expected = domain.PhaseMoving
		}

		switch phase {
		case domain.PhasePlanned, expected:
			return s.validateRebindOfflineVolumes(ctx, session)
		case domain.PhaseCompleted:
			return nil
		default:
			return domain.NewError(
				domain.ErrorPrecondition,
				"resume dry-run",
				fmt.Sprintf("phase %s cannot be resumed for %s", phase, session.Spec.Operation()),
			)
		}
	default:
		return nil
	}
}

func validateSingleResumePhase(operation domain.Operation, phase domain.Phase) error {
	allowed := false
	switch operation {
	case domain.OperationReserve:
		allowed = phase == domain.PhasePlanned || phase == domain.PhaseReserving ||
			phase == domain.PhaseReserved
	case domain.OperationCopy:
		allowed = phase == domain.PhasePlanned || phase == domain.PhaseReserving ||
			phase == domain.PhaseReserved ||
			phase == domain.PhaseWarmCopying ||
			phase == domain.PhaseWarmCopied
	case domain.OperationRename:
		allowed = phase == domain.PhasePlanned || phase == domain.PhaseRenaming
	case domain.OperationMove:
		allowed = phase == domain.PhasePlanned || phase == domain.PhaseMoving
	}

	switch phase {
	case domain.PhaseCompleted,
		domain.PhaseAborted,
		domain.PhaseRolledBack,
		domain.PhaseRollingBack,
		domain.PhaseAborting:
		allowed = true
	}

	if !allowed {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume session",
			fmt.Sprintf("phase %s cannot be resumed for operation %s", phase, operation),
		)
	}

	return nil
}

// ValidateAbort checks the phase and any paused workload that abort would
// resume. It performs no controller or resource mutation.
func (s *Service) ValidateAbort(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "abort dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed || phase == domain.PhaseAborting {
		phase = session.Status.ResumeFrom
	}

	if phase == domain.PhaseRollingBack {
		return domain.NewError(
			domain.ErrorPrecondition,
			"abort dry-run",
			"rollback recovery must continue through session resume or rollback",
		)
	}

	if phase == domain.PhaseActivated || phase == domain.PhaseCompleted ||
		phase == domain.PhaseResuming ||
		session.Status.ResumeFrom == domain.PhaseActivating ||
		session.Status.ResumeFrom == domain.PhaseResuming {
		return domain.NewError(
			domain.ErrorPrecondition,
			"abort dry-run",
			"activated sessions require rollback",
		)
	}

	if abortRequiresWorkloadResume(session) {
		if err := s.verifySourceStorage(ctx, session); err != nil {
			return err
		}
	}

	if phase == domain.PhasePaused || phase == domain.PhaseFinalSyncing ||
		phase == domain.PhaseFinalSynced {
		return s.controllers.VerifyPaused(ctx, session)
	}

	return nil
}

// ValidateRollback verifies the identities and offline boundary needed by a
// rollback. A completed migration is expected to be running, so its active
// PVC bindings are checked instead of requiring a paused workload.
func (s *Service) ValidateRollback(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "rollback dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if session.Spec.Type == domain.SessionTypeBackup {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rollback dry-run",
			"backup sessions do not change PVC identity and cannot be rolled back",
		)
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseRolledBack {
		return nil
	}

	rollbackOrigin := session.Status.ResumeFrom
	if rollbackOrigin == domain.PhaseRollingBack {
		rollbackOrigin = phaseBefore(session, domain.PhaseRollingBack)
	}

	recoveringRollback := phase == domain.PhaseRollingBack ||
		(phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseRollingBack)
	wasRunning := phase == domain.PhaseCompleted ||
		((phase == domain.PhaseFailed || phase == domain.PhaseRollingBack) && (rollbackOrigin == domain.PhaseResuming || rollbackOrigin == domain.PhaseCompleted))
	failedDuringCutover := phase == domain.PhaseFailed &&
		(session.Status.ResumeFrom == domain.PhaseActivating || session.Status.ResumeFrom == domain.PhaseResuming || session.Status.ResumeFrom == domain.PhaseRollingBack)

	valid := wasRunning || phase == domain.PhaseActivated || phase == domain.PhaseActivating ||
		phase == domain.PhaseFinalSynced ||
		phase == domain.PhaseRollingBack ||
		failedDuringCutover
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rollback dry-run",
			fmt.Sprintf("session phase %s cannot roll back", phase),
		)
	}

	if session.Spec.Operation().RebindsPVC() {
		return s.validateRebindRollbackVolumes(ctx, session)
	}

	if err := s.validateRollbackStorage(
		ctx,
		session,
		phase,
		rollbackOrigin,
		recoveringRollback,
		wasRunning,
	); err != nil {
		return err
	}

	if recoveringRollback {
		if wasRunning {
			return s.validateRollbackConsumers(ctx, session)
		}
		return nil
	}

	if wasRunning {
		return s.validateRollbackConsumers(ctx, session)
	}

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}

	return nil
}

// validateRollbackConsumers mirrors the Pod reference guard in RollbackVolume
// while allowing consumers that the recorded workload adapter will pause.
func (s *Service) validateRollbackConsumers(ctx context.Context, session *domain.Session) error {
	allowed := make(map[string]types.UID)

	workload := session.Spec.Workload()
	if workload.Adapter != domain.WorkloadNone {
		current, err := s.controllers.CurrentRollbackPods(ctx, session)
		if err != nil {
			return err
		}

		references := append([]domain.ObjectReference{workload.Pod}, workload.AffectedPods...)

		references = append(references, current...)
		for _, ref := range references {
			if ref.Namespace != "" && ref.Name != "" && ref.UID != "" {
				allowed[ref.Namespace+"/"+ref.Name] = ref.UID
			}
		}
	}

	namespaces := make([]string, 0)

	seenNamespaces := make(map[string]struct{})
	for index := range session.Spec.Volumes {
		namespace := session.Spec.Volumes[index].SourcePVC.Namespace
		if _, exists := seenNamespaces[namespace]; exists {
			continue
		}

		seenNamespaces[namespace] = struct{}{}
		namespaces = append(namespaces, namespace)
	}

	sort.Strings(namespaces)

	type podList struct {
		items []corev1.Pod
		err   error
	}

	results := make([]podList, len(namespaces))
	parallel.For(len(namespaces), func(index int) {
		pods, err := s.client.CoreV1().Pods(namespaces[index]).List(ctx, metav1.ListOptions{})
		if err == nil && pods == nil {
			err = fmt.Errorf("list Pods in %s returned an empty object", namespaces[index])
		}

		if pods != nil {
			results[index].items = pods.Items
		}

		results[index].err = err
	})

	for volumeIndex := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[volumeIndex]

		result := results[sort.SearchStrings(namespaces, volume.SourcePVC.Namespace)]
		if result.err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"rollback dry-run",
				"list Pods in "+volume.SourcePVC.Namespace,
				result.err,
			)
		}

		for podIndex := range result.items {
			pod := &result.items[podIndex]
			if !kube.PodPreventsSafePVCDeletion(pod, volume.SourcePVC.Name) {
				continue
			}

			expectedUID, controlled := allowed[pod.Namespace+"/"+pod.Name]
			if controlled && expectedUID == pod.UID {
				continue
			}

			return domain.NewError(
				domain.ErrorPrecondition,
				"rollback dry-run",
				fmt.Sprintf(
					"PVC %s/%s is referenced by Pod %s, which is outside the recorded workload pause scope",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					pod.Name,
				),
			)
		}
	}

	return nil
}

// validateRebindRollbackVolumes checks the PVC identity currently serving the
// workload. Rename and move sessions replace the original PVC name in place,
// so the recorded source PVC is intentionally absent after cutover.
func (s *Service) validateRebindRollbackVolumes(
	ctx context.Context,
	session *domain.Session,
) error {
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		status := &session.Status.Volumes[index]

		active := status.Activation.ActivePVC
		if active.Namespace == "" {
			active.Namespace = volume.DestinationPVC.Namespace
		}

		if active.Name == "" || active.UID == "" {
			return domain.NewError(
				domain.ErrorPrecondition,
				"rollback dry-run",
				fmt.Sprintf("PVC %s has no recorded active identity", volume.SourcePVC.Name),
			)
		}

		if err := s.validateRebindTransition(
			ctx,
			session,
			volume,
			active,
			volume.SourcePVC,
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) validateOfflineVolumes(ctx context.Context, session *domain.Session) error {
	volumes := make([]*domain.VolumeSpec, len(session.Spec.Volumes))
	for index := range session.Spec.Volumes {
		volumes[index] = &session.Spec.Volumes[index]
	}

	return s.verifyVolumesOffline(ctx, session, volumes)
}

func (s *Service) verifyVolumesOffline(
	ctx context.Context,
	session *domain.Session,
	volumes []*domain.VolumeSpec,
) error {
	if switcher, ok := s.switcher.(sessionBatchVolumeSwitcher); ok {
		return switcher.VerifyVolumesOfflineForSession(ctx, session.ID, volumes)
	}

	if switcher, ok := s.switcher.(batchVolumeSwitcher); ok {
		return switcher.VerifyVolumesOffline(ctx, volumes)
	}

	for _, volume := range volumes {
		if err := s.switcher.VerifyVolumeOffline(ctx, volume); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) validateRebindOfflineVolumes(ctx context.Context, session *domain.Session) error {
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		if err := s.validateRebindTransition(
			ctx,
			session,
			volume,
			volume.SourcePVC,
			volume.DestinationPVC,
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) validateRebindTransition(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	from, to domain.ObjectReference,
) error {
	if from.Namespace == "" || from.Name == "" || to.Namespace == "" || to.Name == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rebind PVC dry-run",
			"source and destination PVC identities are required",
		)
	}

	fromPVC, fromErr := s.client.CoreV1().
		PersistentVolumeClaims(from.Namespace).
		Get(ctx, from.Name, metav1.GetOptions{})
	sameEndpoint := from.Namespace == to.Namespace && from.Name == to.Name

	toPVC, toErr := fromPVC, fromErr
	if !sameEndpoint {
		toPVC, toErr = s.client.CoreV1().
			PersistentVolumeClaims(to.Namespace).
			Get(ctx, to.Name, metav1.GetOptions{})
	}

	if fromErr != nil && !apierrors.IsNotFound(fromErr) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"rebind PVC dry-run",
			fmt.Sprintf("read PVC %s/%s", from.Namespace, from.Name),
			fromErr,
		)
	}

	if toErr != nil && !apierrors.IsNotFound(toErr) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"rebind PVC dry-run",
			fmt.Sprintf("read PVC %s/%s", to.Namespace, to.Name),
			toErr,
		)
	}

	fromExists := fromErr == nil

	toExists := toErr == nil
	if !sameEndpoint && fromExists && toExists {
		return domain.NewError(
			domain.ErrorConflict,
			"rebind PVC dry-run",
			fmt.Sprintf(
				"both PVC endpoints %s/%s and %s/%s exist",
				from.Namespace,
				from.Name,
				to.Namespace,
				to.Name,
			),
		)
	}

	if !fromExists && !toExists {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rebind PVC dry-run",
			fmt.Sprintf(
				"neither PVC endpoint %s/%s nor %s/%s exists",
				from.Namespace,
				from.Name,
				to.Namespace,
				to.Name,
			),
		)
	}

	current := fromPVC

	expected := from
	if !fromExists {
		current = toPVC
		expected = to
	}

	if current == nil || current.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			"rebind PVC dry-run",
			"read PVC endpoint returned an empty object",
		)
	}

	if !fromExists && current.Annotations[kube.SessionKey] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"rebind PVC dry-run",
			fmt.Sprintf(
				"destination PVC %s/%s is not owned by session %s",
				current.Namespace,
				current.Name,
				session.ID,
			),
		)
	}

	if expected.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rebind PVC dry-run",
			fmt.Sprintf("PVC %s/%s has no recorded UID", expected.Namespace, expected.Name),
		)
	}

	if current.UID != expected.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"rebind PVC dry-run",
			fmt.Sprintf("PVC %s/%s UID changed", current.Namespace, current.Name),
		)
	}

	currentRef := domain.ObjectReference{
		APIVersion: domain.CoreAPIVersion,
		Kind:       domain.KindPersistentVolumeClaim,
		Namespace:  current.Namespace,
		Name:       current.Name,
		UID:        current.UID,
	}
	check := *volume
	check.SourcePVC = currentRef
	check.SourcePV = volume.SourcePV
	check.DestinationPVC = currentRef
	check.DestinationPV = volume.SourcePV

	return s.switcher.VerifyVolumeOffline(ctx, &check)
}

// ValidateCleanup checks ownership, reclaim-policy, and deletion prerequisites
// through read-only API calls. It mirrors Cleanup's destructive guards.
func (s *Service) ValidateCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "cleanup dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if !cleanupPhaseAllowed(session) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			fmt.Sprintf("session phase %s is still active", session.Status.Phase),
		)
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	if session.Spec.Type == domain.SessionTypeBackup &&
		(options.Finalize || options.DeleteSession) {
		if err := kube.ValidateBackupCredentialsSecretCleanup(
			ctx,
			s.client,
			backupCredentialsCleanupReference(session),
			session.ID,
		); err != nil {
			return err
		}
	}

	if err := s.validateReservationPods(ctx, session); err != nil {
		return err
	}

	if options.Finalize {
		if err := s.validateStandalonePodOwnershipRelease(ctx, session); err != nil {
			return err
		}
	}

	s.logInfo(
		"cleanup preflight started",
		"session",
		session.ID,
		"volumes",
		len(session.Spec.Volumes),
		"deleteTemporary",
		options.DeleteTemporary,
		"deleteRollback",
		options.DeleteRollback,
		"finalize",
		options.Finalize,
		"deleteSession",
		options.DeleteSession,
	)

	if options.DeleteSession && !options.Finalize {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			"deleting the session requires --finalize",
		)
	}

	for index := range session.Spec.Volumes {
		if err := s.validateCleanupVolume(ctx, session, options, index); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) validateCleanupVolume(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
	index int,
) error {
	volumeValue := session.Spec.Volumes[index]

	volume := &volumeValue
	if !session.Spec.Operation().RebindsPVC() &&
		(options.DeleteTemporary || options.DeleteRollback || options.DeleteSession) {
		recoverySession := *session
		recoverySession.Spec.Volumes = slices.Clone(session.Spec.Volumes)

		recoverySession.Spec.Volumes[index] = volumeValue
		if _, err := s.discoverDestinationRefs(ctx, &recoverySession, index); err != nil {
			return err
		}

		volume = &recoverySession.Spec.Volumes[index]
	}

	active, rollback, policy := cleanupPVRefs(session, volume)
	if options.DeleteSession && rollback.Name != "" && !options.DeleteRollback &&
		!preservesCopyOutput(session, options) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			"deleting the session requires --delete-rollback-pv while a rollback PV is recorded",
		)
	}

	if err := s.validateCleanupPVC(ctx, session, options, volume); err != nil {
		return err
	}

	if err := s.validateCleanupRollbackPV(
		ctx,
		session,
		options,
		index,
		volume,
		rollback,
	); err != nil {
		return err
	}

	if uncheckpointedSource(session, index) {
		return s.validateUncheckpointedSource(ctx, session.ID, volume)
	}

	if !options.Finalize || active.Name == "" {
		return nil
	}

	return s.validateCleanupActivePV(ctx, session, options, index, volume, active, policy)
}

func (s *Service) validateCleanupPVC(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
	volume *domain.VolumeSpec,
) error {
	if !options.DeleteTemporary || volume.DestinationPVC.UID == "" {
		return nil
	}

	pvc, err := s.inspectPVCUnusedForSession(ctx, volume.DestinationPVC, session)
	if err != nil || pvc == nil {
		return err
	}

	if pvc.UID != volume.DestinationPVC.UID || pvc.Labels[kube.SessionKey] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PVC %s/%s identity or session ownership changed", pvc.Namespace, pvc.Name),
		)
	}

	return nil
}

func (s *Service) validateCleanupRollbackPV(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
	index int,
	volume *domain.VolumeSpec,
	rollback domain.ObjectReference,
) error {
	if !options.DeleteRollback || rollback.Name == "" {
		return nil
	}

	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, rollback.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup dry-run",
			"read rollback PV "+rollback.Name,
			err,
		)
	}

	var uncheckpointedClaim *domain.ObjectReference
	if uncheckpointedDestination(session, index) {
		uncheckpointedClaim = &volume.DestinationPVC
	}

	if !cleanupPVIdentityMatches(
		pv,
		rollback,
		session.ID,
		cleanupRollbackRole(session),
		uncheckpointedClaim,
	) {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PV %s identity, ownership, or role changed", pv.Name),
		)
	}

	deletionWillReleaseClaim := options.DeleteTemporary && pv.Status.Phase == corev1.VolumeBound &&
		pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Namespace == volume.DestinationPVC.Namespace &&
		pv.Spec.ClaimRef.Name == volume.DestinationPVC.Name && pv.Spec.ClaimRef.UID != "" &&
		pv.Spec.ClaimRef.UID == volume.DestinationPVC.UID
	if pv.Status.Phase != corev1.VolumeReleased && pv.Status.Phase != corev1.VolumeAvailable &&
		!deletionWillReleaseClaim {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			fmt.Sprintf("PV %s phase %s must be Released or Available", pv.Name, pv.Status.Phase),
		)
	}

	policy := cleanupRollbackReclaimPolicy(session, volume)
	if !cleanupRollbackPVPolicyMatches(pv, policy, uncheckpointedClaim) {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PV %s original reclaim policy changed", pv.Name),
		)
	}

	return nil
}

func (s *Service) validateCleanupActivePV(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
	index int,
	volume *domain.VolumeSpec,
	active domain.ObjectReference,
	policy corev1.PersistentVolumeReclaimPolicy,
) error {
	maySkipMissing := session.Status.Phase == domain.PhaseAborted &&
		session.Status.Volumes[index].Activation.ActivePVC.Name == ""
	if policy == "" && !maySkipMissing {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			fmt.Sprintf("PV %s has no recorded reclaim policy", active.Name),
		)
	}

	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, active.Name, metav1.GetOptions{})
	if maySkipMissing && apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup dry-run",
			"read active PV "+active.Name,
			err,
		)
	}

	if policy == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			fmt.Sprintf("PV %s has no recorded reclaim policy", active.Name),
		)
	}

	if err := validateFinalizablePV(pv, active, session.ID, policy); err != nil {
		return err
	}

	if !preservesCopyOutput(session, options) || volume.DestinationPV.Name == "" {
		return nil
	}

	destinationPV, err := s.client.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.DestinationPV.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup dry-run",
			"read copy destination PV "+volume.DestinationPV.Name,
			err,
		)
	}

	return validateFinalizablePV(
		destinationPV,
		volume.DestinationPV,
		session.ID,
		volume.DestinationPolicy,
	)
}

func validateFinalizablePV(
	pv *corev1.PersistentVolume,
	ref domain.ObjectReference,
	sessionID string,
	policy corev1.PersistentVolumeReclaimPolicy,
) error {
	if policy == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			fmt.Sprintf("PV %s has no recorded reclaim policy", ref.Name),
		)
	}

	if pv.UID != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
		)
	}

	role := pv.Labels[kube.ResourceRoleLabel]
	if pv.Labels[kube.SessionKey] == "" && role == "" &&
		pv.Spec.PersistentVolumeReclaimPolicy == policy &&
		pv.Annotations[kube.OriginalPolicyAnnotation] == "" {
		return nil
	}

	if pv.Labels[kube.SessionKey] != sessionID ||
		(role != kube.ResourceRoleActive && role != kube.ResourceRoleSource && role != kube.ResourceRoleRename && role != kube.ResourceRoleDestination) {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
		)
	}

	return nil
}

// ValidateFinalSync performs the read-only checks required immediately before
// a final synchronization. It leaves the persisted session and workload intact.
func (s *Service) ValidateFinalSync(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "final sync dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if err := validateRetryableSessionFailure(session); err != nil {
		return err
	}

	if !session.Spec.Orchestrated() {
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync dry-run",
			"final sync requires an orchestrated migration session",
		)
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	switch phase {
	case domain.PhaseReserved, domain.PhaseWarmCopied, domain.PhasePausing:
		return s.ValidateReservation(ctx, session)
	case domain.PhasePaused, domain.PhaseFinalSyncing, domain.PhaseFinalSynced:
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync dry-run",
			fmt.Sprintf("session phase %s cannot final-sync", session.Status.Phase),
		)
	}

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}

	if err := s.verifyShrinkUsage(ctx, session); err != nil {
		return err
	}

	return s.validateOfflineVolumes(ctx, session)
}

// ValidateActivation performs activation preconditions through read-only API
// calls. The mutating PV/PVC switch remains behind --dry-run=false.
func (s *Service) ValidateActivation(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "activation dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if !session.Spec.Orchestrated() {
		return domain.NewError(
			domain.ErrorPrecondition,
			"activation dry-run",
			"activation requires an orchestrated migration session",
		)
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseActivated || phase == domain.PhaseResuming ||
		phase == domain.PhaseCompleted ||
		(phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseResuming) {
		return nil
	}

	valid := phase == domain.PhaseFinalSynced || phase == domain.PhaseActivating ||
		(phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseActivating)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"activation dry-run",
			fmt.Sprintf("session phase %s cannot activate", session.Status.Phase),
		)
	}

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		status := &session.Status.Volumes[index]
		if status.Sync.FinalCompletedAt == nil {
			return domain.NewError(
				domain.ErrorPrecondition,
				"activation dry-run",
				fmt.Sprintf("PVC %s has no completed final sync", volume.SourcePVC.Name),
			)
		}
	}

	if err := s.validateActivationPVCPolicies(ctx, session); err != nil {
		return err
	}

	if phase == domain.PhaseActivating || phase == domain.PhaseFailed {
		return s.validateActivationStorage(ctx, session)
	}

	return s.validateOfflineVolumes(ctx, session)
}
