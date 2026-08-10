package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Config struct {
	KubeconfigPath string
	Context        string
	Retries        int
	RetryBackoff   time.Duration
	HelmTimeout    time.Duration
	NoCompress     bool
	Writer         io.Writer
	Logger         *slog.Logger
}

type Service struct {
	client      kubernetes.Interface
	store       kube.SessionStore
	reserver    volumeReserver
	copier      copyengine.Engine
	controllers workloadController
	switcher    volumeSwitcher
	config      Config
	now         func() time.Time
	sleep       func(context.Context, time.Duration) error
}

type volumeReserver interface {
	ReserveVolume(context.Context, *domain.Session, *domain.VolumeSpec, *domain.VolumeStatus, bool) error
}

type workloadController interface {
	Pause(context.Context, *domain.Session) error
	Resume(context.Context, *domain.Session) error
	VerifyPaused(context.Context, *domain.Session) error
}

type volumeSwitcher interface {
	VerifyVolumeOffline(context.Context, *domain.VolumeSpec) error
	ActivateVolume(context.Context, *domain.Session, *domain.VolumeSpec, *domain.VolumeStatus, kube.ProgressFunc) error
	RollbackVolume(context.Context, *domain.Session, *domain.VolumeSpec, *domain.VolumeStatus, kube.ProgressFunc) error
	RenamePVC(context.Context, *domain.Session, *domain.VolumeSpec, kube.ProgressFunc) (*corev1.PersistentVolumeClaim, error)
}

func NewService(
	client kubernetes.Interface,
	store kube.SessionStore,
	reserver volumeReserver,
	copier copyengine.Engine,
	controllers workloadController,
	switcher volumeSwitcher,
	config Config,
) *Service {
	if config.Retries <= 0 {
		config.Retries = 3
	}
	if config.RetryBackoff <= 0 {
		config.RetryBackoff = 2 * time.Second
	}
	if config.HelmTimeout <= 0 {
		config.HelmTimeout = 10 * time.Minute
	}
	if config.Writer == nil {
		config.Writer = io.Discard
	}
	if config.Logger == nil {
		config.Logger = slog.New(slog.DiscardHandler)
	}
	return &Service{
		client:      client,
		store:       store,
		reserver:    reserver,
		copier:      copier,
		controllers: controllers,
		switcher:    switcher,
		config:      config,
		now:         time.Now,
		sleep:       sleepContext,
	}
}

type sessionLockContextKey struct{}

type heldSessionLock struct {
	lock      kube.SessionLock
	namespace string
	id        string
}

// withSessionIDLock serializes every mutating session operation when the
// configured store supports Kubernetes Lease fencing. The context marker
// makes nested stage calls re-entrant while preserving the same lease.
func (s *Service) withSessionIDLock(ctx context.Context, namespace, id string, fn func(context.Context) error) error {
	if namespace == "" || id == "" {
		return domain.NewError(domain.ErrorValidation, "session lock", "session namespace and ID are required")
	}
	if held, ok := ctx.Value(sessionLockContextKey{}).(heldSessionLock); ok && held.namespace == namespace && held.id == id {
		if err := held.lock.Err(); err != nil {
			return err
		}
		return fn(ctx)
	}
	locker, supported := s.store.(kube.SessionLocker)
	if !supported {
		return fn(ctx)
	}
	lock, err := locker.AcquireSessionLock(ctx, namespace, id)
	if err != nil {
		return err
	}
	lockedCtx := context.WithValue(lock.Context(), sessionLockContextKey{}, heldSessionLock{lock: lock, namespace: namespace, id: id})
	operationErr := fn(lockedCtx)
	if leaseErr := lock.Err(); leaseErr != nil {
		operationErr = errors.Join(operationErr, leaseErr)
	}
	releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelRelease()
	releaseErr := lock.Release(releaseCtx)
	return errors.Join(operationErr, releaseErr)
}

func (s *Service) withSessionLock(ctx context.Context, session *domain.Session, fn func(context.Context) error) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "session lock", "session is nil")
	}
	return s.withSessionIDLock(ctx, session.Spec.SessionNamespace, session.ID, fn)
}

func (s *Service) CreateSession(ctx context.Context, plan *domain.MigrationPlan, dryRun bool) (*domain.Session, error) {
	if plan == nil {
		return nil, domain.NewError(domain.ErrorValidation, "create session", "plan is nil")
	}
	if !plan.Ready {
		return nil, domain.NewError(domain.ErrorPrecondition, "create session", "migration plan contains failed checks")
	}
	session := domain.NewSession(plan.SessionID, plan.SessionSpec, s.now())
	if dryRun {
		if err := s.ensureSessionNamespaces(ctx, plan, true); err != nil {
			return nil, err
		}
		if err := s.ValidateReservation(ctx, session); err != nil {
			return nil, err
		}
		return session, nil
	}
	// The Lease used to serialize creation lives in the session namespace.
	// Ensure that namespace exists before attempting to acquire the Lease.
	if err := kube.EnsureNamespace(ctx, s.client, plan.SessionSpec.SessionNamespace, plan.SessionID, false); err != nil {
		return nil, err
	}
	createErr := s.withSessionIDLock(ctx, plan.SessionSpec.SessionNamespace, plan.SessionID, func(lockedCtx context.Context) error {
		if err := s.ensureSessionNamespaces(lockedCtx, plan, false); err != nil {
			return err
		}
		return s.store.Create(lockedCtx, session)
	})
	if createErr != nil {
		return nil, createErr
	}
	return session, nil
}

func (s *Service) ensureSessionNamespaces(ctx context.Context, plan *domain.MigrationPlan, dryRun bool) error {
	if err := kube.EnsureNamespace(ctx, s.client, plan.SessionSpec.SessionNamespace, plan.SessionID, dryRun); err != nil {
		return err
	}
	if plan.SessionSpec.TemporaryNamespace != plan.SessionSpec.SessionNamespace {
		if err := kube.EnsureNamespace(ctx, s.client, plan.SessionSpec.TemporaryNamespace, plan.SessionID, dryRun); err != nil {
			return err
		}
	}
	ensured := map[string]struct{}{plan.SessionSpec.SessionNamespace: {}, plan.SessionSpec.TemporaryNamespace: {}}
	for _, volume := range plan.SessionSpec.Volumes {
		if _, ok := ensured[volume.DestinationPVC.Namespace]; ok {
			continue
		}
		if err := kube.EnsureNamespace(ctx, s.client, volume.DestinationPVC.Namespace, plan.SessionID, dryRun); err != nil {
			return err
		}
		ensured[volume.DestinationPVC.Namespace] = struct{}{}
	}
	return nil
}

func (s *Service) ValidateReservation(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "reservation dry-run", "session is nil")
	}
	if err := session.Validate(); err != nil {
		return err
	}
	for index := range session.Spec.Volumes {
		volume := session.Spec.Volumes[index]
		status := session.Status.Volumes[index]
		if err := s.reserver.ReserveVolume(ctx, session, &volume, &status, true); err != nil {
			return err
		}
	}
	return nil
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
	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}
	switch session.Spec.Operation() {
	case domain.OperationReserve, domain.OperationCopy, domain.OperationRename, domain.OperationMove:
		return s.validateSingleOperationResume(ctx, session, phase)
	}
	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving, domain.PhaseReserved, domain.PhaseWarmCopying, domain.PhaseWarmCopied:
		return s.ValidateReservation(ctx, session)
	case domain.PhasePausing:
		return nil
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
		return domain.NewError(domain.ErrorPrecondition, "resume dry-run", fmt.Sprintf("phase %s cannot be resumed", phase))
	}
}

func (s *Service) validateSingleOperationResume(ctx context.Context, session *domain.Session, phase domain.Phase) error {
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
			return domain.NewError(domain.ErrorPrecondition, "resume dry-run", fmt.Sprintf("phase %s cannot be resumed for %s", phase, session.Spec.Operation()))
		}
	case domain.OperationCopy:
		switch phase {
		case domain.PhasePlanned, domain.PhaseReserving:
			return s.ValidateReservation(ctx, session)
		case domain.PhaseReserved, domain.PhaseWarmCopying:
			for index := range session.Spec.Volumes {
				if err := s.validateCopyConsumers(ctx, session, &session.Spec.Volumes[index]); err != nil {
					return err
				}
			}
			return nil
		case domain.PhaseWarmCopied:
			return nil
		default:
			return domain.NewError(domain.ErrorPrecondition, "resume dry-run", fmt.Sprintf("phase %s cannot be resumed for %s", phase, session.Spec.Operation()))
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
			return domain.NewError(domain.ErrorPrecondition, "resume dry-run", fmt.Sprintf("phase %s cannot be resumed for %s", phase, session.Spec.Operation()))
		}
	default:
		return nil
	}
}

func validateSingleResumePhase(operation domain.Operation, phase domain.Phase) error {
	allowed := false
	switch operation {
	case domain.OperationReserve:
		allowed = phase == domain.PhasePlanned || phase == domain.PhaseReserving || phase == domain.PhaseReserved
	case domain.OperationCopy:
		allowed = phase == domain.PhasePlanned || phase == domain.PhaseReserving || phase == domain.PhaseReserved || phase == domain.PhaseWarmCopying || phase == domain.PhaseWarmCopied
	case domain.OperationRename:
		allowed = phase == domain.PhasePlanned || phase == domain.PhaseRenaming
	case domain.OperationMove:
		allowed = phase == domain.PhasePlanned || phase == domain.PhaseMoving
	}
	switch phase {
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack, domain.PhaseRollingBack, domain.PhaseAborting:
		allowed = true
	}
	if !allowed {
		return domain.NewError(domain.ErrorPrecondition, "resume session", fmt.Sprintf("phase %s cannot be resumed for operation %s", phase, operation))
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
	phase := session.Status.Phase
	if phase == domain.PhaseFailed || phase == domain.PhaseAborting {
		phase = session.Status.ResumeFrom
	}
	if phase == domain.PhaseRollingBack {
		return domain.NewError(domain.ErrorPrecondition, "abort dry-run", "rollback recovery must continue through session resume or rollback")
	}
	if phase == domain.PhaseActivated || phase == domain.PhaseCompleted || phase == domain.PhaseResuming || session.Status.ResumeFrom == domain.PhaseActivating || session.Status.ResumeFrom == domain.PhaseResuming {
		return domain.NewError(domain.ErrorPrecondition, "abort dry-run", "activated sessions require rollback")
	}
	if phase == domain.PhasePaused || phase == domain.PhaseFinalSyncing || phase == domain.PhaseFinalSynced {
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
	phase := session.Status.Phase
	rollbackOrigin := session.Status.ResumeFrom
	if rollbackOrigin == domain.PhaseRollingBack {
		rollbackOrigin = phaseBefore(session, domain.PhaseRollingBack)
	}
	wasRunning := phase == domain.PhaseCompleted || ((phase == domain.PhaseFailed || phase == domain.PhaseRollingBack) && (rollbackOrigin == domain.PhaseResuming || rollbackOrigin == domain.PhaseCompleted))
	failedDuringCutover := phase == domain.PhaseFailed && (session.Status.ResumeFrom == domain.PhaseActivating || session.Status.ResumeFrom == domain.PhaseResuming || session.Status.ResumeFrom == domain.PhaseRollingBack)
	valid := wasRunning || phase == domain.PhaseActivated || phase == domain.PhaseActivating || phase == domain.PhaseFinalSynced || phase == domain.PhaseRollingBack || failedDuringCutover
	if !valid {
		return domain.NewError(domain.ErrorPrecondition, "rollback dry-run", fmt.Sprintf("session phase %s cannot roll back", phase))
	}
	if session.Spec.Operation().RebindsPVC() {
		return s.validateRebindRollbackVolumes(ctx, session)
	}
	if wasRunning {
		return s.verifyActiveVolumes(ctx, session)
	}
	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}
	return s.validateOfflineVolumes(ctx, session)
}

// validateRebindRollbackVolumes checks the PVC identity currently serving the
// workload. Rename and move sessions replace the original PVC name in place,
// so the recorded source PVC is intentionally absent after cutover.
func (s *Service) validateRebindRollbackVolumes(ctx context.Context, session *domain.Session) error {
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		status := &session.Status.Volumes[index]
		active := status.Activation.ActivePVC
		if active.Namespace == "" {
			active.Namespace = volume.DestinationPVC.Namespace
		}
		if active.Name == "" || active.UID == "" {
			return domain.NewError(domain.ErrorPrecondition, "rollback dry-run", fmt.Sprintf("PVC %s has no recorded active identity", volume.SourcePVC.Name))
		}
		check := *volume
		check.SourcePVC = active
		// VerifyVolumeOffline validates both sides of a migration volume. A
		// rebind has one PVC and one retained PV, so use the active identity for
		// both references to apply the same offline and binding checks.
		check.SourcePV = volume.SourcePV
		check.DestinationPVC = active
		check.DestinationPV = volume.SourcePV
		if err := s.switcher.VerifyVolumeOffline(ctx, &check); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) validateOfflineVolumes(ctx context.Context, session *domain.Session) error {
	for index := range session.Spec.Volumes {
		if err := s.switcher.VerifyVolumeOffline(ctx, &session.Spec.Volumes[index]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) validateRebindOfflineVolumes(ctx context.Context, session *domain.Session) error {
	for index := range session.Spec.Volumes {
		volume := session.Spec.Volumes[index]
		check := volume
		check.DestinationPVC = volume.SourcePVC
		check.DestinationPV = volume.SourcePV
		if err := s.switcher.VerifyVolumeOffline(ctx, &check); err != nil {
			return err
		}
	}
	return nil
}

// ValidateCleanup checks ownership, reclaim-policy, and deletion prerequisites
// through read-only API calls. It mirrors Cleanup's destructive guards.
func (s *Service) ValidateCleanup(ctx context.Context, session *domain.Session, options CleanupOptions) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "cleanup dry-run", "session is nil")
	}
	if err := session.Validate(); err != nil {
		return err
	}
	if !cleanupPhaseAllowed(session) {
		return domain.NewError(domain.ErrorPrecondition, "cleanup dry-run", fmt.Sprintf("session phase %s is still active", session.Status.Phase))
	}
	if options.DeleteSession && !options.Finalize {
		return domain.NewError(domain.ErrorPrecondition, "cleanup dry-run", "deleting the session requires --finalize")
	}
	for index := range session.Spec.Volumes {
		volumeValue := session.Spec.Volumes[index]
		volume := &volumeValue
		if !session.Spec.Operation().RebindsPVC() && (options.DeleteTemporary || options.DeleteRollback || options.DeleteSession) {
			if err := s.discoverDestinationRefs(ctx, session.ID, volume); err != nil {
				return err
			}
		}
		active, rollback, policy := cleanupPVRefs(session, volume)
		if options.DeleteSession && rollback.Name != "" && !options.DeleteRollback && !preservesCopyOutput(session, options) {
			return domain.NewError(domain.ErrorPrecondition, "cleanup dry-run", "deleting the session requires --delete-rollback-pv while a rollback PV is recorded")
		}
		if options.DeleteTemporary && volume.DestinationPVC.UID != "" {
			if err := s.ensurePVCUnused(ctx, volume.DestinationPVC); err != nil {
				return err
			}
			pvc, err := s.client.CoreV1().PersistentVolumeClaims(volume.DestinationPVC.Namespace).Get(ctx, volume.DestinationPVC.Name, metav1.GetOptions{})
			if err == nil {
				if pvc.UID != volume.DestinationPVC.UID || pvc.Labels[kube.SessionLabel] != session.ID {
					return domain.NewError(domain.ErrorConflict, "cleanup dry-run", fmt.Sprintf("PVC %s/%s identity or session ownership changed", pvc.Namespace, pvc.Name))
				}
			} else if !apierrors.IsNotFound(err) {
				return domain.WrapError(domain.ErrorKubernetes, "cleanup dry-run", fmt.Sprintf("read PVC %s/%s", volume.DestinationPVC.Namespace, volume.DestinationPVC.Name), err)
			}
		}
		if options.DeleteRollback && rollback.Name != "" {
			expectedRole := cleanupRollbackRole(session)
			pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, rollback.Name, metav1.GetOptions{})
			switch {
			case apierrors.IsNotFound(err):
				// Kubernetes may remove the rollback PV asynchronously. Continue
				// with active-PV validation for this volume.
			case err != nil:
				return domain.WrapError(domain.ErrorKubernetes, "cleanup dry-run", fmt.Sprintf("read rollback PV %s", rollback.Name), err)
			default:
				if pv.UID != rollback.UID || pv.Labels[kube.SessionLabel] != session.ID || pv.Labels[kube.ResourceRoleLabel] != expectedRole {
					return domain.NewError(domain.ErrorConflict, "cleanup dry-run", fmt.Sprintf("PV %s identity, ownership, or role changed", pv.Name))
				}
				if pv.Status.Phase != corev1.VolumeReleased && pv.Status.Phase != corev1.VolumeAvailable {
					deletionWillReleaseClaim := options.DeleteTemporary && pv.Status.Phase == corev1.VolumeBound && pv.Spec.ClaimRef != nil &&
						pv.Spec.ClaimRef.Namespace == volume.DestinationPVC.Namespace && pv.Spec.ClaimRef.Name == volume.DestinationPVC.Name &&
						(pv.Spec.ClaimRef.UID == "" || pv.Spec.ClaimRef.UID == volume.DestinationPVC.UID)
					if !deletionWillReleaseClaim {
						return domain.NewError(domain.ErrorPrecondition, "cleanup dry-run", fmt.Sprintf("PV %s phase %s must be Released or Available", pv.Name, pv.Status.Phase))
					}
				}
			}
		}
		if uncheckpointedSource(session, index) {
			if err := s.validateUncheckpointedSource(ctx, session.ID, volume); err != nil {
				return err
			}
			continue
		}
		if options.Finalize {
			if active.Name == "" {
				continue
			}
			if policy == "" {
				return domain.NewError(domain.ErrorPrecondition, "cleanup dry-run", fmt.Sprintf("PV %s has no recorded reclaim policy", active.Name))
			}
			if session.Status.Phase == domain.PhaseAborted && session.Status.Volumes[index].Activation.ActivePVC.Name == "" {
				if _, err := s.client.CoreV1().PersistentVolumes().Get(ctx, active.Name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
					continue
				} else if err != nil {
					return domain.WrapError(domain.ErrorKubernetes, "cleanup dry-run", fmt.Sprintf("read PV %s", active.Name), err)
				}
			}
			pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, active.Name, metav1.GetOptions{})
			if err != nil {
				return domain.WrapError(domain.ErrorKubernetes, "cleanup dry-run", fmt.Sprintf("read active PV %s", active.Name), err)
			}
			if err := validateFinalizablePV(pv, active, session.ID, policy); err != nil {
				return err
			}
			if preservesCopyOutput(session, options) && volume.DestinationPV.Name != "" {
				destinationPV, err := s.client.CoreV1().PersistentVolumes().Get(ctx, volume.DestinationPV.Name, metav1.GetOptions{})
				if err != nil {
					return domain.WrapError(domain.ErrorKubernetes, "cleanup dry-run", fmt.Sprintf("read copy destination PV %s", volume.DestinationPV.Name), err)
				}
				if err := validateFinalizablePV(destinationPV, volume.DestinationPV, session.ID, volume.DestinationPolicy); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateFinalizablePV(pv *corev1.PersistentVolume, ref domain.ObjectReference, sessionID string, policy corev1.PersistentVolumeReclaimPolicy) error {
	if policy == "" {
		return domain.NewError(domain.ErrorPrecondition, "cleanup dry-run", fmt.Sprintf("PV %s has no recorded reclaim policy", ref.Name))
	}
	if pv.UID != ref.UID {
		return domain.NewError(domain.ErrorConflict, "cleanup dry-run", fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name))
	}
	role := pv.Labels[kube.ResourceRoleLabel]
	if pv.Labels[kube.SessionLabel] == "" && role == "" && pv.Spec.PersistentVolumeReclaimPolicy == policy && pv.Annotations[kube.OriginalPolicyAnnotation] == "" {
		return nil
	}
	if pv.Labels[kube.SessionLabel] != sessionID || (role != "active" && role != "source" && role != "rename" && role != "destination") {
		return domain.NewError(domain.ErrorConflict, "cleanup dry-run", fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name))
	}
	return nil
}

// ValidateFinalSync performs the read-only checks required immediately before
// a final synchronization. It leaves the persisted session and workload intact.
func (s *Service) ValidateFinalSync(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "final sync dry-run", "session is nil")
	}
	if !session.Spec.Orchestrated() {
		return domain.NewError(domain.ErrorPrecondition, "final sync dry-run", "final sync requires an orchestrated migration session")
	}
	valid := session.Status.Phase == domain.PhasePaused || session.Status.Phase == domain.PhaseFinalSyncing || session.Status.Phase == domain.PhaseFinalSynced || (session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseFinalSyncing)
	if !valid {
		return domain.NewError(domain.ErrorPrecondition, "final sync dry-run", fmt.Sprintf("session phase %s cannot final-sync", session.Status.Phase))
	}
	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}
	for index := range session.Spec.Volumes {
		if err := s.switcher.VerifyVolumeOffline(ctx, &session.Spec.Volumes[index]); err != nil {
			return err
		}
	}
	return nil
}

// ValidateActivation performs activation preconditions through read-only API
// calls. The mutating PV/PVC switch remains behind --dry-run=false.
func (s *Service) ValidateActivation(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "activation dry-run", "session is nil")
	}
	if !session.Spec.Orchestrated() {
		return domain.NewError(domain.ErrorPrecondition, "activation dry-run", "activation requires an orchestrated migration session")
	}
	valid := session.Status.Phase == domain.PhaseFinalSynced || session.Status.Phase == domain.PhaseActivating || (session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseActivating)
	if !valid {
		return domain.NewError(domain.ErrorPrecondition, "activation dry-run", fmt.Sprintf("session phase %s cannot activate", session.Status.Phase))
	}
	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		status := &session.Status.Volumes[index]
		if status.Sync.FinalCompletedAt == nil {
			return domain.NewError(domain.ErrorPrecondition, "activation dry-run", fmt.Sprintf("PVC %s has no completed final sync", volume.SourcePVC.Name))
		}
		if err := s.switcher.VerifyVolumeOffline(ctx, volume); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) Reserve(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error { return s.reserve(lockedCtx, session) })
}

func (s *Service) reserve(ctx context.Context, session *domain.Session) error {
	if session.Status.Phase == domain.PhaseReserved || phaseAfter(session.Status.Phase, domain.PhaseReserved) {
		return nil
	}
	if err := s.begin(ctx, session, domain.PhaseReserving, "reserving destination storage"); err != nil {
		return err
	}
	if err := kube.EnsureNamespace(ctx, s.client, session.Spec.TemporaryNamespace, session.ID, false); err != nil {
		return s.failContext(ctx, session, err)
	}
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		status := &session.Status.Volumes[index]
		if status.Reserved && volume.DestinationPV.UID != "" {
			continue
		}
		if err := s.reserver.ReserveVolume(ctx, session, volume, status, false); err != nil {
			return s.failContext(ctx, session, err)
		}
		if err := s.store.Update(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}
	}
	return s.finish(ctx, session, domain.PhaseReserved, "destination storage is provisioned and retained")
}

func (s *Service) WarmCopy(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error { return s.warmCopy(lockedCtx, session) })
}

func (s *Service) warmCopy(ctx context.Context, session *domain.Session) error {
	if session.Status.Phase == domain.PhaseWarmCopied {
		for i := range session.Status.Volumes {
			session.Status.Volumes[i].Sync.WarmCompletedAt = nil
			session.Status.Volumes[i].Sync.LastError = ""
		}
	}
	valid := session.Status.Phase == domain.PhaseReserved ||
		session.Status.Phase == domain.PhaseWarmCopied ||
		session.Status.Phase == domain.PhaseWarmCopying ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseWarmCopying)
	if !valid {
		return domain.NewError(domain.ErrorPrecondition, "warm copy", fmt.Sprintf("session phase %s cannot warm-copy", session.Status.Phase))
	}
	if err := s.begin(ctx, session, domain.PhaseWarmCopying, "running warm copy"); err != nil {
		return err
	}
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		status := &session.Status.Volumes[index]
		if status.Sync.WarmCompletedAt != nil {
			continue
		}
		if err := s.validateCopyConsumers(ctx, session, volume); err != nil {
			return s.failContext(ctx, session, err)
		}
		if err := s.copyWithRetry(ctx, session, volume, status, copyengine.ModeWarm); err != nil {
			return s.failContext(ctx, session, err)
		}
		now := metav1.NewTime(s.now().UTC())
		status.Sync.WarmCompletedAt = &now
		status.Sync.LastError = ""
		if err := s.store.Update(ctx, session); err != nil {
			return err
		}
	}
	return s.finish(ctx, session, domain.PhaseWarmCopied, "warm copy completed for all volumes")
}

func (s *Service) Pause(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error { return s.pause(lockedCtx, session) })
}

func (s *Service) pause(ctx context.Context, session *domain.Session) error {
	if !session.Spec.Orchestrated() {
		return domain.NewError(domain.ErrorPrecondition, "pause workload", "workload pause requires an orchestrated migration session")
	}
	if session.Status.Phase == domain.PhasePaused || session.Status.Phase == domain.PhaseFinalSyncing || session.Status.Phase == domain.PhaseFinalSynced {
		return s.controllers.VerifyPaused(ctx, session)
	}
	valid := session.Status.Phase == domain.PhaseReserved || session.Status.Phase == domain.PhaseWarmCopied || session.Status.Phase == domain.PhasePausing || (session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhasePausing)
	if !valid {
		return domain.NewError(domain.ErrorPrecondition, "pause workload", fmt.Sprintf("session phase %s cannot pause", session.Status.Phase))
	}
	if err := s.begin(ctx, session, domain.PhasePausing, "pausing workload"); err != nil {
		return err
	}
	if err := s.controllers.Pause(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}
	// Controller adapters may record resource UIDs and original pause state
	// while applying the pause. Persist that recovery data before verification.
	if err := s.store.Update(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}
	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}
	return s.finish(ctx, session, domain.PhasePaused, "workload is safely paused")
}

func (s *Service) FinalSync(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error { return s.finalSync(lockedCtx, session) })
}

func (s *Service) finalSync(ctx context.Context, session *domain.Session) error {
	if !session.Spec.Orchestrated() {
		return domain.NewError(domain.ErrorPrecondition, "final sync", "final sync requires an orchestrated migration session")
	}
	valid := session.Status.Phase == domain.PhasePaused || session.Status.Phase == domain.PhaseFinalSyncing || session.Status.Phase == domain.PhaseFinalSynced || (session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseFinalSyncing)
	if !valid {
		return domain.NewError(domain.ErrorPrecondition, "final sync", fmt.Sprintf("session phase %s cannot final-sync", session.Status.Phase))
	}
	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}
	if session.Status.Phase == domain.PhaseFinalSynced {
		for i := range session.Status.Volumes {
			session.Status.Volumes[i].Sync.FinalCompletedAt = nil
			session.Status.Volumes[i].Sync.ChecksumVerified = false
		}
	}
	if err := s.begin(ctx, session, domain.PhaseFinalSyncing, "running offline final sync"); err != nil {
		return err
	}
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		status := &session.Status.Volumes[index]
		if status.Sync.FinalCompletedAt != nil {
			continue
		}
		if err := s.switcher.VerifyVolumeOffline(ctx, volume); err != nil {
			return s.failContext(ctx, session, err)
		}
		if err := s.copyWithRetry(ctx, session, volume, status, copyengine.ModeFinal); err != nil {
			return s.failContext(ctx, session, err)
		}
		now := metav1.NewTime(s.now().UTC())
		status.Sync.FinalCompletedAt = &now
		status.Sync.ChecksumVerified = session.Spec.WorkflowOptions().VerifyChecksum
		status.Sync.LastError = ""
		if err := s.store.Update(ctx, session); err != nil {
			return err
		}
	}
	return s.finish(ctx, session, domain.PhaseFinalSynced, "offline final sync completed for all volumes")
}

func (s *Service) Activate(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error { return s.activate(lockedCtx, session) })
}

func (s *Service) activate(ctx context.Context, session *domain.Session) error {
	if !session.Spec.Orchestrated() {
		return domain.NewError(domain.ErrorPrecondition, "activate", "activation requires an orchestrated migration session")
	}
	if session.Status.Phase == domain.PhaseActivated || session.Status.Phase == domain.PhaseResuming || session.Status.Phase == domain.PhaseCompleted || (session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseResuming) {
		return nil
	}
	valid := session.Status.Phase == domain.PhaseFinalSynced || session.Status.Phase == domain.PhaseActivating || (session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseActivating)
	if !valid {
		return domain.NewError(domain.ErrorPrecondition, "activate", fmt.Sprintf("session phase %s cannot activate", session.Status.Phase))
	}
	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}
	if err := s.begin(ctx, session, domain.PhaseActivating, "activating destination volumes"); err != nil {
		return err
	}
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		status := &session.Status.Volumes[index]
		if status.Activation.ActivatedAt != nil {
			continue
		}
		if err := s.switcher.ActivateVolume(ctx, session, volume, status, func() error {
			return s.store.Update(ctx, session)
		}); err != nil {
			return s.failContext(ctx, session, err)
		}
	}
	return s.finish(ctx, session, domain.PhaseActivated, "all destination volumes are active")
}

func (s *Service) ResumeWorkload(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error { return s.resumeWorkload(lockedCtx, session) })
}

func (s *Service) resumeWorkload(ctx context.Context, session *domain.Session) error {
	valid := session.Status.Phase == domain.PhaseActivated || session.Status.Phase == domain.PhaseResuming || (session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseResuming)
	if !valid {
		return domain.NewError(domain.ErrorPrecondition, "resume workload", fmt.Sprintf("session phase %s cannot resume", session.Status.Phase))
	}
	if err := s.begin(ctx, session, domain.PhaseResuming, "resuming workload"); err != nil {
		return err
	}
	if err := s.controllers.Resume(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}
	if err := s.verifyActiveVolumes(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}
	return s.finish(ctx, session, domain.PhaseCompleted, "migration completed and workload is ready")
}

func (s *Service) Migrate(ctx context.Context, session *domain.Session, warmPasses int) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.migrate(lockedCtx, session, warmPasses)
	})
}

func (s *Service) migrate(ctx context.Context, session *domain.Session, warmPasses int) error {
	if warmPasses < 0 {
		return domain.NewError(domain.ErrorValidation, "migrate", "warm passes must be non-negative")
	}
	if err := s.Reserve(ctx, session); err != nil {
		return err
	}
	for pass := 0; pass < warmPasses; pass++ {
		if err := s.WarmCopy(ctx, session); err != nil {
			return err
		}
	}
	if err := s.Pause(ctx, session); err != nil {
		return err
	}
	if err := s.FinalSync(ctx, session); err != nil {
		return err
	}
	if err := s.Activate(ctx, session); err != nil {
		return err
	}
	return s.ResumeWorkload(ctx, session)
}

func (s *Service) ResumeSession(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error { return s.resumeSession(lockedCtx, session) })
}

func (s *Service) resumeSession(ctx context.Context, session *domain.Session) error {
	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}
	composite := session.Spec.Operation() == domain.OperationMigrate || session.Spec.Operation() == domain.OperationMigratePod
	if !composite {
		if err := validateSingleResumePhase(session.Spec.Operation(), phase); err != nil {
			return err
		}
		switch phase {
		case domain.PhasePlanned:
			switch session.Spec.Operation() {
			case domain.OperationReserve:
				return s.Reserve(ctx, session)
			case domain.OperationCopy:
				if err := s.Reserve(ctx, session); err != nil {
					return err
				}
				return s.WarmCopy(ctx, session)
			case domain.OperationRename, domain.OperationMove:
				return s.Rename(ctx, session)
			default:
				return domain.NewError(domain.ErrorPrecondition, "resume session", fmt.Sprintf("planned phase cannot be resumed for operation %s", session.Spec.Operation()))
			}
		case domain.PhaseReserving:
			if session.Spec.Operation() == domain.OperationCopy {
				if err := s.Reserve(ctx, session); err != nil {
					return err
				}
				return s.WarmCopy(ctx, session)
			}
			return s.Reserve(ctx, session)
		case domain.PhaseReserved:
			if session.Spec.Operation() == domain.OperationCopy {
				return s.WarmCopy(ctx, session)
			}
			return nil
		case domain.PhaseWarmCopying:
			return s.WarmCopy(ctx, session)
		case domain.PhasePausing:
			return s.Pause(ctx, session)
		case domain.PhaseFinalSyncing:
			return s.FinalSync(ctx, session)
		case domain.PhaseActivating:
			return s.Activate(ctx, session)
		case domain.PhaseActivated, domain.PhaseResuming:
			return s.ResumeWorkload(ctx, session)
		case domain.PhaseRollingBack:
			return s.Rollback(ctx, session)
		case domain.PhaseAborting:
			return s.Abort(ctx, session)
		case domain.PhaseRenaming, domain.PhaseMoving:
			if (session.Spec.Operation() == domain.OperationRename && phase == domain.PhaseRenaming) || (session.Spec.Operation() == domain.OperationMove && phase == domain.PhaseMoving) {
				return s.Rename(ctx, session)
			}
			return domain.NewError(domain.ErrorPrecondition, "resume session", fmt.Sprintf("phase %s does not belong to operation %s", phase, session.Spec.Operation()))
		case domain.PhaseWarmCopied, domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
			return nil
		default:
			return domain.NewError(domain.ErrorPrecondition, "resume session", fmt.Sprintf("phase %s cannot be resumed for operation %s", phase, session.Spec.Operation()))
		}
	}
	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving:
		if err := s.Reserve(ctx, session); err != nil {
			return err
		}
		return s.Migrate(ctx, session, 1)
	case domain.PhaseReserved:
		return s.Migrate(ctx, session, 1)
	case domain.PhaseWarmCopying:
		if err := s.WarmCopy(ctx, session); err != nil {
			return err
		}
		return s.Migrate(ctx, session, 0)
	case domain.PhaseWarmCopied, domain.PhasePausing:
		return s.Migrate(ctx, session, 0)
	case domain.PhasePaused, domain.PhaseFinalSyncing:
		if err := s.FinalSync(ctx, session); err != nil {
			return err
		}
		if err := s.Activate(ctx, session); err != nil {
			return err
		}
		return s.ResumeWorkload(ctx, session)
	case domain.PhaseFinalSynced, domain.PhaseActivating:
		if err := s.Activate(ctx, session); err != nil {
			return err
		}
		return s.ResumeWorkload(ctx, session)
	case domain.PhaseActivated, domain.PhaseResuming:
		return s.ResumeWorkload(ctx, session)
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return nil
	case domain.PhaseRollingBack:
		return s.Rollback(ctx, session)
	case domain.PhaseAborting:
		return s.Abort(ctx, session)
	default:
		return domain.NewError(domain.ErrorPrecondition, "resume session", fmt.Sprintf("phase %s cannot be resumed", phase))
	}
}

func (s *Service) Abort(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error { return s.abort(lockedCtx, session) })
}

func (s *Service) abort(ctx context.Context, session *domain.Session) error {
	if session.Status.Phase == domain.PhaseAborted {
		return nil
	}
	if session.Status.Phase == domain.PhaseRollingBack || (session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseRollingBack) {
		return domain.NewError(domain.ErrorPrecondition, "abort session", "rollback recovery must continue through session resume or rollback")
	}
	if session.Status.Phase == domain.PhaseActivated || session.Status.Phase == domain.PhaseCompleted || session.Status.ResumeFrom == domain.PhaseActivating || session.Status.ResumeFrom == domain.PhaseResuming {
		return domain.NewError(domain.ErrorPrecondition, "abort session", "activated sessions require rollback")
	}
	previous := session.Status.Phase
	if session.Status.Phase == domain.PhaseFailed || session.Status.Phase == domain.PhaseAborting {
		previous = session.Status.ResumeFrom
	}
	if previous == domain.PhaseAborting {
		previous = phaseBefore(session, domain.PhaseAborting)
	}
	paused := previous == domain.PhasePausing || previous == domain.PhasePaused || previous == domain.PhaseFinalSyncing || previous == domain.PhaseFinalSynced
	if !paused && (session.Status.Phase == domain.PhaseAborting || session.Status.ResumeFrom == domain.PhaseAborting) {
		paused = abortOriginWasPaused(session)
	}
	if err := s.begin(ctx, session, domain.PhaseAborting, "aborting migration"); err != nil {
		return err
	}
	if paused {
		if err := s.controllers.Resume(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}
	}
	return s.finish(ctx, session, domain.PhaseAborted, "migration aborted; reserved volumes are retained for cleanup")
}

func (s *Service) Rollback(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error { return s.rollback(lockedCtx, session) })
}

func (s *Service) rollback(ctx context.Context, session *domain.Session) error {
	if session.Status.Phase == domain.PhaseRolledBack {
		return nil
	}
	rollbackOrigin := session.Status.ResumeFrom
	if rollbackOrigin == domain.PhaseRollingBack {
		rollbackOrigin = phaseBefore(session, domain.PhaseRollingBack)
	}
	wasRunning := session.Status.Phase == domain.PhaseCompleted || ((session.Status.Phase == domain.PhaseFailed || session.Status.Phase == domain.PhaseRollingBack) && (rollbackOrigin == domain.PhaseResuming || rollbackOrigin == domain.PhaseCompleted))
	failedDuringCutover := session.Status.Phase == domain.PhaseFailed && (session.Status.ResumeFrom == domain.PhaseActivating || session.Status.ResumeFrom == domain.PhaseResuming || session.Status.ResumeFrom == domain.PhaseRollingBack)
	valid := wasRunning || session.Status.Phase == domain.PhaseActivated || session.Status.Phase == domain.PhaseActivating || session.Status.Phase == domain.PhaseFinalSynced || session.Status.Phase == domain.PhaseRollingBack || failedDuringCutover
	if !valid {
		return domain.NewError(domain.ErrorPrecondition, "rollback", fmt.Sprintf("session phase %s cannot roll back", session.Status.Phase))
	}
	if err := s.begin(ctx, session, domain.PhaseRollingBack, "rolling back to source volumes"); err != nil {
		return err
	}
	if session.Spec.Operation().RebindsPVC() {
		if len(session.Spec.Volumes) != 1 {
			return s.failContext(ctx, session, domain.NewError(domain.ErrorInternal, "rollback rename", "rename session must contain one volume"))
		}
		volume := &session.Spec.Volumes[0]
		status := &session.Status.Volumes[0]
		reverse := *volume
		reverse.SourcePVC = volume.DestinationPVC
		reverse.SourcePVC.UID = status.Activation.ActivePVC.UID
		reverse.SourcePVC.ResourceVersion = status.Activation.ActivePVC.ResourceVersion
		reverse.DestinationPVC = volume.SourcePVC
		reverse.DestinationPVC.UID = ""
		reverse.DestinationPVC.ResourceVersion = ""
		pvc, err := s.switcher.RenamePVC(ctx, session, &reverse, func() error { return s.store.Update(ctx, session) })
		if err != nil {
			return s.failContext(ctx, session, err)
		}
		now := metav1.NewTime(s.now().UTC())
		status.Activation.ActivePVC = domain.ObjectReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID, ResourceVersion: pvc.ResourceVersion}
		status.Activation.RolledBackAt = &now
		return s.finish(ctx, session, domain.PhaseRolledBack, "PVC name restored")
	}
	if wasRunning {
		if err := s.controllers.Pause(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}
	}
	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}
	for index := len(session.Spec.Volumes) - 1; index >= 0; index-- {
		volume := &session.Spec.Volumes[index]
		status := &session.Status.Volumes[index]
		if err := s.switcher.RollbackVolume(ctx, session, volume, status, func() error {
			return s.store.Update(ctx, session)
		}); err != nil {
			return s.failContext(ctx, session, err)
		}
	}
	if err := s.controllers.Resume(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}
	return s.finish(ctx, session, domain.PhaseRolledBack, "source volumes restored and workload resumed")
}

func (s *Service) Rename(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error { return s.rename(lockedCtx, session) })
}

func (s *Service) rename(ctx context.Context, session *domain.Session) error {
	if !session.Spec.Operation().RebindsPVC() || len(session.Spec.Volumes) != 1 {
		return domain.NewError(domain.ErrorValidation, "rebind PVC", "session is not a single-volume PVC identity operation")
	}
	if session.Status.Phase == domain.PhaseCompleted {
		return nil
	}
	phase := domain.PhaseRenaming
	message := "renaming PVC while retaining its PV"
	if session.Spec.Operation() == domain.OperationMove {
		phase = domain.PhaseMoving
		message = "moving PVC while retaining its PV"
	}
	valid := session.Status.Phase == domain.PhasePlanned || session.Status.Phase == phase || (session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == phase)
	if !valid {
		return domain.NewError(domain.ErrorPrecondition, "rebind PVC", fmt.Sprintf("session phase %s cannot rebind PVC", session.Status.Phase))
	}
	if err := s.begin(ctx, session, phase, message); err != nil {
		return err
	}
	volume := &session.Spec.Volumes[0]
	status := &session.Status.Volumes[0]
	pvc, err := s.switcher.RenamePVC(ctx, session, volume, func() error { return s.store.Update(ctx, session) })
	if err != nil {
		return s.failContext(ctx, session, err)
	}
	now := metav1.NewTime(s.now().UTC())
	status.Activation.ActivePVC = domain.ObjectReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID, ResourceVersion: pvc.ResourceVersion}
	status.Activation.ActivatedAt = &now
	if session.Spec.Operation() == domain.OperationMove {
		return s.finish(ctx, session, domain.PhaseCompleted, "PVC move completed")
	}
	return s.finish(ctx, session, domain.PhaseCompleted, "PVC rename completed")
}

func (s *Service) validateCopyConsumers(ctx context.Context, session *domain.Session, volume *domain.VolumeSpec) error {
	if session.Spec.Operation() != domain.OperationCopy {
		return nil
	}
	pods, err := s.client.CoreV1().Pods(volume.SourcePVC.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "copy preflight", fmt.Sprintf("list PVC consumers in %s", volume.SourcePVC.Namespace), err)
	}
	active := make([]*corev1.Pod, 0)
	nodes := map[string]struct{}{}
	for index := range pods.Items {
		pod := &pods.Items[index]
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed || !kube.PodUsesPVC(pod, volume.SourcePVC.Name) {
			continue
		}
		active = append(active, pod)
		if pod.Spec.NodeName != "" {
			nodes[pod.Spec.NodeName] = struct{}{}
		}
	}
	if len(active) == 0 {
		return nil
	}
	options := session.Spec.WorkflowOptionsPtr()
	if options == nil {
		return domain.NewError(domain.ErrorValidation, "copy preflight", "copy session workflow options are missing")
	}
	if !session.Spec.Online() {
		return domain.NewError(domain.ErrorPrecondition, "copy preflight", fmt.Sprintf("offline copy requires PVC %s/%s to have zero active Pod consumers", volume.SourcePVC.Namespace, volume.SourcePVC.Name))
	}
	if kube.HasAccessMode(volume.AccessModes, corev1.ReadWriteOncePod) {
		return domain.NewError(domain.ErrorPrecondition, "copy preflight", fmt.Sprintf("active RWOP PVC %s/%s cannot be warm-copied", volume.SourcePVC.Namespace, volume.SourcePVC.Name))
	}
	if len(nodes) > 1 {
		return domain.NewError(domain.ErrorPrecondition, "copy preflight", fmt.Sprintf("online copy consumers for PVC %s/%s moved across multiple nodes", volume.SourcePVC.Namespace, volume.SourcePVC.Name))
	}
	if options.SourceNode == "" && len(nodes) == 1 {
		for node := range nodes {
			options.SourceNode = node
		}
		if s.store != nil {
			if err := s.store.Update(ctx, session); err != nil {
				return domain.WrapError(domain.ErrorKubernetes, "copy preflight", "persist inferred source node", err)
			}
		}
	}
	for node := range nodes {
		if options.SourceNode != "" && node != options.SourceNode {
			return domain.NewError(domain.ErrorConflict, "copy preflight", fmt.Sprintf("PVC %s/%s consumer runs on %s, session source node is %s", volume.SourcePVC.Namespace, volume.SourcePVC.Name, node, options.SourceNode))
		}
	}
	return nil
}

func (s *Service) copyWithRetry(ctx context.Context, session *domain.Session, volume *domain.VolumeSpec, status *domain.VolumeStatus, mode copyengine.Mode) error {
	values, err := s.helmSchedulingValues(ctx, session)
	if err != nil {
		return err
	}
	var last error
	options := session.Spec.WorkflowOptions()
	for retryIndex := 0; retryIndex < s.config.Retries; retryIndex++ {
		status.Sync.Attempts++
		status.Sync.LastError = ""
		if err := s.store.Update(ctx, session); err != nil {
			return err
		}
		request := copyengine.Request{
			SessionID:             session.ID,
			ToolImage:             options.ToolImage,
			Source:                volume.SourcePVC,
			Destination:           volume.DestinationPVC,
			Mode:                  mode,
			Attempt:               status.Sync.Attempts,
			KubeconfigPath:        s.config.KubeconfigPath,
			Context:               s.config.Context,
			Strategies:            options.Strategies,
			DeleteExtraneousFiles: options.DeleteExtraneous,
			VerifyChecksum:        mode == copyengine.ModeFinal && options.VerifyChecksum,
			NoCompress:            s.config.NoCompress,
			HelmTimeout:           s.config.HelmTimeout,
			HelmStringValues:      values,
			Writer:                s.config.Writer,
			Logger:                s.config.Logger,
		}
		copyErr := s.copier.Copy(ctx, request, func(progress copyengine.Progress) {
			s.config.Logger.Info("copy progress", "session", session.ID, "pvc", volume.SourcePVC.Name, "mode", progress.Mode, "attempt", progress.Attempt, "state", progress.State, "message", progress.Message)
		})
		last = errors.Join(copyErr, s.waitForCopyHelperRelease(ctx, volume))
		if last == nil {
			return nil
		}
		status.Sync.LastError = last.Error()
		if err := s.store.Update(ctx, session); err != nil {
			// Preserve the copy failure when the operation context was canceled;
			// failContext checkpoints the updated status with an independent context.
			if ctx.Err() != nil {
				return last
			}
			return err
		}
		if retryIndex+1 < s.config.Retries {
			delay := time.Duration(math.Pow(2, float64(retryIndex))) * s.config.RetryBackoff
			if err := s.sleep(ctx, delay); err != nil {
				return domain.WrapError(domain.ErrorTimeout, "copy retry", "context ended during retry backoff", err)
			}
		}
	}
	return last
}

func (s *Service) waitForCopyHelperRelease(ctx context.Context, volume *domain.VolumeSpec) error {
	claims := map[string]map[string]struct{}{}
	for _, ref := range []domain.ObjectReference{volume.SourcePVC, volume.DestinationPVC} {
		if claims[ref.Namespace] == nil {
			claims[ref.Namespace] = map[string]struct{}{}
		}
		claims[ref.Namespace][ref.Name] = struct{}{}
	}
	return kube.WaitFor(ctx, time.Second, fmt.Sprintf("pv-migrate helpers to release PVC %s/%s", volume.SourcePVC.Namespace, volume.SourcePVC.Name), func(waitCtx context.Context) (bool, error) {
		for namespace, namespaceClaims := range claims {
			pods, err := s.client.CoreV1().Pods(namespace).List(waitCtx, metav1.ListOptions{})
			if err != nil {
				return false, domain.WrapError(domain.ErrorKubernetes, "copy cleanup", fmt.Sprintf("list Pods in %s", namespace), err)
			}
			for i := range pods.Items {
				if isPVMigrateHelperForClaims(&pods.Items[i], namespaceClaims) {
					return false, nil
				}
			}
		}
		return true, nil
	})
}

func isPVMigrateHelperForClaims(pod *corev1.Pod, claims map[string]struct{}) bool {
	instance := pod.Labels["app.kubernetes.io/instance"]
	component := pod.Labels["app.kubernetes.io/component"]
	if !strings.HasPrefix(instance, "pv-migrate-") || (component != "sshd" && component != "rsync" && component != "rclone") {
		return false
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		if _, exists := claims[volume.PersistentVolumeClaim.ClaimName]; exists {
			return true
		}
	}
	return false
}

func (s *Service) helmSchedulingValues(ctx context.Context, session *domain.Session) ([]string, error) {
	values := kube.ZeroResourceHelmValues()
	options := session.Spec.WorkflowOptions()
	type schedulingTarget struct {
		component string
		nodes     []string
		pinNode   bool
	}
	targets := []schedulingTarget{{component: "rsync", nodes: []string{options.TargetNode}, pinNode: true}}
	if slices.Contains(options.Strategies, "local") {
		// The local strategy deploys an SSHD on both sides. PVC topology places
		// each Pod on its volume's node, while the combined tolerations allow
		// both source and destination nodes to accept their respective Pod.
		targets = append(targets, schedulingTarget{component: "sshd", nodes: []string{options.SourceNode, options.TargetNode}})
	} else {
		targets = append(targets, schedulingTarget{component: "sshd", nodes: []string{options.SourceNode}, pinNode: true})
	}
	for _, target := range targets {
		tolerationIndex := 0
		seenNodes := map[string]struct{}{}
		seenTolerations := map[string]struct{}{}
		for _, nodeName := range target.nodes {
			if nodeName == "" {
				continue
			}
			if _, seen := seenNodes[nodeName]; seen {
				continue
			}
			seenNodes[nodeName] = struct{}{}
			node, err := s.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
			if err != nil {
				return nil, domain.WrapError(domain.ErrorKubernetes, "copy scheduling", fmt.Sprintf("read node %s", nodeName), err)
			}
			if target.pinNode {
				hostname := node.Labels[corev1.LabelHostname]
				if hostname == "" {
					return nil, domain.NewError(domain.ErrorPrecondition, "copy scheduling", fmt.Sprintf("node %s lacks %s", nodeName, corev1.LabelHostname))
				}
				values = append(values, fmt.Sprintf("%s.nodeSelector.kubernetes\\.io/hostname=%s", target.component, hostname))
			}
			for _, taint := range node.Spec.Taints {
				if taint.Effect != corev1.TaintEffectNoSchedule && taint.Effect != corev1.TaintEffectNoExecute {
					continue
				}
				signature := taint.Key + "\x00" + taint.Value + "\x00" + string(taint.Effect)
				if _, seen := seenTolerations[signature]; seen {
					continue
				}
				seenTolerations[signature] = struct{}{}
				prefix := fmt.Sprintf("%s.tolerations[%d]", target.component, tolerationIndex)
				values = append(values, prefix+".key="+taint.Key, prefix+".effect="+string(taint.Effect))
				if taint.Value == "" {
					values = append(values, prefix+".operator=Exists")
				} else {
					values = append(values, prefix+".operator=Equal", prefix+".value="+taint.Value)
				}
				tolerationIndex++
			}
		}
	}
	return values, nil
}

func (s *Service) verifyActiveStorage(ctx context.Context, session *domain.Session) error {
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		pvc, err := s.client.CoreV1().PersistentVolumeClaims(volume.SourcePVC.Namespace).Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(domain.ErrorKubernetes, "verify migration", fmt.Sprintf("read PVC %s/%s", volume.SourcePVC.Namespace, volume.SourcePVC.Name), err)
		}
		if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName != volume.DestinationPV.Name || pvc.Annotations[kube.SessionAnnotation] != session.ID {
			return domain.NewError(domain.ErrorConflict, "verify migration", fmt.Sprintf("PVC %s/%s is not active on destination PV %s", pvc.Namespace, pvc.Name, volume.DestinationPV.Name))
		}
		active := session.Status.Volumes[index].Activation.ActivePVC
		if active.UID != "" && pvc.UID != active.UID {
			return domain.NewError(domain.ErrorConflict, "verify migration", fmt.Sprintf("active PVC %s/%s UID changed", pvc.Namespace, pvc.Name))
		}
		if active.UID != "" && volume.DestinationPV.UID != "" {
			pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, volume.DestinationPV.Name, metav1.GetOptions{})
			if err != nil {
				return domain.WrapError(domain.ErrorKubernetes, "verify migration", fmt.Sprintf("read active PV %s", volume.DestinationPV.Name), err)
			}
			if pv.UID != volume.DestinationPV.UID || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != pvc.Namespace || pv.Spec.ClaimRef.Name != pvc.Name || pv.Spec.ClaimRef.UID != pvc.UID {
				return domain.NewError(domain.ErrorConflict, "verify migration", fmt.Sprintf("active PV %s identity or claimRef changed", pv.Name))
			}
		}
	}
	return nil
}

func (s *Service) validateWorkloadResume(ctx context.Context, session *domain.Session) error {
	if err := s.verifyActiveStorage(ctx, session); err != nil {
		return err
	}
	options := session.Spec.WorkflowOptions()
	// TargetNode is the exact placement contract only for the standalone
	// adapter. Controller-managed workloads keep their own scheduling policy;
	// the helper node can become unavailable while the workload still has a
	// valid placement elsewhere.
	if session.Spec.Workload().Adapter != domain.WorkloadStandalone || options.TargetNode == "" {
		return nil
	}
	node, err := s.client.CoreV1().Nodes().Get(ctx, options.TargetNode, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "resume dry-run", "read target node", err)
	}
	if !nodeReadyAndSchedulable(node) {
		return domain.NewError(domain.ErrorPrecondition, "resume dry-run", fmt.Sprintf("target node %s must be Ready and schedulable", node.Name))
	}
	return nil
}

func (s *Service) verifyActiveVolumes(ctx context.Context, session *domain.Session) error {
	if err := s.verifyActiveStorage(ctx, session); err != nil {
		return err
	}
	options := session.Spec.WorkflowOptions()
	// TargetNode pins reservation and copy helpers for every workload. The
	// standalone adapter also pins the recreated Pod; controller-managed
	// workloads retain their own scheduler policy and may validly land on a
	// different node when the destination volume is topology-independent.
	if session.Spec.Workload().Adapter == domain.WorkloadStandalone && options.TargetNode != "" {
		pod, err := s.client.CoreV1().Pods(session.Spec.Workload().Pod.Namespace).Get(ctx, session.Spec.Workload().Pod.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(domain.ErrorKubernetes, "verify migration", "read resumed Pod", err)
		}
		if pod.Spec.NodeName != options.TargetNode {
			return domain.NewError(domain.ErrorPrecondition, "verify migration", fmt.Sprintf("Pod %s/%s runs on %s, expected %s", pod.Namespace, pod.Name, pod.Spec.NodeName, options.TargetNode))
		}
	}
	return nil
}

func nodeReadyAndSchedulable(node *corev1.Node) bool {
	if node == nil || node.Spec.Unschedulable {
		return false
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (s *Service) begin(ctx context.Context, session *domain.Session, phase domain.Phase, message string) error {
	if session.Status.Phase == phase {
		return nil
	}
	if session.Status.Phase == domain.PhaseFailed {
		if phase != domain.PhaseRollingBack && phase != domain.PhaseAborting && session.Status.ResumeFrom != phase {
			return domain.NewError(domain.ErrorPrecondition, "resume phase", fmt.Sprintf("failed session resumes from %s, requested %s", session.Status.ResumeFrom, phase))
		}
	}
	if err := session.Transition(phase, message, s.now()); err != nil {
		return err
	}
	return s.persist(ctx, session)
}

func (s *Service) finish(ctx context.Context, session *domain.Session, phase domain.Phase, message string) error {
	if err := session.Transition(phase, message, s.now()); err != nil {
		return err
	}
	return s.persist(ctx, session)
}

func (s *Service) fail(ctx context.Context, session *domain.Session, cause error) error {
	return s.failContext(ctx, session, cause)
}

func (s *Service) failContext(ctx context.Context, session *domain.Session, cause error) error {
	if session.Status.Phase != domain.PhaseFailed {
		if err := session.Transition(domain.PhaseFailed, cause.Error(), s.now()); err != nil {
			return errors.Join(cause, err)
		}
		// The operation context may already be canceled when a stage fails.
		// Keep the failure checkpoint alive long enough to persist ResumeFrom,
		// while checking the session fence before and after the write.
		if held, ok := ctx.Value(sessionLockContextKey{}).(heldSessionLock); ok {
			if err := held.lock.Err(); err != nil {
				return errors.Join(cause, err)
			}
		}
		checkpointCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := s.store.Update(checkpointCtx, session)
		cancel()
		if held, ok := ctx.Value(sessionLockContextKey{}).(heldSessionLock); ok {
			if lockErr := held.lock.Err(); lockErr != nil {
				err = errors.Join(err, lockErr)
			}
		}
		if err != nil {
			return errors.Join(cause, err)
		}
	}
	return cause
}

func (s *Service) persist(ctx context.Context, session *domain.Session) error {
	persistCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return s.store.Update(persistCtx, session)
}

func phaseAfter(current, reference domain.Phase) bool {
	order := []domain.Phase{
		domain.PhasePlanned,
		domain.PhaseReserving,
		domain.PhaseReserved,
		domain.PhaseWarmCopying,
		domain.PhaseWarmCopied,
		domain.PhasePausing,
		domain.PhasePaused,
		domain.PhaseFinalSyncing,
		domain.PhaseFinalSynced,
		domain.PhaseActivating,
		domain.PhaseActivated,
		domain.PhaseResuming,
		domain.PhaseCompleted,
	}
	index := func(value domain.Phase) int {
		for i, phase := range order {
			if value == phase {
				return i
			}
		}
		return -1
	}
	return index(current) > index(reference)
}

func phaseBefore(session *domain.Session, target domain.Phase) domain.Phase {
	for index := len(session.Status.History) - 1; index > 0; index-- {
		if session.Status.History[index].Phase == target {
			return session.Status.History[index-1].Phase
		}
	}
	return ""
}

func abortOriginWasPaused(session *domain.Session) bool {
	for index := len(session.Status.History) - 1; index >= 0; index-- {
		switch session.Status.History[index].Phase {
		case domain.PhaseFailed, domain.PhaseAborting:
			continue
		case domain.PhasePausing, domain.PhasePaused, domain.PhaseFinalSyncing, domain.PhaseFinalSynced:
			return true
		default:
			return false
		}
	}
	return false
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
