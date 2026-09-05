package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

const openEBSLVMSharedMountCleanupTimeout = 10 * time.Second

type Config struct {
	KubeconfigPath  string
	Context         string
	Retries         int
	RetryBackoff    time.Duration
	HelmTimeout     time.Duration
	NoCompress      bool
	StreamToolLogs  bool
	StructuredLogs  bool
	Writer          io.Writer
	Logger          *slog.Logger
	ToolImageProber kube.ToolImageProber
	// TrustedToolImage overrides the workflow image in controller mode. The
	// controller owns the data-mover image so tenant CRs cannot execute an
	// arbitrary image, and upgrades can continue existing sessions safely.
	TrustedToolImage              string
	VolumeUsageReader             kube.VolumeUsageReader
	OpenEBSLVMSharedVolumeManager kube.OpenEBSLVMSharedVolumeManager
}

func (s *Service) toolImage(session *domain.Session) string {
	if s != nil {
		if trusted := strings.TrimSpace(s.config.TrustedToolImage); trusted != "" {
			return trusted
		}
	}

	if session == nil {
		return ""
	}

	return session.Spec.WorkflowOptions().ToolImage
}

type Service struct {
	client      kubernetes.Interface
	store       kube.LockingSessionStore
	reserver    volumeReserver
	copier      copyengine.Engine
	controllers workloadController
	switcher    volumeSwitcher
	config      Config
	now         func() time.Time
	sleep       func(context.Context, time.Duration) error
}

type volumeReserver interface {
	ReserveVolume(
		ctx context.Context,
		session *domain.Session,
		volume *domain.VolumeSpec,
		status *domain.VolumeStatus,
		recreate bool,
	) error
}

type workloadController interface {
	Pause(ctx context.Context, session *domain.Session) error
	ValidateResume(ctx context.Context, session *domain.Session) error
	Resume(ctx context.Context, session *domain.Session) error
	VerifyPaused(ctx context.Context, session *domain.Session) error
	CurrentRollbackPods(
		ctx context.Context,
		session *domain.Session,
	) ([]domain.ObjectReference, error)
}

type volumeSwitcher interface {
	VerifyVolumeOffline(ctx context.Context, volume *domain.VolumeSpec) error
	VerifyVolumesOfflineForSession(
		ctx context.Context,
		sessionID string,
		volumes []*domain.VolumeSpec,
	) error
	ActivateVolume(
		ctx context.Context,
		session *domain.Session,
		volume *domain.VolumeSpec,
		status *domain.VolumeStatus,
		progress kube.ProgressFunc,
	) error
	RollbackVolume(
		ctx context.Context,
		session *domain.Session,
		volume *domain.VolumeSpec,
		status *domain.VolumeStatus,
		progress kube.ProgressFunc,
	) error
	RenamePVC(
		ctx context.Context,
		session *domain.Session,
		volume *domain.VolumeSpec,
		progress kube.ProgressFunc,
	) (*corev1.PersistentVolumeClaim, error)
}

func NewService(
	client kubernetes.Interface,
	store kube.LockingSessionStore,
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

	config.Writer = kube.NewSynchronizedWriter(config.Writer)

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

// withSessionIDLock serializes every mutating session operation. The context
// marker makes nested stage calls re-entrant while preserving the same lease.
func (s *Service) withSessionIDLock(
	ctx context.Context,
	namespace, id string,
	fn func(context.Context) error,
) error {
	if namespace == "" || id == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"session lock",
			"session namespace and ID are required",
		)
	}

	if s == nil || s.store == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"session lock",
			"session store is required",
		)
	}

	if held, ok := ctx.Value(sessionLockContextKey{}).(heldSessionLock); ok &&
		held.namespace == namespace &&
		held.id == id {
		if err := held.lock.Err(); err != nil {
			return err
		}
		return fn(ctx)
	}

	s.logInfo("acquiring session lock", "session", id, "namespace", namespace)

	lock, err := kube.AcquireRequiredSessionLock(ctx, s.store, namespace, id)
	if err != nil {
		return err
	}

	operationCtx, cancelOperation := lock.Bind(ctx)
	defer cancelOperation()

	lockedCtx := context.WithValue(
		operationCtx,
		sessionLockContextKey{},
		heldSessionLock{lock: lock, namespace: namespace, id: id},
	)

	operationErr := fn(lockedCtx)
	if leaseErr := lock.Err(); leaseErr != nil {
		operationErr = errors.Join(operationErr, leaseErr)
	}

	releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelRelease()

	releaseErr := lock.Release(releaseCtx)
	if releaseErr != nil {
		s.logWarn(
			"session lock release failed",
			"session",
			id,
			"namespace",
			namespace,
			"error",
			releaseErr,
		)
	} else {
		s.logInfo("session lock released", "session", id, "namespace", namespace)
	}

	return errors.Join(operationErr, releaseErr)
}

func (s *Service) withSessionLock(
	ctx context.Context,
	session *domain.Session,
	fn func(context.Context) error,
) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "session lock", "session is nil")
	}

	return s.withSessionIDLock(
		ctx,
		session.Spec.SessionNamespace,
		kube.SessionLockID(session),
		fn,
	)
}

func (s *Service) CreateSession(
	ctx context.Context,
	plan *domain.MigrationPlan,
	dryRun bool,
) (*domain.Session, error) {
	if plan == nil {
		return nil, domain.NewError(domain.ErrorValidation, "create session", "plan is nil")
	}

	if !plan.Ready {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"create session",
			"migration plan contains failed checks",
		)
	}

	session := domain.NewSession(plan.SessionID, plan.SessionSpec, s.now())
	if s.store.StorageBackend() == kube.SessionBackendCRD {
		if err := session.Validate(); err != nil {
			return nil, err
		}

		if !dryRun {
			if err := s.store.Create(ctx, session); err != nil {
				return nil, err
			}
		}

		return session, nil
	}

	if dryRun {
		if err := s.ensureSessionNamespaces(ctx, plan, true); err != nil {
			return nil, err
		}

		if err := s.ValidateReservation(ctx, session); err != nil {
			return nil, err
		}

		return session, nil
	}

	s.logInfo(
		"creating migration session",
		"session",
		plan.SessionID,
		"sessionNamespace",
		plan.SessionSpec.SessionNamespace,
		"temporaryNamespace",
		plan.SessionSpec.TemporaryNamespace,
	)
	// The Lease used to serialize creation lives in the session namespace.
	// Ensure that namespace exists before attempting to acquire the Lease.
	if err := kube.EnsureNamespace(
		ctx,
		s.client,
		plan.SessionSpec.SessionNamespace,
		plan.SessionID,
		false,
	); err != nil {
		return nil, err
	}

	createErr := s.withSessionIDLock(
		ctx,
		plan.SessionSpec.SessionNamespace,
		kube.SessionLockID(session),
		func(lockedCtx context.Context) error {
			if err := s.ensureSessionNamespaces(lockedCtx, plan, false); err != nil {
				return err
			}
			return s.store.Create(lockedCtx, session)
		},
	)
	if createErr != nil {
		return nil, createErr
	}

	return session, nil
}

func (s *Service) begin(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
	message string,
) error {
	if session.Status.Phase == phase {
		s.logInfo(
			"migration stage resumed",
			"session",
			session.ID,
			"phase",
			phase,
			"message",
			message,
		)

		return nil
	}

	if session.Status.Phase == domain.PhaseFailed {
		if phase != domain.PhaseRollingBack && phase != domain.PhaseAborting &&
			session.Status.ResumeFrom != phase {
			return domain.NewError(
				domain.ErrorPrecondition,
				"resume phase",
				fmt.Sprintf(
					"failed session resumes from %s, requested %s",
					session.Status.ResumeFrom,
					phase,
				),
			)
		}
	}

	previousStatus := session.Status
	if err := session.Transition(phase, message, s.now()); err != nil {
		return err
	}

	if err := s.persist(ctx, session); err != nil {
		session.Status = previousStatus
		s.logError(
			"migration stage persistence failed",
			"session",
			session.ID,
			"phase",
			phase,
			"message",
			message,
			"error",
			err,
		)

		return err
	}

	s.logInfo("migration stage started", "session", session.ID, "phase", phase, "message", message)

	return nil
}

func (s *Service) finish(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
	message string,
) error {
	previousStatus := session.Status
	if err := session.Transition(phase, message, s.now()); err != nil {
		return err
	}

	if err := s.persist(ctx, session); err != nil {
		session.Status = previousStatus
		s.logError(
			"migration stage persistence failed",
			"session",
			session.ID,
			"phase",
			phase,
			"message",
			message,
			"error",
			err,
		)

		return err
	}

	s.logInfo(
		"migration stage completed",
		"session",
		session.ID,
		"phase",
		phase,
		"message",
		message,
	)

	return nil
}

func (s *Service) fail(ctx context.Context, session *domain.Session, cause error) error {
	return s.failContext(ctx, session, cause)
}

func (s *Service) failContext(ctx context.Context, session *domain.Session, cause error) error {
	if session.Status.Phase != domain.PhaseFailed {
		session.Status.FailureReason = failureReason(cause)
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

func failureReason(err error) domain.SessionFailureReason {
	if isDestinationNoSpaceError(err) {
		return domain.FailureDestinationCapacityExhausted
	}
	return ""
}

func validateRetryableSessionFailure(session *domain.Session) error {
	if session != nil && session.Status.Phase == domain.PhaseFailed &&
		session.Status.FailureReason == domain.FailureDestinationCapacityExhausted {
		message := "destination capacity was exhausted and cannot be changed in this session; abort and clean up this session, then create a new session with a larger --destination-capacity"
		if kubeblocks, ok := session.Spec.KubeBlocksPodMigration(); ok {
			message = fmt.Sprintf(
				"destination capacity was exhausted for KubeBlocks Cluster %s component %s; update the component volumeClaimTemplates storage request, abort and clean up this session, then create a new migrate-pod session",
				kubeblocks.Cluster,
				kubeblocks.Component,
			)
		}

		return domain.NewError(
			domain.ErrorConflict,
			"resume session",
			message,
		)
	}

	return nil
}

func (s *Service) persist(ctx context.Context, session *domain.Session) error {
	persistCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return s.store.Update(persistCtx, session)
}

func (s *Service) logInfo(message string, args ...any) {
	if s != nil && s.config.Logger != nil {
		s.config.Logger.Info(message, args...)
	}
}

func (s *Service) logWarn(message string, args ...any) {
	if s != nil && s.config.Logger != nil {
		s.config.Logger.Warn(message, args...)
	}
}

func (s *Service) logError(message string, args ...any) {
	if s != nil && s.config.Logger != nil {
		s.config.Logger.Error(message, args...)
	}
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
	for index, entry := range slices.Backward(session.Status.History) {
		if entry.Phase != target {
			continue
		}

		for previous := index - 1; previous >= 0; previous-- {
			phase := session.Status.History[previous].Phase
			if phase == domain.PhaseFailed || phase == target {
				continue
			}

			return phase
		}

		return ""
	}

	return ""
}

func abortRequiresWorkloadResume(session *domain.Session) bool {
	if session == nil || session.Spec.Operation() != domain.OperationMigratePod {
		return false
	}

	previous := session.Status.Phase
	if previous == domain.PhaseFailed || previous == domain.PhaseAborting {
		previous = session.Status.ResumeFrom
	}

	if previous == domain.PhaseAborting {
		previous = phaseBefore(session, domain.PhaseAborting)
	}

	switch previous {
	case domain.PhasePausing, domain.PhasePaused, domain.PhaseFinalSyncing, domain.PhaseFinalSynced:
		return true
	}

	if session.Status.Phase == domain.PhaseAborting ||
		session.Status.ResumeFrom == domain.PhaseAborting {
		return abortOriginWasPaused(session)
	}

	return false
}

func abortOriginWasPaused(session *domain.Session) bool {
	for _, v := range slices.Backward(session.Status.History) {
		switch v.Phase {
		case domain.PhaseFailed, domain.PhaseAborting:
			continue
		case domain.PhasePausing,
			domain.PhasePaused,
			domain.PhaseFinalSyncing,
			domain.PhaseFinalSynced:
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
