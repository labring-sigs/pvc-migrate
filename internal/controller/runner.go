package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/backup"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"k8s.io/client-go/kubernetes"
)

// Runner reconciles every workflow CR by delegating its stages to app.Service.
// The service remains the single owner of phase transitions, persistence and
// resource fencing; this runner only provides event polling and dispatch.
type Runner struct {
	service      workflowResumer
	store        kube.SessionStore
	client       kubernetes.Interface
	namespace    string
	pollInterval time.Duration
	logger       *slog.Logger
	openEBS      kube.OpenEBSLVMSharedVolumeManager
}

// workflowResumer is the controller-facing subset of app.Service. Keeping the
// runner dependent on this narrow contract makes the operation dispatch
// explicit and allows every workflow kind to be tested without constructing a
// full Kubernetes service fixture.
type workflowResumer interface {
	ResumeReserve(ctx context.Context, session *domain.Session) error
	ResumeOfflineMigration(ctx context.Context, session *domain.Session) error
	ResumePodMigration(ctx context.Context, session *domain.Session) error
	ResumeCopy(ctx context.Context, session *domain.Session) error
	ResumeRename(ctx context.Context, session *domain.Session) error
	ResumeMove(ctx context.Context, session *domain.Session) error
}

func (r *Runner) WithKubernetesClient(client kubernetes.Interface) *Runner {
	if r != nil {
		r.client = client
	}
	return r
}

func (r *Runner) WithOpenEBSLVMSharedVolumeManager(
	manager kube.OpenEBSLVMSharedVolumeManager,
) *Runner {
	if r != nil {
		r.openEBS = manager
	}
	return r
}

func NewRunner(service workflowResumer, store kube.SessionStore, namespace string) *Runner {
	return &Runner{
		service:      service,
		store:        store,
		namespace:    namespace,
		pollInterval: 5 * time.Second,
		logger:       slog.Default(),
	}
}

func (r *Runner) WithPollInterval(interval time.Duration) *Runner {
	if interval > 0 {
		r.pollInterval = interval
	}
	return r
}

func (r *Runner) WithLogger(logger *slog.Logger) *Runner {
	if logger != nil {
		r.logger = logger
	}
	return r
}

// Run blocks until ctx is canceled. A reconciliation pass runs immediately
// so newly submitted resources do not wait for the first ticker interval.
func (r *Runner) Run(ctx context.Context) error {
	if r == nil || r.service == nil || r.store == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"controller",
			"service and session store are required",
		)
	}

	if err := r.ReconcileOnce(ctx); err != nil {
		r.logger.Warn("controller reconciliation pass failed", "error", err)
	}

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.ReconcileOnce(ctx); err != nil {
				r.logger.Warn("controller reconciliation pass failed", "error", err)
			}
		}
	}
}

// ReconcileOnce processes all active workflow CRs in the configured
// namespace. One broken resource does not prevent unrelated resources from
// progressing; the returned error aggregates failures for observability.
func (r *Runner) ReconcileOnce(ctx context.Context) error {
	if r == nil || r.store == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"controller reconcile",
			"session store is required",
		)
	}

	sessions, err := r.store.List(ctx, r.namespace)
	if err != nil {
		return err
	}

	var failures []error
	for _, session := range sessions {
		if session == nil || terminalSession(session) {
			continue
		}

		if err := r.reconcileSession(ctx, session); err != nil {
			failureErr := r.checkpointFailure(ctx, session, err)

			failures = append(failures, fmt.Errorf("session %s: %w", session.ID, failureErr))
		}
	}

	return errors.Join(failures...)
}

// checkpointFailure records errors that happen before a workflow service can
// acquire its own session lock. Re-read the session while holding the Lease so
// a stale reconcile cannot overwrite a newer phase written by another worker.
func (r *Runner) checkpointFailure(
	ctx context.Context,
	session *domain.Session,
	cause error,
) error {
	if session == nil {
		return cause
	}

	locker, supported := r.store.(kube.SessionLocker)
	if !supported {
		return r.transitionFailure(ctx, session, cause)
	}

	namespace := session.Spec.SessionNamespace

	lock, err := locker.AcquireSessionLock(ctx, namespace, session.ID)
	if err != nil {
		return errors.Join(cause, err)
	}

	operationCtx, cancelOperation := lock.Bind(ctx)
	operationErr := func() error {
		latest, getErr := r.store.Get(operationCtx, namespace, session.ID)
		if getErr != nil {
			return errors.Join(cause, getErr)
		}

		if latest == nil || terminalSession(latest) || latest.Status.Phase == domain.PhaseFailed {
			return cause
		}

		return r.transitionFailure(operationCtx, latest, cause)
	}()

	cancelOperation()

	if lockErr := lock.Err(); lockErr != nil {
		operationErr = errors.Join(operationErr, lockErr)
	}

	releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 10*time.Second)
	releaseErr := lock.Release(releaseCtx)

	cancelRelease()

	return errors.Join(operationErr, releaseErr)
}

func (r *Runner) transitionFailure(
	ctx context.Context,
	session *domain.Session,
	cause error,
) error {
	if session == nil || terminalSession(session) || session.Status.Phase == domain.PhaseFailed {
		return cause
	}

	if err := session.Transition(domain.PhaseFailed, cause.Error(), time.Now()); err != nil {
		return errors.Join(cause, err)
	}

	return errors.Join(cause, r.store.Update(ctx, session))
}

func (r *Runner) reconcileSession(ctx context.Context, session *domain.Session) error {
	if r == nil || r.service == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"controller reconcile",
			"service is required",
		)
	}

	if !kube.ControllerSessionSupported(session) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller reconcile",
			"workflow is outside the controller contract; use the CLI/session backend",
		)
	}

	r.logger.Info(
		"reconciling migration",
		"session",
		session.ID,
		"type",
		session.Spec.Type,
		"phase",
		session.Status.Phase,
	)

	switch session.Spec.Type {
	case domain.SessionTypeReserve:
		return r.service.ResumeReserve(ctx, session)
	case domain.SessionTypeMigrate:
		return r.service.ResumeOfflineMigration(ctx, session)
	case domain.SessionTypeMigratePod:
		return r.service.ResumePodMigration(ctx, session)
	case domain.SessionTypeCopy:
		return r.service.ResumeCopy(ctx, session)
	case domain.SessionTypeRename:
		return r.service.ResumeRename(ctx, session)
	case domain.SessionTypeMove:
		return r.service.ResumeMove(ctx, session)
	case domain.SessionTypeBackup:
		return r.resumeBackup(ctx, session)
	case domain.SessionTypeRestore:
		return r.resumeRestore(ctx, session)
	default:
		return domain.NewError(
			domain.ErrorValidation,
			"controller reconcile",
			fmt.Sprintf("unsupported session type %q", session.Spec.Type),
		)
	}
}

func (r *Runner) resumeBackup(ctx context.Context, session *domain.Session) error {
	if r.client == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller reconcile",
			"backup controller execution requires a Kubernetes client",
		)
	}

	if session == nil || session.Spec.Backup == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"controller reconcile",
			"backup session payload is required",
		)
	}

	payload := session.Spec.Backup
	request := backup.Request{
		ID:                    session.ID,
		ToolImage:             payload.ToolImage,
		Namespace:             payload.SourcePVC.Namespace,
		PVCName:               payload.SourcePVC.Name,
		Path:                  payload.Path,
		Online:                payload.Online,
		DeleteExtraneousFiles: payload.DeleteExtraneous,
		SessionStore:          r.store,
		SessionNamespace:      session.Spec.SessionNamespace,
		OpenEBSLVMManager:     r.openEBS,
		ObjectStoreFactory:    objectstore.New,
		Writer:                io.Discard,
		Logger:                r.logger,
		BackupSession:         session,
	}

	return backup.Resume(ctx, r.client, request, session)
}

func (r *Runner) resumeRestore(ctx context.Context, session *domain.Session) error {
	if r.client == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller reconcile",
			"restore controller execution requires a Kubernetes client",
		)
	}

	if session == nil || session.Spec.Restore == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"controller reconcile",
			"restore session payload is required",
		)
	}

	payload := session.Spec.Restore

	ref := payload.CredentialsSecret
	if ref.Namespace == "" || ref.Name == "" {
		ref = domain.ObjectReference{
			APIVersion: "v1", Kind: "Secret",
			Namespace: session.Spec.SessionNamespace,
			Name:      kube.BackupCredentialsSecretName(session.ID),
		}
	}

	secret, err := kube.GetBackupCredentialsSecret(ctx, r.client, ref, session.ID)
	if err != nil {
		return err
	}

	config := objectstore.Config{
		Bucket:                payload.Bucket,
		Prefix:                payload.Prefix,
		Name:                  payload.Name,
		Provider:              payload.Provider,
		Endpoint:              payload.Endpoint,
		Region:                payload.Region,
		AccessKey:             string(secret.Data[kube.BackupAccessKeyDataKey]),
		SecretKey:             string(secret.Data[kube.BackupSecretKeyDataKey]),
		SessionToken:          string(secret.Data[kube.BackupSessionTokenDataKey]),
		AllowInsecureEndpoint: payload.AllowInsecureEndpoint,
		ForcePathStyle:        payload.Endpoint != "",
		ServerSideEncryption:  payload.ServerSideEncryption,
		SSEKMSKeyID:           payload.SSEKMSKeyID,
	}

	store, err := objectstore.New(ctx, config)
	if err != nil {
		return err
	}

	request := backup.Request{
		ID:                      session.ID,
		ToolImage:               payload.ToolImage,
		Namespace:               payload.DestinationPVC.Namespace,
		PVCName:                 payload.DestinationPVC.Name,
		CreatePVC:               payload.CreatePVC,
		DestinationStorageClass: payload.DestinationStorageClass,
		DestinationAccessMode:   payload.DestinationAccessMode,
		DestinationCapacity:     payload.DestinationCapacity,
		TargetNode:              payload.TargetNode,
		Path:                    payload.Path,
		AllowMounted:            payload.AllowMounted,
		DeleteExtraneousFiles:   payload.DeleteExtraneous,
		SessionStore:            r.store,
		SessionNamespace:        session.Spec.SessionNamespace,
		Store:                   store,
		OpenEBSLVMManager:       r.openEBS,
		Writer:                  io.Discard,
		Logger:                  r.logger,
	}

	return backup.ResumeRestore(ctx, r.client, request, session)
}

func terminalSession(session *domain.Session) bool {
	if session == nil {
		return true
	}

	switch session.Status.Phase {
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return true
	}

	switch session.Spec.Type {
	case domain.SessionTypeReserve:
		return session.Status.Phase == domain.PhaseReserved
	case domain.SessionTypeCopy:
		return session.Status.Phase == domain.PhaseWarmCopied
	default:
		return false
	}
}
