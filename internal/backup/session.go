package backup

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

func buildBackupSession(
	req Request,
	pvc *corev1.PersistentVolumeClaim,
	pv *corev1.PersistentVolume,
) (*domain.Session, error) {
	if req.SessionStore == nil {
		return nil, nil
	}

	if strings.TrimSpace(req.SessionNamespace) == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"backup session",
			"session namespace is required",
		)
	}

	if pvc == nil || pv == nil || pvc.UID == "" || pv.UID == "" {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"backup session",
			"source PVC and PV identities are required",
		)
	}

	id := strings.TrimSpace(req.ID)
	if id == "" {
		generated, err := domain.NewSessionID(time.Now())
		if err != nil {
			return nil, err
		}

		id = generated
	}

	if err := domain.ValidateSessionID(id); err != nil {
		return nil, err
	}

	options := domain.SessionWorkflowOptions{
		ToolImage:              req.ToolImage,
		DeleteExtraneous:       req.DeleteExtraneousFiles,
		OpenEBSLVMEnableShared: req.OpenEBSLVMEnableShared,
	}
	spec := domain.NewSessionSpec(
		domain.OperationBackup,
		domain.SessionCommon{
			SourceNamespace:  req.Namespace,
			SessionNamespace: req.SessionNamespace,
			Volumes:          nil,
			CreatedBy:        "pvc-migrate",
		},
		domain.WorkloadSpec{Adapter: domain.WorkloadNone},
		req.Online,
		options,
	)
	spec.Backup.SourcePVC = domain.ObjectReference{
		APIVersion: "v1",
		Kind:       "PersistentVolumeClaim",
		Namespace:  pvc.Namespace,
		Name:       pvc.Name,
		UID:        pvc.UID,
	}
	spec.Backup.SourcePV = domain.ObjectReference{
		APIVersion: "v1", Kind: "PersistentVolume", Name: pv.Name, UID: pv.UID,
	}
	spec.Backup.Path = req.Path
	spec.Backup.Backend = "s3"
	spec.Backup.Bucket = req.Store.Config().Bucket
	spec.Backup.Prefix = req.Store.Config().Prefix
	spec.Backup.Name = req.Store.Config().Name
	spec.Backup.Provider = req.Store.Config().Provider
	spec.Backup.Endpoint = req.Store.Config().Endpoint
	spec.Backup.Region = req.Store.Config().Region
	spec.Backup.AllowInsecureEndpoint = req.Store.Config().AllowInsecureEndpoint
	spec.Backup.ServerSideEncryption = req.Store.Config().ServerSideEncryption
	spec.Backup.SSEKMSKeyID = req.Store.Config().SSEKMSKeyID

	return domain.NewSession(id, spec, time.Now()), nil
}

func persistBackupCredentials(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	session *domain.Session,
) error {
	if session == nil || session.Spec.Backup == nil || req.Store == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"backup credentials",
			"backup session and object store are required",
		)
	}

	credentials := req.Store.Credentials()

	secret, err := kube.CreateBackupCredentialsSecret(
		ctx,
		client,
		session.Spec.SessionNamespace,
		session.ID,
		map[string][]byte{
			kube.BackupAccessKeyDataKey:    []byte(credentials.AccessKey),
			kube.BackupSecretKeyDataKey:    []byte(credentials.SecretKey),
			kube.BackupSessionTokenDataKey: []byte(credentials.SessionToken),
		},
	)
	if err != nil {
		return err
	}

	session.Spec.Backup.CredentialsSecret = domain.ObjectReference{
		APIVersion: "v1",
		Kind:       "Secret",
		Namespace:  secret.Namespace,
		Name:       secret.Name,
		UID:        secret.UID,
	}
	if err := req.SessionStore.Update(ctx, session); err != nil {
		// The API server may have persisted the checkpoint even when the client
		// receives an error. Keep the deterministic Secret for Resume or cleanup.
		return err
	}

	return nil
}

func loadBackupCredentials(
	ctx context.Context,
	client kubernetes.Interface,
	session *domain.Session,
) (objectstore.Credentials, error) {
	if session == nil || session.Spec.Backup == nil {
		return objectstore.Credentials{}, domain.NewError(
			domain.ErrorValidation,
			backupResumePhase,
			"backup session payload is required",
		)
	}

	ref := session.Spec.Backup.CredentialsSecret
	if ref.Namespace == "" || ref.Name == "" {
		ref = domain.ObjectReference{
			APIVersion: "v1",
			Kind:       "Secret",
			Namespace:  session.Spec.SessionNamespace,
			Name:       kube.BackupCredentialsSecretName(session.ID),
		}
	}

	secret, err := kube.GetBackupCredentialsSecret(ctx, client, ref, session.ID)
	if err != nil {
		return objectstore.Credentials{}, err
	}

	read := func(key string) string { return string(secret.Data[key]) }

	return objectstore.Credentials{
		AccessKey:    read(kube.BackupAccessKeyDataKey),
		SecretKey:    read(kube.BackupSecretKeyDataKey),
		SessionToken: read(kube.BackupSessionTokenDataKey),
	}, nil
}

func updateBackupSession(
	ctx context.Context,
	req Request,
	session *domain.Session,
	phase domain.Phase,
	message string,
) error {
	if session == nil || req.SessionStore == nil {
		return nil
	}

	previousStatus := session.Status
	if err := session.Transition(phase, message, time.Now()); err != nil {
		return err
	}

	session.Status.Message = message
	if err := req.SessionStore.Update(ctx, session); err != nil {
		session.Status = previousStatus
		return err
	}

	return nil
}

func failBackupSession(
	ctx context.Context,
	req Request,
	session *domain.Session,
	cause error,
) error {
	if session == nil || req.SessionStore == nil {
		return nil
	}

	if err := backupSessionFenceError(ctx); err != nil {
		return err
	}

	message := "backup failed"
	if cause != nil {
		message = cause.Error()
	}

	if session.Status.Phase != domain.PhaseFailed {
		if err := session.Transition(domain.PhaseFailed, message, time.Now()); err != nil {
			return err
		}
	}

	session.Status.Message = message
	updateErr := req.SessionStore.Update(ctx, session)

	return errors.Join(updateErr, backupSessionFenceError(ctx))
}

func restoreBackupSharedMounts(
	ctx context.Context,
	req Request,
	session *domain.Session,
) error {
	if session == nil || req.OpenEBSLVMManager == nil ||
		len(session.Status.OpenEBSLVMSharedMounts) == 0 {
		return nil
	}

	if err := backupSessionFenceError(ctx); err != nil {
		return err
	}

	var result error

	remaining := make([]domain.OpenEBSLVMSharedMount, 0, len(session.Status.OpenEBSLVMSharedMounts))
	for _, mount := range slices.Backward(session.Status.OpenEBSLVMSharedMounts) {
		if err := backupSessionFenceError(ctx); err != nil {
			return errors.Join(result, err)
		}

		if err := req.OpenEBSLVMManager.RestoreShared(ctx, session.ID, mount); err != nil {
			result = errors.Join(result, err)

			remaining = append(remaining, mount)
		}

		if err := backupSessionFenceError(ctx); err != nil {
			return errors.Join(result, err)
		}
	}

	slices.Reverse(remaining)

	if err := backupSessionFenceError(ctx); err != nil {
		return errors.Join(result, err)
	}

	session.Status.OpenEBSLVMSharedMounts = remaining
	if req.SessionStore != nil {
		result = errors.Join(result, req.SessionStore.Update(ctx, session))
	}

	return errors.Join(result, backupSessionFenceError(ctx))
}

func backupSessionOpenEBSState(
	ctx context.Context,
	req Request,
	session *domain.Session,
	info *PVCInfo,
) error {
	if session == nil || info == nil || info.PV == nil || len(info.Consumers) == 0 {
		return nil
	}

	if info.PV.Spec.CSI == nil || info.PV.Spec.CSI.Driver != kube.OpenEBSLVMCSIDriver {
		return nil
	}

	if req.OpenEBSLVMManager == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"backup",
			"OpenEBS LVM manager is required for an active OpenEBS LVM PVC",
		)
	}

	if existing, found := backupSessionSharedMount(session, info.PV); found {
		needsEnable, err := inspectBackupSessionSharedMount(ctx, req, session, existing)
		if err != nil {
			return err
		}

		if needsEnable {
			return req.OpenEBSLVMManager.EnableShared(ctx, session.ID, existing)
		}

		return nil
	}

	prepared, err := req.OpenEBSLVMManager.PrepareShared(ctx, domain.ObjectReference{
		Kind: "PersistentVolume", Name: info.PV.Name, UID: info.PV.UID,
	})
	if err != nil {
		return err
	}

	if !prepared.NeedsChange {
		return nil
	}

	if !req.OpenEBSLVMEnableShared {
		return domain.NewError(
			domain.ErrorPrecondition,
			"backup",
			fmt.Sprintf(
				"source PVC %s/%s is active and its OpenEBS LVMVolume is unshared; retry with --openebs-lvm-enable-shared or stop consumers",
				info.PVC.Namespace,
				info.PVC.Name,
			),
		)
	}

	mount := domain.OpenEBSLVMSharedMount{
		SourcePV: domain.ObjectReference{
			Kind: "PersistentVolume",
			Name: info.PV.Name,
			UID:  info.PV.UID,
		},
		LVMVolume:         prepared.LVMVolume,
		PreviousShared:    prepared.PreviousShared,
		PreviousSharedSet: prepared.PreviousSharedSet,
	}

	session.Status.OpenEBSLVMSharedMounts = append(session.Status.OpenEBSLVMSharedMounts, mount)
	if err := req.SessionStore.Update(ctx, session); err != nil {
		return err
	}

	if enableErr := req.OpenEBSLVMManager.EnableShared(ctx, session.ID, mount); enableErr != nil {
		owned, ownershipErr := req.OpenEBSLVMManager.Shared(
			ctx,
			mount.SourcePV,
			mount.LVMVolume,
			session.ID,
		)
		if (ownershipErr == nil && !owned) ||
			domain.CategoryOf(ownershipErr) == domain.ErrorConflict {
			session.Status.OpenEBSLVMSharedMounts = session.Status.OpenEBSLVMSharedMounts[:len(session.Status.OpenEBSLVMSharedMounts)-1]
			return errors.Join(enableErr, req.SessionStore.Update(ctx, session))
		}

		return errors.Join(enableErr, ownershipErr)
	}

	return nil
}

func backupSessionSharedMount(
	session *domain.Session,
	pv *corev1.PersistentVolume,
) (domain.OpenEBSLVMSharedMount, bool) {
	if session == nil || pv == nil {
		return domain.OpenEBSLVMSharedMount{}, false
	}

	for _, mount := range session.Status.OpenEBSLVMSharedMounts {
		if mount.SourcePV.Name == pv.Name && mount.SourcePV.UID == pv.UID {
			return mount, true
		}
	}

	return domain.OpenEBSLVMSharedMount{}, false
}

// A shared-mount checkpoint is persisted before the LVMVolume is changed. If
// the process exits between those operations, the unchanged original state is
// safe to enable when the backup resumes.
func inspectBackupSessionSharedMount(
	ctx context.Context,
	req Request,
	session *domain.Session,
	mount domain.OpenEBSLVMSharedMount,
) (bool, error) {
	shared, err := req.OpenEBSLVMManager.Shared(
		ctx,
		mount.SourcePV,
		mount.LVMVolume,
		session.ID,
	)
	if err == nil {
		if !shared {
			return false, domain.NewError(
				domain.ErrorConflict,
				backupResumePhase,
				"session-managed OpenEBS LVM shared mount is no longer enabled",
			)
		}

		return false, nil
	}

	if domain.CategoryOf(err) != domain.ErrorConflict {
		return false, err
	}

	if restoreErr := req.OpenEBSLVMManager.ValidateRestoreShared(
		ctx,
		session.ID,
		mount,
	); restoreErr != nil {
		return false, restoreErr
	}

	return true, nil
}

func validateBackupOpenEBSState(ctx context.Context, req Request, info *PVCInfo) error {
	if info == nil || info.PV == nil || len(info.Consumers) == 0 ||
		info.PV.Spec.CSI == nil || info.PV.Spec.CSI.Driver != kube.OpenEBSLVMCSIDriver {
		return nil
	}

	if req.OpenEBSLVMManager == nil {
		return domain.NewError(
			domain.ErrorInternal,
			backupPreflightPhase,
			"OpenEBS LVM manager is required to inspect an active OpenEBS LVM PVC",
		)
	}

	if req.BackupSession != nil {
		if existing, found := backupSessionSharedMount(req.BackupSession, info.PV); found {
			_, err := inspectBackupSessionSharedMount(ctx, req, req.BackupSession, existing)
			return err
		}
	}

	prepared, err := req.OpenEBSLVMManager.PrepareShared(ctx, domain.ObjectReference{
		Kind: "PersistentVolume", Name: info.PV.Name, UID: info.PV.UID,
	})
	if err != nil {
		return err
	}

	if prepared.NeedsChange && !req.OpenEBSLVMEnableShared {
		return domain.NewError(
			domain.ErrorPrecondition,
			backupPreflightPhase,
			fmt.Sprintf(
				"source PVC %s/%s is active and its OpenEBS LVMVolume is unshared; retry with --openebs-lvm-enable-shared or stop consumers",
				info.PVC.Namespace,
				info.PVC.Name,
			),
		)
	}

	if prepared.NeedsChange && req.SessionStore == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			backupPreflightPhase,
			"a session store is required to recover temporary OpenEBS LVM shared state",
		)
	}

	return nil
}

func buildResumeRequest(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	session *domain.Session,
) (*Request, error) {
	if client == nil || req.SessionStore == nil || session == nil || session.Spec.Backup == nil {
		return nil, domain.NewError(
			domain.ErrorValidation,
			backupResumePhase,
			"Kubernetes client, session store, and backup session are required",
		)
	}

	if err := session.Validate(); err != nil {
		return nil, err
	}

	if err := validateBackupResumePhase(session); err != nil {
		return nil, err
	}

	credentials, err := loadBackupCredentials(ctx, client, session)
	if err != nil {
		return nil, err
	}

	payload := session.Spec.Backup
	config := objectstore.Config{
		Bucket:                payload.Bucket,
		Prefix:                payload.Prefix,
		Name:                  payload.Name,
		Provider:              payload.Provider,
		Endpoint:              payload.Endpoint,
		Region:                payload.Region,
		AccessKey:             credentials.AccessKey,
		SecretKey:             credentials.SecretKey,
		SessionToken:          credentials.SessionToken,
		AllowInsecureEndpoint: payload.AllowInsecureEndpoint,
		ForcePathStyle:        payload.Endpoint != "",
		ServerSideEncryption:  payload.ServerSideEncryption,
		SSEKMSKeyID:           payload.SSEKMSKeyID,
	}

	factory := req.ObjectStoreFactory
	if factory == nil {
		factory = objectstore.New
	}

	store, err := factory(ctx, config)
	if err != nil {
		return nil, err
	}

	req.ID = session.ID
	req.Namespace = payload.SourcePVC.Namespace
	req.PVCName = payload.SourcePVC.Name
	req.Path = payload.Path
	req.Online = payload.Online
	req.ToolImage = payload.ToolImage
	req.OpenEBSLVMEnableShared = payload.OpenEBSLVMEnableShared
	req.Store = store
	req.SessionNamespace = session.Spec.SessionNamespace
	req.BackupSession = session

	return &req, nil
}

func validateBackupResumePhase(session *domain.Session) error {
	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	switch phase {
	case domain.PhasePlanned,
		domain.PhaseWarmCopying,
		domain.PhaseWarmCopied,
		domain.PhaseCompleted:
		return nil
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			backupResumePhase,
			fmt.Sprintf("phase %s cannot be resumed for backup", phase),
		)
	}
}

func validatePublishedBackupSession(
	ctx context.Context,
	req Request,
	manifest *objectstore.Manifest,
) error {
	if manifest == nil || req.BackupSession == nil || req.BackupSession.Spec.Backup == nil {
		return domain.NewError(
			domain.ErrorValidation,
			backupResumePhase,
			"published manifest and backup session are required",
		)
	}

	payload := req.BackupSession.Spec.Backup
	if manifest.SessionID != req.BackupSession.ID ||
		manifest.SourceNamespace != payload.SourcePVC.Namespace ||
		manifest.SourcePVC != payload.SourcePVC.Name ||
		manifest.SourcePVCUID != string(payload.SourcePVC.UID) ||
		manifest.SourcePV != payload.SourcePV.Name ||
		manifest.SourcePVUID != string(payload.SourcePV.UID) ||
		manifest.Path != payload.Path ||
		manifest.Consistency != backupConsistency(payload.Online) {
		return domain.NewError(
			domain.ErrorConflict,
			backupResumePhase,
			"published completion manifest does not belong to this backup session",
		)
	}

	if err := req.Store.VerifyInventory(ctx, *manifest); err != nil {
		return wrapBackupError(
			domain.ErrorConflict,
			backupResumePhase,
			"verify published backup inventory",
			err,
		)
	}

	return nil
}

func validateBackupSharedMountRestore(
	ctx context.Context,
	req Request,
	session *domain.Session,
) error {
	if session == nil || len(session.Status.OpenEBSLVMSharedMounts) == 0 {
		return nil
	}

	if req.OpenEBSLVMManager == nil {
		return domain.NewError(
			domain.ErrorInternal,
			backupResumePhase,
			"OpenEBS LVM manager is required to restore session-managed shared mounts",
		)
	}

	for _, mount := range session.Status.OpenEBSLVMSharedMounts {
		if err := req.OpenEBSLVMManager.ValidateRestoreShared(ctx, session.ID, mount); err != nil {
			return err
		}
	}

	return nil
}

func completePublishedBackupSession(
	ctx context.Context,
	req Request,
	session *domain.Session,
) error {
	manifest, err := req.Store.Manifest(ctx)
	if err != nil {
		return err
	}

	if err := validatePublishedBackupSession(ctx, req, manifest); err != nil {
		return err
	}

	if err := restoreBackupSharedMounts(ctx, req, session); err != nil {
		return err
	}

	if session.Status.Phase == domain.PhaseCompleted {
		return nil
	}

	if session.Status.Phase != domain.PhaseWarmCopied {
		if err := updateBackupSession(
			ctx,
			req,
			session,
			domain.PhaseWarmCopying,
			"published backup recovered",
		); err != nil {
			return err
		}

		if err := updateBackupSession(
			ctx,
			req,
			session,
			domain.PhaseWarmCopied,
			"published backup verified",
		); err != nil {
			return err
		}
	}

	return updateBackupSession(ctx, req, session, domain.PhaseCompleted, "backup completed")
}

func ValidateResume(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	session *domain.Session,
) error {
	if session != nil && session.Status.Phase == domain.PhaseCompleted {
		return session.Validate()
	}

	resumeReq, err := buildResumeRequest(ctx, client, req, session)
	if err != nil {
		return err
	}

	if err := validateBackupSharedMountRestore(ctx, *resumeReq, session); err != nil {
		return err
	}

	payload := session.Spec.Backup

	plan, err := preflight(ctx, client, *resumeReq, false, "resume revalidation")
	if err != nil {
		return err
	}

	if plan.PVCUID != string(payload.SourcePVC.UID) || plan.PVUID != string(payload.SourcePV.UID) {
		return domain.NewError(
			domain.ErrorConflict,
			backupResumePhase,
			"source PVC or PV identity changed",
		)
	}

	if plan.ManifestPresent {
		manifest, err := resumeReq.Store.Manifest(ctx)
		if err != nil {
			return err
		}

		return validatePublishedBackupSession(ctx, *resumeReq, manifest)
	}

	return nil
}

func Resume(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	session *domain.Session,
) error {
	if session != nil && session.Status.Phase == domain.PhaseCompleted {
		return session.Validate()
	}

	return withBackupSessionLock(ctx, req, session, func(runCtx context.Context) error {
		resumeReq, err := buildResumeRequest(runCtx, client, req, session)
		if err != nil {
			return err
		}

		if err := cleanupBackupSessionToolProbePods(runCtx, client, session); err != nil {
			return err
		}

		lockedReq := *resumeReq

		payload := session.Spec.Backup
		if session.Status.Phase == domain.PhaseCompleted {
			return nil
		}

		if err := validateBackupSharedMountRestore(runCtx, lockedReq, session); err != nil {
			return err
		}

		plan, err := preflight(runCtx, client, lockedReq, false, "resume revalidation")
		if err != nil {
			return err
		}

		if plan.PVCUID != string(payload.SourcePVC.UID) ||
			plan.PVUID != string(payload.SourcePV.UID) {
			return domain.NewError(
				domain.ErrorConflict,
				backupResumePhase,
				"source PVC or PV identity changed",
			)
		}

		if plan.ManifestPresent {
			return completePublishedBackupSession(runCtx, lockedReq, session)
		}

		return runBackupSession(runCtx, client, lockedReq, session)
	})
}

func cleanupBackupSessionToolProbePods(
	ctx context.Context,
	client kubernetes.Interface,
	session *domain.Session,
) error {
	if err := backupSessionFenceError(ctx); err != nil {
		return err
	}

	if session == nil || session.Spec.Backup == nil {
		return domain.NewError(
			domain.ErrorValidation,
			backupResumePhase,
			"backup session payload is required",
		)
	}

	if err := kube.CleanupSessionToolProbePods(
		ctx,
		client,
		session.ID,
		[]string{session.Spec.Backup.SourcePVC.Namespace},
	); err != nil {
		return err
	}

	return backupSessionFenceError(ctx)
}

func withBackupSessionLock(
	ctx context.Context,
	req Request,
	session *domain.Session,
	run func(context.Context) error,
) error {
	if session == nil || strings.TrimSpace(session.Spec.SessionNamespace) == "" ||
		strings.TrimSpace(session.ID) == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"backup session lock",
			"backup session namespace and ID are required",
		)
	}

	locker, ok := req.SessionStore.(kube.SessionLocker)
	if !ok {
		return run(ctx)
	}

	lock, err := locker.AcquireSessionLock(ctx, session.Spec.SessionNamespace, session.ID)
	if err != nil {
		return err
	}

	boundCtx, cancel := lock.Bind(ctx)
	defer cancel()

	lockedCtx := context.WithValue(boundCtx, backupSessionLockContextKey{}, lock)

	operationErr := lock.Err()
	if operationErr == nil {
		operationErr = run(lockedCtx)
	}

	operationErr = errors.Join(operationErr, lock.Err())
	releaseErr := runWithCleanupTimeout(lockReleaseTimeout, lock.Release)

	return errors.Join(operationErr, releaseErr)
}

type backupSessionLockContextKey struct{}

func backupSessionFenceError(ctx context.Context) error {
	lock, _ := ctx.Value(backupSessionLockContextKey{}).(kube.SessionLock)
	if lock == nil {
		return nil
	}

	return lock.Err()
}
