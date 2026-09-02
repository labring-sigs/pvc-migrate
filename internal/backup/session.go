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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
		ToolImage:        req.ToolImage,
		DeleteExtraneous: req.DeleteExtraneousFiles,
	}
	spec := domain.NewSessionSpec(
		domain.OperationBackup,
		domain.SessionCommon{
			SourceNamespace:  req.Namespace,
			SessionNamespace: req.SessionNamespace,
			Volumes:          nil,
		},

		req.Online,
		options,
	)

	spec.Backup.OpenEBSLVMEnableShared = req.OpenEBSLVMEnableShared
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
	spec.Backup.Name = req.Store.Config().Name
	spec.Backup.BackupRepository = strings.TrimSpace(req.BackupRepository)

	spec.Backup.BackupRepositoryNamespace = strings.TrimSpace(req.BackupRepositoryNamespace)
	if spec.Backup.BackupRepository != "" && spec.Backup.BackupRepositoryNamespace == "" {
		spec.Backup.BackupRepositoryNamespace = req.Namespace
	}

	if spec.Backup.BackupRepository == "" {
		spec.Backup.Backend = domain.BackupBackendS3
		cfg := req.Store.Config()
		spec.Backup.Bucket = cfg.Bucket
		spec.Backup.Prefix = cfg.Prefix
		spec.Backup.Provider = cfg.Provider
		spec.Backup.Endpoint = cfg.Endpoint
		spec.Backup.Region = cfg.Region
		spec.Backup.AllowInsecureEndpoint = cfg.AllowInsecureEndpoint
		spec.Backup.ServerSideEncryption = cfg.ServerSideEncryption
		spec.Backup.SSEKMSKeyID = cfg.SSEKMSKeyID
	}

	return domain.NewSession(id, spec, time.Now()), nil
}

// Submit creates the durable backup session and persists its object-store
// credentials in the per-session Secret. The transfer itself is intentionally
// separate so a controller can acknowledge the CRD and execute asynchronously.
func Submit(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	expectedPVCUID, expectedPVUID string,
) (*domain.Session, error) {
	if req.SessionStore == nil {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"backup session",
			"session store is required",
		)
	}

	session, err := prepareBackupSession(ctx, client, req, expectedPVCUID, expectedPVUID)
	if err != nil {
		return nil, err
	}

	err = withBackupSessionLock(ctx, req, session, func(lockedCtx context.Context) error {
		if err := req.SessionStore.Create(lockedCtx, session); err != nil {
			return err
		}

		if strings.TrimSpace(req.BackupRepository) != "" {
			// Controller mode resolves credentials from the referenced repository.
			// Never materialize repository credentials in a workflow-owned Secret.
			if session.Backend != kube.SessionBackendCRD {
				return domain.NewError(
					domain.ErrorPrecondition,
					"backup credentials",
					"BackupRepository requires a controller-backed workflow",
				)
			}

			return nil
		}

		return persistBackupCredentials(lockedCtx, client, req, session)
	})
	if err != nil {
		return nil, err
	}

	return session, nil
}

func prepareBackupSession(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	expectedPVCUID, expectedPVUID string,
) (*domain.Session, error) {
	pvc, pv, err := verifyPVCIdentity(
		ctx,
		client,
		req.Namespace,
		req.PVCName,
		expectedPVCUID,
		expectedPVUID,
	)
	if err != nil {
		return nil, err
	}

	return buildBackupSession(req, pvc, pv)
}

// SubmitRestore creates a durable restore workflow and stores its object-store
// credentials in a Secret. The controller can then execute the transfer with
// the same request contract as the synchronous CLI.
func SubmitRestore(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	plan Plan,
) (*domain.Session, error) {
	if req.SessionStore == nil || req.Store == nil {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"restore session",
			"session store and object store are required",
		)
	}

	id := req.ID
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

	cfg := req.Store.Config()
	spec := domain.NewSessionSpec(
		domain.OperationRestore,
		domain.SessionCommon{
			SourceNamespace:      req.Namespace,
			DestinationNamespace: req.Namespace,
			SessionNamespace:     req.SessionNamespace,
		},
		false,
		domain.SessionWorkflowOptions{
			ToolImage:        req.ToolImage,
			DeleteExtraneous: req.DeleteExtraneousFiles,
		},
	)
	spec.Restore.DestinationPVC = domain.ObjectReference{
		APIVersion: "v1",
		Kind:       "PersistentVolumeClaim",
		Namespace:  req.Namespace,
		Name:       req.PVCName,
		UID:        types.UID(plan.PVCUID),
	}
	spec.Restore.Path = req.Path

	spec.Restore.Name = cfg.Name
	spec.Restore.BackupRepository = strings.TrimSpace(req.BackupRepository)

	spec.Restore.BackupRepositoryNamespace = strings.TrimSpace(req.BackupRepositoryNamespace)
	if spec.Restore.BackupRepository != "" && spec.Restore.BackupRepositoryNamespace == "" {
		spec.Restore.BackupRepositoryNamespace = req.Namespace
	}

	if spec.Restore.BackupRepository == "" {
		spec.Restore.Backend = domain.BackupBackendS3
		spec.Restore.Bucket = cfg.Bucket
		spec.Restore.Prefix = cfg.Prefix
		spec.Restore.Provider = cfg.Provider
		spec.Restore.Endpoint = cfg.Endpoint
		spec.Restore.Region = cfg.Region
		spec.Restore.AllowInsecureEndpoint = cfg.AllowInsecureEndpoint
		spec.Restore.ServerSideEncryption = cfg.ServerSideEncryption
		spec.Restore.SSEKMSKeyID = cfg.SSEKMSKeyID
	}

	spec.Restore.CreatePVC = req.CreatePVC
	spec.Restore.DestinationStorageClass = req.DestinationStorageClass
	spec.Restore.DestinationAccessMode = req.DestinationAccessMode
	spec.Restore.DestinationCapacity = req.DestinationCapacity
	spec.Restore.TargetNode = req.TargetNode
	spec.Restore.AllowMounted = req.AllowMounted
	spec.Restore.DeleteExtraneous = req.DeleteExtraneousFiles
	session := domain.NewSession(id, spec, time.Now())

	err := withBackupSessionLock(ctx, req, session, func(lockedCtx context.Context) error {
		if err := req.SessionStore.Create(lockedCtx, session); err != nil {
			return err
		}

		if strings.TrimSpace(req.BackupRepository) != "" {
			if session.Backend != kube.SessionBackendCRD {
				return domain.NewError(
					domain.ErrorPrecondition,
					"restore credentials",
					"BackupRepository requires a controller-backed workflow",
				)
			}

			return nil
		}

		return persistRestoreCredentials(lockedCtx, client, req, session)
	})
	if err != nil {
		return nil, err
	}

	return session, nil
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
		backupCredentialsOwner(session)...,
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

func persistRestoreCredentials(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	session *domain.Session,
) error {
	if session == nil || session.Spec.Restore == nil || req.Store == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"restore credentials",
			"restore session and object store are required",
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
		backupCredentialsOwner(session)...,
	)
	if err != nil {
		return err
	}

	session.Spec.Restore.CredentialsSecret = domain.ObjectReference{
		APIVersion: "v1",
		Kind:       "Secret",
		Namespace:  secret.Namespace,
		Name:       secret.Name,
		UID:        secret.UID,
	}

	return req.SessionStore.Update(ctx, session)
}

func backupCredentialsOwner(session *domain.Session) []metav1.OwnerReference {
	if session == nil || session.Backend != kube.SessionBackendCRD || session.BackendUID == "" {
		return nil
	}

	controller := true
	blockOwnerDeletion := true

	return []metav1.OwnerReference{{
		APIVersion:         domain.SessionAPIVersion,
		Kind:               string(session.BackendResource),
		Name:               session.ID,
		UID:                session.BackendUID,
		Controller:         &controller,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}}
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

func loadRestoreCredentials(
	ctx context.Context,
	client kubernetes.Interface,
	session *domain.Session,
) (objectstore.Credentials, error) {
	if session == nil || session.Spec.Restore == nil {
		return objectstore.Credentials{}, domain.NewError(
			domain.ErrorValidation,
			restoreLockPhase,
			"restore session payload is required",
		)
	}

	ref := session.Spec.Restore.CredentialsSecret
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

	payload := session.Spec.Backup
	if req.Store == nil {
		credentials, err := loadBackupCredentials(ctx, client, session)
		if err != nil {
			return nil, err
		}

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

		req.Store = store
	}

	req.ID = session.ID
	req.Namespace = payload.SourcePVC.Namespace
	req.PVCName = payload.SourcePVC.Name
	req.Path = payload.Path
	req.Online = payload.Online
	req.ToolImage = payload.ToolImage
	req.OpenEBSLVMEnableShared = payload.OpenEBSLVMEnableShared
	req.SessionNamespace = session.Spec.SessionNamespace

	req.BackupSession = session
	if err := pinBackupRepository(ctx, req, session); err != nil {
		return nil, err
	}

	return &req, nil
}

// pinBackupRepository records the repository object identity for the lifetime
// of a running workflow. Location changes take effect only for a new workflow;
// Secret data may rotate in place; replacing the referenced Secret changes its
// identity and requires a new workflow.
func pinBackupRepository(ctx context.Context, req Request, session *domain.Session) error {
	if session == nil || strings.TrimSpace(req.BackupRepository) == "" || req.SessionStore == nil {
		return nil
	}

	requested := req.BackupRepositoryBinding
	if requested == nil || requested.Type == "" || requested.UID == "" ||
		requested.Generation <= 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"backup repository",
			"resolved BackupRepository type, UID, and generation are required",
		)
	}

	if err := validateRepositoryBinding(requested); err != nil {
		return err
	}

	if session.Status.BackupRepository == nil {
		session.Status.BackupRepository = copyRepositoryBinding(requested)
		return req.SessionStore.Update(ctx, session)
	}

	current := session.Status.BackupRepository
	if current.Type != requested.Type {
		return repositoryBindingConflict(
			"BackupRepository backend changed while the workflow was running",
		)
	}

	if current.UID != requested.UID {
		return repositoryBindingConflict(
			"BackupRepository was replaced while the workflow was running",
		)
	}

	if current.Generation != requested.Generation {
		return repositoryBindingConflict("BackupRepository changed while the workflow was running")
	}

	switch requested.Type {
	case domain.BackupRepositoryTypeS3:
		if current.S3 == nil ||
			current.S3.CredentialsSecretUID != requested.S3.CredentialsSecretUID {
			return repositoryBindingConflict(
				"BackupRepository credentials Secret was replaced while the workflow was running",
			)
		}
	case domain.BackupRepositoryTypePVC:
		if current.PVC == nil ||
			current.PVC.ClaimUID != requested.PVC.ClaimUID {
			return repositoryBindingConflict(
				"BackupRepository PVC was replaced while the workflow was running",
			)
		}
	}

	return nil
}

func validateRepositoryBinding(binding *domain.BackupRepositoryBindingStatus) error {
	if binding == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"backup repository",
			"resolved repository binding is required",
		)
	}

	switch binding.Type {
	case domain.BackupRepositoryTypeS3:
		if binding.S3 == nil || binding.PVC != nil || binding.S3.CredentialsSecretUID == "" {
			return domain.NewError(
				domain.ErrorPrecondition,
				"backup repository",
				"resolved S3 repository requires a credentials Secret UID",
			)
		}
	case domain.BackupRepositoryTypePVC:
		if binding.PVC == nil || binding.S3 != nil || binding.PVC.ClaimUID == "" {
			return domain.NewError(
				domain.ErrorPrecondition,
				"backup repository",
				"resolved PVC repository requires a claim UID",
			)
		}
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"backup repository",
			fmt.Sprintf("resolved repository backend %q is unsupported", binding.Type),
		)
	}

	return nil
}

func copyRepositoryBinding(
	binding *domain.BackupRepositoryBindingStatus,
) *domain.BackupRepositoryBindingStatus {
	if binding == nil {
		return nil
	}

	out := *binding
	if binding.S3 != nil {
		s3 := *binding.S3
		out.S3 = &s3
	}

	if binding.PVC != nil {
		pvc := *binding.PVC
		out.PVC = &pvc
	}

	return &out
}

func repositoryBindingConflict(message string) error {
	return domain.NewError(
		domain.ErrorConflict,
		"backup repository",
		message+"; create a new workflow",
	)
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

	if session != nil && session.Backend == kube.SessionBackendCRD && session.Spec.Backup != nil &&
		session.Spec.Backup.BackupRepository != "" && req.Store == nil {
		if err := session.Validate(); err != nil {
			return err
		}
		return validateBackupResumePhase(session)
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

func buildRestoreResumeRequest(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	session *domain.Session,
) (*Request, error) {
	if client == nil || req.SessionStore == nil || session == nil || session.Spec.Restore == nil {
		return nil, domain.NewError(
			domain.ErrorValidation,
			restoreLockPhase,
			"Kubernetes client, session store, and restore session are required",
		)
	}

	if err := session.Validate(); err != nil {
		return nil, err
	}

	if err := validateRestoreResumePhase(session); err != nil {
		return nil, err
	}

	payload := session.Spec.Restore
	if req.Store == nil && payload.BackupRepository == "" {
		credentials, err := loadRestoreCredentials(ctx, client, session)
		if err != nil {
			return nil, err
		}

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

		req.Store = store
	}

	req.ID = session.ID
	req.Namespace = payload.DestinationPVC.Namespace
	req.PVCName = payload.DestinationPVC.Name
	req.Path = payload.Path
	req.ToolImage = payload.ToolImage
	req.CreatePVC = payload.CreatePVC
	req.DestinationStorageClass = payload.DestinationStorageClass
	req.DestinationAccessMode = payload.DestinationAccessMode
	req.DestinationCapacity = payload.DestinationCapacity
	req.TargetNode = payload.TargetNode
	req.AllowMounted = payload.AllowMounted
	req.DeleteExtraneousFiles = payload.DeleteExtraneous
	req.SessionNamespace = session.Spec.SessionNamespace
	req.BackupRepository = payload.BackupRepository
	req.BackupRepositoryNamespace = payload.BackupRepositoryNamespace
	req.BackupSession = session

	return &req, nil
}

func validateRestoreResumePhase(session *domain.Session) error {
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
			restoreLockPhase,
			fmt.Sprintf("phase %s cannot be resumed for restore", phase),
		)
	}
}

// ValidateRestoreResume performs the same durable destination and recovery
// point checks as execution. Repository-backed workflows defer remote checks
// to the controller because the submitting identity intentionally cannot read
// repository credentials.
func ValidateRestoreResume(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	session *domain.Session,
) error {
	if session != nil && session.Status.Phase == domain.PhaseCompleted {
		return session.Validate()
	}

	if session != nil && session.Backend == kube.SessionBackendCRD && session.Spec.Restore != nil &&
		session.Spec.Restore.BackupRepository != "" && req.Store == nil {
		if err := session.Validate(); err != nil {
			return err
		}
		return validateRestoreResumePhase(session)
	}

	resumeReq, err := buildRestoreResumeRequest(ctx, client, req, session)
	if err != nil {
		return err
	}

	if _, err := preflight(ctx, client, *resumeReq, true, "resume revalidation"); err != nil {
		return err
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

// ResumeRestore advances a controller-backed restore while holding the
// per-session Lease for the entire status and transfer sequence. Keeping the
// checkpoint writes inside the same lock prevents duplicate reconciles from
// overwriting a newer phase after the restore tool has started.
func ResumeRestore(
	ctx context.Context,
	client kubernetes.Interface,
	req Request,
	session *domain.Session,
) error {
	if session == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"restore resume",
			"restore session is required",
		)
	}

	if req.SessionStore == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"restore resume",
			"session store is required",
		)
	}

	if session.Status.Phase == domain.PhaseCompleted {
		return session.Validate()
	}

	return withBackupSessionLock(ctx, req, session, func(runCtx context.Context) error {
		resumeReq, err := buildRestoreResumeRequest(runCtx, client, req, session)
		if err != nil {
			failureErr := err
			if transitionErr := session.Transition(
				domain.PhaseFailed,
				err.Error(),
				time.Now(),
			); transitionErr == nil {
				failureErr = errors.Join(err, req.SessionStore.Update(runCtx, session))
			}

			return failureErr
		}

		if resumeReq.Store == nil {
			return domain.NewError(
				domain.ErrorPrecondition,
				"restore resume",
				"object store must be resolved before restore execution",
			)
		}

		lockedReq := *resumeReq

		if session.Status.Phase == domain.PhaseCompleted {
			return nil
		}

		if err := pinBackupRepository(runCtx, lockedReq, session); err != nil {
			return err
		}

		if session.Status.Phase == domain.PhasePlanned ||
			session.Status.Phase == domain.PhaseFailed {
			if err := updateBackupSession(
				runCtx,
				lockedReq,
				session,
				domain.PhaseWarmCopying,
				"restore started",
			); err != nil {
				return err
			}
		}

		if err := Run(runCtx, client, lockedReq, true); err != nil {
			failureErr := err
			if transitionErr := session.Transition(
				domain.PhaseFailed,
				err.Error(),
				time.Now(),
			); transitionErr == nil {
				failureErr = errors.Join(err, lockedReq.SessionStore.Update(runCtx, session))
			}

			return failureErr
		}

		if err := updateBackupSession(
			runCtx,
			lockedReq,
			session,
			domain.PhaseWarmCopied,
			"restore transfer completed",
		); err != nil {
			return err
		}

		return updateBackupSession(
			runCtx,
			lockedReq,
			session,
			domain.PhaseCompleted,
			"restore completed",
		)
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
