package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"strings"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/backup"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func isSHA256Fingerprint(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}

	_, err := hex.DecodeString(value)
	return err == nil
}

// Runner reconciles every workflow CR by delegating its stages to app.Service.
// The service remains the single owner of phase transitions, persistence and
// resource fencing; this runner only provides event polling and dispatch.
type Runner struct {
	service             workflowResumer
	store               kube.SessionStore
	client              kubernetes.Interface
	controllerClient    crclient.Reader
	namespace           string
	controllerNamespace string
	clusterIdentity     string
	trustedToolImage    string
	pollInterval        time.Duration
	logger              *slog.Logger
	openEBS             kube.OpenEBSLVMSharedVolumeManager
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

// WithControllerClient supplies the typed client used for cluster-scoped
// administrator configuration such as ObjectStoreProfile.
func (r *Runner) WithControllerClient(client crclient.Reader) *Runner {
	if r != nil {
		r.controllerClient = client
	}
	return r
}

// WithControllerNamespace limits administrator-owned profile Secrets to the
// controller's installation namespace. The namespace is optional for unit
// tests and embedded callers; production manager construction always sets it.
func (r *Runner) WithControllerNamespace(namespace string) *Runner {
	if r != nil {
		r.controllerNamespace = namespace
	}
	return r
}

// WithClusterIdentity scopes controller-backed object-store paths to this
// Kubernetes cluster. The value is the stable kube-system namespace UID and
// is hashed before it is placed in an object key so a shared bucket cannot
// collide across clusters or expose the raw cluster identifier.
func (r *Runner) WithClusterIdentity(identity string) *Runner {
	if r != nil {
		r.clusterIdentity = strings.TrimSpace(identity)
	}
	return r
}

// WithTrustedToolImage pins controller-created data mover Pods to the image
// selected by the administrator. A workflow CR is tenant input and must not
// be able to turn a credential-bearing transfer Pod into arbitrary code.
func (r *Runner) WithTrustedToolImage(image string) *Runner {
	if r != nil {
		r.trustedToolImage = strings.TrimSpace(image)
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

	if err := r.validateTrustedToolImage(session); err != nil {
		return err
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

func (r *Runner) validateTrustedToolImage(session *domain.Session) error {
	if r == nil || session == nil || strings.TrimSpace(r.trustedToolImage) == "" {
		return nil
	}

	trusted, err := kube.NormalizeToolImage(r.trustedToolImage)
	if err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"controller tool image",
			"controller trusted tool image is invalid",
			err,
		)
	}

	requested, err := kube.NormalizeToolImage(session.Spec.WorkflowOptions().ToolImage)
	if err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"controller tool image",
			"workflow tool image is invalid; use the controller-configured image",
			err,
		)
	}

	if requested != trusted {
		return domain.NewError(
			domain.ErrorPrecondition,
			"controller tool image",
			"workflow toolImage must match the controller-configured image",
		)
	}

	return nil
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
	config, err := r.objectStoreConfig(ctx, session, payload.Bucket, payload.Prefix, payload.Name, payload.Provider, payload.Endpoint, payload.Region, payload.AllowInsecureEndpoint, payload.ServerSideEncryption, payload.SSEKMSKeyID)
	if err != nil {
		return err
	}
	request := backup.Request{
		ID:                                   session.ID,
		ToolImage:                            payload.ToolImage,
		Namespace:                            payload.SourcePVC.Namespace,
		PVCName:                              payload.SourcePVC.Name,
		Path:                                 payload.Path,
		Online:                               payload.Online,
		DeleteExtraneousFiles:                payload.DeleteExtraneous,
		SessionStore:                         r.store,
		SessionNamespace:                     session.Spec.SessionNamespace,
		OpenEBSLVMManager:                    r.openEBS,
		ObjectStoreFactory:                   objectstore.New,
		Store:                                nil,
		Writer:                               io.Discard,
		Logger:                               r.logger,
		BackupSession:                        session,
		ObjectStoreProfile:                   payload.ObjectStoreProfile,
		ObjectStoreProfileUID:                types.UID(config.ProfileUID),
		ObjectStoreProfileGeneration:         config.ProfileGeneration,
		ObjectStoreCredentialsSecretUID:      types.UID(config.CredentialsSecretUID),
		ObjectStoreServiceAccountUID:         types.UID(config.ServiceAccountUID),
		ObjectStoreServiceAccountFingerprint: config.ServiceAccountFingerprint,
		ToolServiceAccountName:               config.ServiceAccountName,
	}

	request.Store, err = objectstore.New(ctx, config)
	if err != nil {
		return err
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

	config, err := r.objectStoreConfig(ctx, session, payload.Bucket, payload.Prefix, payload.Name, payload.Provider, payload.Endpoint, payload.Region, payload.AllowInsecureEndpoint, payload.ServerSideEncryption, payload.SSEKMSKeyID)
	if err != nil {
		return err
	}

	store, err := objectstore.New(ctx, config)
	if err != nil {
		return err
	}

	request := backup.Request{
		ID:                                   session.ID,
		ToolImage:                            payload.ToolImage,
		Namespace:                            payload.DestinationPVC.Namespace,
		PVCName:                              payload.DestinationPVC.Name,
		CreatePVC:                            payload.CreatePVC,
		DestinationStorageClass:              payload.DestinationStorageClass,
		DestinationAccessMode:                payload.DestinationAccessMode,
		DestinationCapacity:                  payload.DestinationCapacity,
		TargetNode:                           payload.TargetNode,
		Path:                                 payload.Path,
		AllowMounted:                         payload.AllowMounted,
		DeleteExtraneousFiles:                payload.DeleteExtraneous,
		SessionStore:                         r.store,
		SessionNamespace:                     session.Spec.SessionNamespace,
		Store:                                store,
		OpenEBSLVMManager:                    r.openEBS,
		Writer:                               io.Discard,
		Logger:                               r.logger,
		ObjectStoreProfile:                   payload.ObjectStoreProfile,
		ObjectStoreProfileUID:                types.UID(config.ProfileUID),
		ObjectStoreProfileGeneration:         config.ProfileGeneration,
		ObjectStoreCredentialsSecretUID:      types.UID(config.CredentialsSecretUID),
		ObjectStoreServiceAccountUID:         types.UID(config.ServiceAccountUID),
		ObjectStoreServiceAccountFingerprint: config.ServiceAccountFingerprint,
		ToolServiceAccountName:               config.ServiceAccountName,
	}

	return backup.ResumeRestore(ctx, r.client, request, session)
}

func (r *Runner) objectStoreConfig(
	ctx context.Context,
	session *domain.Session,
	bucket, prefix, name, provider, endpoint, region string,
	allowInsecure bool,
	serverSideEncryption, kmsKeyID string,
) (objectstore.Config, error) {
	if session == nil {
		return objectstore.Config{}, domain.NewError(domain.ErrorValidation, "controller object store", "session is required")
	}

	config := objectstore.Config{
		Bucket: bucket, Prefix: prefix, Name: name, Provider: provider,
		Endpoint: endpoint, Region: region, AllowInsecureEndpoint: allowInsecure,
		ForcePathStyle: endpoint != "", ServerSideEncryption: serverSideEncryption,
		SSEKMSKeyID: kmsKeyID,
	}

	profileName := ""
	if session.Spec.Backup != nil {
		profileName = session.Spec.Backup.ObjectStoreProfile
	}
	if session.Spec.Restore != nil {
		profileName = session.Spec.Restore.ObjectStoreProfile
	}
	if profileName == "" {
		return objectstore.Config{}, domain.NewError(domain.ErrorPrecondition, "controller object store", "controller workflows require spec.objectStoreProfile")
	}
	if r.controllerClient == nil {
		return objectstore.Config{}, domain.NewError(domain.ErrorKubernetes, "controller object store", "controller-runtime client is not configured")
	}

	profile := &v1alpha1.ObjectStoreProfile{}
	if err := r.controllerClient.Get(ctx, crclient.ObjectKey{Name: profileName}, profile); err != nil {
		if apierrors.IsNotFound(err) {
			return objectstore.Config{}, domain.WrapError(domain.ErrorPrecondition, "controller object store", "ObjectStoreProfile "+profileName+" does not exist", err)
		}
		return objectstore.Config{}, domain.WrapError(domain.ErrorKubernetes, "controller object store", "read ObjectStoreProfile "+profileName, err)
	}
	if profile.DeletionTimestamp != nil {
		return objectstore.Config{}, domain.NewError(
			domain.ErrorPrecondition,
			"controller object store",
			"ObjectStoreProfile is being deleted; create a new workflow after deletion completes",
		)
	}

	if profile.Spec.Backend != v1alpha1.ObjectStoreBackendS3 {
		return objectstore.Config{}, domain.NewError(domain.ErrorValidation, "controller object store", "ObjectStoreProfile backend must be s3")
	}
	usesWorkloadIdentity := len(profile.Spec.ServiceAccountRefs) > 0
	if !usesWorkloadIdentity && !profile.Spec.AllowStaticCredentialsInTenantNamespace {
		return objectstore.Config{}, domain.NewError(
			domain.ErrorPrecondition,
			"controller object store",
			"ObjectStoreProfile static credentials require explicit tenant-namespace projection approval",
		)
	}
	if usesWorkloadIdentity && profile.Spec.AllowStaticCredentialsInTenantNamespace {
		return objectstore.Config{}, domain.NewError(
			domain.ErrorValidation,
			"controller object store",
			"ObjectStoreProfile tenant-namespace projection approval cannot be combined with workload identity",
		)
	}
	if err := objectstore.ValidateConfig(objectstore.Config{
		Bucket:               profile.Spec.Bucket,
		Prefix:               profile.Spec.Prefix,
		Name:                 "profile",
		Provider:             profile.Spec.Provider,
		Endpoint:             profile.Spec.Endpoint,
		Region:               profile.Spec.Region,
		ForcePathStyle:       profile.Spec.ForcePathStyle,
		ServerSideEncryption: profile.Spec.ServerSideEncryption,
		SSEKMSKeyID:          profile.Spec.SSEKMSKeyID,
	}); err != nil {
		return objectstore.Config{}, domain.WrapError(
			domain.ErrorValidation,
			"controller object store",
			"validate ObjectStoreProfile location",
			err,
		)
	}

	if bucket != "" && bucket != profile.Spec.Bucket {
		return objectstore.Config{}, domain.NewError(domain.ErrorPrecondition, "controller object store", "workflow bucket is outside ObjectStoreProfile scope")
	}
	if prefix != "" && prefix != profile.Spec.Prefix {
		return objectstore.Config{}, domain.NewError(domain.ErrorPrecondition, "controller object store", "workflow prefix is outside ObjectStoreProfile scope")
	}
	if !usesWorkloadIdentity && len(profile.Spec.AllowedNamespaces) != 1 {
		return objectstore.Config{}, domain.NewError(
			domain.ErrorPrecondition,
			"controller object store",
			"static-credential ObjectStoreProfiles must allow exactly one tenant namespace",
		)
	}
	for _, namespace := range profile.Spec.AllowedNamespaces {
		namespaceName := string(namespace)
		if namespaceName == "" || strings.TrimSpace(namespaceName) != namespaceName || len(validation.IsDNS1123Label(namespaceName)) > 0 {
			return objectstore.Config{}, domain.NewError(
				domain.ErrorValidation,
				"controller object store",
				"ObjectStoreProfile allowedNamespaces must contain DNS labels",
			)
		}
	}
	var serviceAccountRef v1alpha1.ObjectStoreServiceAccountReference
	serviceAccountMatches := 0
	seenServiceAccountNamespaces := make(map[string]struct{}, len(profile.Spec.ServiceAccountRefs))
	for _, candidate := range profile.Spec.ServiceAccountRefs {
		if candidate.Name == "" || strings.TrimSpace(candidate.Name) != candidate.Name || len(validation.IsDNS1123Subdomain(candidate.Name)) > 0 ||
			candidate.Namespace == "" || strings.TrimSpace(candidate.Namespace) != candidate.Namespace || len(validation.IsDNS1123Label(candidate.Namespace)) > 0 ||
			strings.TrimSpace(candidate.UID) == "" ||
			!isSHA256Fingerprint(candidate.IdentityFingerprint) {
			return objectstore.Config{}, domain.NewError(domain.ErrorValidation, "controller object store", "ObjectStoreProfile serviceAccountRefs must contain valid namespace, name, UID, and identityFingerprint")
		}
		if _, duplicate := seenServiceAccountNamespaces[candidate.Namespace]; duplicate {
			return objectstore.Config{}, domain.NewError(domain.ErrorValidation, "controller object store", "ObjectStoreProfile serviceAccountRefs must contain at most one binding per namespace")
		}
		seenServiceAccountNamespaces[candidate.Namespace] = struct{}{}
		if candidate.Namespace == session.Spec.SessionNamespace {
			serviceAccountRef = candidate
			serviceAccountMatches++
		}
	}

	if usesWorkloadIdentity && len(profile.Spec.AllowedNamespaces) > 0 {
		return objectstore.Config{}, domain.NewError(domain.ErrorValidation, "controller object store", "ObjectStoreProfile allowedNamespaces is only valid for static credential profiles")
	}
	if serviceAccountMatches > 1 {
		return objectstore.Config{}, domain.NewError(domain.ErrorValidation, "controller object store", "ObjectStoreProfile has multiple ServiceAccount bindings for workflow namespace")
	}
	if !usesWorkloadIdentity {
		allowed := false
		for _, candidate := range profile.Spec.AllowedNamespaces {
			if string(candidate) == session.Spec.SessionNamespace {
				allowed = true
				break
			}
		}
		if !allowed {
			return objectstore.Config{}, domain.NewError(domain.ErrorPrecondition, "controller object store", "workflow namespace is not allowed by ObjectStoreProfile")
		}
	}
	if provider != "" || region != "" || endpoint != "" || allowInsecure ||
		serverSideEncryption != "" || kmsKeyID != "" {
		return objectstore.Config{}, domain.NewError(domain.ErrorPrecondition, "controller object store", "workflow connection and encryption overrides are forbidden; configure them in ObjectStoreProfile")
	}
	config.Provider = profile.Spec.Provider
	config.Endpoint = profile.Spec.Endpoint
	config.Region = profile.Spec.Region
	config.Bucket = profile.Spec.Bucket
	basePrefix := profile.Spec.Prefix
	// Every controller workflow receives a namespace-owned object prefix. A
	// shared administrator profile therefore cannot be used by one tenant to
	// address another tenant's recovery points by guessing its name.
	config.Prefix = path.Join(basePrefix, "namespaces", session.Spec.SessionNamespace)
	if r.clusterIdentity != "" {
		// Profiles and buckets may be configured identically in multiple
		// clusters. Include a stable cluster scope before the tenant scope so
		// recovery points cannot overwrite or be confused across clusters.
		config.Prefix = path.Join(basePrefix, "clusters", clusterScopeSegment(r.clusterIdentity), "namespaces", session.Spec.SessionNamespace)
	}
	config.AllowInsecureEndpoint = false
	config.ForcePathStyle = profile.Spec.ForcePathStyle || config.Endpoint != ""
	config.ServerSideEncryption = profile.Spec.ServerSideEncryption
	config.SSEKMSKeyID = profile.Spec.SSEKMSKeyID

	hasServiceAccountRef := serviceAccountMatches == 1
	ref := profile.Spec.CredentialsSecret
	if ref != nil && strings.TrimSpace(ref.Name) == "" {
		return objectstore.Config{}, domain.NewError(domain.ErrorValidation, "controller object store", "ObjectStoreProfile credentialsSecret name is required when configured")
	}
	if !usesWorkloadIdentity && (ref == nil || ref.Name == "") {
		return objectstore.Config{}, domain.NewError(
			domain.ErrorPrecondition,
			"controller object store",
			"ObjectStoreProfile static credential profiles require credentialsSecret",
		)
	}
	if ref != nil && ref.Name != "" {
		controllerNamespace := strings.TrimSpace(r.controllerNamespace)
		if controllerNamespace == "" {
			return objectstore.Config{}, domain.NewError(
				domain.ErrorPrecondition,
				"controller object store",
				"controller installation namespace is not configured; refusing to read profile credentials",
			)
		}
		if r.client == nil {
			return objectstore.Config{}, domain.NewError(domain.ErrorKubernetes, "controller object store", "Kubernetes client is not configured")
		}
		secret, err := r.client.CoreV1().Secrets(controllerNamespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return objectstore.Config{}, domain.WrapError(domain.ErrorPrecondition, "controller object store", "ObjectStoreProfile credentials Secret does not exist", err)
		}
		if err != nil {
			return objectstore.Config{}, domain.WrapError(domain.ErrorKubernetes, "controller object store", "read ObjectStoreProfile credentials Secret", err)
		}
		if err := kube.ValidateS3CredentialsData(secret.Data); err != nil {
			return objectstore.Config{}, domain.WrapError(domain.ErrorPrecondition, "controller object store", "ObjectStoreProfile credentials Secret is invalid", err)
		}
		config.AccessKey = string(secret.Data[kube.BackupAccessKeyDataKey])
		config.SecretKey = string(secret.Data[kube.BackupSecretKeyDataKey])
		config.SessionToken = string(secret.Data[kube.BackupSessionTokenDataKey])
		config.CredentialsSecretUID = string(secret.UID)
	}
	if usesWorkloadIdentity {
		if !hasServiceAccountRef {
			return objectstore.Config{}, domain.NewError(domain.ErrorPrecondition, "controller object store", "ObjectStoreProfile has no ServiceAccount binding for workflow namespace")
		}
		config.UseAmbientCredentials = true
		if r.client == nil {
			return objectstore.Config{}, domain.NewError(domain.ErrorKubernetes, "controller object store", "Kubernetes client is not configured")
		}
		serviceAccount, err := r.client.CoreV1().ServiceAccounts(serviceAccountRef.Namespace).Get(ctx, serviceAccountRef.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return objectstore.Config{}, domain.WrapError(domain.ErrorPrecondition, "controller object store", "ObjectStoreProfile ServiceAccount does not exist in its bound namespace", err)
		} else if err != nil {
			return objectstore.Config{}, domain.WrapError(domain.ErrorKubernetes, "controller object store", "read ObjectStoreProfile service account", err)
		}
		if string(serviceAccount.UID) != serviceAccountRef.UID {
			return objectstore.Config{}, domain.NewError(domain.ErrorPrecondition, "controller object store", "ObjectStoreProfile ServiceAccount UID does not match the administrator binding")
		}
		if fingerprint := kube.ServiceAccountIdentityFingerprint(serviceAccount); fingerprint != serviceAccountRef.IdentityFingerprint {
			return objectstore.Config{}, domain.NewError(
				domain.ErrorPrecondition,
				"controller object store",
				"ObjectStoreProfile ServiceAccount identity fingerprint does not match the administrator binding",
			)
		}
		if serviceAccount.AutomountServiceAccountToken != nil && !*serviceAccount.AutomountServiceAccountToken {
			return objectstore.Config{}, domain.NewError(
				domain.ErrorPrecondition,
				"controller object store",
				"ObjectStoreProfile ServiceAccount must enable automountServiceAccountToken for workload identity",
			)
		}
		config.ServiceAccountName = serviceAccountRef.Name
		config.ServiceAccountUID = string(serviceAccount.UID)
		config.ServiceAccountFingerprint = kube.ServiceAccountIdentityFingerprint(serviceAccount)
		if config.ServiceAccountFingerprint == "" {
			return objectstore.Config{}, domain.NewError(
				domain.ErrorInternal,
				"controller object store",
				"failed to fingerprint ObjectStoreProfile service account",
			)
		}
	}
	config.ProfileUID = string(profile.UID)
	config.ProfileGeneration = profile.Generation
	return config, nil
}

func clusterScopeSegment(identity string) string {
	digest := sha256.Sum256([]byte(identity))
	// 128 bits is ample for the number of Kubernetes clusters sharing an
	// object store while keeping the generated prefix comfortably within S3
	// and rclone path limits.
	return hex.EncodeToString(digest[:16])
}

func terminalSession(session *domain.Session) bool {
	if session == nil {
		return true
	}

	switch session.Status.Phase {
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack, domain.PhaseFailed:
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
