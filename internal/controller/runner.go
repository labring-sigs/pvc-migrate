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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// Runner reconciles every workflow CR by delegating its stages to app.Service.
// The service remains the single owner of phase transitions, persistence and
// resource fencing; this runner only provides event polling and dispatch.
type Runner struct {
	service          workflowResumer
	store            kube.ControllerSessionStore
	client           kubernetes.Interface
	controllerClient crclient.Reader
	namespace        string
	clusterIdentity  string
	trustedToolImage string
	kubeconfigPath   string
	kubeContext      string
	logger           *slog.Logger
	openEBS          kube.OpenEBSLVMSharedVolumeManager
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

// WithControllerClient supplies the typed client used for repository
// configuration and other controller-owned resources.
func (r *Runner) WithControllerClient(client crclient.Reader) *Runner {
	if r != nil {
		r.controllerClient = client
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

// WithKubeconfig supplies the connection used by pv-migrate's Helm-backed
// backup and restore runners. An empty path keeps the in-cluster client
// behavior used by controller deployments.
func (r *Runner) WithKubeconfig(path, context string) *Runner {
	if r != nil {
		r.kubeconfigPath = strings.TrimSpace(path)
		r.kubeContext = strings.TrimSpace(context)
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

func NewRunner(
	service workflowResumer,
	store kube.ControllerSessionStore,
	namespace string,
) *Runner {
	return &Runner{
		service:   service,
		store:     store,
		namespace: namespace,
		logger:    slog.Default(),
	}
}

func (r *Runner) WithLogger(logger *slog.Logger) *Runner {
	if logger != nil {
		r.logger = logger
	}
	return r
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

// checkpointFailure is the CLI-facing form of failure checkpointing. It keeps
// the original business error in the returned value so controller --once can
// report a non-zero exit status after persisting Failed.
func (r *Runner) checkpointFailure(
	ctx context.Context,
	session *domain.Session,
	cause error,
) error {
	return errors.Join(cause, r.persistFailure(ctx, session, cause))
}

// checkpointFailureForController records a business failure and returns only
// errors that prevented the controller from persisting it. A workflow that
// reaches Failed is a normal terminal outcome for reconcile and must not be
// surfaced as a controller-runtime Reconciler error.
func (r *Runner) checkpointFailureForController(
	ctx context.Context,
	session *domain.Session,
	cause error,
) error {
	return r.persistFailure(ctx, session, cause)
}

// persistFailure records errors that happen before a workflow service can
// acquire its own session lock. Re-read the session while holding the Lease so
// a stale reconcile cannot overwrite a newer phase written by another worker.
func (r *Runner) persistFailure(
	ctx context.Context,
	session *domain.Session,
	cause error,
) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}

	if cause == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"controller checkpoint",
			"workflow failure cause is required",
		)
	}

	if session == nil {
		return nil
	}

	if session.BackendResource == "" {
		return domain.NewError(
			domain.ErrorInternal,
			"controller checkpoint",
			"workflow backend resource kind is required",
		)
	}

	namespace := session.Spec.SessionNamespace

	lock, err := kube.AcquireRequiredSessionLock(
		ctx,
		r.store,
		namespace,
		kube.SessionLockID(session),
	)
	if err != nil {
		resource, ok := domain.ControllerResourceForKind(session.BackendResource)
		if r.client != nil && ok && resource.Cluster && apierrors.IsNotFound(err) {
			namespaceErr := kube.RequireNamespace(ctx, r.client, namespace)
			if apierrors.IsNotFound(namespaceErr) {
				// An absent namespace cannot hold a Lease. The CR's resourceVersion
				// still fences this failure checkpoint against concurrent updates.
				return r.transitionFailure(ctx, session, cause)
			}
		}

		return err
	}

	operationCtx, cancelOperation := lock.Bind(ctx)
	operationErr := func() error {
		latest, getErr := r.store.GetByKind(
			operationCtx,
			namespace,
			session.ID,
			session.BackendResource,
		)
		if getErr != nil {
			return getErr
		}

		if latest == nil || terminalSession(latest) || latest.Status.Phase == domain.PhaseFailed {
			return nil
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
		return err
	}

	if err := r.store.Update(ctx, session); err != nil {
		return err
	}

	r.logger.Warn(
		"workflow entered failed state",
		"workflow",
		types.NamespacedName{Namespace: session.Spec.SessionNamespace, Name: session.ID},
		"phase",
		domain.PhaseFailed,
		"reason",
		cause,
	)

	return nil
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

	if err := kube.ControllerNamespaceBoundaryError(session); err != nil {
		return err
	}

	if r.client != nil {
		for _, namespace := range []string{session.Spec.SessionNamespace, session.Spec.TemporaryNamespace, session.Spec.DestinationNamespace} {
			if namespace == "" {
				continue
			}

			if err := kube.RequireNamespace(
				ctx,
				r.client,
				namespace,
			); err != nil {
				return err
			}
		}
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

	// Rename and Move operate on PVC/PV metadata only and never create a
	// data-mover Pod. Their CRDs intentionally do not expose a tool image.
	if operation := session.Spec.Operation(); operation == domain.OperationRename ||
		operation == domain.OperationMove {
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

	// The workflow spec is tenant input and may contain an older or arbitrary
	// toolImage. Execution always uses the administrator-selected image; the
	// spec value is intentionally ignored so controller upgrades remain able to
	// resume existing sessions without granting image selection to tenants.
	r.trustedToolImage = trusted

	return nil
}

func (r *Runner) executionToolImage(session *domain.Session) string {
	if r != nil && strings.TrimSpace(r.trustedToolImage) != "" {
		return r.trustedToolImage
	}

	if session == nil {
		return ""
	}

	return session.Spec.WorkflowOptions().ToolImage
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

	config, err := r.objectStoreConfig(
		ctx,
		session,
		payload.Bucket,
		payload.Prefix,
		payload.Name,
		payload.Provider,
		payload.Endpoint,
		payload.Region,
		payload.AllowInsecureEndpoint,
		payload.ServerSideEncryption,
		payload.SSEKMSKeyID,
	)
	if err != nil {
		return err
	}

	request := backup.Request{
		ID:                      session.ID,
		ToolImage:               r.executionToolImage(session),
		KubeconfigPath:          r.kubeconfigPath,
		KubeContext:             r.kubeContext,
		Namespace:               payload.SourcePVC.Namespace,
		PVCName:                 payload.SourcePVC.Name,
		Path:                    payload.Path,
		Online:                  payload.Online,
		DeleteExtraneousFiles:   payload.DeleteExtraneous,
		SessionStore:            r.store,
		SessionNamespace:        session.Spec.SessionNamespace,
		OpenEBSLVMManager:       r.openEBS,
		ObjectStoreFactory:      objectstore.New,
		ToolImageProber:         kube.NewToolImageProber(r.client),
		Store:                   nil,
		Writer:                  io.Discard,
		Logger:                  r.logger,
		BackupSession:           session,
		BackupRepository:        payload.BackupRepository,
		BackupRepositoryBinding: s3RepositoryBinding(config),
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

	config, err := r.objectStoreConfig(
		ctx,
		session,
		payload.Bucket,
		payload.Prefix,
		payload.Name,
		payload.Provider,
		payload.Endpoint,
		payload.Region,
		payload.AllowInsecureEndpoint,
		payload.ServerSideEncryption,
		payload.SSEKMSKeyID,
	)
	if err != nil {
		return err
	}

	store, err := objectstore.New(ctx, config)
	if err != nil {
		return err
	}

	request := backup.Request{
		ID:                      session.ID,
		ToolImage:               r.executionToolImage(session),
		KubeconfigPath:          r.kubeconfigPath,
		KubeContext:             r.kubeContext,
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
		ToolImageProber:         kube.NewToolImageProber(r.client),
		Writer:                  io.Discard,
		Logger:                  r.logger,
		BackupRepository:        payload.BackupRepository,
		BackupRepositoryBinding: s3RepositoryBinding(config),
	}

	return backup.ResumeRestore(ctx, r.client, request, session)
}

func s3RepositoryBinding(config objectstore.Config) *domain.BackupRepositoryBindingStatus {
	return &domain.BackupRepositoryBindingStatus{
		Type:       domain.BackupRepositoryTypeS3,
		UID:        types.UID(config.RepositoryUID),
		Generation: config.RepositoryGeneration,
		S3: &domain.S3BackupRepositoryBindingStatus{
			CredentialsSecretUID: types.UID(config.CredentialsSecretUID),
		},
	}
}

func (r *Runner) objectStoreConfig(
	ctx context.Context,
	session *domain.Session,
	bucket, prefix, name, provider, endpoint, region string,
	allowInsecure bool,
	serverSideEncryption, kmsKeyID string,
) (objectstore.Config, error) {
	if session == nil {
		return objectstore.Config{}, domain.NewError(
			domain.ErrorValidation,
			"controller object store",
			"session is required",
		)
	}

	// BackupRepository is the current API. It represents a user-selected
	// object-store location, not a tenant authorization lease. Resolve it in
	// the workflow namespace and keep the credential Secret namespace-local.
	repositoryName := ""
	if session.Spec.Backup != nil {
		repositoryName = session.Spec.Backup.BackupRepository
	}

	if session.Spec.Restore != nil {
		repositoryName = session.Spec.Restore.BackupRepository
	}

	if repositoryName != "" {
		return r.backupRepositoryConfig(
			ctx,
			session,
			repositoryName,
			name,
			bucket,
			prefix,
			provider,
			endpoint,
			region,
			allowInsecure,
			serverSideEncryption,
			kmsKeyID,
		)
	}

	return objectstore.Config{}, domain.NewError(
		domain.ErrorPrecondition,
		"controller object store",
		"controller workflows require spec.repositoryRef",
	)
}

func clusterScopeSegment(identity string) string {
	digest := sha256.Sum256([]byte(identity))
	// 128 bits is ample for the number of Kubernetes clusters sharing an
	// object store while keeping the generated prefix comfortably within S3
	// and rclone path limits.
	return hex.EncodeToString(digest[:16])
}

func repositoryNamespaceForSession(session *domain.Session) (string, error) {
	repositoryNamespace := ""
	if session.Spec.Backup != nil {
		repositoryNamespace = session.Spec.Backup.BackupRepositoryNamespace
	}

	if session.Spec.Restore != nil {
		repositoryNamespace = session.Spec.Restore.BackupRepositoryNamespace
	}

	if repositoryNamespace == "" {
		repositoryNamespace = session.Spec.SessionNamespace
	}

	if repositoryNamespace != session.Spec.SessionNamespace {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"controller object store",
			"BackupRepository must be in the workflow namespace",
		)
	}

	return repositoryNamespace, nil
}

func s3RepositorySpec(
	repository *v1alpha1.BackupRepository,
) (*v1alpha1.S3BackupRepositorySpec, error) {
	if repository.Spec.Type == v1alpha1.BackupRepositoryTypePVC {
		claim := ""
		if repository.Spec.PVC != nil {
			claim = repository.Spec.PVC.ClaimRef.Name
		}

		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"controller object store",
			fmt.Sprintf(
				"BackupRepository backend pvc is not supported by this controller yet (claim %q)",
				claim,
			),
		)
	}

	if repository.Spec.Type != v1alpha1.BackupRepositoryTypeS3 || repository.Spec.S3 == nil {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"controller object store",
			"BackupRepository requires type s3 with an s3 configuration",
		)
	}

	return repository.Spec.S3, nil
}

func controllerObjectStoreOverridesError(
	bucket, prefix, provider, endpoint, region, serverSideEncryption, kmsKeyID string,
	allowInsecure bool,
) error {
	if bucket == "" && prefix == "" && provider == "" && endpoint == "" && region == "" &&
		!allowInsecure && serverSideEncryption == "" && kmsKeyID == "" {
		return nil
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		"controller object store",
		"workflow object-store settings must be configured in BackupRepository",
	)
}

func (r *Runner) backupRepositorySecret(
	ctx context.Context,
	namespace, name string,
) (*corev1.Secret, error) {
	if r.client == nil {
		return nil, domain.NewError(
			domain.ErrorKubernetes,
			"controller object store",
			"Kubernetes client is not configured",
		)
	}

	secret, err := r.client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, domain.WrapError(
			domain.ErrorPrecondition,
			"controller object store",
			"BackupRepository credentials Secret does not exist",
			err,
		)
	}

	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"controller object store",
			"read BackupRepository credentials Secret",
			err,
		)
	}

	if err := kube.ValidateS3CredentialsData(secret.Data); err != nil {
		return nil, domain.WrapError(
			domain.ErrorPrecondition,
			"controller object store",
			"BackupRepository credentials Secret is invalid",
			err,
		)
	}

	return secret, nil
}

func (r *Runner) backupRepositoryConfig(
	ctx context.Context,
	session *domain.Session,
	repositoryName, name, bucket, prefix, provider, endpoint, region string,
	allowInsecure bool,
	serverSideEncryption, kmsKeyID string,
) (objectstore.Config, error) {
	if r.controllerClient == nil {
		return objectstore.Config{}, domain.NewError(
			domain.ErrorKubernetes,
			"controller object store",
			"controller-runtime client is not configured",
		)
	}

	if session == nil || session.Spec.SessionNamespace == "" {
		return objectstore.Config{}, domain.NewError(
			domain.ErrorValidation,
			"controller object store",
			"workflow namespace is required",
		)
	}

	config := objectstore.Config{Name: name}
	repository := &v1alpha1.BackupRepository{}

	repositoryNamespace, err := repositoryNamespaceForSession(session)
	if err != nil {
		return objectstore.Config{}, err
	}

	key := crclient.ObjectKey{Namespace: repositoryNamespace, Name: repositoryName}
	if err := r.controllerClient.Get(ctx, key, repository); err != nil {
		if apierrors.IsNotFound(err) {
			return objectstore.Config{}, domain.WrapError(
				domain.ErrorPrecondition,
				"controller object store",
				"BackupRepository "+repositoryName+" does not exist",
				err,
			)
		}

		return objectstore.Config{}, domain.WrapError(
			domain.ErrorKubernetes,
			"controller object store",
			"read BackupRepository "+repositoryName,
			err,
		)
	}

	if repository.DeletionTimestamp != nil {
		return objectstore.Config{}, domain.NewError(
			domain.ErrorPrecondition,
			"controller object store",
			"BackupRepository is being deleted; create a new workflow after deletion completes",
		)
	}

	s3Spec, err := s3RepositorySpec(repository)
	if err != nil {
		return objectstore.Config{}, err
	}

	locationBucket := s3Spec.Bucket
	locationPrefix := s3Spec.Prefix
	locationProvider := s3Spec.Provider
	locationEndpoint := s3Spec.Endpoint
	locationRegion := s3Spec.Region
	forcePathStyle := s3Spec.ForcePathStyle
	locationAllowInsecure := s3Spec.AllowInsecureEndpoint
	locationEncryption := s3Spec.ServerSideEncryption
	locationKMSKeyID := s3Spec.SSEKMSKeyID
	secretNamespace := repositoryNamespace

	secretName := s3Spec.CredentialsSecret.Name
	if secretName == "" {
		return objectstore.Config{}, domain.NewError(
			domain.ErrorPrecondition,
			"controller object store",
			"BackupRepository credentialsSecret is required",
		)
	}

	if err := controllerObjectStoreOverridesError(
		bucket,
		prefix,
		provider,
		endpoint,
		region,
		serverSideEncryption,
		kmsKeyID,
		allowInsecure,
	); err != nil {
		return objectstore.Config{}, err
	}

	if err := objectstore.ValidateConfig(objectstore.Config{
		Bucket:                locationBucket,
		Prefix:                locationPrefix,
		Name:                  name,
		Provider:              locationProvider,
		Endpoint:              locationEndpoint,
		Region:                locationRegion,
		AllowInsecureEndpoint: locationAllowInsecure,
		ForcePathStyle:        forcePathStyle,
		ServerSideEncryption:  locationEncryption,
		SSEKMSKeyID:           locationKMSKeyID,
	}); err != nil {
		return objectstore.Config{}, domain.WrapError(
			domain.ErrorValidation,
			"controller object store",
			"validate BackupRepository location",
			err,
		)
	}

	secret, err := r.backupRepositorySecret(ctx, secretNamespace, secretName)
	if err != nil {
		return objectstore.Config{}, err
	}

	config.Bucket = locationBucket
	config.Provider = locationProvider
	config.Endpoint = locationEndpoint
	config.Region = locationRegion
	config.AllowInsecureEndpoint = locationAllowInsecure
	config.ForcePathStyle = forcePathStyle || locationEndpoint != ""
	config.ServerSideEncryption = locationEncryption
	config.SSEKMSKeyID = locationKMSKeyID

	dataNamespace := session.Spec.SourceNamespace
	if session.Spec.Type == domain.SessionTypeRestore {
		dataNamespace = session.Spec.DestinationNamespace
	}

	if dataNamespace == "" {
		dataNamespace = session.Spec.SessionNamespace
	}

	config.Prefix = path.Join(
		locationPrefix,
		"clusters",
		clusterScopeSegment(r.clusterIdentity),
		"namespaces",
		dataNamespace,
	)
	if r.clusterIdentity == "" {
		config.Prefix = path.Join(locationPrefix, "namespaces", dataNamespace)
	}

	config.AccessKey = string(secret.Data[kube.BackupAccessKeyDataKey])
	config.SecretKey = string(secret.Data[kube.BackupSecretKeyDataKey])
	config.SessionToken = string(secret.Data[kube.BackupSessionTokenDataKey])
	config.CredentialsSecretUID = string(secret.UID)
	config.RepositoryUID = string(repository.UID)
	config.RepositoryGeneration = repository.Generation

	return config, nil
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
