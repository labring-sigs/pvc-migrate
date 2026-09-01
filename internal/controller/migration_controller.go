package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// WorkflowReconciler bridges every operation-specific workflow Kind to the
// existing service state machine. The historical MigrationReconciler alias is
// retained for source compatibility; reconciliation is intentionally not
// limited to Migration resources.
type WorkflowReconciler struct {
	store               kube.SessionStore
	service             *app.Service
	kubeClient          kubernetes.Interface
	controllerClient    crclient.Reader
	openEBS             kube.OpenEBSLVMSharedVolumeManager
	namespace           string
	controllerNamespace string
	clusterIdentity     string
	trustedToolImage    string
	requeueAfter        time.Duration
	supportedKinds      map[domain.ControllerKind]struct{}
}

// MigrationReconciler is kept as an alias for callers that used the original
// controller name before operation-specific workflow Kinds were added.
type MigrationReconciler = WorkflowReconciler

func NewWorkflowReconciler(service *app.Service, store kube.SessionStore) *WorkflowReconciler {
	return &WorkflowReconciler{service: service, store: store, requeueAfter: 5 * time.Second}
}

func NewMigrationReconciler(service *app.Service, store kube.SessionStore) *WorkflowReconciler {
	return NewWorkflowReconciler(service, store)
}

// WithNamespace optionally limits reconciliation to one tenant namespace.
// Production managers leave it unset so every namespaced workflow is watched;
// the reconciler still enforces that referenced objects stay in the CR
// namespace.
func (r *WorkflowReconciler) WithNamespace(namespace string) *WorkflowReconciler {
	if r != nil {
		r.namespace = namespace
	}

	return r
}

// WithSupportedKinds restricts watches to CRDs served by the target cluster.
// An empty list means all workflow kinds, preserving the explicit complete
// installation behavior and keeping unit-test construction simple.
func (r *WorkflowReconciler) WithSupportedKinds(kinds []domain.ControllerKind) *WorkflowReconciler {
	if r == nil || len(kinds) == 0 {
		return r
	}

	r.supportedKinds = make(map[domain.ControllerKind]struct{}, len(kinds))
	for _, kind := range kinds {
		r.supportedKinds[kind] = struct{}{}
	}

	return r
}

func (r *WorkflowReconciler) supportsKind(kind domain.ControllerKind) bool {
	if r == nil || len(r.supportedKinds) == 0 {
		return true
	}

	_, ok := r.supportedKinds[kind]

	return ok
}

func (r *WorkflowReconciler) WithKubernetesClient(
	client kubernetes.Interface,
) *WorkflowReconciler {
	if r != nil {
		r.kubeClient = client
	}
	return r
}

// WithControllerClient supplies the typed client used for cluster-scoped
// administrator configuration such as ObjectStoreProfile.
func (r *WorkflowReconciler) WithControllerClient(client crclient.Reader) *WorkflowReconciler {
	if r != nil {
		r.controllerClient = client
	}
	return r
}

func (r *WorkflowReconciler) WithControllerNamespace(namespace string) *WorkflowReconciler {
	if r != nil {
		r.controllerNamespace = namespace
	}
	return r
}

// WithClusterIdentity scopes controller-backed object-store paths to the
// cluster serving this manager. StartManager populates it from kube-system's
// stable namespace UID.
func (r *WorkflowReconciler) WithClusterIdentity(identity string) *WorkflowReconciler {
	if r != nil {
		r.clusterIdentity = strings.TrimSpace(identity)
	}
	return r
}

// WithTrustedToolImage pins all controller-created data mover Pods to the
// administrator-selected image. Tenants must not be able to choose code that
// receives a PVC or object-store identity.
func (r *WorkflowReconciler) WithTrustedToolImage(image string) *WorkflowReconciler {
	if r != nil {
		r.trustedToolImage = image
	}
	return r
}

func (r *WorkflowReconciler) WithOpenEBSLVMSharedVolumeManager(
	manager kube.OpenEBSLVMSharedVolumeManager,
) *WorkflowReconciler {
	if r != nil {
		r.openEBS = manager
	}
	return r
}

func (r *WorkflowReconciler) runner(namespace string) *Runner {
	return NewRunner(r.service, r.store, namespace).
		WithKubernetesClient(r.kubeClient).
		WithControllerClient(r.controllerClient).
		WithControllerNamespace(r.controllerNamespace).
		WithClusterIdentity(r.clusterIdentity).
		WithTrustedToolImage(r.trustedToolImage).
		WithOpenEBSLVMSharedVolumeManager(r.openEBS)
}

func (r *WorkflowReconciler) Reconcile(
	ctx context.Context,
	request reconcile.Request,
) (reconcile.Result, error) {
	if r == nil || r.store == nil || r.service == nil {
		return reconcile.Result{}, errors.New("migration reconciler is not configured")
	}

	if r.namespace != "" && request.Namespace != r.namespace {
		return reconcile.Result{}, nil
	}

	session, err := r.store.Get(ctx, request.Namespace, request.Name)
	if err != nil {
		// The SessionStore classifies a missing CRD object as validation; the
		// controller should treat that as a normal delete event as well.
		if kube.IsSessionNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	// A workflow's spec is the authorization and execution input. Once the
	// controller has observed a generation, changing that input would let a
	// tenant retarget an in-flight operation (for example to another recovery
	// point or object-store profile). CRD status updates do not change
	// generation, so this check does not interfere with normal progress.
	if err := workflowSpecMutationError(session); err != nil {
		if !terminalSession(session) {
			runner := r.runner(request.Namespace)
			return reconcile.Result{}, runner.checkpointFailure(ctx, session, err)
		}
		return reconcile.Result{}, err
	}

	// Declarative CRs do not pass through CRDSessionStore.Create, so they may
	// arrive without the session-protection finalizer. Add it before any
	// execution or terminal-state handling to preserve the explicit cleanup
	// contract for every controller-backed workflow.
	if ensurer, ok := r.store.(kube.SessionProtectionEnsurer); ok {
		if err := ensurer.EnsureSessionProtection(ctx, session); err != nil {
			return reconcile.Result{}, err
		}
	}
	if session.Deleting {
		// A deletion request must not start or advance a workflow. Existing
		// finalizers keep cleanup explicit; resources without our finalizer can
		// finish deletion normally.
		return reconcile.Result{}, nil
	}

	// Status is a controller-owned subresource. A declarative create ignores a
	// user-supplied status, so a terminal status with no observed generation is
	// stale or forged. Reinitialize it to the durable Planned checkpoint before
	// deciding whether there is any work left to do.
	if err := resetUnobservedTerminalStatus(ctx, r.store, session); err != nil {
		return reconcile.Result{}, err
	}

	if terminalSession(session) {
		if session.Status.Phase == domain.PhaseFailed {
			// Failed workflows are quiescent until an explicit resume changes
			// the phase. Poll them slowly because status updates are filtered to
			// avoid a controller-owned feedback loop.
			return reconcile.Result{RequeueAfter: r.requeueAfter}, nil
		}
		return reconcile.Result{}, nil
	}
	if boundaryErr := kube.ControllerNamespaceBoundaryError(session); boundaryErr != nil {
		runner := r.runner(request.Namespace)
		return reconcile.Result{}, runner.checkpointFailure(ctx, session, boundaryErr)
	}
	if err := r.validateDeclarativeSourceVolumes(ctx, session); err != nil {
		runner := r.runner(request.Namespace)
		return reconcile.Result{}, runner.checkpointFailure(ctx, session, err)
	}
	if err := r.ensureStandalonePodSnapshot(ctx, session); err != nil {
		runner := r.runner(request.Namespace)
		return reconcile.Result{}, runner.checkpointFailure(ctx, session, err)
	}

	runner := r.runner(request.Namespace)
	if err := runner.reconcileSession(ctx, session); err != nil {
		err = runner.checkpointFailure(ctx, session, err)

		return reconcile.Result{}, err
	}

	return reconcile.Result{RequeueAfter: r.requeueAfter}, nil
}

func resetUnobservedTerminalStatus(
	ctx context.Context,
	store kube.SessionStore,
	session *domain.Session,
) error {
	if session == nil || !terminalSession(session) ||
		session.Status.ObservedGeneration != 0 || store == nil {
		return nil
	}

	planned := domain.NewSession(session.ID, session.Spec, time.Now())
	planned.Generation = session.Generation
	planned.ResourceVersion = session.ResourceVersion
	planned.Backend = session.Backend
	planned.BackendResource = session.BackendResource
	planned.BackendUID = session.BackendUID
	session.Status = planned.Status

	return store.Update(ctx, session)
}

// SetupWithManager installs namespaced watches for every operation-specific
// workflow resource. Generation changes enqueue spec changes; status updates
// are handled by the explicit requeue, preventing a status-write feedback loop.
func (r *WorkflowReconciler) SetupWithManager(manager ctrl.Manager) error {
	predicates := builder.WithPredicates(predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectNew.GetGeneration() != e.ObjectOld.GetGeneration()
		},
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return true },
	})

	var controllerBuilder *builder.Builder

	objects := []struct {
		kind   domain.ControllerKind
		object crclient.Object
	}{
		{kind: domain.ControllerKindMigration, object: &v1alpha1.Migration{}},
		{kind: domain.ControllerKindPodMigration, object: &v1alpha1.PodMigration{}},
		{kind: domain.ControllerKindReservation, object: &v1alpha1.Reservation{}},
		{kind: domain.ControllerKindCopy, object: &v1alpha1.Copy{}},
		{kind: domain.ControllerKindBackup, object: &v1alpha1.Backup{}},
		{kind: domain.ControllerKindRestore, object: &v1alpha1.Restore{}},
		{kind: domain.ControllerKindRename, object: &v1alpha1.Rename{}},
		{kind: domain.ControllerKindMove, object: &v1alpha1.Move{}},
	}
	for _, object := range objects {
		if !r.supportsKind(object.kind) {
			continue
		}

		if controllerBuilder == nil {
			controllerBuilder = ctrl.NewControllerManagedBy(manager).For(object.object, predicates)
			continue
		}

		controllerBuilder = controllerBuilder.Watches(
			object.object,
			&handler.EnqueueRequestForObject{},
			predicates,
		)
	}

	if controllerBuilder == nil {
		return errors.New("no workflow CRDs are served by the target cluster")
	}

	return controllerBuilder.Complete(r)
}

// StartManager creates the production controller-runtime manager. Leader
// election is enabled so multiple replicas can be deployed safely; the app
// service's per-session Lease still fences individual CLI/controller calls.
func StartManager(
	ctx context.Context,
	config *rest.Config,
	service *app.Service,
	store kube.SessionStore,
	namespace string,
	kubeClient kubernetes.Interface,
	openEBSManager kube.OpenEBSLVMSharedVolumeManager,
	trustedToolImage ...string,
) error {
	return StartManagerWithKinds(
		ctx, config, service, store, namespace, kubeClient, openEBSManager, nil,
		trustedToolImage...,
	)
}

// StartManagerWithKinds creates a manager that watches only the discovered
// workflow kinds. This allows auto mode to reconcile a partial CRD rollout
// while keeping ConfigMap fallback for operations not yet installed.
func StartManagerWithKinds(
	ctx context.Context,
	config *rest.Config,
	service *app.Service,
	store kube.SessionStore,
	namespace string,
	kubeClient kubernetes.Interface,
	openEBSManager kube.OpenEBSLVMSharedVolumeManager,
	supportedKinds []domain.ControllerKind,
	trustedToolImage ...string,
) error {
	if config == nil {
		return errors.New("kubernetes REST config is required")
	}

	if namespace == "" {
		return errors.New("controller namespace is required")
	}

	if kubeClient == nil {
		return errors.New("controller Kubernetes client is required")
	}
	cluster, err := kube.Identity(ctx, &kube.Clients{Kubernetes: kubeClient})
	if err != nil {
		return fmt.Errorf("resolve controller cluster identity: %w", err)
	}

	// Controller-created transfer Pods may receive PVC mounts, static object
	// store credentials, or workload identity. Require an administrator-pinned
	// image before constructing the manager so an embedded caller cannot
	// accidentally delegate image selection to a tenant workflow.
	normalizedTrustedImage, err := normalizeTrustedToolImage(firstString(trustedToolImage))
	if err != nil {
		return err
	}

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return err
	}

	manager, err := ctrl.NewManager(config, ctrl.Options{
		Scheme: scheme,
		// Workflow CRs are tenant-namespaced. The controller watches every
		// namespace; the reconciler enforces that each referenced object stays
		// in the CR namespace.
		Cache:                   cache.Options{},
		LeaderElection:          true,
		LeaderElectionID:        "pvc-migrate-controller",
		LeaderElectionNamespace: namespace,
		HealthProbeBindAddress:  ":8081",
		Metrics:                 metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		return err
	}

	if err := manager.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}

	if err := manager.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}

	reconciler := NewWorkflowReconciler(service, store).
		// Profile reads are authorization inputs. Use the uncached API reader so
		// deletion, replacement, and allowlist changes fail closed immediately.
		WithControllerClient(manager.GetAPIReader()).
		WithControllerNamespace(namespace).
		WithClusterIdentity(cluster.ID).
		WithTrustedToolImage(normalizedTrustedImage).
		WithSupportedKinds(supportedKinds)
	reconciler.WithKubernetesClient(kubeClient).
		WithOpenEBSLVMSharedVolumeManager(openEBSManager)

	if err := reconciler.SetupWithManager(manager); err != nil {
		return err
	}

	return manager.Start(ctx)
}

// ValidateTrustedToolImage checks the administrator-selected image before a
// controller execution path is started. Controller mode must never silently
// fall back to the image supplied in a tenant workflow.
func ValidateTrustedToolImage(image string) error {
	_, err := normalizeTrustedToolImage(image)
	return err
}

func normalizeTrustedToolImage(image string) (string, error) {
	image = strings.TrimSpace(image)
	if image == "" {
		return "", errors.New("trusted tool image is required for controller mode")
	}

	normalized, err := kube.NormalizeToolImage(image)
	if err != nil {
		return "", fmt.Errorf("invalid trusted tool image: %w", err)
	}

	return normalized, nil
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

var _ reconcile.Reconciler = (*WorkflowReconciler)(nil)

func workflowSpecMutationError(session *domain.Session) error {
	if session == nil || session.Status.ObservedGeneration == 0 ||
		session.Generation == session.Status.ObservedGeneration {
		return nil
	}

	return domain.NewError(
		domain.ErrorConflict,
		"controller reconcile",
		"workflow spec changed after execution started; create a new workflow instead",
	)
}
