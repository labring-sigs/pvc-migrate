package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/go-logr/logr"
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
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// WorkflowReconciler bridges every operation-specific workflow Kind to the
// existing service state machine.
type WorkflowReconciler struct {
	store            kube.SessionStore
	service          *app.Service
	kubeClient       kubernetes.Interface
	controllerClient crclient.Reader
	openEBS          kube.OpenEBSLVMSharedVolumeManager
	namespace        string
	clusterIdentity  string
	trustedToolImage string
	kubeconfigPath   string
	kubeContext      string
	requeueAfter     time.Duration
	supportedKinds   map[domain.ControllerKind]struct{}
}

func NewWorkflowReconciler(service *app.Service, store kube.SessionStore) *WorkflowReconciler {
	return &WorkflowReconciler{service: service, store: store, requeueAfter: 5 * time.Second}
}

// WithNamespace optionally limits reconciliation to one durable session
// namespace. Production managers leave it unset so every namespaced workflow
// and cluster workflow is watched.
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

// WithControllerClient supplies the typed client used for repository
// configuration and other controller-owned resources.
func (r *WorkflowReconciler) WithControllerClient(client crclient.Reader) *WorkflowReconciler {
	if r != nil {
		r.controllerClient = client
	}
	return r
}

// WithClusterIdentity scopes controller-backed object-store paths to the
// cluster serving this manager. StartManagerWithKinds populates it from
// kube-system's stable namespace UID.
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

// WithKubeconfig supplies the connection used by pv-migrate's Helm-backed
// backup and restore runners. An empty path keeps the in-cluster client
// behavior used by controller deployments.
func (r *WorkflowReconciler) WithKubeconfig(path, context string) *WorkflowReconciler {
	if r != nil {
		r.kubeconfigPath = strings.TrimSpace(path)
		r.kubeContext = strings.TrimSpace(context)
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
		WithClusterIdentity(r.clusterIdentity).
		WithTrustedToolImage(r.trustedToolImage).
		WithKubeconfig(r.kubeconfigPath, r.kubeContext).
		WithOpenEBSLVMSharedVolumeManager(r.openEBS)
}

func (r *WorkflowReconciler) Reconcile(
	ctx context.Context,
	request reconcile.Request,
) (reconcile.Result, error) {
	return r.reconcile(ctx, request, "")
}

type kindWorkflowReconciler struct {
	parent *WorkflowReconciler
	kind   domain.ControllerKind
}

type kindSessionStore interface {
	GetByKind(
		ctx context.Context,
		namespace string,
		id string,
		kind domain.ControllerKind,
	) (*domain.Session, error)
}

func (r *kindWorkflowReconciler) Reconcile(
	ctx context.Context,
	request reconcile.Request,
) (reconcile.Result, error) {
	if r == nil {
		return reconcile.Result{}, errors.New("workflow reconciler is not configured")
	}

	return r.parent.reconcile(ctx, request, r.kind)
}

func (r *WorkflowReconciler) reconcile(
	ctx context.Context,
	request reconcile.Request,
	kind domain.ControllerKind,
) (reconcile.Result, error) {
	if r == nil || r.store == nil || r.service == nil {
		return reconcile.Result{}, errors.New("workflow reconciler is not configured")
	}

	if r.namespace != "" && request.Namespace != r.namespace {
		return reconcile.Result{}, nil
	}

	var session *domain.Session

	var err error

	if kindStore, ok := r.store.(kindSessionStore); ok && kind != "" {
		session, err = kindStore.GetByKind(ctx, request.Namespace, request.Name, kind)
	} else {
		session, err = r.store.Get(ctx, request.Namespace, request.Name)
	}

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
	// point or backup repository). CRD status updates do not change
	// generation, so this check does not interfere with normal progress.
	if err := workflowSpecMutationError(session); err != nil {
		if !terminalSession(session) {
			runner := r.runner(request.Namespace)
			return reconcile.Result{}, runner.checkpointFailure(ctx, session, err)
		}

		ctrl.LoggerFrom(ctx).
			Error(err, "terminal workflow spec changed; reconciliation stopped", "workflow", request.NamespacedName)

		return reconcile.Result{}, nil
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
	// user-supplied status, and the CLI may still be writing its initial status
	// checkpoint while the create event is delivered. Do not begin business
	// execution until that checkpoint is observed: executing with the create
	// resource version would race the status write and self-fail with an
	// optimistic-lock conflict.
	if session.Status.ObservedGeneration == 0 {
		if err := initializeUnobservedStatus(ctx, r.store, session); err != nil {
			if domain.CategoryOf(err) == domain.ErrorConflict {
				return reconcile.Result{RequeueAfter: 1 * time.Second}, nil
			}

			return reconcile.Result{}, err
		}

		return reconcile.Result{RequeueAfter: r.requeueAfter}, nil
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

func initializeUnobservedStatus(
	ctx context.Context,
	store kube.SessionStore,
	session *domain.Session,
) error {
	if session == nil || session.Status.ObservedGeneration != 0 || store == nil {
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

// SetupWithManager installs one kind-aware controller for every served
// operation-specific workflow resource. A separate reconciler per kind keeps
// same-name resources in different CRDs independent.
func (r *WorkflowReconciler) SetupWithManager(manager ctrl.Manager) error {
	predicates := builder.WithPredicates(predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectNew.GetGeneration() != e.ObjectOld.GetGeneration()
		},
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return true },
	})

	objects := []struct {
		kind    domain.ControllerKind
		object  crclient.Object
		cluster bool
	}{
		{kind: domain.ControllerKindMigration, object: &v1alpha1.Migration{}},
		{
			kind:    domain.ControllerKindClusterMigration,
			object:  &v1alpha1.ClusterMigration{},
			cluster: true,
		},
		{kind: domain.ControllerKindPodMigration, object: &v1alpha1.PodMigration{}},
		{
			kind:    domain.ControllerKindClusterPodMigration,
			object:  &v1alpha1.ClusterPodMigration{},
			cluster: true,
		},
		{kind: domain.ControllerKindReservation, object: &v1alpha1.Reservation{}},
		{
			kind:    domain.ControllerKindClusterReservation,
			object:  &v1alpha1.ClusterReservation{},
			cluster: true,
		},
		{kind: domain.ControllerKindCopy, object: &v1alpha1.Copy{}},
		{kind: domain.ControllerKindClusterCopy, object: &v1alpha1.ClusterCopy{}, cluster: true},
		{kind: domain.ControllerKindBackup, object: &v1alpha1.Backup{}},
		{kind: domain.ControllerKindRestore, object: &v1alpha1.Restore{}},
		{kind: domain.ControllerKindRename, object: &v1alpha1.Rename{}},
		{kind: domain.ControllerKindClusterMove, object: &v1alpha1.ClusterMove{}, cluster: true},
	}
	for _, object := range objects {
		if !r.supportsKind(object.kind) {
			continue
		}

		name := "workflow-" + strings.ToLower(string(object.kind))
		if err := ctrl.NewControllerManagedBy(manager).
			Named(name).
			For(object.object, predicates).
			Complete(&kindWorkflowReconciler{parent: r, kind: object.kind}); err != nil {
			return err
		}
	}

	if len(r.supportedKinds) > 0 {
		served := 0
		for _, object := range objects {
			if r.supportsKind(object.kind) {
				served++
			}
		}

		if served == 0 {
			return errors.New("no workflow CRDs are served by the target cluster")
		}
	} else if len(objects) == 0 {
		return errors.New("no workflow CRDs are served by the target cluster")
	}

	return nil
}

// StartManagerWithKinds creates a manager that watches only the discovered
// workflow kinds, which allows controller mode to run with a partial CRD
// installation while each unsupported operation reports its missing kind.
func StartManagerWithKinds(
	ctx context.Context,
	config *rest.Config,
	service *app.Service,
	store kube.SessionStore,
	namespace string,
	kubeClient kubernetes.Interface,
	openEBSManager kube.OpenEBSLVMSharedVolumeManager,
	kubeconfigPath string,
	kubeContext string,
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

	// controller-runtime emits recorder and leader-election diagnostics through
	// logr. Bridge it to the CLI's structured slog handler so failover does not
	// produce the "log.SetLogger was never called" warning or lose context.
	crlog.SetLogger(logr.FromSlogHandler(slog.Default().Handler()))

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
		// Repository reads use the uncached API reader so deletion, replacement,
		// and credential changes fail closed immediately.
		WithControllerClient(manager.GetAPIReader()).
		WithClusterIdentity(cluster.ID).
		WithTrustedToolImage(normalizedTrustedImage).
		WithKubeconfig(kubeconfigPath, kubeContext).
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
