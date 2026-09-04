package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	crmanager "sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// WorkflowReconciler bridges every operation-specific workflow Kind to the
// existing service state machine.
type WorkflowReconciler struct {
	store            kube.ControllerSessionStore
	service          workflowResumer
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
	logger           *slog.Logger
}

func NewWorkflowReconciler(
	service workflowResumer,
	store kube.ControllerSessionStore,
) *WorkflowReconciler {
	return &WorkflowReconciler{
		service:      service,
		store:        store,
		requeueAfter: 5 * time.Second,
		logger:       slog.Default(),
	}
}

// WithLogger supplies the structured logger owned by the controller process.
// Reconciliation must not fall back to the process-global slog default, which
// is commonly discarded or configured for a different CLI command.
func (r *WorkflowReconciler) WithLogger(logger *slog.Logger) *WorkflowReconciler {
	if r != nil && logger != nil {
		r.logger = logger
	}

	return r
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
		WithLogger(r.logger).
		WithKubernetesClient(r.kubeClient).
		WithControllerClient(r.controllerClient).
		WithClusterIdentity(r.clusterIdentity).
		WithTrustedToolImage(r.trustedToolImage).
		WithKubeconfig(r.kubeconfigPath, r.kubeContext).
		WithOpenEBSLVMSharedVolumeManager(r.openEBS)
}

type kindWorkflowReconciler struct {
	parent *WorkflowReconciler
	kind   domain.ControllerKind
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

	if kind == "" {
		return reconcile.Result{}, errors.New("workflow kind is required")
	}

	if r.namespace != "" && request.Namespace != r.namespace {
		return reconcile.Result{}, nil
	}

	session, err := r.store.GetByKind(
		ctx,
		request.Namespace,
		request.Name,
		kind,
	)
	if err != nil {
		// The SessionStore classifies a missing CRD object as validation; the
		// controller should treat that as a normal delete event as well.
		if kube.IsSessionNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	if session.Deleting {
		return reconcile.Result{}, r.reconcileDeletingWorkflow(ctx, request, session, kind)
	}

	if err := r.store.CheckWorkflowNameCollision(ctx, session); err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			ctrl.LoggerFrom(ctx).Info(
				"workflow is waiting for a same-name resource conflict to be removed",
				"workflow",
				request.NamespacedName,
				"error",
				err,
			)

			return reconcile.Result{RequeueAfter: r.requeueAfter}, nil
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
			return r.checkpointBusinessFailure(ctx, runner, session, err, request)
		}

		ctrl.LoggerFrom(ctx).
			Info("ignored spec change for terminal workflow", "workflow", request.NamespacedName, "reason", err)

		return reconcile.Result{}, nil
	}

	// Declarative CRs do not pass through CRDSessionStore.Create, so they may
	// arrive without the session-protection finalizer. Add it before any
	// execution or terminal-state handling to preserve the explicit cleanup
	// contract for every controller-backed workflow.
	if err := r.store.EnsureSessionProtection(ctx, session); err != nil {
		return reconcile.Result{}, err
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
		return r.checkpointBusinessFailure(ctx, runner, session, boundaryErr, request)
	}

	if err := r.validateDeclarativeSourceVolumes(ctx, session); err != nil {
		runner := r.runner(request.Namespace)
		return r.checkpointBusinessFailure(ctx, runner, session, err, request)
	}

	if err := r.ensureStandalonePodSnapshot(ctx, session); err != nil {
		runner := r.runner(request.Namespace)
		return r.checkpointBusinessFailure(ctx, runner, session, err, request)
	}

	runner := r.runner(request.Namespace)
	if err := runner.reconcileSession(ctx, session); err != nil {
		return r.checkpointBusinessFailure(ctx, runner, session, err, request)
	}

	return reconcile.Result{RequeueAfter: r.requeueAfter}, nil
}

func (r *WorkflowReconciler) checkpointBusinessFailure(
	ctx context.Context,
	runner *Runner,
	session *domain.Session,
	cause error,
	request reconcile.Request,
) (reconcile.Result, error) {
	if err := runner.checkpointFailureForController(ctx, session, cause); err != nil {
		return reconcile.Result{}, err
	}

	r.logger.Warn(
		"workflow entered failed state",
		"workflow",
		request.NamespacedName,
		"phase",
		domain.PhaseFailed,
		"reason",
		cause,
	)

	return reconcile.Result{}, nil
}

func (r *WorkflowReconciler) reconcileDeletingWorkflow(
	ctx context.Context,
	request reconcile.Request,
	session *domain.Session,
	kind domain.ControllerKind,
) error {
	resourceKind := session.BackendResource
	if resourceKind == "" {
		resourceKind = kind
	}

	// Direct deletion of a workflow keeps the explicit cleanup contract. A
	// namespaced workflow must not hold its namespace in Terminating forever;
	// Kubernetes will remove namespaced data-plane resources as part of that
	// deletion, and orphan recovery remains available for cluster-scoped PVs.
	if request.Namespace == "" || domain.IsClusterControllerKind(resourceKind) {
		return nil
	}

	if r.kubeClient == nil {
		return errors.New("controller Kubernetes client is required to reconcile workflow deletion")
	}

	namespace, err := r.kubeClient.CoreV1().Namespaces().Get(
		ctx,
		request.Namespace,
		metav1.GetOptions{},
	)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if err == nil && namespace.DeletionTimestamp == nil {
		return nil
	}

	ctrl.LoggerFrom(ctx).Info(
		"releasing workflow protection for terminating namespace",
		"workflow",
		request.NamespacedName,
	)

	return r.store.Delete(ctx, session)
}

func initializeUnobservedStatus(
	ctx context.Context,
	store kube.ControllerSessionStore,
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
// dispatch unambiguous; the shared collision guard prevents unsafe concurrent
// execution when different Kinds use the same data-plane session identity.
func (r *WorkflowReconciler) SetupWithManager(manager ctrl.Manager) error {
	predicates := builder.WithPredicates(workflowEventPredicate())

	kinds := make([]domain.ControllerKind, 0, len(domain.ControllerWorkflows())*2)
	for _, workflow := range domain.ControllerWorkflows() {
		if workflow.Kind != "" {
			kinds = append(kinds, workflow.Kind)
		}

		if workflow.ClusterKind != "" {
			kinds = append(kinds, workflow.ClusterKind)
		}
	}

	served := 0
	for _, kind := range kinds {
		if !r.supportsKind(kind) {
			continue
		}

		object := kube.WorkflowObjectForKind(kind)
		if object == nil {
			return fmt.Errorf("workflow %s has no registered API object", kind)
		}

		name := "workflow-" + strings.ToLower(string(kind))
		if err := ctrl.NewControllerManagedBy(manager).
			Named(name).
			For(object, predicates).
			Complete(&kindWorkflowReconciler{parent: r, kind: kind}); err != nil {
			return err
		}

		served++
	}

	if served == 0 {
		return errors.New("no workflow CRDs are served by the target cluster")
	}

	return nil
}

func workflowEventPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectNew.GetGeneration() != e.ObjectOld.GetGeneration() ||
				e.ObjectOld.GetDeletionTimestamp() == nil &&
					e.ObjectNew.GetDeletionTimestamp() != nil
		},
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return true },
	}
}

type ManagerOptions struct {
	Namespace                     string
	KubernetesClient              kubernetes.Interface
	OpenEBSLVMSharedVolumeManager kube.OpenEBSLVMSharedVolumeManager
	KubeconfigPath                string
	KubeContext                   string
	SupportedKinds                []domain.ControllerKind
	TrustedToolImage              string
	Logger                        *slog.Logger
	HealthProbeBindAddress        string
}

// cacheReadiness runs outside leader election. Controller-runtime starts
// non-leader runnables only after its cache has synchronized, so standby Pods
// can become Ready during a rolling update without claiming the leader Lease.
type cacheReadiness struct {
	ready atomic.Bool
}

func (r *cacheReadiness) Start(ctx context.Context) error {
	r.ready.Store(true)
	defer r.ready.Store(false)

	<-ctx.Done()

	return nil
}

func (*cacheReadiness) NeedLeaderElection() bool {
	return false
}

func (r *cacheReadiness) Check(*http.Request) error {
	if r.ready.Load() {
		return nil
	}

	return errors.New("controller cache has not synced")
}

// StartManager creates a manager that watches only the discovered workflow
// kinds. Partial CRD installations remain supported while missing operation
// kinds are reported explicitly at submission time.
func StartManager(
	ctx context.Context,
	config *rest.Config,
	service *app.Service,
	store kube.ControllerSessionStore,
	options ManagerOptions,
) error {
	if config == nil {
		return errors.New("kubernetes REST config is required")
	}

	if options.Namespace == "" {
		return errors.New("controller namespace is required")
	}

	// Controller-created transfer Pods may receive PVC mounts, static object
	// store credentials, or workload identity. Require an administrator-pinned
	// image before constructing the manager so an embedded caller cannot
	// accidentally delegate image selection to a tenant workflow.
	normalizedTrustedImage, err := normalizeTrustedToolImage(options.TrustedToolImage)
	if err != nil {
		return err
	}

	if options.KubernetesClient == nil {
		return errors.New("controller Kubernetes client is required")
	}

	cluster, err := kube.Identity(
		ctx,
		&kube.Clients{Kubernetes: options.KubernetesClient},
	)
	if err != nil {
		return fmt.Errorf("resolve controller cluster identity: %w", err)
	}

	healthProbeBindAddress := strings.TrimSpace(options.HealthProbeBindAddress)
	if healthProbeBindAddress == "" {
		healthProbeBindAddress = ":8081"
	}

	// controller-runtime emits recorder and leader-election diagnostics through
	// logr. Bridge it to the controller-owned structured logger so failover does
	// not produce the "log.SetLogger was never called" warning or lose context.
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}

	logger = NewControllerLogger(logger)
	crlog.SetLogger(logr.FromSlogHandler(logger.Handler()))

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
		LeaderElectionNamespace: options.Namespace,
		HealthProbeBindAddress:  healthProbeBindAddress,
		Metrics:                 metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		return err
	}

	if err := manager.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}

	cacheReady := &cacheReadiness{}
	if err := manager.Add(cacheReady); err != nil {
		return err
	}

	if err := manager.AddReadyzCheck("cache-sync", cacheReady.Check); err != nil {
		return err
	}

	reconciler := NewWorkflowReconciler(service, store).
		WithLogger(logger.With("component", "workflow-controller")).
		// Repository reads use the uncached API reader so deletion, replacement,
		// and credential changes fail closed immediately.
		WithControllerClient(manager.GetAPIReader()).
		WithClusterIdentity(cluster.ID).
		WithTrustedToolImage(normalizedTrustedImage).
		WithKubeconfig(options.KubeconfigPath, options.KubeContext).
		WithSupportedKinds(options.SupportedKinds)
	reconciler.WithKubernetesClient(options.KubernetesClient).
		WithOpenEBSLVMSharedVolumeManager(options.OpenEBSLVMSharedVolumeManager)

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

var (
	_ reconcile.Reconciler             = (*kindWorkflowReconciler)(nil)
	_ crmanager.LeaderElectionRunnable = (*cacheReadiness)(nil)
)

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
