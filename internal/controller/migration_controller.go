package controller

import (
	"context"
	"errors"
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
	store          kube.SessionStore
	service        *app.Service
	kubeClient     kubernetes.Interface
	openEBS        kube.OpenEBSLVMSharedVolumeManager
	namespace      string
	requeueAfter   time.Duration
	supportedKinds map[domain.ControllerKind]struct{}
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

// WithNamespace limits reconciliation to the control namespace selected by
// the deployment. The CRD remains namespaced while the controller avoids
// accidentally adopting resources from unrelated namespaces.
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

func (r *WorkflowReconciler) WithOpenEBSLVMSharedVolumeManager(
	manager kube.OpenEBSLVMSharedVolumeManager,
) *WorkflowReconciler {
	if r != nil {
		r.openEBS = manager
	}
	return r
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

	if terminalSession(session) {
		return reconcile.Result{}, nil
	}

	runner := NewRunner(r.service, r.store, request.Namespace).
		WithKubernetesClient(r.kubeClient).
		WithOpenEBSLVMSharedVolumeManager(r.openEBS)
	if err := runner.reconcileSession(ctx, session); err != nil {
		err = runner.checkpointFailure(ctx, session, err)

		return reconcile.Result{}, err
	}

	return reconcile.Result{RequeueAfter: r.requeueAfter}, nil
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
) error {
	return StartManagerWithKinds(
		ctx, config, service, store, namespace, kubeClient, openEBSManager, nil,
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
) error {
	if config == nil {
		return errors.New("kubernetes REST config is required")
	}

	if namespace == "" {
		return errors.New("controller namespace is required")
	}

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return err
	}

	manager, err := ctrl.NewManager(config, ctrl.Options{
		Scheme: scheme,
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{namespace: {}},
		},
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
		WithNamespace(namespace).
		WithSupportedKinds(supportedKinds)
	reconciler.WithKubernetesClient(kubeClient).
		WithOpenEBSLVMSharedVolumeManager(openEBSManager)

	if err := reconciler.SetupWithManager(manager); err != nil {
		return err
	}

	return manager.Start(ctx)
}

var _ reconcile.Reconciler = (*WorkflowReconciler)(nil)
