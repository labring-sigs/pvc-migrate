package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-logr/logr"
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
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

// WorkflowReconciler wires the per-kind reconcilers into one manager: shared
// recorders, queue predicates, and the trusted execution environment. Every
// queue routes to an operation-specific reconciler that operates directly on
// its concrete CRD type.
type WorkflowReconciler struct {
	backup                 *BackupReconciler
	restore                *RestoreReconciler
	rename                 *RenameReconciler
	move                   *MoveReconciler
	reservation            *ClusterReservationReconciler
	namespacedReservation  *ReservationReconciler
	namespacedCopy         *CopyReconciler
	copy                   *ClusterCopyReconciler
	migration              *ClusterMigrationReconciler
	namespacedMigration    *MigrationReconciler
	podMigration           *ClusterPodMigrationReconciler
	namespacedPodMigration *PodMigrationReconciler
	kubeClient             kubernetes.Interface
	clusterIdentity        string
	trustedToolImage       string
	supportedKinds         map[domain.ControllerKind]struct{}
	logger                 *slog.Logger
	activeWorkflows        sync.Map // CR UID -> context.CancelFunc
	recorder               events.EventRecorder
}

func NewWorkflowReconciler() *WorkflowReconciler {
	return &WorkflowReconciler{logger: slog.Default()}
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

	if r.kind == domain.ControllerKindRestore {
		if r.parent.restore == nil {
			return reconcile.Result{}, errors.New("restore reconciler is not configured")
		}
		return r.parent.restore.Reconcile(ctx, request)
	}

	if r.kind == domain.ControllerKindBackup {
		if r.parent.backup == nil {
			return reconcile.Result{}, errors.New("backup reconciler is not configured")
		}
		return r.parent.backup.Reconcile(ctx, request)
	}

	if r.kind == domain.ControllerKindRename {
		if r.parent.rename == nil {
			return reconcile.Result{}, errors.New("rename reconciler is not configured")
		}

		return r.parent.rename.Reconcile(ctx, request)
	}

	if r.kind == domain.ControllerKindMove {
		if r.parent.move == nil {
			return reconcile.Result{}, errors.New("move reconciler is not configured")
		}
		return r.parent.move.Reconcile(ctx, request)
	}

	if r.kind == domain.ControllerKindClusterReservation {
		if r.parent.reservation == nil {
			return reconcile.Result{}, errors.New("reservation reconciler is not configured")
		}
		return r.parent.reservation.Reconcile(ctx, request)
	}

	if r.kind == domain.ControllerKindReservation {
		if r.parent.namespacedReservation == nil {
			return reconcile.Result{}, errors.New(
				"namespaced reservation reconciler is not configured",
			)
		}

		return r.parent.namespacedReservation.Reconcile(ctx, request)
	}

	if r.kind == domain.ControllerKindMigration {
		if r.parent.namespacedMigration == nil {
			return reconcile.Result{}, errors.New(
				"namespaced migration reconciler is not configured",
			)
		}

		return r.parent.namespacedMigration.Reconcile(ctx, request)
	}

	if r.kind == domain.ControllerKindClusterMigration {
		if r.parent.migration == nil {
			return reconcile.Result{}, errors.New("migration reconciler is not configured")
		}
		return r.parent.migration.Reconcile(ctx, request)
	}

	if r.kind == domain.ControllerKindClusterPodMigration {
		if r.parent.podMigration == nil {
			return reconcile.Result{}, errors.New("pod migration reconciler is not configured")
		}
		return r.parent.podMigration.Reconcile(ctx, request)
	}

	if r.kind == domain.ControllerKindPodMigration {
		if r.parent.namespacedPodMigration == nil {
			return reconcile.Result{}, errors.New(
				"namespaced pod migration reconciler is not configured",
			)
		}

		return r.parent.namespacedPodMigration.Reconcile(ctx, request)
	}

	if r.kind == domain.ControllerKindClusterCopy {
		if r.parent.copy == nil {
			return reconcile.Result{}, errors.New("copy reconciler is not configured")
		}
		return r.parent.copy.Reconcile(ctx, request)
	}

	if r.kind == domain.ControllerKindCopy {
		if r.parent.namespacedCopy == nil {
			return reconcile.Result{}, errors.New("namespaced copy reconciler is not configured")
		}
		return r.parent.namespacedCopy.Reconcile(ctx, request)
	}

	return reconcile.Result{}, fmt.Errorf(
		"workflow kind %q has no operation-specific reconciler",
		r.kind,
	)
}

// SetupWithManager installs one kind-aware controller for every served
// operation-specific workflow resource. A separate reconciler per kind keeps
// dispatch unambiguous; the shared collision guard prevents unsafe concurrent
// execution when different Kinds use the same data-plane session identity.
func (r *WorkflowReconciler) SetupWithManager(manager ctrl.Manager) error {
	for _, workflow := range domain.ControllerWorkflows() {
		for _, kind := range []domain.ControllerKind{workflow.Kind, workflow.ClusterKind} {
			if kind == "" || !r.supportsKind(kind) {
				continue
			}

			if err := r.requireReconciler(kind); err != nil {
				return err
			}
		}
	}

	r.recorder = manager.GetEventRecorder("pvc-migrate-controller")
	if r.backup != nil {
		r.backup.recorder = r.recorder
	}

	if r.restore != nil {
		r.restore.recorder = r.recorder
	}

	if r.rename != nil {
		r.rename.recorder = r.recorder
	}

	if r.move != nil {
		r.move.recorder = r.recorder
	}

	if r.reservation != nil {
		r.reservation.recorder = r.recorder
	}

	if r.namespacedReservation != nil {
		r.namespacedReservation.recorder = r.recorder
	}

	if r.namespacedCopy != nil {
		r.namespacedCopy.recorder = r.recorder
	}

	if r.copy != nil {
		r.copy.recorder = r.recorder
	}

	if r.migration != nil {
		r.migration.recorder = r.recorder
	}

	if r.namespacedMigration != nil {
		r.namespacedMigration.recorder = r.recorder
	}

	if r.podMigration != nil {
		r.podMigration.recorder = r.recorder
	}

	if r.namespacedPodMigration != nil {
		r.namespacedPodMigration.recorder = r.recorder
	}

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

		// For() starts its source only after leader election. Register the
		// informer now so standby readiness waits for the same initial snapshot.
		if _, err := manager.GetCache().
			GetInformer(context.Background(), object, cache.BlockUntilSynced(false)); err != nil {
			return fmt.Errorf("register %s informer: %w", kind, err)
		}

		// Interrupted cutovers and deletion must make progress while new
		// transfers run. All queues share the informer and session Lease fence.
		queues := []struct {
			suffix string
			filter predicate.Predicate
		}{
			{filter: workflowQueuePredicate(false, r.cancelWorkflow)},
			{suffix: "-recovery", filter: workflowRecoveryQueuePredicate(r.cancelWorkflow)},
			{suffix: "-deletion", filter: workflowQueuePredicate(true, r.cancelWorkflow)},
		}
		for _, queue := range queues {
			name := "workflow-" + strings.ToLower(string(kind)) + queue.suffix

			if err := ctrl.NewControllerManagedBy(manager).
				Named(name).
				For(object, builder.WithPredicates(queue.filter)).
				Complete(&kindWorkflowReconciler{parent: r, kind: kind}); err != nil {
				return err
			}
		}

		served++
	}

	if served == 0 {
		return errors.New("no workflow CRDs are served by the target cluster")
	}

	return nil
}

func (r *WorkflowReconciler) requireReconciler(kind domain.ControllerKind) error {
	var configured bool
	switch kind {
	case domain.ControllerKindBackup:
		configured = r.backup != nil
	case domain.ControllerKindRestore:
		configured = r.restore != nil
	case domain.ControllerKindRename:
		configured = r.rename != nil
	case domain.ControllerKindMove:
		configured = r.move != nil
	case domain.ControllerKindReservation:
		configured = r.namespacedReservation != nil
	case domain.ControllerKindClusterReservation:
		configured = r.reservation != nil
	case domain.ControllerKindCopy:
		configured = r.namespacedCopy != nil
	case domain.ControllerKindClusterCopy:
		configured = r.copy != nil
	case domain.ControllerKindMigration:
		configured = r.namespacedMigration != nil
	case domain.ControllerKindClusterMigration:
		configured = r.migration != nil
	case domain.ControllerKindPodMigration:
		configured = r.namespacedPodMigration != nil
	case domain.ControllerKindClusterPodMigration:
		configured = r.podMigration != nil
	default:
		return fmt.Errorf("workflow kind %q is not registered", kind)
	}

	if !configured {
		return fmt.Errorf("%s reconciler is required", kind)
	}

	return nil
}

func (r *WorkflowReconciler) cancelWorkflow(object crclient.Object) {
	if cancel, ok := r.activeWorkflows.Load(object.GetUID()); ok {
		if cancelFunc, ok := cancel.(context.CancelFunc); ok {
			cancelFunc()
		}
	}
}

func workflowQueuePredicate(deleting bool, onDelete func(crclient.Object)) predicate.Predicate {
	return predicate.And(
		workflowEventPredicate(onDelete),
		predicate.NewPredicateFuncs(func(object crclient.Object) bool {
			if object.GetDeletionTimestamp() != nil {
				return deleting
			}
			return !deleting && !workflowNeedsRecovery(object)
		}),
	)
}

func workflowRecoveryQueuePredicate(onDelete func(crclient.Object)) predicate.Predicate {
	return predicate.And(
		workflowEventPredicate(onDelete),
		predicate.NewPredicateFuncs(func(object crclient.Object) bool {
			return object.GetDeletionTimestamp() == nil && workflowNeedsRecovery(object)
		}),
	)
}

func workflowNeedsRecovery(object crclient.Object) bool {
	if kube.RequireWorkflowHandoffComplete(object) != nil {
		return true
	}

	status := workflowStatus(object)

	phase := status.Phase
	if phase == domain.PhaseFailed {
		// Failed workflows still require explicit resume. Routing their spec
		// changes here also keeps workload restoration out of the transfer queue.
		phase = status.ResumeFrom
	}

	switch phase {
	case domain.PhasePausing, domain.PhasePaused, domain.PhaseFinalSyncing,
		domain.PhaseFinalSynced, domain.PhaseActivating, domain.PhaseActivated,
		domain.PhaseResuming, domain.PhaseRollingBack, domain.PhaseAborting,
		domain.PhaseRenaming, domain.PhaseMoving:
		return true
	default:
		return false
	}
}

func workflowEventPredicate(onDelete ...func(crclient.Object)) predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectNew.GetDeletionTimestamp() != nil {
				for _, cancel := range onDelete {
					cancel(e.ObjectNew)
				}
			}

			return e.ObjectNew.GetGeneration() != e.ObjectOld.GetGeneration() ||
				kube.WorkflowHandoffChanged(e.ObjectOld, e.ObjectNew) ||
				e.ObjectOld.GetDeletionTimestamp() == nil &&
					e.ObjectNew.GetDeletionTimestamp() != nil ||
				workflowResumeStatusChanged(e.ObjectOld, e.ObjectNew) ||
				workflowExecutionPhaseChanged(e.ObjectOld, e.ObjectNew) ||
				workflowExecutionProgressChanged(e.ObjectOld, e.ObjectNew)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			for _, cancel := range onDelete {
				cancel(e.Object)
			}
			return false
		},
		GenericFunc: func(event.GenericEvent) bool { return true },
	}
}

// workflowExecutionPhaseChanged admits the durable phase checkpoint that
// follows a successful transition. A reconcile can be interrupted after that
// checkpoint and before it returns its explicit requeue; admitting the phase
// event lets the queue recover without treating a durable business failure as
// an implicit resume request.
func workflowExecutionPhaseChanged(oldObject, newObject crclient.Object) bool {
	oldPhase := workflowStatusPhase(oldObject)
	newPhase := workflowStatusPhase(newObject)

	return oldPhase != newPhase && newPhase != domain.PhaseFailed
}

// workflowExecutionProgressChanged admits durable operation-specific
// checkpoints even when the workflow phase is unchanged. Executors save a
// progress checkpoint before returning their explicit requeue; if cancellation
// or process loss happens in that window, the status event is the only recovery
// signal left. Shared WorkflowStatus metadata is intentionally excluded so
// ordinary heartbeat/status bookkeeping cannot create a feedback loop.
func workflowExecutionProgressChanged(oldObject, newObject crclient.Object) bool {
	oldProgress, newProgress := workflowExecutionProgress(
		oldObject,
	), workflowExecutionProgress(
		newObject,
	)
	if oldProgress == nil || newProgress == nil {
		return false
	}

	return !reflect.DeepEqual(oldProgress, newProgress)
}

func workflowExecutionProgress(object crclient.Object) any {
	switch typed := object.(type) {
	case *v1alpha1.Migration:
		status := typed.Status.DeepCopy()
		status.WorkflowStatus = v1alpha1.WorkflowStatus{}
		return status
	case *v1alpha1.PodMigration:
		status := typed.Status.DeepCopy()
		status.WorkflowStatus = v1alpha1.WorkflowStatus{}
		return status
	case *v1alpha1.Reservation:
		status := typed.Status.DeepCopy()
		status.WorkflowStatus = v1alpha1.WorkflowStatus{}
		return status
	case *v1alpha1.Copy:
		status := typed.Status.DeepCopy()
		status.WorkflowStatus = v1alpha1.WorkflowStatus{}
		return status
	case *v1alpha1.Backup:
		status := typed.Status.DeepCopy()
		status.WorkflowStatus = v1alpha1.WorkflowStatus{}
		return status
	case *v1alpha1.Restore:
		status := typed.Status.DeepCopy()
		status.WorkflowStatus = v1alpha1.WorkflowStatus{}
		return status
	case *v1alpha1.Rename:
		status := typed.Status.DeepCopy()
		status.WorkflowStatus = v1alpha1.WorkflowStatus{}
		return status
	case *v1alpha1.ClusterMigration:
		status := typed.Status.DeepCopy()
		status.WorkflowStatus = v1alpha1.WorkflowStatus{}
		return status
	case *v1alpha1.ClusterPodMigration:
		status := typed.Status.DeepCopy()
		status.WorkflowStatus = v1alpha1.WorkflowStatus{}
		return status
	case *v1alpha1.ClusterReservation:
		status := typed.Status.DeepCopy()
		status.WorkflowStatus = v1alpha1.WorkflowStatus{}
		return status
	case *v1alpha1.ClusterCopy:
		status := typed.Status.DeepCopy()
		status.WorkflowStatus = v1alpha1.WorkflowStatus{}
		return status
	case *v1alpha1.Move:
		status := typed.Status.DeepCopy()
		status.WorkflowStatus = v1alpha1.WorkflowStatus{}
		return status
	default:
		return nil
	}
}

// workflowResumeStatusChanged admits explicit resume and repeated Copy passes.
// Ordinary controller checkpoints stay filtered to avoid a feedback loop.
func workflowResumeStatusChanged(oldObject, newObject crclient.Object) bool {
	oldStatus, newStatus := workflowStatus(oldObject), workflowStatus(newObject)

	switch newObject.(type) {
	case *v1alpha1.Copy, *v1alpha1.ClusterCopy:
		if oldStatus.Phase == domain.PhaseWarmCopied && newStatus.Phase == domain.PhaseWarmCopying {
			return true
		}
	}

	resumeFrom := oldStatus.ResumeFrom
	if resumeFrom == "" {
		resumeFrom = domain.PhasePlanned
	}

	return oldStatus.Phase == domain.PhaseFailed &&
		newStatus.Phase == resumeFrom
}

func workflowStatusPhase(object crclient.Object) v1alpha1.WorkflowPhase {
	return workflowStatus(object).Phase
}

func workflowStatus(object crclient.Object) v1alpha1.WorkflowStatus {
	switch typed := object.(type) {
	case *v1alpha1.Migration:
		return typed.Status.WorkflowStatus
	case *v1alpha1.PodMigration:
		return typed.Status.WorkflowStatus
	case *v1alpha1.Reservation:
		return typed.Status.WorkflowStatus
	case *v1alpha1.Copy:
		return typed.Status.WorkflowStatus
	case *v1alpha1.Backup:
		return typed.Status.WorkflowStatus
	case *v1alpha1.Restore:
		return typed.Status.WorkflowStatus
	case *v1alpha1.Rename:
		return typed.Status.WorkflowStatus
	case *v1alpha1.ClusterMigration:
		return typed.Status.WorkflowStatus
	case *v1alpha1.ClusterPodMigration:
		return typed.Status.WorkflowStatus
	case *v1alpha1.ClusterReservation:
		return typed.Status.WorkflowStatus
	case *v1alpha1.ClusterCopy:
		return typed.Status.WorkflowStatus
	case *v1alpha1.Move:
		return typed.Status.WorkflowStatus
	default:
		return v1alpha1.WorkflowStatus{}
	}
}

type ManagerOptions struct {
	BackupPlanner                 BackupPlanner
	RestorePlanner                RestorePlanner
	RenamePlanner                 RenamePlanner
	MovePlanner                   MovePlanner
	ReservationPlanner            ReservationPlanner
	NamespacedReservationPlanner  NamespacedReservationPlanner
	CopyPlanner                   CopyPlanner
	MigrationPlanner              MigrationPlanner
	NamespacedMigrationPlanner    NamespacedMigrationPlanner
	PodMigrationPlanner           PodMigrationPlanner
	NamespacedPodMigrationPlanner NamespacedPodMigrationPlanner
	PodMigrationExecution         app.PodMigrationExecutorConfig
	NamespacedCopyPlanner         NamespacedCopyPlanner
	TransferExecution             app.VolumeCopyConfig
	Namespace                     string
	KubernetesClient              kubernetes.Interface
	OpenEBSLVMSharedVolumeManager kube.OpenEBSLVMSharedVolumeManager
	KubeconfigPath                string
	KubeContext                   string
	WorkloadManager               *Manager
	SupportedKinds                []domain.ControllerKind
	TrustedToolImage              string
	Logger                        *slog.Logger
	HealthProbeBindAddress        string
}

func workflowCacheOptions() cache.Options {
	return cache.Options{
		ReaderFailOnMissingInformer: true,
		DefaultTransform:            cache.TransformStripManagedFields(),
	}
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
		// Watch namespaced and cluster workflows without label selectors:
		// declaratively submitted CRs may have no CLI ownership labels.
		Cache:                   workflowCacheOptions(),
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

	reconciler := NewWorkflowReconciler().
		WithLogger(logger.With("component", "workflow-controller")).
		WithClusterIdentity(cluster.ID).
		WithTrustedToolImage(normalizedTrustedImage).
		WithSupportedKinds(options.SupportedKinds)
	reconciler.WithKubernetesClient(options.KubernetesClient)

	// Uncached reads keep the Lease identity check independent of informer lag.
	directClient, err := crclient.New(config, crclient.Options{Scheme: scheme})
	if err != nil {
		return err
	}

	locker := kube.NewCRDWorkflowLocker(options.KubernetesClient)
	if err := reconciler.configureBackupController(directClient, options, locker); err != nil {
		return err
	}

	if err := reconciler.configureRestoreController(directClient, options, locker); err != nil {
		return err
	}

	if err := reconciler.configureIdentityControllers(directClient, options, locker); err != nil {
		return err
	}

	if err := reconciler.configureTransferControllers(
		directClient,
		options,
		locker,
		normalizedTrustedImage,
		logger,
	); err != nil {
		return err
	}

	if err := reconciler.SetupWithManager(manager); err != nil {
		return err
	}

	return manager.Start(ctx)
}

func (reconciler *WorkflowReconciler) configureIdentityControllers(
	directClient crclient.Client,
	options ManagerOptions,
	locker kube.SessionLocker,
) error {
	if reconciler.supportsKind(domain.ControllerKindRename) {
		if options.RenamePlanner == nil {
			return errors.New("rename planner is required")
		}

		renameStore, err := kube.NewCRDWorkflowStore(
			directClient,
			func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
		)
		if err != nil {
			return err
		}

		reconciler.rename = &RenameReconciler{
			store: renameStore, client: options.KubernetesClient, planner: options.RenamePlanner,
			locker: locker, active: &reconciler.activeWorkflows,
			checkCollision: func(ctx context.Context, namespace, name string) error {
				return kube.CheckWorkflowIdentityCollision(
					ctx,
					directClient,
					options.KubernetesClient,
					options.SupportedKinds,
					name,
					domain.ControllerKindRename,
					[]string{namespace},
					true,
				)
			},
		}
	}

	if reconciler.supportsKind(domain.ControllerKindMove) {
		if options.MovePlanner == nil {
			return errors.New("move planner is required")
		}

		moveStore, err := kube.NewCRDWorkflowStore(
			directClient,
			func() *v1alpha1.Move { return &v1alpha1.Move{} },
		)
		if err != nil {
			return err
		}

		reconciler.move = &MoveReconciler{
			store: moveStore, client: options.KubernetesClient, planner: options.MovePlanner,
			locker: locker, active: &reconciler.activeWorkflows,
			checkCollision: func(ctx context.Context, name string, namespaces []string) error {
				return kube.CheckWorkflowIdentityCollision(
					ctx,
					directClient,
					options.KubernetesClient,
					options.SupportedKinds,
					name,
					domain.ControllerKindMove,
					namespaces,
					true,
				)
			},
		}
	}

	return nil
}

func (r *WorkflowReconciler) configureTransferControllers(
	client crclient.Client,
	options ManagerOptions,
	locker kube.SessionLocker,
	image string,
	logger *slog.Logger,
) error {
	transfer := options.TransferExecution
	transfer.KubeconfigPath = options.KubeconfigPath
	transfer.Context = options.KubeContext
	transfer.TrustedToolImage = image
	transfer.Logger = logger
	transfer.StructuredLogs = true
	transfer.StreamToolLogs = false
	transfer.Writer = nil

	copyConfig := app.CopyExecutorConfig{
		Transfer:            transfer,
		ToolImageProber:     kube.NewToolImageProber(options.KubernetesClient),
		SharedVolumeManager: options.OpenEBSLVMSharedVolumeManager,
	}

	engine := copyengine.NewPVMigrate()
	if r.supportsKind(domain.ControllerKindMigration) {
		if options.NamespacedMigrationPlanner == nil {
			return errors.New("namespaced migration planner is required")
		}

		store, err := kube.NewCRDWorkflowStore(
			client,
			func() *v1alpha1.Migration { return &v1alpha1.Migration{} },
		)
		if err != nil {
			return err
		}

		r.namespacedMigration = &MigrationReconciler{
			store:   store,
			client:  options.KubernetesClient,
			planner: options.NamespacedMigrationPlanner,
			locker:  locker,
			active:  &r.activeWorkflows,
			engine:  engine,
			config: app.MigrationExecutorConfig{
				Transfer:        transfer,
				ToolImageProber: kube.NewToolImageProber(options.KubernetesClient),
			},
			checkCollision: func(ctx context.Context, name string, namespaces []string) error {
				return kube.CheckWorkflowIdentityCollision(
					ctx,
					client,
					options.KubernetesClient,
					options.SupportedKinds,
					name,
					domain.ControllerKindMigration,
					namespaces,
					true,
				)
			},
		}
	}

	if r.supportsKind(domain.ControllerKindClusterMigration) {
		if options.MigrationPlanner == nil {
			return errors.New("migration planner is required")
		}

		store, err := kube.NewCRDWorkflowStore(
			client,
			func() *v1alpha1.ClusterMigration { return &v1alpha1.ClusterMigration{} },
		)
		if err != nil {
			return err
		}

		r.migration = &ClusterMigrationReconciler{
			store:   store,
			client:  options.KubernetesClient,
			planner: options.MigrationPlanner,
			locker:  locker,
			active:  &r.activeWorkflows,
			engine:  engine,
			config: app.MigrationExecutorConfig{
				Transfer:        transfer,
				ToolImageProber: kube.NewToolImageProber(options.KubernetesClient),
			},
			checkCollision: func(ctx context.Context, name string, namespaces []string) error {
				return kube.CheckWorkflowIdentityCollision(
					ctx,
					client,
					options.KubernetesClient,
					options.SupportedKinds,
					name,
					domain.ControllerKindClusterMigration,
					namespaces,
					true,
				)
			},
		}
	}

	podConfig := app.PodMigrationExecutorConfig{
		Storage: app.MigrationExecutorConfig{
			Transfer:        transfer,
			ToolImageProber: kube.NewToolImageProber(options.KubernetesClient),
			ProbeTimeout:    transfer.HelmTimeout,
		},
		SharedVolumes: options.OpenEBSLVMSharedVolumeManager,
		Workloads:     options.WorkloadManager,
	}

	if r.supportsKind(domain.ControllerKindClusterPodMigration) {
		if options.PodMigrationPlanner == nil {
			return errors.New("pod migration planner is required")
		}

		store, err := kube.NewCRDWorkflowStore(
			client,
			func() *v1alpha1.ClusterPodMigration { return &v1alpha1.ClusterPodMigration{} },
		)
		if err != nil {
			return err
		}

		r.podMigration = &ClusterPodMigrationReconciler{
			store:   store,
			client:  options.KubernetesClient,
			planner: options.PodMigrationPlanner,
			locker:  locker,
			active:  &r.activeWorkflows,
			engine:  engine,
			config:  podConfig,
			checkCollision: func(ctx context.Context, name string, namespaces []string) error {
				return kube.CheckWorkflowIdentityCollision(
					ctx,
					client,
					options.KubernetesClient,
					options.SupportedKinds,
					name,
					domain.ControllerKindClusterPodMigration,
					namespaces,
					true,
				)
			},
		}
	}

	if r.supportsKind(domain.ControllerKindPodMigration) {
		if options.NamespacedPodMigrationPlanner == nil {
			return errors.New("namespaced pod migration planner is required")
		}

		store, err := kube.NewCRDWorkflowStore(
			client,
			func() *v1alpha1.PodMigration { return &v1alpha1.PodMigration{} },
		)
		if err != nil {
			return err
		}

		r.namespacedPodMigration = &PodMigrationReconciler{
			store:   store,
			client:  options.KubernetesClient,
			planner: options.NamespacedPodMigrationPlanner,
			locker:  locker,
			active:  &r.activeWorkflows,
			engine:  engine,
			config:  podConfig,
			checkCollision: func(ctx context.Context, name string, namespaces []string) error {
				return kube.CheckWorkflowIdentityCollision(
					ctx,
					client,
					options.KubernetesClient,
					options.SupportedKinds,
					name,
					domain.ControllerKindPodMigration,
					namespaces,
					true,
				)
			},
		}
	}

	if r.supportsKind(domain.ControllerKindReservation) {
		if options.NamespacedReservationPlanner == nil {
			return errors.New("namespaced reservation planner is required")
		}

		store, err := kube.NewCRDWorkflowStore(
			client,
			func() *v1alpha1.Reservation { return &v1alpha1.Reservation{} },
		)
		if err != nil {
			return err
		}

		r.namespacedReservation = &ReservationReconciler{
			store:   store,
			client:  options.KubernetesClient,
			planner: options.NamespacedReservationPlanner,
			locker:  locker,
			active:  &r.activeWorkflows,
			config: app.ReservationExecutorConfig{
				TrustedToolImage: image,
				Logger:           logger,
				ToolImageProber:  copyConfig.ToolImageProber,
			},
			checkCollision: func(ctx context.Context, name string, namespaces []string) error {
				return kube.CheckWorkflowIdentityCollision(
					ctx,
					client,
					options.KubernetesClient,
					options.SupportedKinds,
					name,
					domain.ControllerKindReservation,
					namespaces,
					true,
				)
			},
		}
	}

	if r.supportsKind(domain.ControllerKindClusterReservation) {
		if options.ReservationPlanner == nil {
			return errors.New("reservation planner is required")
		}

		store, err := kube.NewCRDWorkflowStore(
			client,
			func() *v1alpha1.ClusterReservation { return &v1alpha1.ClusterReservation{} },
		)
		if err != nil {
			return err
		}

		r.reservation = &ClusterReservationReconciler{
			store:   store,
			client:  options.KubernetesClient,
			planner: options.ReservationPlanner,
			locker:  locker,
			active:  &r.activeWorkflows,
			config: app.ReservationExecutorConfig{
				TrustedToolImage: image,
				Logger:           logger,
				ToolImageProber:  copyConfig.ToolImageProber,
			},
			checkCollision: func(ctx context.Context, name string, namespaces []string) error {
				return kube.CheckWorkflowIdentityCollision(
					ctx,
					client,
					options.KubernetesClient,
					options.SupportedKinds,
					name,
					domain.ControllerKindClusterReservation,
					namespaces,
					true,
				)
			},
		}
	}

	if r.supportsKind(domain.ControllerKindCopy) {
		if options.NamespacedCopyPlanner == nil {
			return errors.New("namespaced copy planner is required")
		}

		store, err := kube.NewCRDWorkflowStore(
			client,
			func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
		)
		if err != nil {
			return err
		}

		r.namespacedCopy = &CopyReconciler{
			store: store, client: options.KubernetesClient, planner: options.NamespacedCopyPlanner,
			locker: locker, active: &r.activeWorkflows, engine: engine, config: copyConfig,
			checkCollision: func(ctx context.Context, name string, namespaces []string) error {
				return kube.CheckWorkflowIdentityCollision(
					ctx,
					client,
					options.KubernetesClient,
					options.SupportedKinds,
					name,
					domain.ControllerKindCopy,
					namespaces,
					true,
				)
			},
		}
	}

	if r.supportsKind(domain.ControllerKindClusterCopy) {
		if options.CopyPlanner == nil {
			return errors.New("copy planner is required")
		}

		store, err := kube.NewCRDWorkflowStore(
			client,
			func() *v1alpha1.ClusterCopy { return &v1alpha1.ClusterCopy{} },
		)
		if err != nil {
			return err
		}

		r.copy = &ClusterCopyReconciler{
			store: store, client: options.KubernetesClient, planner: options.CopyPlanner,
			locker: locker, active: &r.activeWorkflows, engine: engine, config: copyConfig,
			checkCollision: func(ctx context.Context, name string, namespaces []string) error {
				return kube.CheckWorkflowIdentityCollision(
					ctx,
					client,
					options.KubernetesClient,
					options.SupportedKinds,
					name,
					domain.ControllerKindClusterCopy,
					namespaces,
					true,
				)
			},
		}
	}

	if r.namespacedReservation != nil && r.namespacedCopy != nil {
		handoff := &namespacedReservationCopyRecovery{
			client: client, kubernetes: options.KubernetesClient, locker: locker,
			reservations: r.namespacedReservation.store, copies: r.namespacedCopy.store,
			engine: engine, copyConfig: copyConfig,
		}
		r.namespacedReservation.handoff = handoff
		r.namespacedCopy.handoff = handoff
	}

	if r.reservation != nil && r.copy != nil {
		handoff := &reservationCopyRecovery{
			client:       client,
			kubernetes:   options.KubernetesClient,
			locker:       locker,
			reservations: r.reservation.store,
			copies:       r.copy.store,
			engine:       engine,
			copyConfig:   copyConfig,
		}
		r.reservation.handoff = handoff
		r.copy.handoff = handoff
	}

	return nil
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

func workflowSpecMutationError(
	observedHash, currentHash string,
	generation, observedGeneration int64,
	deleting bool,
	conditions []v1alpha1.WorkflowCondition,
) error {
	if observedHash != "" {
		if observedHash == currentHash {
			return nil
		}
		return changedWorkflowDefinitionError()
	}

	if observedGeneration == 0 || generation == observedGeneration {
		return nil
	}
	// The API server increments generation when it first sets deletionTimestamp,
	// even though spec is unchanged. Accept that single increment only until a
	// deletion checkpoint has been observed; later spec changes remain fenced.
	if deleting && generation == observedGeneration+1 {
		observedDeletion := false
		for _, condition := range conditions {
			if condition.Type == "Deleting" || condition.Type == "DeletionBlocked" {
				observedDeletion = true
				break
			}
		}

		if !observedDeletion {
			return nil
		}
	}

	return changedWorkflowDefinitionError()
}

func changedWorkflowDefinitionError() error {
	return domain.NewError(
		domain.ErrorConflict,
		"controller reconcile",
		"workflow spec changed after execution started; create a new workflow instead",
	)
}
