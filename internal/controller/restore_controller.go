package controller

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/backup"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type RestorePlanner func(context.Context, *v1alpha1.Restore, string) error

type RestoreReconciler struct {
	store          *kube.CRDWorkflowStore[*v1alpha1.Restore]
	client         kubernetes.Interface
	planner        RestorePlanner
	locker         kube.SessionLocker
	config         backup.RestoreExecutorConfig
	checkCollision func(context.Context, string, string) error
	active         *sync.Map
	recorder       events.EventRecorder
}

func (r *RestoreReconciler) executor(namespace string) *backup.RestoreExecutor {
	return backup.NewRestoreExecutor(r.client, r.store, r.locker, namespace, r.config)
}

func (r *RestoreReconciler) Reconcile(
	ctx context.Context,
	request reconcile.Request,
) (reconcile.Result, error) {
	object, err := r.store.Load(ctx, request.NamespacedName)
	if apierrors.IsNotFound(err) {
		return reconcile.Result{}, nil
	}

	if err != nil {
		return reconcile.Result{}, err
	}

	if object.DeletionTimestamp != nil {
		cancelActiveReconcile(r.active, object.UID)
		return workflowReconcileResult(r.executor(object.Namespace).FinalizeDeleted(ctx, object))
	}

	return runActiveReconcile(
		ctx,
		r.active,
		object.UID,
		func(ctx context.Context) (reconcile.Result, error) {
			latest, err := r.store.Load(ctx, request.NamespacedName)
			if apierrors.IsNotFound(err) {
				return reconcile.Result{}, nil
			}

			if err != nil {
				return reconcile.Result{}, err
			}

			if latest.UID != object.UID || latest.DeletionTimestamp != nil {
				return reconcile.Result{RequeueAfter: time.Millisecond}, nil
			}

			object = latest
			if err := r.checkCollision(ctx, object.Namespace, object.Name); err != nil {
				return workflowReconcileResult(err)
			}

			if err := r.store.EnsureProtection(ctx, object); err != nil {
				return workflowReconcileResult(err)
			}

			switch object.Status.Phase {
			case domain.PhaseFailed:
				if object.Status.Plan != nil || !retryCorrectedPlanning(
					object.Status.Phase, object.Status.ResumeFrom,
					object.Status.ObservedGeneration, object.Generation,
				) {
					return reconcile.Result{}, nil
				}
			case domain.PhaseCompleted, domain.PhaseAborted:
				return reconcile.Result{}, nil
			}

			if object.Status.Plan == nil {
				if object.Status.Phase == domain.PhaseAborting {
					return workflowReconcileResult(r.executor(object.Namespace).Abort(ctx, object))
				}

				if err := r.plan(ctx, object); err != nil {
					return workflowReconcileResult(err)
				}

				if object.Status.Phase == domain.PhasePlanned {
					return reconcile.Result{RequeueAfter: time.Millisecond}, nil
				}

				return reconcile.Result{}, nil
			}

			if specErr := verifyAdmittedSpec(
				ctx,
				r.store,
				r.locker,
				object.Namespace,
				object,
				r.recorder,
			); specErr != nil {
				err = specErr
			} else {
				err = r.executor(object.Namespace).Run(ctx, object)
			}

			if object.Status.Phase == domain.PhaseFailed {
				// A durable business failure waits for an explicit resume request.
				latest, readErr := r.store.Load(ctx, request.NamespacedName)
				if readErr == nil && latest.UID == object.UID &&
					latest.ResourceVersion == object.ResourceVersion &&
					latest.Status.Phase == domain.PhaseFailed {
					return reconcile.Result{}, nil
				}
			}

			return workflowReconcileResult(err)
		},
	)
}

func (r *RestoreReconciler) plan(ctx context.Context, object *v1alpha1.Restore) error {
	if r.planner == nil {
		return errors.New("restore planner is required")
	}

	return withPlanningLease(
		ctx,
		r.store,
		r.locker,
		object.Namespace,
		object,
		func(ctx context.Context) error {
			if err := r.checkCollision(ctx, object.Namespace, object.Name); err != nil {
				return err
			}

			before := object.Status.DeepCopy()

			cause := kube.RequireNamespace(ctx, r.client, object.Namespace)
			if cause == nil {
				cause = r.planner(ctx, object, r.config.TrustedToolImage)
			}

			if cause == nil && object.Status.Plan == nil {
				cause = errors.New("restore planner returned no execution plan")
			}

			if cause == nil {
				recordPlanningOutcome(&object.Status.WorkflowStatus, object.Generation, nil)
				cause = r.executor(object.Namespace).Prepare(ctx, object)
			}

			if cause != nil {
				object.Status = *before
			}

			recordPlanningOutcome(&object.Status.WorkflowStatus, object.Generation, cause)

			if err := savePlannedWorkflow(ctx, r.store, object); err != nil {
				object.Status = *before
				return errors.Join(cause, err)
			}

			recordPlanningEvent(r.recorder, object, cause, object.Status.Message)

			return nil
		},
	)
}

func (r *WorkflowReconciler) configureRestoreController(
	client crclient.Client,
	options ManagerOptions,
	locker kube.SessionLocker,
) error {
	if !r.supportsKind(domain.ControllerKindRestore) {
		return nil
	}

	if options.RestorePlanner == nil {
		return errors.New("restore planner is required")
	}

	store, err := kube.NewCRDWorkflowStore(
		client,
		func() *v1alpha1.Restore { return &v1alpha1.Restore{} },
	)
	if err != nil {
		return err
	}

	r.restore = &RestoreReconciler{
		store: store, client: options.KubernetesClient, planner: options.RestorePlanner,
		locker: locker, active: &r.activeWorkflows,
		config: backup.RestoreExecutorConfig{
			TrustedToolImage: r.trustedToolImage,
			Repository: backup.NewS3RepositoryResolver(
				func(ctx context.Context, key crclient.ObjectKey) (*v1alpha1.BackupRepository, error) {
					repository := &v1alpha1.BackupRepository{}
					err := client.Get(ctx, key, repository)
					return repository, err
				},
				options.KubernetesClient,
				nil,
			),
			Tools: backup.ToolRuntime{
				KubeconfigPath: options.KubeconfigPath, KubeContext: options.KubeContext,
				HelmTimeout:     options.TransferExecution.HelmTimeout,
				ToolImageProber: kube.NewToolImageProber(options.KubernetesClient),
				Writer:          io.Discard, Logger: r.logger, StructuredLogs: true,
			},
		},
		checkCollision: func(ctx context.Context, namespace, name string) error {
			return kube.CheckWorkflowIdentityCollision(
				ctx,
				client,
				options.KubernetesClient,
				options.SupportedKinds,
				name,
				domain.ControllerKindRestore,
				[]string{namespace},
				true,
			)
		},
	}

	return nil
}
