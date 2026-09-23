package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// One-shot reconciliation is an administrative command. A broken or
	// permanently unavailable dependency must not keep the command alive
	// forever while the normal manager continues to use controller-runtime's
	// queue backoff.
	maxOneShotReconciliations = 64
	maxOneShotRequeueDelay    = 30 * time.Second
)

// ReconcileWorkflowsOnce routes a single inventory pass through the same
// operation reconcilers as the manager, including deletion and handoff recovery.
func ReconcileWorkflowsOnce(
	ctx context.Context,
	client crclient.Client,
	options ManagerOptions,
) error {
	image, err := normalizeTrustedToolImage(options.TrustedToolImage)
	if err != nil {
		return err
	}

	if client == nil || options.KubernetesClient == nil {
		return errors.New("controller clients are required")
	}

	cluster, err := kube.Identity(ctx, &kube.Clients{Kubernetes: options.KubernetesClient})
	if err != nil {
		return fmt.Errorf("resolve controller cluster identity: %w", err)
	}

	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}

	r := NewWorkflowReconciler().
		WithLogger(logger).
		WithKubernetesClient(options.KubernetesClient).
		WithClusterIdentity(cluster.ID).
		WithTrustedToolImage(image).
		WithSupportedKinds(options.SupportedKinds)

	locker := kube.NewCRDWorkflowLocker(options.KubernetesClient)
	if err := r.configureBackupController(client, options, locker); err != nil {
		return err
	}

	if err := r.configureRestoreController(client, options, locker); err != nil {
		return err
	}

	if err := r.configureIdentityControllers(client, options, locker); err != nil {
		return err
	}

	if err := r.configureTransferControllers(client, options, locker, image, logger); err != nil {
		return err
	}

	return r.reconcileInventory(ctx, client)
}

func (r *WorkflowReconciler) reconcileInventory(ctx context.Context, client crclient.Client) error {
	var failures []error
	for _, workflow := range domain.ControllerWorkflows() {
		for _, kind := range []domain.ControllerKind{workflow.Kind, workflow.ClusterKind} {
			if kind == "" || !r.supportsKind(kind) {
				continue
			}

			if err := r.reconcileKindInventory(ctx, client, kind); err != nil {
				failures = append(failures, fmt.Errorf("%s: %w", kind, err))
			}
		}
	}

	return errors.Join(failures...)
}

func (r *WorkflowReconciler) reconcileKindInventory(
	ctx context.Context,
	client crclient.Client,
	kind domain.ControllerKind,
) error {
	prototype := kube.WorkflowObjectForKind(kind)
	if prototype == nil {
		return fmt.Errorf("workflow %s has no registered API object", kind)
	}
	// Inventory needs only metadata. Concrete decoding happens once at the
	// operation entry; it never reconstructs an intermediate session model.
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind(string(kind) + "List"))

	if err := client.List(ctx, list); err != nil {
		return err
	}

	entry := &kindWorkflowReconciler{parent: r, kind: kind}

	var failures []error
	for index := range list.Items {
		key := crclient.ObjectKeyFromObject(&list.Items[index])
		if err := reconcileUntilStable(
			ctx,
			reconcile.Request{NamespacedName: key},
			entry.Reconcile,
		); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", key, err))
		}

		current := kube.WorkflowObjectForKind(kind)
		if err := client.Get(ctx, key, current); err != nil {
			if !apierrors.IsNotFound(err) {
				failures = append(failures, fmt.Errorf("%s: %w", key, err))
			}
			continue
		}

		status := workflowStatus(current)
		if status.Phase == domain.PhaseFailed {
			failures = append(failures, fmt.Errorf("%s: %s", key, status.Message))
		}
	}

	return errors.Join(failures...)
}

func reconcileUntilStable(
	ctx context.Context,
	request reconcile.Request,
	reconcileOne func(context.Context, reconcile.Request) (reconcile.Result, error),
) error {
	for pass := 1; ; pass++ {
		result, err := reconcileOne(ctx, request)
		if err != nil {
			return err
		}

		needsRequeue := result.RequeueAfter > 0

		if !needsRequeue {
			//nolint:staticcheck // one-shot must honor legacy Requeue results too
			needsRequeue = result.Requeue
		}

		if !needsRequeue {
			return nil
		}

		if pass >= maxOneShotReconciliations {
			return fmt.Errorf(
				"one-shot reconciliation exceeded %d passes without becoming stable",
				maxOneShotReconciliations,
			)
		}

		delay := result.RequeueAfter
		if delay <= 0 {
			delay = time.Millisecond
		}

		if delay > maxOneShotRequeueDelay {
			return fmt.Errorf(
				"one-shot reconciliation requested a requeue delay of %s, exceeding the %s limit",
				delay,
				maxOneShotRequeueDelay,
			)
		}

		// A bounded planning-retry backoff waits on wall-clock progress, not
		// on reconciliation stability: record the state and let the manager's
		// timer drive the retry. Flow-control requeues stay well below this
		// floor, so a one-shot pass still drives them to completion.
		if result.RequeueAfter >= planningRetryFloor {
			return nil
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf("one-shot reconciliation stopped: %w", ctx.Err())
		case <-timer.C:
		}
	}
}
