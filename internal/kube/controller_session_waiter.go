package kube

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

const controllerWatchReconnectDelay = 200 * time.Millisecond

// ControllerSessionWaiter follows one workflow resource until the callback
// reports completion. It re-establishes watches from a fresh GET so status
// changes are not lost when an API server closes or expires a watch.
type ControllerSessionWaiter struct {
	client dynamic.Interface
}

func NewControllerSessionWaiter(client dynamic.Interface) *ControllerSessionWaiter {
	return &ControllerSessionWaiter{client: client}
}

func (w *ControllerSessionWaiter) Wait(
	ctx context.Context,
	initial *domain.Session,
	onUpdate func(*domain.Session) (bool, error),
) (*domain.Session, error) {
	if onUpdate == nil {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"wait for controller workflow",
			"update callback is required",
		)
	}

	resource, kind, err := w.resourceFor(initial)
	if err != nil {
		return nil, err
	}

	var uid types.UID
	for {
		current, getErr := resource.Get(ctx, initial.ID, metav1.GetOptions{})
		if getErr != nil {
			if apierrors.IsNotFound(getErr) {
				return nil, deletedControllerWorkflowError(initial, kind)
			}

			return nil, controllerWaitError(ctx, "read workflow resource", getErr)
		}

		if uid == "" {
			uid = current.GetUID()
		} else if current.GetUID() != uid {
			return nil, replacedControllerWorkflowError(initial, kind)
		}

		session, decodeErr := decodeControllerWatchObject(current, kind)
		if decodeErr != nil {
			return nil, decodeErr
		}

		done, updateErr := onUpdate(session)
		if updateErr != nil || done {
			return session, updateErr
		}

		workflowWatch, watchErr := resource.Watch(ctx, metav1.ListOptions{
			FieldSelector:       fields.OneTermEqualSelector("metadata.name", initial.ID).String(),
			ResourceVersion:     current.GetResourceVersion(),
			AllowWatchBookmarks: true,
		})
		if watchErr != nil {
			if apierrors.IsResourceExpired(watchErr) {
				continue
			}

			return nil, controllerWaitError(ctx, "watch workflow resource", watchErr)
		}

		result, restart, consumeErr := consumeControllerWatch(
			ctx,
			workflowWatch,
			initial,
			kind,
			uid,
			onUpdate,
		)
		workflowWatch.Stop()

		if consumeErr != nil || result != nil {
			return result, consumeErr
		}

		if !restart || !waitControllerWatchReconnect(ctx) {
			return nil, controllerWaitError(ctx, "watch workflow resource", ctx.Err())
		}
	}
}

func (w *ControllerSessionWaiter) resourceFor(
	session *domain.Session,
) (dynamic.ResourceInterface, domain.ControllerKind, error) {
	if w == nil || w.client == nil {
		return nil, "", domain.NewError(
			domain.ErrorKubernetes,
			"wait for controller workflow",
			"dynamic Kubernetes client is not configured",
		)
	}

	if session == nil || session.ID == "" || session.Spec.SessionNamespace == "" {
		return nil, "", domain.NewError(
			domain.ErrorValidation,
			"wait for controller workflow",
			"workflow session identity is required",
		)
	}

	workflow, ok := domain.ControllerWorkflowForType(session.Spec.Type)
	if !ok {
		return nil, "", domain.NewError(
			domain.ErrorValidation,
			"wait for controller workflow",
			fmt.Sprintf("unsupported controller workflow type %q", session.Spec.Type),
		)
	}

	groupVersion, err := schema.ParseGroupVersion(domain.SessionAPIVersion)
	if err != nil {
		return nil, "", domain.WrapError(
			domain.ErrorInternal,
			"wait for controller workflow",
			"parse workflow API version",
			err,
		)
	}

	gvr := groupVersion.WithResource(workflow.Resource)

	return w.client.Resource(gvr).Namespace(session.Spec.SessionNamespace), workflow.Kind, nil
}

func consumeControllerWatch(
	ctx context.Context,
	workflowWatch watch.Interface,
	initial *domain.Session,
	kind domain.ControllerKind,
	uid types.UID,
	onUpdate func(*domain.Session) (bool, error),
) (*domain.Session, bool, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, false, controllerWaitError(ctx, "watch workflow resource", ctx.Err())
		case event, open := <-workflowWatch.ResultChan():
			if !open {
				return nil, true, nil
			}

			result, done, restart, err := handleControllerWatchEvent(
				event, initial, kind, uid, onUpdate,
			)
			if err != nil || done || restart {
				return result, restart, err
			}
		}
	}
}

func handleControllerWatchEvent(
	event watch.Event,
	initial *domain.Session,
	kind domain.ControllerKind,
	uid types.UID,
	onUpdate func(*domain.Session) (bool, error),
) (*domain.Session, bool, bool, error) {
	if event.Type == watch.Error {
		err := apierrors.FromObject(event.Object)
		if apierrors.IsResourceExpired(err) {
			return nil, false, true, nil
		}

		return nil, false, false, controllerWaitError(
			context.Background(), "watch workflow resource", err,
		)
	}

	object, ok := event.Object.(*unstructured.Unstructured)
	if !ok || object.GetName() != initial.ID {
		return nil, false, false, nil
	}

	if uid != "" && object.GetUID() != uid {
		return nil, false, false, replacedControllerWorkflowError(initial, kind)
	}

	if event.Type == watch.Deleted {
		return nil, false, false, deletedControllerWorkflowError(initial, kind)
	}

	if event.Type != watch.Added && event.Type != watch.Modified {
		return nil, false, false, nil
	}

	session, err := decodeControllerWatchObject(object, kind)
	if err != nil {
		return nil, false, false, err
	}

	done, err := onUpdate(session)

	return session, done, false, err
}

func decodeControllerWatchObject(
	object *unstructured.Unstructured,
	kind domain.ControllerKind,
) (*domain.Session, error) {
	typed := newWorkflowObject(kind)
	if typed == nil {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"wait for controller workflow",
			"unsupported workflow resource kind "+string(kind),
		)
	}

	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
		object.Object,
		typed,
	); err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"wait for controller workflow",
			"decode "+string(kind),
			err,
		)
	}

	return DecodeWorkflow(typed)
}

func replacedControllerWorkflowError(session *domain.Session, kind domain.ControllerKind) error {
	return domain.NewError(
		domain.ErrorConflict,
		"wait for controller workflow",
		fmt.Sprintf(
			"%s %s/%s was replaced while waiting",
			kind,
			session.Spec.SessionNamespace,
			session.ID,
		),
	)
}

func deletedControllerWorkflowError(session *domain.Session, kind domain.ControllerKind) error {
	return domain.NewError(
		domain.ErrorConflict,
		"wait for controller workflow",
		fmt.Sprintf(
			"%s %s/%s was deleted before completion",
			kind,
			session.Spec.SessionNamespace,
			session.ID,
		),
	)
}

func controllerWaitError(ctx context.Context, action string, err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return domain.WrapError(
			domain.ErrorTimeout,
			"wait for controller workflow",
			"operation timed out while attempting to "+action,
			context.DeadlineExceeded,
		)
	}

	if err == nil {
		err = context.Canceled
	}

	return domain.WrapError(
		domain.ErrorKubernetes,
		"wait for controller workflow",
		"failed to "+action,
		err,
	)
}

func waitControllerWatchReconnect(ctx context.Context) bool {
	timer := time.NewTimer(controllerWatchReconnectDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
