package kube

import (
	"context"
	"errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// WaitForWorkflow follows the exact object created by the caller. Reconnecting
// starts with a fresh GET and resourceVersion, while the original UID stays
// pinned across every reconnect. Decoding occurs only at this storage boundary.
func WaitForWorkflow[T crclient.Object](
	ctx context.Context,
	resource dynamic.ResourceInterface,
	initial T,
	newObject func() T,
	onUpdate func(T) (bool, error),
) (T, error) {
	var zero T
	if resource == nil || newObject == nil || onUpdate == nil {
		return zero, errors.New(
			"workflow watch requires a resource, object factory and update callback",
		)
	}

	gvk, err := workflowStoreKind(initial)
	if err != nil {
		return zero, err
	}

	if err := requireWorkflowStorageVersion(initial); err != nil {
		return zero, err
	}

	decode := func(raw *unstructured.Unstructured) (T, error) {
		if raw.GetUID() != initial.GetUID() || raw.GetName() != initial.GetName() ||
			raw.GetNamespace() != initial.GetNamespace() {
			return zero, workflowStoreConflict("watch", "workflow identity changed while waiting")
		}

		if raw.GroupVersionKind() != gvk {
			return zero, workflowStoreConflict("watch", "workflow kind changed while waiting")
		}

		object := newObject()

		kind, err := workflowStoreKind(object)
		if err != nil {
			return zero, err
		}

		if kind != gvk {
			return zero, workflowStoreConflict(
				"watch",
				"object factory does not match the watched workflow kind",
			)
		}

		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
			raw.Object,
			object,
		); err != nil {
			return zero, err
		}

		return object, nil
	}
	for {
		current, err := resource.Get(ctx, initial.GetName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return zero, workflowStoreConflict("watch", "workflow was deleted before completion")
		}

		if err != nil {
			return zero, controllerWaitError(ctx, "read workflow resource", err)
		}

		object, err := decode(current)
		if err != nil {
			return zero, err
		}

		if done, err := onUpdate(object); done || err != nil {
			return object, err
		}

		stream, err := resource.Watch(ctx, metav1.ListOptions{
			FieldSelector: fields.OneTermEqualSelector("metadata.name", initial.GetName()).
				String(),
			ResourceVersion:     current.GetResourceVersion(),
			AllowWatchBookmarks: true,
		})
		if apierrors.IsResourceExpired(err) {
			continue
		}

		if err != nil {
			return zero, controllerWaitError(ctx, "watch workflow resource", err)
		}

		result, done, err := consumeWorkflowEvents(ctx, stream, decode, onUpdate)
		stream.Stop()

		if done || err != nil {
			return result, err
		}

		if !waitControllerWatchReconnect(ctx) {
			return zero, controllerWaitError(ctx, "reconnect workflow watch", ctx.Err())
		}
	}
}

func consumeWorkflowEvents[T crclient.Object](ctx context.Context, stream watch.Interface,
	decode func(*unstructured.Unstructured) (T, error), onUpdate func(T) (bool, error),
) (T, bool, error) {
	var zero T
	for {
		select {
		case <-ctx.Done():
			return zero, false, controllerWaitError(ctx, "watch workflow resource", ctx.Err())
		case event, open := <-stream.ResultChan():
			if !open {
				return zero, false, nil
			}

			if event.Type == watch.Bookmark {
				continue
			}

			if event.Type == watch.Error {
				err := apierrors.FromObject(event.Object)
				if apierrors.IsResourceExpired(err) {
					return zero, false, nil
				}

				return zero, false, controllerWaitError(ctx, "watch workflow resource", err)
			}

			raw, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				return zero, false, errors.New("workflow watch returned an unexpected object")
			}

			object, err := decode(raw)
			if err != nil {
				return zero, false, err
			}

			if event.Type == watch.Deleted {
				return zero, false, workflowStoreConflict(
					"watch",
					"workflow was deleted before completion",
				)
			}

			if event.Type != watch.Added && event.Type != watch.Modified {
				continue
			}

			if done, err := onUpdate(object); done || err != nil {
				return object, done, err
			}
		}
	}
}
