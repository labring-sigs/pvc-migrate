package kube

import (
	"context"
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type conflictRetryStore struct {
	loaded  *v1alpha1.Copy
	loadErr error
}

func (s *conflictRetryStore) Create(_ context.Context, _ *v1alpha1.Copy) error {
	return nil
}

func (s *conflictRetryStore) Load(_ context.Context, _ crclient.ObjectKey) (*v1alpha1.Copy, error) {
	return s.loaded, s.loadErr
}

func (s *conflictRetryStore) List(_ context.Context, _ string) ([]*v1alpha1.Copy, error) {
	return nil, nil
}

func (s *conflictRetryStore) Save(_ context.Context, _ *v1alpha1.Copy) error {
	return nil
}

func (s *conflictRetryStore) Delete(_ context.Context, _ *v1alpha1.Copy) error {
	return nil
}

func conflictRetryObject(uid types.UID, resourceVersion string) *v1alpha1.Copy {
	return &v1alpha1.Copy{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "copy",
			Namespace:       "tenant",
			UID:             uid,
			ResourceVersion: resourceVersion,
		},
	}
}

func TestRetryStaleWorkflowWriteReloadsAndConverges(t *testing.T) {
	store := &conflictRetryStore{loaded: conflictRetryObject("workflow", "2")}

	runs := 0

	err := RetryStaleWorkflowWrite(
		t.Context(),
		store,
		conflictRetryObject("workflow", "1"),
		func(_ context.Context, object *v1alpha1.Copy) error {
			runs++
			if runs == 1 {
				return domain.WrapError(
					domain.ErrorConflict,
					"write workflow",
					"workflow changed after it was loaded",
					ErrWorkflowStaleLoad,
				)
			}

			if object.ResourceVersion != "2" {
				t.Fatal("retry did not receive the reloaded workflow")
			}

			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if runs != 2 {
		t.Fatalf("expected one retry, got %d runs", runs)
	}

	runs = 0

	err = RetryStaleWorkflowWrite(
		t.Context(),
		store,
		conflictRetryObject("workflow", "1"),
		func(_ context.Context, _ *v1alpha1.Copy) error {
			runs++
			if runs == 1 {
				return apierrors.NewConflict(
					schema.GroupResource{Group: "migrate.sealos.io", Resource: "copies"},
					"copy",
					errors.New("object was modified"),
				)
			}

			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if runs != 2 {
		t.Fatalf("raw conflict did not retry: %d runs", runs)
	}
}

func TestRetryStaleWorkflowWriteStopsForNonStaleErrors(t *testing.T) {
	rejection := domain.NewError(domain.ErrorPrecondition, "cleanup", "workflow already completed")

	runs := 0

	err := RetryStaleWorkflowWrite(
		t.Context(),
		&conflictRetryStore{loaded: conflictRetryObject("workflow", "2")},
		conflictRetryObject("workflow", "1"),
		func(_ context.Context, _ *v1alpha1.Copy) error {
			runs++
			return rejection
		},
	)
	if !errors.Is(err, rejection) {
		t.Fatalf("durable rejection was retried or replaced: %v", err)
	}

	if runs != 1 {
		t.Fatalf("durable rejection ran %d times", runs)
	}
}

func TestRetryStaleWorkflowWriteBoundsAndIdentity(t *testing.T) {
	stale := domain.WrapError(
		domain.ErrorConflict,
		"write workflow",
		"workflow changed after it was loaded",
		ErrWorkflowStaleLoad,
	)

	runs := 0

	err := RetryStaleWorkflowWrite(
		t.Context(),
		&conflictRetryStore{loaded: conflictRetryObject("workflow", "1")},
		conflictRetryObject("workflow", "1"),
		func(_ context.Context, _ *v1alpha1.Copy) error {
			runs++
			return stale
		},
	)
	if !errors.Is(err, ErrWorkflowStaleLoad) {
		t.Fatalf("persistent staleness was not reported: %v", err)
	}

	if runs != 3 {
		t.Fatalf("expected bounded attempts, got %d", runs)
	}

	// A name reused after deletion must never receive the pass.
	runs = 0

	err = RetryStaleWorkflowWrite(
		t.Context(),
		&conflictRetryStore{loaded: conflictRetryObject("reused", "9")},
		conflictRetryObject("original", "1"),
		func(_ context.Context, _ *v1alpha1.Copy) error {
			runs++
			return stale
		},
	)
	if !errors.Is(err, ErrWorkflowStaleLoad) {
		t.Fatalf("identity change was not reported: %v", err)
	}

	if runs != 1 {
		t.Fatalf("identity change retried the pass %d times", runs)
	}
}
