package kube

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func namespacedCRDHandoffFixture(
	t *testing.T,
) (crclient.WithWatch, *v1alpha1.Reservation, *v1alpha1.Copy) {
	t.Helper()

	_, source, destination := namespacedConfigMapHandoffFixture(t)
	source.Finalizers = []string{SessionFinalizer}
	source.Status.Plan = &v1alpha1.ReservationPlan{}
	destination.Status.Plan = &v1alpha1.CopyPlan{}

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	client := crfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.Reservation{}, &v1alpha1.Copy{}).
		WithObjects(source).Build()

	return client, source, destination
}

func TestNamespacedCRDReservationHandoffRecoversEachPersistenceFailure(t *testing.T) {
	for _, step := range []string{"freeze", "create", "checkpoint", "release", "delete", "activate"} {
		t.Run(step, func(t *testing.T) {
			base, source, destination := namespacedCRDHandoffFixture(t)
			before := destination.DeepCopy()
			failure := errors.New("injected handoff failure")
			failing := true
			client := failingNamespacedHandoffClient(base, step, &failing, failure)

			if err := NamespacedHandoffCRDReservationToCopy(
				t.Context(),
				client,
				source,
				destination,
			); !errors.Is(
				err,
				failure,
			) {
				t.Fatalf("error = %v", err)
			}

			if !reflect.DeepEqual(destination, before) {
				t.Fatal("failed handoff changed the caller's Copy")
			}

			key := crclient.ObjectKey{Namespace: source.Namespace, Name: source.Name}
			persistedSource := &v1alpha1.Reservation{}
			sourceErr := base.Get(t.Context(), key, persistedSource)
			persistedTarget := &v1alpha1.Copy{}

			targetErr := base.Get(t.Context(), key, persistedTarget)
			if sourceErr != nil && targetErr != nil {
				t.Fatal("handoff lost both workflow records")
			}

			if targetErr == nil &&
				(!slices.Contains(persistedTarget.Finalizers, SessionFinalizer) || RequireWorkflowHandoffComplete(persistedTarget) == nil) {
				t.Fatal("pending Copy is not protected from deletion and execution")
			}

			if sourceErr == nil && step != "freeze" &&
				RequireWorkflowHandoffComplete(persistedSource) == nil {
				t.Fatal("pending Reservation is not protected from execution")
			}

			failing = false

			if apierrors.IsNotFound(sourceErr) {
				if err := NamespacedActivateCRDCopyHandoff(
					t.Context(),
					client,
					persistedTarget,
				); err != nil {
					t.Fatal(err)
				}
			} else {
				if sourceErr != nil {
					t.Fatal(sourceErr)
				}

				if err := NamespacedHandoffCRDReservationToCopy(
					t.Context(),
					client,
					persistedSource,
					destination,
				); err != nil {
					t.Fatal(err)
				}
			}

			if err := base.Get(t.Context(), key, persistedTarget); err != nil {
				t.Fatal(err)
			}

			if err := base.Get(t.Context(), key, persistedSource); !apierrors.IsNotFound(err) {
				t.Fatalf("source was not removed: %v", err)
			}

			if RequireWorkflowHandoffComplete(persistedTarget) != nil ||
				!reflect.DeepEqual(persistedTarget.Status.Plan, before.Status.Plan) ||
				!reflect.DeepEqual(persistedTarget.Status.Volumes, before.Status.Volumes) {
				t.Fatal("handoff recovery lost progress or failed to activate Copy")
			}
		})
	}
}

func failingNamespacedHandoffClient(
	base crclient.WithWatch,
	step string,
	failing *bool,
	failure error,
) crclient.WithWatch {
	return interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, client crclient.WithWatch, object crclient.Object, options ...crclient.CreateOption) error {
			if *failing && step == "create" {
				return failure
			}

			object.SetUID("copy-uid")

			return client.Create(ctx, object, options...)
		},
		Update: func(ctx context.Context, client crclient.WithWatch, object crclient.Object, options ...crclient.UpdateOption) error {
			if *failing {
				switch object.(type) {
				case *v1alpha1.Reservation:
					protected := slices.Contains(object.GetFinalizers(), SessionFinalizer)
					if (step == "freeze" && protected) ||
						(step == "release" && !protected) {
						return failure
					}
				case *v1alpha1.Copy:
					if step == "activate" {
						return failure
					}
				}
			}

			return client.Update(ctx, object, options...)
		},
		SubResourceUpdate: func(ctx context.Context, client crclient.Client, subresource string, object crclient.Object, options ...crclient.SubResourceUpdateOption) error {
			if *failing && step == "checkpoint" {
				return failure
			}
			return client.SubResource(subresource).Update(ctx, object, options...)
		},
		Delete: func(ctx context.Context, client crclient.WithWatch, object crclient.Object, options ...crclient.DeleteOption) error {
			if *failing && step == "delete" {
				return failure
			}
			return client.Delete(ctx, object, options...)
		},
	})
}
