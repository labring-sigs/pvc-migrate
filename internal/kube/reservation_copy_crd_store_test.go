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

func crdHandoffFixture(
	t *testing.T,
) (crclient.WithWatch, *v1alpha1.ClusterReservation, *v1alpha1.ClusterCopy) {
	t.Helper()

	_, source, destination := configMapHandoffFixture(t)
	source.Finalizers = []string{SessionFinalizer}
	source.Status.Plan = &v1alpha1.ClusterReservationPlan{
		SourceNamespace:      "source",
		DestinationNamespace: "destination",
	}
	destination.Status.Plan = &v1alpha1.ClusterCopyPlan{
		SourceNamespace:      "source",
		DestinationNamespace: "destination",
	}

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	client := crfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.ClusterReservation{}, &v1alpha1.ClusterCopy{}).
		WithObjects(source).Build()

	return client, source, destination
}

func TestCRDReservationHandoffRecoversEachPersistenceFailure(t *testing.T) {
	for _, step := range []string{"freeze", "create", "checkpoint", "release", "delete", "activate"} {
		t.Run(step, func(t *testing.T) {
			base, source, destination := crdHandoffFixture(t)
			before := destination.DeepCopy()
			failure := errors.New("injected handoff failure")
			failing := true
			client := failingHandoffClient(base, step, &failing, failure)

			if err := HandoffCRDReservationToCopy(
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

			key := crclient.ObjectKey{Name: source.Name}
			persistedSource := &v1alpha1.ClusterReservation{}
			sourceErr := base.Get(t.Context(), key, persistedSource)
			persistedTarget := &v1alpha1.ClusterCopy{}

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
				if err := ActivateCRDCopyHandoff(t.Context(), client, persistedTarget); err != nil {
					t.Fatal(err)
				}
			} else {
				if sourceErr != nil {
					t.Fatal(sourceErr)
				}

				if err := HandoffCRDReservationToCopy(
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

func TestCRDReservationHandoffRejectsLeaseLossAfterEachWrite(t *testing.T) {
	lost := errors.New("lease lost after handoff write")

	for _, step := range []string{"freeze", "create", "checkpoint", "delete", "activate"} {
		t.Run(step, func(t *testing.T) {
			base, source, destination := crdHandoffFixture(t)
			fence := &testLeaseFence{}
			client := fenceAfterHandoffClient(base, step, fence, lost)

			err := HandoffCRDReservationToCopy(
				WithLeaseFence(t.Context(), fence),
				client,
				source,
				destination,
			)
			if !errors.Is(err, lost) {
				t.Fatalf("error = %v, want lease loss after %s", err, step)
			}
		})
	}
}

func failingHandoffClient(
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
				case *v1alpha1.ClusterReservation:
					protected := slices.Contains(object.GetFinalizers(), SessionFinalizer)
					if (step == "freeze" && protected) ||
						(step == "release" && !protected) {
						return failure
					}
				case *v1alpha1.ClusterCopy:
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

func fenceAfterHandoffClient(
	base crclient.WithWatch,
	step string,
	fence *testLeaseFence,
	lost error,
) crclient.WithWatch {
	return interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, client crclient.WithWatch, object crclient.Object, options ...crclient.CreateOption) error {
			if object.GetUID() == "" {
				object.SetUID("copy-uid")
			}

			err := client.Create(ctx, object, options...)
			if err == nil && step == "create" {
				fence.err = lost
			}

			return err
		},
		Update: func(ctx context.Context, client crclient.WithWatch, object crclient.Object, options ...crclient.UpdateOption) error {
			err := client.Update(ctx, object, options...)
			if err == nil {
				switch object := object.(type) {
				case *v1alpha1.ClusterReservation:
					protected := slices.Contains(object.Finalizers, SessionFinalizer)

					pending := object.Annotations[reservationCopyPendingAnnotation] != ""
					if step == "freeze" && protected && pending {
						fence.err = lost
					}
				case *v1alpha1.ClusterCopy:
					if step == "activate" {
						fence.err = lost
					}
				}
			}

			return err
		},
		SubResourceUpdate: func(ctx context.Context, client crclient.Client, subresource string, object crclient.Object, options ...crclient.SubResourceUpdateOption) error {
			err := client.SubResource(subresource).Update(ctx, object, options...)
			if err == nil && step == "checkpoint" {
				fence.err = lost
			}

			return err
		},
		Delete: func(ctx context.Context, client crclient.WithWatch, object crclient.Object, options ...crclient.DeleteOption) error {
			err := client.Delete(ctx, object, options...)
			if err == nil && step == "delete" {
				fence.err = lost
			}

			return err
		},
	})
}
