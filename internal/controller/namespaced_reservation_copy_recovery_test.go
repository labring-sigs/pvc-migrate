package controller

import (
	"context"
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func newNamespacedTransferControllerFixture(
	t *testing.T,
	objects ...crclient.Object,
) (*WorkflowReconciler, crclient.WithWatch) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	client := crfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.Reservation{}, &v1alpha1.Copy{}).
		WithObjects(objects...).Build()
	r := NewWorkflowReconciler().WithSupportedKinds([]domain.ControllerKind{
		domain.ControllerKindReservation, domain.ControllerKindCopy,
	})

	opts := ManagerOptions{
		KubernetesClient: fake.NewClientset(
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "data"}},
		),
		NamespacedReservationPlanner: func(context.Context, *v1alpha1.Reservation, string) (*domain.TransferPlan, error) {
			return nil, errors.New("source unavailable")
		},
		NamespacedCopyPlanner: func(context.Context, *v1alpha1.Copy, string) (*domain.TransferPlan, error) {
			return nil, errors.New("source unavailable")
		},
	}
	if err := r.configureTransferControllers(
		client,
		opts,
		&moveControllerLocker{},
		"trusted/tool:v1",
		nil,
	); err != nil {
		t.Fatal(err)
	}

	r.namespacedReservation.checkCollision = func(context.Context, string, []string) error { return nil }
	r.namespacedCopy.checkCollision = func(context.Context, string, []string) error { return nil }

	return r, client
}

func completedNamespacedReservationForController() *v1alpha1.Reservation {
	return &v1alpha1.Reservation{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "transfer",
			Namespace:  "data",
			UID:        "reservation",
			Finalizers: []string{kube.SessionFinalizer},
		},
		Spec: v1alpha1.ReservationSpec{},
		Status: v1alpha1.ReservationStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseReserved},
			Plan: &v1alpha1.ReservationPlan{
				Strategies: []string{domain.StrategyClusterIP},
				Volumes: []v1alpha1.VolumeSpec{
					{
						SourcePVC: v1alpha1.LocalResourceReference{
							Name: "source",
							UID:  "source-uid",
						},
						SourcePV: v1alpha1.LocalResourceReference{
							Name: "source-pv",
							UID:  "source-pv-uid",
						},
						DestinationPVC: v1alpha1.LocalResourceReference{Name: "destination"},
						SourceCapacity: "1Gi",
						Capacity:       "1Gi",
					},
				},
			},
			Volumes: []v1alpha1.ReservationVolumeStatus{
				{
					SourcePVCName: "source",
					Reserved:      true,
					DestinationPVC: &v1alpha1.LocalResourceReference{
						Name: "destination",
						UID:  "destination-uid",
					},
					DestinationPV: &v1alpha1.LocalResourceReference{
						Name: "destination-pv",
						UID:  "destination-pv-uid",
					},
				},
			},
		},
	}
}

func TestNamespacedTransferControllersRecoverHandoffBeforeCollisionChecks(t *testing.T) {
	for _, step := range []string{"create", "activate", "cancel"} {
		t.Run(step, func(t *testing.T) {
			source := completedNamespacedReservationForController()
			r, base := newNamespacedTransferControllerFixture(t, source)

			key := crclient.ObjectKeyFromObject(source)
			if err := base.Get(t.Context(), key, source); err != nil {
				t.Fatal(err)
			}

			target, err := app.NamespacedCopyFromReservation(
				source,
				app.NamespacedCopySpecFromReservation(source.Spec),
			)
			if err != nil {
				t.Fatal(err)
			}

			failure := errors.New("handoff interrupted")

			client := interceptor.NewClient(base, interceptor.Funcs{
				Create: func(ctx context.Context, underlying crclient.WithWatch, object crclient.Object, options ...crclient.CreateOption) error {
					if step == "create" {
						return failure
					}

					object.SetUID("copy")

					return underlying.Create(ctx, object, options...)
				},
				Update: func(ctx context.Context, underlying crclient.WithWatch, object crclient.Object, options ...crclient.UpdateOption) error {
					if _, isCopy := object.(*v1alpha1.Copy); isCopy && step == "activate" {
						return failure
					}

					if _, reservation := object.(*v1alpha1.Reservation); reservation &&
						step == "cancel" &&
						len(object.GetFinalizers()) == 0 {
						return failure
					}

					return underlying.Update(ctx, object, options...)
				},
			})
			if err := kube.NamespacedHandoffCRDReservationToCopy(
				t.Context(),
				client,
				source,
				target,
			); !errors.Is(
				err,
				failure,
			) {
				t.Fatalf("failed to stage handoff: %v", err)
			}

			if step == "cancel" {
				if err := base.Get(t.Context(), key, target); err != nil {
					t.Fatal(err)
				}

				if err := base.Delete(t.Context(), target); err != nil {
					t.Fatal(err)
				}
			}

			r.namespacedReservation.checkCollision = func(context.Context, string, []string) error {
				t.Fatal("recovery reached normal reservation collision check")
				return nil
			}
			r.namespacedCopy.checkCollision = func(context.Context, string, []string) error {
				t.Fatal("recovery reached normal copy collision check")
				return nil
			}

			kind := domain.ControllerKindCopy
			if step == "create" {
				kind = domain.ControllerKindReservation
			}

			entry := &kindWorkflowReconciler{parent: r, kind: kind}
			if result, err := entry.Reconcile(
				t.Context(),
				reconcile.Request{NamespacedName: key},
			); err != nil ||
				result.RequeueAfter == 0 {
				t.Fatalf("recovery did not schedule reload: %+v; %v", result, err)
			}

			if step == "activate" {
				if err := base.Get(t.Context(), key, target); err != nil {
					t.Fatal(err)
				}

				if err := kube.RequireWorkflowHandoffComplete(target); err != nil {
					t.Fatal(err)
				}

				if !target.Status.Volumes[0].Reserved ||
					target.Status.Volumes[0].Sync.Attempts != 0 {
					t.Fatal("activation changed transfer progress")
				}
			} else {
				if err := base.Get(t.Context(), key, source); err != nil {
					t.Fatal(err)
				}

				if err := kube.RequireWorkflowHandoffComplete(source); err != nil {
					t.Fatal(err)
				}

				if err := base.Get(t.Context(), key, target); !apierrors.IsNotFound(err) {
					t.Fatalf("canceled Copy remains: %v", err)
				}
			}
		})
	}
}
