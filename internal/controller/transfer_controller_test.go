package controller

import (
	"context"
	"errors"
	"reflect"
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

func newTransferControllerFixture(
	t *testing.T,
	objects ...crclient.Object,
) (*WorkflowReconciler, crclient.WithWatch) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	client := crfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.ClusterReservation{}, &v1alpha1.ClusterCopy{}).
		WithObjects(objects...).Build()
	r := NewWorkflowReconciler().WithSupportedKinds([]domain.ControllerKind{
		domain.ControllerKindClusterReservation, domain.ControllerKindClusterCopy,
	})

	opts := ManagerOptions{
		KubernetesClient: fake.NewClientset(
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}},
		),
		ReservationPlanner: func(context.Context, *v1alpha1.ClusterReservation, string) (*domain.TransferPlan, error) {
			return nil, errors.New("source unavailable")
		},
		CopyPlanner: func(context.Context, *v1alpha1.ClusterCopy, string) (*domain.TransferPlan, error) {
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

	r.reservation.checkCollision = func(context.Context, string, []string) error { return nil }
	r.copy.checkCollision = func(context.Context, string, []string) error { return nil }

	return r, client
}

func TestTransferControllerPlanningFailureRequiresExplicitResume(t *testing.T) {
	for _, kind := range []domain.ControllerKind{domain.ControllerKindClusterReservation, domain.ControllerKindClusterCopy} {
		t.Run(string(kind), func(t *testing.T) {
			meta := metav1.ObjectMeta{Name: "transfer", UID: "workflow", Generation: 1}
			reservation := &v1alpha1.ClusterReservation{
				ObjectMeta: meta,
				Spec: v1alpha1.ClusterReservationSpec{
					SourceNamespace:      "source",
					DestinationNamespace: "destination",
					SessionNamespace:     "system",
				},
			}
			copyObject := &v1alpha1.ClusterCopy{
				ObjectMeta: meta,
				Spec: v1alpha1.ClusterCopySpec{
					SourceNamespace:      "source",
					DestinationNamespace: "destination",
					SessionNamespace:     "system",
				},
			}

			var initial crclient.Object = reservation
			if kind == domain.ControllerKindClusterCopy {
				initial = copyObject
			}

			r, _ := newTransferControllerFixture(t, initial)
			plans := 0
			r.reservation.planner = func(context.Context, *v1alpha1.ClusterReservation, string) (*domain.TransferPlan, error) {
				plans++
				return nil, errors.New("source unavailable")
			}
			r.copy.planner = func(context.Context, *v1alpha1.ClusterCopy, string) (*domain.TransferPlan, error) {
				plans++
				return nil, errors.New("source unavailable")
			}
			entry := &kindWorkflowReconciler{parent: r, kind: kind}

			request := reconcile.Request{NamespacedName: crclient.ObjectKey{Name: "transfer"}}
			for range 2 {
				if _, err := entry.Reconcile(t.Context(), request); err != nil {
					t.Fatal(err)
				}
			}

			if plans != 1 {
				t.Fatalf("failed planning retried without resume: %d", plans)
			}

			if kind == domain.ControllerKindClusterReservation {
				object, err := r.reservation.store.Load(t.Context(), request.NamespacedName)
				if err != nil {
					t.Fatal(err)
				}

				if err := r.reservation.executor(object).
					RequestResume(t.Context(), object); err != nil {
					t.Fatal(err)
				}
			} else {
				object, err := r.copy.store.Load(t.Context(), request.NamespacedName)
				if err != nil {
					t.Fatal(err)
				}

				if err := r.copy.executor(object).RequestResume(t.Context(), object); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := entry.Reconcile(t.Context(), request); err != nil || plans != 2 {
				t.Fatalf("explicit resume did not retry planning: %d; %v", plans, err)
			}
		})
	}
}

func TestCopyControllerPlanningSaveFailureRestoresStatus(t *testing.T) {
	object := &v1alpha1.ClusterCopy{
		ObjectMeta: metav1.ObjectMeta{Name: "copy", UID: "workflow", Generation: 1},
		Spec: v1alpha1.ClusterCopySpec{
			SourceNamespace:      "source",
			DestinationNamespace: "destination",
			SessionNamespace:     "system",
		},
	}
	r, base := newTransferControllerFixture(t, object)
	failure := errors.New("status write failed")
	client := interceptor.NewClient(base, interceptor.Funcs{
		SubResourceUpdate: func(context.Context, crclient.Client, string, crclient.Object, ...crclient.SubResourceUpdateOption) error {
			return failure
		},
	})

	store, err := kube.NewCRDWorkflowStore(
		client,
		func() *v1alpha1.ClusterCopy { return &v1alpha1.ClusterCopy{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	r.copy.store = store
	r.copy.planner = func(_ context.Context, object *v1alpha1.ClusterCopy, image string) (*domain.TransferPlan, error) {
		if image != "trusted/tool:v1" {
			t.Fatal("planner lost administrator image")
		}

		object.Status.Plan = &v1alpha1.ClusterCopyPlan{
			SourceNamespace:      "source",
			DestinationNamespace: "destination",
			SessionNamespace:     "system",
		}

		return &domain.TransferPlan{PlanSummary: domain.PlanSummary{Ready: true}}, nil
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	before := loaded.Status.DeepCopy()
	if err := r.copy.plan(t.Context(), loaded); !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}

	if !reflect.DeepEqual(loaded.Status, *before) {
		t.Fatal("failed save leaked speculative plan or lifecycle")
	}
}

func TestTransferControllerDeletesUnplannedObjects(t *testing.T) {
	for _, kind := range []domain.ControllerKind{domain.ControllerKindClusterReservation, domain.ControllerKindClusterCopy} {
		t.Run(string(kind), func(t *testing.T) {
			meta := metav1.ObjectMeta{
				Name:       "transfer",
				UID:        "workflow",
				Finalizers: []string{kube.SessionFinalizer},
			}

			var object crclient.Object = &v1alpha1.ClusterReservation{ObjectMeta: meta, Spec: v1alpha1.ClusterReservationSpec{SourceNamespace: "system"}}
			if kind == domain.ControllerKindClusterCopy {
				object = &v1alpha1.ClusterCopy{
					ObjectMeta: meta,
					Spec:       v1alpha1.ClusterCopySpec{SourceNamespace: "system"},
				}
			}

			r, client := newTransferControllerFixture(t, object)
			if err := client.Delete(t.Context(), object); err != nil {
				t.Fatal(err)
			}

			request := reconcile.Request{NamespacedName: crclient.ObjectKeyFromObject(object)}

			entry := &kindWorkflowReconciler{parent: r, kind: kind}
			if _, err := entry.Reconcile(t.Context(), request); err != nil {
				t.Fatal(err)
			}

			if err := client.Get(
				t.Context(),
				request.NamespacedName,
				object,
			); !apierrors.IsNotFound(
				err,
			) {
				t.Fatalf("deletion did not release finalizer: %v", err)
			}
		})
	}
}

func completedReservationForController() *v1alpha1.ClusterReservation {
	return &v1alpha1.ClusterReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "transfer",
			UID:        "reservation",
			Finalizers: []string{kube.SessionFinalizer},
		},
		Spec: v1alpha1.ClusterReservationSpec{
			SourceNamespace:      "source",
			DestinationNamespace: "destination",
			SessionNamespace:     "system",
		},
		Status: v1alpha1.ClusterReservationStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseReserved},
			Plan: &v1alpha1.ClusterReservationPlan{
				SourceNamespace:      "source",
				DestinationNamespace: "destination",
				SessionNamespace:     "system",
				Strategies:           []string{domain.StrategyClusterIP},
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
			Volumes: []v1alpha1.ClusterReservationVolumeStatus{
				{
					SourcePVCName: "source",
					Reserved:      true,
					DestinationPVC: &v1alpha1.ObjectReference{
						Namespace: "destination",
						Name:      "destination",
						UID:       "destination-uid",
					},
					DestinationPV: &v1alpha1.ObjectReference{
						Name: "destination-pv",
						UID:  "destination-pv-uid",
					},
				},
			},
		},
	}
}

func TestTransferControllersRecoverHandoffBeforeCollisionChecks(t *testing.T) {
	for _, step := range []string{"create", "activate", "cancel"} {
		t.Run(step, func(t *testing.T) {
			source := completedReservationForController()
			r, base := newTransferControllerFixture(t, source)

			key := crclient.ObjectKeyFromObject(source)
			if err := base.Get(t.Context(), key, source); err != nil {
				t.Fatal(err)
			}

			target, err := app.CopyFromReservation(source, app.CopySpecFromReservation(source.Spec))
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
					if _, isCopy := object.(*v1alpha1.ClusterCopy); isCopy && step == "activate" {
						return failure
					}

					if _, reservation := object.(*v1alpha1.ClusterReservation); reservation &&
						step == "cancel" &&
						len(object.GetFinalizers()) == 0 {
						return failure
					}

					return underlying.Update(ctx, object, options...)
				},
			})
			if err := kube.HandoffCRDReservationToCopy(
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

			r.reservation.checkCollision = func(context.Context, string, []string) error {
				t.Fatal("recovery reached normal reservation collision check")
				return nil
			}
			r.copy.checkCollision = func(context.Context, string, []string) error {
				t.Fatal("recovery reached normal copy collision check")
				return nil
			}

			kind := domain.ControllerKindClusterCopy
			if step == "create" {
				kind = domain.ControllerKindClusterReservation
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
