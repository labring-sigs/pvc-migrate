package kube

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func storedRename() *v1alpha1.Rename {
	return &v1alpha1.Rename{
		TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "Rename"},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "rename",
			Namespace:       "data",
			UID:             "record",
			ResourceVersion: "1",
		},
		Spec: v1alpha1.RenameSpec{
			SourcePVC:      v1alpha1.LocalResourceReference{Name: "source"},
			DestinationPVC: v1alpha1.LocalResourceReference{Name: "target"},
		},
		Status: v1alpha1.RenameStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned},
		},
	}
}

func TestWorkflowStoresPreserveConcreteStatusAndRejectStaleWrites(t *testing.T) {
	for _, backend := range []string{"configmap", "crd"} {
		t.Run(backend, func(t *testing.T) {
			original := storedRename()

			var (
				store WorkflowStore[*v1alpha1.Rename]
				err   error
			)
			if backend == "configmap" {
				data, marshalErr := json.Marshal(original)
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}

				client := fake.NewClientset(&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:            SessionConfigMapName(original.Name),
						Namespace:       "sessions",
						UID:             original.UID,
						ResourceVersion: original.ResourceVersion,
						Labels:          sessionLabels(original.Name),
					},
					Data: map[string]string{SessionDataKey: string(data)},
				})
				client.PrependReactor(
					"update",
					"configmaps",
					func(action ktesting.Action) (bool, runtime.Object, error) {
						update, ok := action.(ktesting.UpdateAction)
						if !ok {
							t.Fatal("expected update")
						}

						updated, ok := update.GetObject().(metav1.Object)
						if !ok {
							t.Fatal("expected Kubernetes object metadata")
						}

						updated.SetResourceVersion("2")

						return false, nil, nil
					},
				)
				store, err = NewConfigMapWorkflowStore(
					client,
					"sessions",
					func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
				)
			} else {
				scheme := runtime.NewScheme()
				if err := v1alpha1.AddToScheme(scheme); err != nil {
					t.Fatal(err)
				}

				client := crfake.NewClientBuilder().
					WithScheme(scheme).
					WithStatusSubresource(&v1alpha1.Rename{}).
					WithObjects(original.DeepCopy()).
					Build()
				store, err = NewCRDWorkflowStore(
					client,
					func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
				)
			}

			if err != nil {
				t.Fatal(err)
			}

			key := crclient.ObjectKeyFromObject(original)

			loaded, err := store.Load(t.Context(), key)
			if err != nil {
				t.Fatal(err)
			}

			stale := loaded.DeepCopy()
			lost := errors.New("lease lost before checkpoint")

			loaded.Status.Message = "uncommitted checkpoint"
			if err := store.Save(
				WithLeaseFence(t.Context(), &testLeaseFence{err: lost}),
				loaded,
			); !errors.Is(
				err,
				lost,
			) {
				t.Fatalf("lost lease allowed checkpoint: %v", err)
			}

			persisted, err := store.Load(t.Context(), key)
			if err != nil || persisted.Status.Message != stale.Status.Message ||
				persisted.ResourceVersion != stale.ResourceVersion {
				t.Fatalf("lost lease changed durable state: object=%+v err=%v", persisted, err)
			}

			loaded.Status.Message = stale.Status.Message

			loaded.Spec.DestinationPVC.Name = "changed"
			if err := store.Save(
				t.Context(),
				loaded,
			); domain.CategoryOf(
				err,
			) != domain.ErrorConflict {
				t.Fatalf("spec mutation accepted: %v", err)
			}

			loaded.Spec = original.Spec
			loaded.Status.Phase = domain.PhaseRenaming

			loaded.Status.Plan = &v1alpha1.RenamePlan{PVCIdentityFields: v1alpha1.PVCIdentityFields{
				SourcePVC:      v1alpha1.LocalResourceReference{Name: "source", UID: "source-uid"},
				SourcePV:       v1alpha1.LocalResourceReference{Name: "pv", UID: "pv-uid"},
				DestinationPVC: original.Spec.DestinationPVC,
			}}
			if err := store.Save(t.Context(), loaded); err != nil {
				t.Fatal(err)
			}

			if loaded.ResourceVersion == stale.ResourceVersion {
				t.Fatal("save did not advance resourceVersion")
			}

			if err := store.Save(
				t.Context(),
				stale,
			); domain.CategoryOf(
				err,
			) != domain.ErrorConflict {
				t.Fatalf("stale save accepted: %v", err)
			}

			if err := store.Delete(
				t.Context(),
				stale,
			); domain.CategoryOf(
				err,
			) != domain.ErrorConflict {
				t.Fatalf("stale deletion accepted: %v", err)
			}

			saved, err := store.Load(t.Context(), key)
			if err != nil {
				t.Fatal(err)
			}

			if saved.Status.Phase != domain.PhaseRenaming ||
				saved.Status.Plan.SourcePV.UID != "pv-uid" {
				t.Fatalf("lost concrete plan/status: %+v", saved.Status)
			}

			saved.UID = types.UID("replacement")
			if err := store.Save(
				t.Context(),
				saved,
			); domain.CategoryOf(
				err,
			) != domain.ErrorConflict {
				t.Fatalf("replacement UID accepted: %v", err)
			}
		})
	}
}

func TestConfigMapWorkflowStoreRejectsDifferentOperation(t *testing.T) {
	object := storedRename()

	data, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}

	client := fake.NewClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      SessionConfigMapName(object.Name),
			Namespace: "sessions",
			Labels:    sessionLabels(object.Name),
		},
		Data: map[string]string{SessionDataKey: string(data)},
	})

	store, err := NewConfigMapWorkflowStore(
		client,
		"sessions",
		func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object)); err == nil {
		t.Fatal("rename decoded as copy")
	}
}

func TestConfigMapWorkflowFailedWritePreservesCallerVersion(t *testing.T) {
	object := storedRename()

	data, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}

	client := fake.NewClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            SessionConfigMapName(object.Name),
			Namespace:       "sessions",
			UID:             object.UID,
			ResourceVersion: object.ResourceVersion,
			Labels:          sessionLabels(object.Name),
		},
		Data: map[string]string{SessionDataKey: string(data)},
	})
	failure := errors.New("write unavailable")
	client.PrependReactor(
		"update",
		"configmaps",
		func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, failure },
	)

	store, err := NewConfigMapWorkflowStore(
		client,
		"sessions",
		func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	object.Status.Phase = domain.PhaseRenaming
	if err := store.Save(context.Background(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if object.ResourceVersion != "1" || object.UID != "record" {
		t.Fatal("failed write changed caller fencing identity")
	}
}

func TestWorkflowStoreRequiresConcreteFactory(t *testing.T) {
	for _, factory := range []func() *v1alpha1.Rename{nil, func() *v1alpha1.Rename { return nil }} {
		if _, err := NewConfigMapWorkflowStore(
			fake.NewClientset(),
			"sessions",
			factory,
		); err == nil {
			t.Fatal("ConfigMap store accepted an absent concrete factory")
		}

		if _, err := NewCRDWorkflowStore(crfake.NewClientBuilder().Build(), factory); err == nil {
			t.Fatal("CRD store accepted an absent concrete factory")
		}
	}
}

func TestCRDWorkflowListKeepsKindAndNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	one := storedRename()
	other := one.DeepCopy()
	other.Namespace = "other"
	client := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(one, other,
		&v1alpha1.Copy{ObjectMeta: metav1.ObjectMeta{Name: "copy", Namespace: one.Namespace}},
	).Build()

	store, err := NewCRDWorkflowStore(client, func() *v1alpha1.Rename { return &v1alpha1.Rename{} })
	if err != nil {
		t.Fatal(err)
	}

	items, err := store.List(t.Context(), one.Namespace)
	if err != nil {
		t.Fatal(err)
	}

	if len(items) != 1 || items[0].Namespace != one.Namespace || items[0].Kind != "Rename" {
		t.Fatalf("list lost its operation scope: %+v", items)
	}

	items, err = store.List(t.Context(), "")
	if err != nil || len(items) != 2 {
		t.Fatalf("all namespace list: %v; %d", err, len(items))
	}
}

func TestCRDWorkflowProtectionPreservesStatusAndRejectsStaleIdentity(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	original := storedRename()
	original.Finalizers = []string{"example.org/owner"}
	client := crfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(original).WithObjects(original).Build()

	store, err := NewCRDWorkflowStore(client, func() *v1alpha1.Rename { return &v1alpha1.Rename{} })
	if err != nil {
		t.Fatal(err)
	}

	object, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(original))
	if err != nil {
		t.Fatal(err)
	}

	stale := object.DeepCopy()

	object.Status.Message = "unsaved planning result"
	if err := store.EnsureProtection(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if len(object.Finalizers) != 2 || object.Finalizers[0] != "example.org/owner" ||
		object.Status.Message != "unsaved planning result" {
		t.Fatalf("protection corrupted caller: %+v", object)
	}

	persisted, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(original))
	if err != nil || persisted.Status.Message != "" {
		t.Fatalf("protection wrote uncommitted status: %v; %+v", err, persisted)
	}

	if err := store.EnsureProtection(t.Context(), stale); err == nil {
		t.Fatal("stale version accepted")
	}

	version := object.ResourceVersion
	if err := store.EnsureProtection(
		t.Context(),
		object,
	); err != nil ||
		object.ResourceVersion != version {
		t.Fatalf("protection is not idempotent: %v", err)
	}

	if err := client.Delete(t.Context(), persisted); err != nil {
		t.Fatal(err)
	}

	deleting, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(original))
	if err != nil {
		t.Fatal(err)
	}

	if err := store.EnsureProtection(t.Context(), deleting); err == nil {
		t.Fatal("deleting workflow accepted for execution protection")
	}
}

func TestCRDWorkflowStoreDeleteConvergesDeletingWorkflowWithStaleSnapshot(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	record := storedRename()
	record.Finalizers = []string{SessionFinalizer}
	now := metav1.Now()
	record.DeletionTimestamp = &now

	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(record.DeepCopy()).
		Build()

	store, err := NewCRDWorkflowStore(client, func() *v1alpha1.Rename {
		return &v1alpha1.Rename{}
	})
	if err != nil {
		t.Fatal(err)
	}

	// The caller's snapshot predates the server-side Deleting condition and
	// status writes that terminal cleanup performed, so its resourceVersion
	// is stale. Deletion convergence must still release the finalizer.
	stale := record.DeepCopy()
	stale.ResourceVersion = "1"

	if err := store.Delete(t.Context(), stale); err != nil {
		t.Fatalf("delete converging deleting workflow: %v", err)
	}

	live := &v1alpha1.Rename{}
	if err := client.Get(t.Context(), crclient.ObjectKey{
		Namespace: record.Namespace, Name: record.Name,
	}, live); !apierrors.IsNotFound(err) {
		t.Fatalf("deleting workflow still exists: %v (err=%v)", live, err)
	}
}
