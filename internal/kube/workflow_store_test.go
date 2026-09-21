package kube

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
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
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

func TestWorkflowStoreDeleteRejectsLostLeaseBeforeFinalizerMutation(t *testing.T) {
	lost := errors.New("lease lost before finalizer cleanup")

	t.Run("configmap", func(t *testing.T) {
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
				Finalizers:      []string{SessionFinalizer},
				Labels:          sessionLabels(object.Name),
			},
			Data: map[string]string{SessionDataKey: string(data)},
		})
		store, err := NewConfigMapWorkflowStore(
			client,
			"sessions",
			func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
		)
		if err != nil {
			t.Fatal(err)
		}

		loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
		if err != nil {
			t.Fatal(err)
		}

		err = store.Delete(
			WithLeaseFence(t.Context(), &testLeaseFence{err: lost}),
			loaded,
		)
		if !errors.Is(err, lost) {
			t.Fatalf("delete error = %v, want lease loss", err)
		}

		current, err := client.CoreV1().ConfigMaps("sessions").Get(
			t.Context(),
			SessionConfigMapName(object.Name),
			metav1.GetOptions{},
		)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(current.Finalizers, []string{SessionFinalizer}) {
			t.Fatalf("lost lease mutated ConfigMap finalizers: %v", current.Finalizers)
		}
	})

	t.Run("crd", func(t *testing.T) {
		scheme := runtime.NewScheme()
		if err := v1alpha1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}

		object := storedRename()
		object.Finalizers = []string{SessionFinalizer}
		client := crfake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(object.DeepCopy()).
			Build()
		store, err := NewCRDWorkflowStore(
			client,
			func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
		)
		if err != nil {
			t.Fatal(err)
		}

		loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
		if err != nil {
			t.Fatal(err)
		}

		err = store.Delete(
			WithLeaseFence(t.Context(), &testLeaseFence{err: lost}),
			loaded,
		)
		if !errors.Is(err, lost) {
			t.Fatalf("delete error = %v, want lease loss", err)
		}

		current := &v1alpha1.Rename{}
		if err := client.Get(t.Context(), crclient.ObjectKeyFromObject(object), current); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(current.Finalizers, []string{SessionFinalizer}) {
			t.Fatalf("lost lease mutated CRD finalizers: %v", current.Finalizers)
		}
	})
}

func TestWorkflowStoresRejectLeaseLossAfterDurableWrite(t *testing.T) {
	lost := errors.New("lease lost after workflow write")

	for _, backend := range []string{"configmap", "crd"} {
		t.Run(backend+"-create", func(t *testing.T) {
			object := storedRename()
			object.Name = "create-after-write"
			object.UID = ""
			object.ResourceVersion = ""
			fence := &testLeaseFence{}
			var store WorkflowStore[*v1alpha1.Rename]

			if backend == "configmap" {
				client := fake.NewClientset()
				client.PrependReactor("create", "*", func(ktesting.Action) (bool, runtime.Object, error) {
					fence.err = lost
					return false, nil, nil
				})
				var err error
				store, err = NewConfigMapWorkflowStore(
					client,
					"sessions",
					func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
				)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				scheme := runtime.NewScheme()
				if err := v1alpha1.AddToScheme(scheme); err != nil {
					t.Fatal(err)
				}

				client := crfake.NewClientBuilder().
					WithScheme(scheme).
					WithInterceptorFuncs(interceptor.Funcs{
						Create: func(
							ctx context.Context,
							client crclient.WithWatch,
							object crclient.Object,
							opts ...crclient.CreateOption,
						) error {
							err := client.Create(ctx, object, opts...)
							fence.err = lost
							return err
						},
					}).
					Build()
				var err error
				store, err = NewCRDWorkflowStore(
					client,
					func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
				)
				if err != nil {
					t.Fatal(err)
				}
			}

			if err := store.Create(WithLeaseFence(t.Context(), fence), object); !errors.Is(err, lost) {
				t.Fatalf("create error = %v, want lease loss after create", err)
			}

			if _, err := store.Load(t.Context(), crclient.ObjectKey{
				Namespace: object.Namespace,
				Name:      object.Name,
			}); err != nil {
				t.Fatalf("durable workflow was not created: %v", err)
			}
		})
	}

	for _, backend := range []string{"configmap", "crd"} {
		t.Run(backend+"-save", func(t *testing.T) {
			original := storedRename()
			fence := &testLeaseFence{}
			var store WorkflowStore[*v1alpha1.Rename]

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
				client.PrependReactor("update", "*", func(ktesting.Action) (bool, runtime.Object, error) {
					fence.err = lost
					return false, nil, nil
				})
				var err error
				store, err = NewConfigMapWorkflowStore(
					client,
					"sessions",
					func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
				)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				scheme := runtime.NewScheme()
				if err := v1alpha1.AddToScheme(scheme); err != nil {
					t.Fatal(err)
				}

				client := crfake.NewClientBuilder().
					WithScheme(scheme).
					WithStatusSubresource(&v1alpha1.Rename{}).
					WithObjects(original.DeepCopy()).
					WithInterceptorFuncs(interceptor.Funcs{
						SubResourceUpdate: func(
							ctx context.Context,
							client crclient.Client,
							subResource string,
							object crclient.Object,
							opts ...crclient.SubResourceUpdateOption,
						) error {
							err := client.SubResource(subResource).Update(ctx, object, opts...)
							fence.err = lost
							return err
						},
					}).
					Build()
				var err error
				store, err = NewCRDWorkflowStore(
					client,
					func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
				)
				if err != nil {
					t.Fatal(err)
				}
			}

			loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(original))
			if err != nil {
				t.Fatal(err)
			}
			loaded.Status.Message = "durable checkpoint"

			if err := store.Save(WithLeaseFence(t.Context(), fence), loaded); !errors.Is(err, lost) {
				t.Fatalf("save error = %v, want lease loss after write", err)
			}

			persisted, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(original))
			if err != nil {
				t.Fatal(err)
			}
			if persisted.Status.Message != "durable checkpoint" {
				t.Fatalf("durable status was lost after reported lease error: %q", persisted.Status.Message)
			}
		})

		t.Run(backend+"-delete", func(t *testing.T) {
			original := storedRename()
			original.Name = "delete-after-write"
			fence := &testLeaseFence{}
			var store WorkflowStore[*v1alpha1.Rename]

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
				client.PrependReactor("delete", "*", func(ktesting.Action) (bool, runtime.Object, error) {
					fence.err = lost
					return false, nil, nil
				})
				var err error
				store, err = NewConfigMapWorkflowStore(
					client,
					"sessions",
					func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
				)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				scheme := runtime.NewScheme()
				if err := v1alpha1.AddToScheme(scheme); err != nil {
					t.Fatal(err)
				}

				client := crfake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(original.DeepCopy()).
					WithInterceptorFuncs(interceptor.Funcs{
						Delete: func(
							ctx context.Context,
							client crclient.WithWatch,
							object crclient.Object,
							opts ...crclient.DeleteOption,
						) error {
							err := client.Delete(ctx, object, opts...)
							fence.err = lost
							return err
						},
					}).
					Build()
				var err error
				store, err = NewCRDWorkflowStore(
					client,
					func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
				)
				if err != nil {
					t.Fatal(err)
				}
			}

			loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(original))
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Delete(WithLeaseFence(t.Context(), fence), loaded); !errors.Is(err, lost) {
				t.Fatalf("delete error = %v, want lease loss after delete", err)
			}

			if _, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(original)); !apierrors.IsNotFound(err) {
				t.Fatalf("durable workflow survived delete: %v", err)
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

func TestConfigMapWorkflowListIsolatesOperationKinds(t *testing.T) {
	copyObject := &v1alpha1.Copy{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "Copy"},
		ObjectMeta: metav1.ObjectMeta{Name: "copy", Namespace: "data"},
	}

	copyData, err := json.Marshal(copyObject)
	if err != nil {
		t.Fatal(err)
	}

	client := fake.NewClientset(
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      SessionConfigMapName("copy"),
				Namespace: "sessions",
				Labels:    sessionLabels("copy", "Copy"),
			},
			Data: map[string]string{SessionDataKey: string(copyData)},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      SessionConfigMapName("backup"),
				Namespace: "sessions",
				Labels:    sessionLabels("backup", "Backup"),
			},
			Data: map[string]string{SessionDataKey: "{broken"},
		},
	)

	store, err := NewConfigMapWorkflowStore(
		client,
		"sessions",
		func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	items, err := store.List(t.Context(), "data")
	if err != nil {
		t.Fatal(err)
	}

	if len(items) != 1 || items[0].Name != "copy" {
		t.Fatalf("operation list was blocked or mixed by another kind: %+v", items)
	}

	badCopy := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      SessionConfigMapName("bad-copy"),
			Namespace: "sessions",
			Labels:    sessionLabels("bad-copy", "Copy"),
		},
		Data: map[string]string{SessionDataKey: "{broken"},
	}
	if _, err := client.CoreV1().ConfigMaps("sessions").Create(
		t.Context(),
		badCopy,
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	if _, err := store.List(t.Context(), ""); err == nil {
		t.Fatal("corrupt record for the requested operation was silently ignored")
	}
}

func TestConfigMapWorkflowDeleteFencesCorruptPayload(t *testing.T) {
	object := storedRename()
	client := fake.NewClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            SessionConfigMapName(object.Name),
			Namespace:       "sessions",
			UID:             types.UID("replacement"),
			ResourceVersion: "2",
			Labels:          sessionLabels(object.Name, "Rename"),
		},
		Data: map[string]string{SessionDataKey: "{broken"},
	})

	store, err := NewConfigMapWorkflowStore(
		client,
		"sessions",
		func() *v1alpha1.Rename { return &v1alpha1.Rename{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Delete(t.Context(), object); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("corrupt payload bypassed storage fencing: %v", err)
	}

	if _, err := client.CoreV1().ConfigMaps("sessions").Get(
		t.Context(), SessionConfigMapName(object.Name), metav1.GetOptions{},
	); err != nil {
		t.Fatalf("fenced record was deleted: %v", err)
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

func TestCRDWorkflowProtectionReportsFenceLossAfterFinalizerWrite(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	original := storedRename()
	fence := &testLeaseFence{}
	lost := errors.New("lease lost after protection update")
	var updatedFinalizers []string
	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(original).
		WithObjects(original.DeepCopy()).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(
				ctx context.Context,
				client crclient.WithWatch,
				object crclient.Object,
				opts ...crclient.UpdateOption,
			) error {
				err := client.Update(ctx, object, opts...)
				updatedFinalizers = slices.Clone(object.GetFinalizers())
				fence.err = lost
				return err
			},
		}).
		Build()

	store, err := NewCRDWorkflowStore(client, func() *v1alpha1.Rename { return &v1alpha1.Rename{} })
	if err != nil {
		t.Fatal(err)
	}
	object, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(original))
	if err != nil {
		t.Fatal(err)
	}

	if err := store.EnsureProtection(WithLeaseFence(t.Context(), fence), object); !errors.Is(err, lost) {
		t.Fatalf("protection error = %v, want lease loss after finalizer write", err)
	}
	if !slices.Contains(updatedFinalizers, SessionFinalizer) {
		t.Fatal("protection finalizer was not persisted before reporting lease loss")
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

func TestCRDWorkflowStoreDeleteReportsFenceLossAfterFinalizerWrite(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	record := storedRename()
	record.Finalizers = []string{SessionFinalizer}
	now := metav1.Now()
	record.DeletionTimestamp = &now

	fence := &testLeaseFence{}
	lost := errors.New("lease lost after finalizer update")
	var updatedFinalizers []string
	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(record.DeepCopy()).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(
				ctx context.Context,
				client crclient.WithWatch,
				object crclient.Object,
				opts ...crclient.UpdateOption,
			) error {
				err := client.Update(ctx, object, opts...)
				updatedFinalizers = slices.Clone(object.GetFinalizers())
				fence.err = lost
				return err
			},
		}).
		Build()

	store, err := NewCRDWorkflowStore(client, func() *v1alpha1.Rename {
		return &v1alpha1.Rename{}
	})
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(record))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(WithLeaseFence(t.Context(), fence), loaded); !errors.Is(err, lost) {
		t.Fatalf("delete error = %v, want lease loss after finalizer write", err)
	}

	if slices.Contains(updatedFinalizers, SessionFinalizer) {
		t.Fatal("finalizer update was not persisted before reporting lease loss")
	}
}

type failingSessionLocker struct{}

func (failingSessionLocker) AcquireSessionLock(
	context.Context, string, string,
) (SessionLock, error) {
	return nil, ErrSessionNamespaceTerminating
}

func TestWithWorkflowLeaseDeletingObjectConvergesWhenLockUnavailable(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	now := metav1.Now()
	record := storedRename()
	record.Finalizers = []string{SessionFinalizer}
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

	locker := failingSessionLocker{}

	live := record.DeepCopy()
	if err := client.Get(t.Context(), crclient.ObjectKeyFromObject(live), live); err != nil {
		t.Fatal(err)
	}

	ran := false

	err = WithWorkflowLease(t.Context(), store, locker, record.Namespace, live, true,
		func(context.Context, SessionLock) error {
			ran = true
			return nil
		})
	if err != nil {
		t.Fatalf("deleting workflow must converge without a lock: %v", err)
	}

	if ran {
		t.Fatal("convergence must not run the operation body")
	}

	final := &v1alpha1.Rename{}
	if err := client.Get(
		t.Context(),
		crclient.ObjectKeyFromObject(record),
		final,
	); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("deleting workflow still exists: %v (err=%v)", final, err)
	}
}

func TestWithWorkflowLeaseClusterScopedDeletionConvergesWhenLockNamespaceTerminates(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	now := metav1.Now()
	record := &v1alpha1.ClusterCopy{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "cluster-copy",
			UID:               types.UID("copy-uid"),
			ResourceVersion:   "1",
			Finalizers:        []string{SessionFinalizer},
			DeletionTimestamp: &now,
		},
	}

	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(record.DeepCopy()).
		Build()

	store, err := NewCRDWorkflowStore(client, func() *v1alpha1.ClusterCopy {
		return &v1alpha1.ClusterCopy{}
	})
	if err != nil {
		t.Fatal(err)
	}

	err = WithWorkflowLease(
		t.Context(),
		store,
		failingSessionLocker{},
		"sessions",
		record,
		true,
		func(context.Context, SessionLock) error { return nil },
	)
	if err != nil {
		t.Fatalf("cluster-scoped deletion error = %v, want nil", err)
	}

	final := &v1alpha1.ClusterCopy{}
	if err := client.Get(t.Context(), crclient.ObjectKey{Name: record.Name}, final); err == nil {
		if slices.Contains(final.Finalizers, SessionFinalizer) {
			t.Fatal("cluster-scoped workflow finalizer was not removed")
		}
	} else if !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
}

type contendedSessionLocker struct{}

func (contendedSessionLocker) AcquireSessionLock(
	context.Context, string, string,
) (SessionLock, error) {
	return nil, domain.WrapError(
		domain.ErrorConflict,
		"acquire session lock",
		"workflow is already being changed",
		ErrSessionLockContention,
	)
}

func TestWithWorkflowLeaseDeletingObjectRetainsFinalizerOnLockContention(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	now := metav1.Now()
	record := storedRename()
	record.Finalizers = []string{SessionFinalizer}
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

	live := record.DeepCopy()
	if err := client.Get(t.Context(), crclient.ObjectKeyFromObject(live), live); err != nil {
		t.Fatal(err)
	}

	err = WithWorkflowLease(t.Context(), store, contendedSessionLocker{}, record.Namespace, live, true,
		func(context.Context, SessionLock) error { return nil })
	if !errors.Is(err, ErrSessionLockContention) {
		t.Fatalf("lock contention error = %v, want contention", err)
	}

	final := &v1alpha1.Rename{}
	if err := client.Get(t.Context(), crclient.ObjectKeyFromObject(record), final); err != nil {
		t.Fatal(err)
	}

	if !slices.Contains(final.Finalizers, SessionFinalizer) {
		t.Fatal("lock contention removed the workflow finalizer")
	}
}
