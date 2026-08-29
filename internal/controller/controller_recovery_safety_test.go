package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestControllerCRUIDChangesRejectPause(t *testing.T) {
	t.Run("VMCluster", func(t *testing.T) {
		replicas, ordinal := int32(1), int32(0)
		vm := vmClusterObject("new-uid", false)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "vm",
				Name:      "vmstorage-metrics",
				UID:       "sts-uid",
			},
			Spec: appsv1.StatefulSetSpec{Replicas: &replicas},
		}
		dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), vm)
		manager := NewManager(fake.NewClientset(sts), dynamicClient, nil)

		session := controllerSession(domain.WorkloadSpec{
			Adapter:          domain.WorkloadVMCluster,
			Pod:              domain.ObjectReference{Namespace: "vm", Name: "vmstorage-metrics-0"},
			Controller:       domain.ObjectReference{Namespace: "vm", Name: sts.Name, UID: sts.UID},
			OriginalReplicas: &replicas,
			Ordinal:          &ordinal,
			VMCluster: &domain.VMClusterSpec{
				APIVersion: vmClusterAPIVersion,
				Name:       "metrics",
				UID:        "old-uid",
				Component:  "vmstorage",
			},
		})
		if err := manager.Pause(
			context.Background(),
			session,
		); domain.CategoryOf(
			err,
		) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}

		if countDynamicActions(dynamicClient.Actions(), "update", vmClusterResource) != 0 {
			t.Fatal("VMCluster was updated after UID replacement")
		}
	})

	t.Run("Grafana", func(t *testing.T) {
		replicas := int32(1)
		grafana := grafanaObject("new-uid", false)
		deployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "vm", Name: "grafana", UID: "deployment-uid"},
			Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		}
		dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana)
		manager := NewManager(fake.NewClientset(deployment), dynamicClient, nil)

		session := controllerSession(domain.WorkloadSpec{
			Adapter: domain.WorkloadGrafana,
			Pod:     domain.ObjectReference{Namespace: "vm", Name: "grafana-pod"},
			Controller: domain.ObjectReference{
				Namespace: "vm",
				Name:      deployment.Name,
				UID:       deployment.UID,
			},
			OriginalReplicas: &replicas,
			Grafana: &domain.GrafanaSpec{
				APIVersion:       grafanaAPIVersion,
				Name:             "grafana",
				UID:              "old-uid",
				OriginalReplicas: replicas,
			},
		})
		if err := manager.Pause(
			context.Background(),
			session,
		); domain.CategoryOf(
			err,
		) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}

		if countDynamicActions(dynamicClient.Actions(), "update", grafanaResource) != 0 {
			t.Fatal("Grafana was updated after UID replacement")
		}
	})
}

func TestControllerScaleConflictsPreserveCategoryAndPauseState(t *testing.T) {
	t.Run("VMCluster pause", func(t *testing.T) {
		currentReplicas, originalReplicas, ordinal := int32(3), int32(2), int32(1)
		vm := vmClusterObject("vm-uid", false)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "vm",
				Name:      "vmstorage-metrics",
				UID:       "sts-uid",
			},
			Spec: appsv1.StatefulSetSpec{Replicas: &currentReplicas},
		}
		dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), vm)
		manager := NewManager(fake.NewClientset(sts), dynamicClient, nil)

		session := controllerSession(domain.WorkloadSpec{
			Adapter:          domain.WorkloadVMCluster,
			Pod:              domain.ObjectReference{Namespace: "vm", Name: "vmstorage-metrics-1"},
			Controller:       domain.ObjectReference{Namespace: "vm", Name: sts.Name, UID: sts.UID},
			OriginalReplicas: &originalReplicas,
			Ordinal:          &ordinal,
			VMCluster: &domain.VMClusterSpec{
				APIVersion: vmClusterAPIVersion,
				Name:       "metrics",
				UID:        "vm-uid",
				Component:  "vmstorage",
			},
		})
		if err := manager.Pause(
			context.Background(),
			session,
		); domain.CategoryOf(
			err,
		) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}

		current, _ := dynamicClient.Resource(mustGVR(vmClusterAPIVersion, vmClusterResource)).
			Namespace("vm").
			Get(context.Background(), "metrics", metav1.GetOptions{})

		paused, _, _ := unstructured.NestedBool(current.Object, "spec", "vmstorage", "paused")
		if paused {
			t.Fatal("VMCluster pause state was not compensated")
		}
	})

	t.Run("Grafana pause", func(t *testing.T) {
		currentReplicas, originalReplicas := int32(2), int32(1)
		grafana := grafanaObject("grafana-uid", false)
		deployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "vm", Name: "grafana", UID: "deployment-uid"},
			Spec:       appsv1.DeploymentSpec{Replicas: &currentReplicas},
		}
		dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana)
		manager := NewManager(fake.NewClientset(deployment), dynamicClient, nil)

		session := controllerSession(domain.WorkloadSpec{
			Adapter: domain.WorkloadGrafana,
			Pod:     domain.ObjectReference{Namespace: "vm", Name: "grafana-pod"},
			Controller: domain.ObjectReference{
				Namespace: "vm",
				Name:      deployment.Name,
				UID:       deployment.UID,
			},
			OriginalReplicas: &originalReplicas,
			Grafana: &domain.GrafanaSpec{
				APIVersion:       grafanaAPIVersion,
				Name:             "grafana",
				UID:              "grafana-uid",
				OriginalReplicas: originalReplicas,
			},
		})
		if err := manager.Pause(
			context.Background(),
			session,
		); domain.CategoryOf(
			err,
		) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}

		current, _ := dynamicClient.Resource(mustGVR(grafanaAPIVersion, grafanaResource)).
			Namespace("vm").
			Get(context.Background(), "grafana", metav1.GetOptions{})

		suspended, _, _ := unstructured.NestedBool(current.Object, "spec", "suspend")
		if suspended {
			t.Fatal("Grafana suspend state was not compensated")
		}
	})
}

func TestControllerResumeScaleConflictsPreserveCategory(t *testing.T) {
	t.Run("VMCluster", func(t *testing.T) {
		currentReplicas, originalReplicas, ordinal := int32(3), int32(2), int32(1)
		vm := vmClusterObject("vm-uid", true)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "vm",
				Name:      "vmstorage-metrics",
				UID:       "sts-uid",
			},
			Spec: appsv1.StatefulSetSpec{Replicas: &currentReplicas},
		}
		manager := NewManager(
			fake.NewClientset(sts),
			dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), vm),
			nil,
		)

		session := controllerSession(domain.WorkloadSpec{
			Adapter:          domain.WorkloadVMCluster,
			Pod:              domain.ObjectReference{Namespace: "vm", Name: "vmstorage-metrics-1"},
			Controller:       domain.ObjectReference{Namespace: "vm", Name: sts.Name, UID: sts.UID},
			OriginalReplicas: &originalReplicas,
			Ordinal:          &ordinal,
			VMCluster: &domain.VMClusterSpec{
				APIVersion: vmClusterAPIVersion,
				Name:       "metrics",
				UID:        "vm-uid",
				Component:  "vmstorage",
			},
		})
		if err := manager.Resume(
			context.Background(),
			session,
		); domain.CategoryOf(
			err,
		) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("Grafana", func(t *testing.T) {
		currentReplicas, originalReplicas := int32(2), int32(1)
		grafana := grafanaObject("grafana-uid", true)
		deployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "vm", Name: "grafana", UID: "deployment-uid"},
			Spec:       appsv1.DeploymentSpec{Replicas: &currentReplicas},
		}
		manager := NewManager(
			fake.NewClientset(deployment),
			dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana),
			nil,
		)

		session := controllerSession(domain.WorkloadSpec{
			Adapter: domain.WorkloadGrafana,
			Pod:     domain.ObjectReference{Namespace: "vm", Name: "grafana-pod"},
			Controller: domain.ObjectReference{
				Namespace: "vm",
				Name:      deployment.Name,
				UID:       deployment.UID,
			},
			OriginalReplicas: &originalReplicas,
			Grafana: &domain.GrafanaSpec{
				APIVersion:       grafanaAPIVersion,
				Name:             "grafana",
				UID:              "grafana-uid",
				OriginalReplicas: originalReplicas,
			},
		})
		if err := manager.Resume(
			context.Background(),
			session,
		); domain.CategoryOf(
			err,
		) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
}

func TestVMClusterInvalidSessionDoesNotPauseCR(t *testing.T) {
	vm := vmClusterObject("vm-uid", false)
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), vm)
	manager := NewManager(fake.NewClientset(), dynamicClient, nil)

	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadVMCluster,
		Pod:     domain.ObjectReference{Namespace: "vm"},
		VMCluster: &domain.VMClusterSpec{
			APIVersion: vmClusterAPIVersion,
			Name:       "metrics",
			UID:        "vm-uid",
			Component:  "vmstorage",
		},
	})
	if err := manager.Pause(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorInternal {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if countDynamicActions(dynamicClient.Actions(), "update", vmClusterResource) != 0 {
		t.Fatal("invalid session updated VMCluster")
	}
}

func TestGrafanaPauseNoOpAvoidsUpdates(t *testing.T) {
	grafana := grafanaObject("grafana-uid", true)
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana)
	manager := NewManager(fake.NewClientset(), dynamicClient, nil)

	session := controllerSession(domain.WorkloadSpec{
		Pod: domain.ObjectReference{Namespace: "vm"},
		Grafana: &domain.GrafanaSpec{
			APIVersion:      grafanaAPIVersion,
			Name:            "grafana",
			UID:             "grafana-uid",
			OriginalSuspend: true,
		},
	})
	if err := manager.setGrafanaPaused(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	updates := countDynamicActions(dynamicClient.Actions(), "update", grafanaResource)

	if err := manager.setGrafanaPaused(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if countDynamicActions(dynamicClient.Actions(), "update", grafanaResource) != updates {
		t.Fatal("idempotent Grafana pause issued an update")
	}
}

func TestGrafanaSuspendRestoresOmittedField(t *testing.T) {
	dynamicClient := dynamicfake.NewSimpleDynamicClient(
		runtime.NewScheme(),
		grafanaObject("grafana-uid", false),
	)

	object, err := dynamicClient.Resource(mustGVR(grafanaAPIVersion, grafanaResource)).
		Namespace("vm").
		Get(context.Background(), "grafana", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	unstructured.RemoveNestedField(object.Object, "spec", "suspend")

	if _, err := dynamicClient.Resource(mustGVR(grafanaAPIVersion, grafanaResource)).
		Namespace("vm").
		Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	manager := NewManager(fake.NewClientset(), dynamicClient, nil)

	session := controllerSession(domain.WorkloadSpec{
		Pod: domain.ObjectReference{Namespace: "vm"},
		Grafana: &domain.GrafanaSpec{
			APIVersion:                grafanaAPIVersion,
			Name:                      "grafana",
			UID:                       "grafana-uid",
			OriginalSuspend:           false,
			OriginalSuspendConfigured: false,
		},
	})
	if err := manager.setGrafanaPaused(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if err := manager.restoreGrafanaPause(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	current, err := dynamicClient.Resource(mustGVR(grafanaAPIVersion, grafanaResource)).
		Namespace("vm").
		Get(context.Background(), "grafana", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, found, nestedErr := unstructured.NestedFieldNoCopy(
		current.Object,
		"spec",
		"suspend",
	); nestedErr != nil ||
		found {
		t.Fatalf("suspend field was added after restore: found=%t err=%v", found, nestedErr)
	}
}

func TestRestoreVMClusterPauseClearsOwnerAndAllowsNextSession(t *testing.T) {
	dynamicClient := dynamicfake.NewSimpleDynamicClient(
		runtime.NewScheme(),
		vmClusterObject("vm-uid", true),
	)
	manager := NewManager(fake.NewClientset(), dynamicClient, nil)

	session := controllerSession(domain.WorkloadSpec{
		Pod: domain.ObjectReference{Namespace: "vm"},
		VMCluster: &domain.VMClusterSpec{
			APIVersion:     vmClusterAPIVersion,
			Name:           "metrics",
			UID:            "vm-uid",
			Component:      "vmstorage",
			OriginalPaused: true,
		},
	})
	if err := manager.setVMClusterPaused(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if err := manager.restoreVMClusterPause(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	current, err := dynamicClient.Resource(mustGVR(vmClusterAPIVersion, vmClusterResource)).
		Namespace("vm").
		Get(context.Background(), "metrics", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if current.GetAnnotations()[pauseSessionAnnotation] != "" {
		t.Fatalf("pause owner=%q", current.GetAnnotations()[pauseSessionAnnotation])
	}

	next := controllerSession(domain.WorkloadSpec{
		Pod: domain.ObjectReference{Namespace: "vm"},
		VMCluster: &domain.VMClusterSpec{
			APIVersion:     vmClusterAPIVersion,
			Name:           "metrics",
			UID:            "vm-uid",
			Component:      "vmstorage",
			OriginalPaused: true,
		},
	})

	next.ID = "next-session"
	if err := manager.setVMClusterPaused(context.Background(), next); err != nil {
		t.Fatal(err)
	}

	current, err = dynamicClient.Resource(mustGVR(vmClusterAPIVersion, vmClusterResource)).
		Namespace("vm").
		Get(context.Background(), "metrics", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if current.GetAnnotations()[pauseSessionAnnotation] != next.ID {
		t.Fatalf(
			"next pause owner=%q want=%q",
			current.GetAnnotations()[pauseSessionAnnotation],
			next.ID,
		)
	}
}

func TestRestoreGrafanaPauseClearsOwnerAndAllowsNextSession(t *testing.T) {
	dynamicClient := dynamicfake.NewSimpleDynamicClient(
		runtime.NewScheme(),
		grafanaObject("grafana-uid", true),
	)
	manager := NewManager(fake.NewClientset(), dynamicClient, nil)

	session := controllerSession(domain.WorkloadSpec{
		Pod: domain.ObjectReference{Namespace: "vm"},
		Grafana: &domain.GrafanaSpec{
			APIVersion:      grafanaAPIVersion,
			Name:            "grafana",
			UID:             "grafana-uid",
			OriginalSuspend: true,
		},
	})
	if err := manager.setGrafanaPaused(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if err := manager.restoreGrafanaPause(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	current, err := dynamicClient.Resource(mustGVR(grafanaAPIVersion, grafanaResource)).
		Namespace("vm").
		Get(context.Background(), "grafana", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if current.GetAnnotations()[pauseSessionAnnotation] != "" {
		t.Fatalf("pause owner=%q", current.GetAnnotations()[pauseSessionAnnotation])
	}

	next := controllerSession(domain.WorkloadSpec{
		Pod: domain.ObjectReference{Namespace: "vm"},
		Grafana: &domain.GrafanaSpec{
			APIVersion:      grafanaAPIVersion,
			Name:            "grafana",
			UID:             "grafana-uid",
			OriginalSuspend: true,
		},
	})

	next.ID = "next-session"
	if err := manager.setGrafanaPaused(context.Background(), next); err != nil {
		t.Fatal(err)
	}

	current, err = dynamicClient.Resource(mustGVR(grafanaAPIVersion, grafanaResource)).
		Namespace("vm").
		Get(context.Background(), "grafana", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if current.GetAnnotations()[pauseSessionAnnotation] != next.ID {
		t.Fatalf(
			"next pause owner=%q want=%q",
			current.GetAnnotations()[pauseSessionAnnotation],
			next.ID,
		)
	}
}

func TestControllerPauseDetectsExternalCRPauseState(t *testing.T) {
	t.Run("VMCluster", func(t *testing.T) {
		vm := vmClusterObject("vm-uid", true)
		dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), vm)
		manager := NewManager(fake.NewClientset(), dynamicClient, nil)

		session := controllerSession(
			domain.WorkloadSpec{
				Pod: domain.ObjectReference{Namespace: "vm"},
				VMCluster: &domain.VMClusterSpec{
					APIVersion:     vmClusterAPIVersion,
					Name:           "metrics",
					UID:            "vm-uid",
					Component:      "vmstorage",
					OriginalPaused: false,
				},
			},
		)
		if err := manager.setVMClusterPaused(
			context.Background(),
			session,
		); domain.CategoryOf(
			err,
		) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("Grafana", func(t *testing.T) {
		grafana := grafanaObject("grafana-uid", true)
		dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana)
		manager := NewManager(fake.NewClientset(), dynamicClient, nil)

		session := controllerSession(
			domain.WorkloadSpec{
				Pod: domain.ObjectReference{Namespace: "vm"},
				Grafana: &domain.GrafanaSpec{
					APIVersion:      grafanaAPIVersion,
					Name:            "grafana",
					UID:             "grafana-uid",
					OriginalSuspend: false,
				},
			},
		)
		if err := manager.setGrafanaPaused(
			context.Background(),
			session,
		); domain.CategoryOf(
			err,
		) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
}

func TestGrafanaSuspendOwnershipConflictUsesSuspendTerminology(t *testing.T) {
	grafana := grafanaObject("grafana-uid", false)
	grafana.SetAnnotations(map[string]string{pauseSessionAnnotation: "other-session"})
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana)
	manager := NewManager(fake.NewClientset(), dynamicClient, nil)
	session := controllerSession(
		domain.WorkloadSpec{
			Pod: domain.ObjectReference{Namespace: "vm"},
			Grafana: &domain.GrafanaSpec{
				APIVersion:      grafanaAPIVersion,
				Name:            "grafana",
				UID:             "grafana-uid",
				OriginalSuspend: false,
			},
		},
	)

	err := manager.setGrafanaPaused(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "suspend is owned") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestGrafanaResumeUsesCompleteDeploymentSelector(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	replicas := int32(0)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "vm",
			Name:      "grafana",
			UID:       "deployment-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: grafanaAPIVersion,
				Kind:       domain.KindGrafana,
				Name:       "grafana",
				UID:        "grafana-uid",
				Controller: new(true),
			}},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "grafana"},
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key: "tenant", Operator: metav1.LabelSelectorOpIn, Values: []string{"selected"},
				}},
			},
		},
	}
	wrong := readyPod("vm", "wrong", "node-a")
	wrong.Labels = map[string]string{"app": "grafana", "tenant": "other"}
	right := readyPod("vm", "right", "node-a")
	right.Labels = map[string]string{"app": "grafana", "tenant": "selected"}
	replicaSet := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: "vm",
		Name:      "grafana-rs",
		UID:       "grafana-rs-uid",
		OwnerReferences: []metav1.OwnerReference{
			{
				APIVersion: domain.AppsAPIVersion,
				Kind:       domain.KindDeployment,
				Name:       deployment.Name,
				UID:        deployment.UID,
				Controller: new(true),
			},
		},
	}}
	right.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: domain.AppsAPIVersion,
			Kind:       domain.KindReplicaSet,
			Name:       replicaSet.Name,
			UID:        replicaSet.UID,
			Controller: new(true),
		},
	}
	grafana := grafanaObject("grafana-uid", true)
	grafana.SetAnnotations(map[string]string{pauseSessionAnnotation: "session"})
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana)
	typedClient := fake.NewClientset(deployment, replicaSet, wrong, right)
	typedClient.PrependReactor(
		"update",
		"deployments",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			updated := testutil.MustActionObject[*appsv1.Deployment](t, action)
			replicas := deploymentReplicas(updated)
			updated.Status = appsv1.DeploymentStatus{
				ObservedGeneration: updated.Generation,
				Replicas:           replicas,
				UpdatedReplicas:    replicas,
				ReadyReplicas:      replicas,
				AvailableReplicas:  replicas,
			}

			return false, nil, nil
		},
	)
	manager := NewManager(
		typedClient,
		dynamicClient,
		nil,
	)
	manager.poll = time.Millisecond
	originalReplicas := int32(1)

	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadGrafana,
		Pod:     domain.ObjectReference{Namespace: "vm", Name: "old", UID: "old-uid"},
		Controller: domain.ObjectReference{
			Namespace: "vm",
			Name:      deployment.Name,
			UID:       deployment.UID,
		},
		OriginalReplicas: &originalReplicas,
		AffectedPods:     []domain.ObjectReference{{Namespace: "vm", Name: "old", UID: "old-uid"}},
		Grafana: &domain.GrafanaSpec{
			APIVersion:       grafanaAPIVersion,
			Name:             "grafana",
			UID:              "grafana-uid",
			OriginalReplicas: originalReplicas,
		},
	})
	if err := manager.Resume(ctx, session); err != nil {
		t.Fatal(err)
	}

	workload := session.Spec.Workload()
	if workload.Pod.Name != right.Name || workload.Pod.UID != right.UID {
		t.Fatalf("resumed Pod=%+v want=%s/%s", workload.Pod, right.Name, right.UID)
	}

	if len(workload.AffectedPods) != 1 || workload.AffectedPods[0].Name != right.Name ||
		workload.AffectedPods[0].UID != right.UID {
		t.Fatalf("affected Pods=%+v want=%s/%s", workload.AffectedPods, right.Name, right.UID)
	}
}

func TestVMClusterReplicaDriftIsNotOverwritten(t *testing.T) {
	vm := vmClusterObject("vm-uid", true)
	vm.SetAnnotations(map[string]string{pauseSessionAnnotation: "session"})

	if err := unstructured.SetNestedField(
		vm.Object,
		int64(3),
		"spec",
		"vmstorage",
		"replicaCount",
	); err != nil {
		t.Fatal(err)
	}

	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), vm)
	manager := NewManager(fake.NewClientset(), dynamicClient, nil)

	session := controllerSession(domain.WorkloadSpec{
		Pod: domain.ObjectReference{Namespace: "vm"},
		VMCluster: &domain.VMClusterSpec{
			APIVersion: vmClusterAPIVersion, Name: "metrics", UID: "vm-uid", Component: "vmstorage",
			OriginalReplicas: 2, OriginalReplicasConfigured: true,
		},
	})
	if err := manager.setVMClusterReplicaCount(
		context.Background(),
		session,
		1,
		2,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	current, err := dynamicClient.Resource(mustGVR(vmClusterAPIVersion, vmClusterResource)).
		Namespace("vm").
		Get(context.Background(), "metrics", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if replicas, _, _ := unstructured.NestedInt64(
		current.Object,
		"spec",
		"vmstorage",
		"replicaCount",
	); replicas != 3 {
		t.Fatalf("replicaCount=%d want=3", replicas)
	}
}

func TestRestoreVMClusterPauseRejectsActiveSessionDrift(t *testing.T) {
	replicaDrift := int64(3)
	pausedOrdinal := int32(1)

	tests := []struct {
		name             string
		paused           bool
		replicaCount     *int64
		originalPaused   bool
		originalReplicas int32
		ordinal          *int32
		configured       bool
	}{
		{
			name:             "replica count changed from pause ordinal",
			paused:           true,
			replicaCount:     &replicaDrift,
			originalReplicas: 2,
			ordinal:          &pausedOrdinal,
			configured:       true,
		},
		{
			name:           "paused state changed to original false",
			paused:         false,
			originalPaused: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vm := vmClusterObject("vm-uid", tt.paused)
			vm.SetAnnotations(map[string]string{pauseSessionAnnotation: "session"})

			if tt.replicaCount != nil {
				if err := unstructured.SetNestedField(
					vm.Object,
					*tt.replicaCount,
					"spec",
					"vmstorage",
					"replicaCount",
				); err != nil {
					t.Fatal(err)
				}
			}

			dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), vm)
			manager := NewManager(fake.NewClientset(), dynamicClient, nil)

			session := controllerSession(domain.WorkloadSpec{
				Pod:     domain.ObjectReference{Namespace: "vm"},
				Ordinal: tt.ordinal,
				VMCluster: &domain.VMClusterSpec{
					APIVersion:                 vmClusterAPIVersion,
					Name:                       "metrics",
					UID:                        "vm-uid",
					Component:                  "vmstorage",
					OriginalPaused:             tt.originalPaused,
					OriginalReplicas:           tt.originalReplicas,
					OriginalReplicasConfigured: tt.configured,
				},
			})
			if err := manager.restoreVMClusterPause(
				context.Background(),
				session,
			); domain.CategoryOf(
				err,
			) != domain.ErrorConflict {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}

			current, err := dynamicClient.Resource(mustGVR(vmClusterAPIVersion, vmClusterResource)).
				Namespace("vm").
				Get(context.Background(), "metrics", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			if owner := current.GetAnnotations()[pauseSessionAnnotation]; owner != "session" {
				t.Fatalf("pause owner=%q want session", owner)
			}
		})
	}
}

func TestRestoreGrafanaPauseRejectsActiveSessionDrift(t *testing.T) {
	grafana := grafanaObject("grafana-uid", false)
	grafana.SetAnnotations(map[string]string{pauseSessionAnnotation: "session"})
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana)
	manager := NewManager(fake.NewClientset(), dynamicClient, nil)

	session := controllerSession(domain.WorkloadSpec{
		Pod: domain.ObjectReference{Namespace: "vm"},
		Grafana: &domain.GrafanaSpec{
			APIVersion:      grafanaAPIVersion,
			Name:            "grafana",
			UID:             "grafana-uid",
			OriginalSuspend: false,
		},
	})
	if err := manager.restoreGrafanaPause(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	current, err := dynamicClient.Resource(mustGVR(grafanaAPIVersion, grafanaResource)).
		Namespace("vm").
		Get(context.Background(), "grafana", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if owner := current.GetAnnotations()[pauseSessionAnnotation]; owner != "session" {
		t.Fatalf("pause owner=%q want session", owner)
	}
}

func TestClearVictoriaLogsPauseOwnerRejectsReplicaDrift(t *testing.T) {
	currentReplicas := int32(3)
	originalReplicas := int32(2)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "logs",
			Name:        "victoria-logs-vlstorage",
			UID:         types.UID("sts-uid"),
			Annotations: map[string]string{pauseSessionAnnotation: "session"},
		},
		Spec: appsv1.StatefulSetSpec{Replicas: &currentReplicas},
	}
	manager := NewManager(fake.NewClientset(sts), nil, nil)

	session := controllerSession(domain.WorkloadSpec{
		Controller:       domain.ObjectReference{Namespace: "logs", Name: sts.Name, UID: sts.UID},
		OriginalReplicas: &originalReplicas,
	})
	if err := manager.clearVictoriaLogsPauseOwner(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	current, err := manager.typed.AppsV1().
		StatefulSets("logs").
		Get(context.Background(), sts.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if owner := current.GetAnnotations()[pauseSessionAnnotation]; owner != "session" {
		t.Fatalf("pause owner=%q want session", owner)
	}

	if replicas := statefulSetReplicas(current); replicas != currentReplicas {
		t.Fatalf("replicas=%d want %d", replicas, currentReplicas)
	}
}

func vmClusterObject(uid types.UID, paused bool) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": vmClusterAPIVersion,
		"kind":       "VMCluster",
		"metadata":   map[string]any{"name": "metrics", "namespace": "vm", "uid": string(uid)},
		"spec":       map[string]any{"vmstorage": map[string]any{"paused": paused}},
	}}
}

func grafanaObject(uid types.UID, suspended bool) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": grafanaAPIVersion,
		"kind":       "Grafana",
		"metadata":   map[string]any{"name": "grafana", "namespace": "vm", "uid": string(uid)},
		"spec":       map[string]any{"suspend": suspended},
	}}
}
