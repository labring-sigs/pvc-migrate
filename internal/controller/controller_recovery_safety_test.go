package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestControllerCRUIDChangesRejectPause(t *testing.T) {
	t.Run("VMCluster", func(t *testing.T) {
		replicas, ordinal := int32(1), int32(0)
		vm := vmClusterObject("new-uid", false)
		sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Namespace: "vm", Name: "vmstorage-metrics", UID: "sts-uid"}, Spec: appsv1.StatefulSetSpec{Replicas: &replicas}}
		dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), vm)
		manager := NewManager(fake.NewClientset(sts), dynamicClient, nil)
		session := controllerSession(domain.WorkloadSpec{
			Adapter:          domain.WorkloadVMCluster,
			Pod:              domain.ObjectReference{Namespace: "vm", Name: "vmstorage-metrics-0"},
			Controller:       domain.ObjectReference{Namespace: "vm", Name: sts.Name, UID: sts.UID},
			OriginalReplicas: &replicas,
			Ordinal:          &ordinal,
			VMCluster:        &domain.VMClusterSpec{APIVersion: vmClusterAPIVersion, Name: "metrics", UID: "old-uid", Component: "vmstorage"},
		})
		if err := manager.Pause(context.Background(), session); domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
		if countDynamicActions(dynamicClient.Actions(), "update", vmClusterResource) != 0 {
			t.Fatal("VMCluster was updated after UID replacement")
		}
	})

	t.Run("Grafana", func(t *testing.T) {
		replicas := int32(1)
		grafana := grafanaObject("new-uid", false)
		deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "vm", Name: "grafana", UID: "deployment-uid"}, Spec: appsv1.DeploymentSpec{Replicas: &replicas}}
		dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana)
		manager := NewManager(fake.NewClientset(deployment), dynamicClient, nil)
		session := controllerSession(domain.WorkloadSpec{
			Adapter:          domain.WorkloadGrafana,
			Pod:              domain.ObjectReference{Namespace: "vm", Name: "grafana-pod"},
			Controller:       domain.ObjectReference{Namespace: "vm", Name: deployment.Name, UID: deployment.UID},
			OriginalReplicas: &replicas,
			Grafana:          &domain.GrafanaSpec{APIVersion: grafanaAPIVersion, Name: "grafana", UID: "old-uid", OriginalReplicas: replicas},
		})
		if err := manager.Pause(context.Background(), session); domain.CategoryOf(err) != domain.ErrorConflict {
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
		sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Namespace: "vm", Name: "vmstorage-metrics", UID: "sts-uid"}, Spec: appsv1.StatefulSetSpec{Replicas: &currentReplicas}}
		dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), vm)
		manager := NewManager(fake.NewClientset(sts), dynamicClient, nil)
		session := controllerSession(domain.WorkloadSpec{
			Adapter:          domain.WorkloadVMCluster,
			Pod:              domain.ObjectReference{Namespace: "vm", Name: "vmstorage-metrics-1"},
			Controller:       domain.ObjectReference{Namespace: "vm", Name: sts.Name, UID: sts.UID},
			OriginalReplicas: &originalReplicas,
			Ordinal:          &ordinal,
			VMCluster:        &domain.VMClusterSpec{APIVersion: vmClusterAPIVersion, Name: "metrics", UID: "vm-uid", Component: "vmstorage"},
		})
		if err := manager.Pause(context.Background(), session); domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
		current, _ := dynamicClient.Resource(mustGVR(vmClusterAPIVersion, vmClusterResource)).Namespace("vm").Get(context.Background(), "metrics", metav1.GetOptions{})
		paused, _, _ := unstructured.NestedBool(current.Object, "spec", "vmstorage", "paused")
		if paused {
			t.Fatal("VMCluster pause state was not compensated")
		}
	})

	t.Run("Grafana pause", func(t *testing.T) {
		currentReplicas, originalReplicas := int32(2), int32(1)
		grafana := grafanaObject("grafana-uid", false)
		deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "vm", Name: "grafana", UID: "deployment-uid"}, Spec: appsv1.DeploymentSpec{Replicas: &currentReplicas}}
		dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana)
		manager := NewManager(fake.NewClientset(deployment), dynamicClient, nil)
		session := controllerSession(domain.WorkloadSpec{
			Adapter:          domain.WorkloadGrafana,
			Pod:              domain.ObjectReference{Namespace: "vm", Name: "grafana-pod"},
			Controller:       domain.ObjectReference{Namespace: "vm", Name: deployment.Name, UID: deployment.UID},
			OriginalReplicas: &originalReplicas,
			Grafana:          &domain.GrafanaSpec{APIVersion: grafanaAPIVersion, Name: "grafana", UID: "grafana-uid", OriginalReplicas: originalReplicas},
		})
		if err := manager.Pause(context.Background(), session); domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
		current, _ := dynamicClient.Resource(mustGVR(grafanaAPIVersion, grafanaResource)).Namespace("vm").Get(context.Background(), "grafana", metav1.GetOptions{})
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
		sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Namespace: "vm", Name: "vmstorage-metrics", UID: "sts-uid"}, Spec: appsv1.StatefulSetSpec{Replicas: &currentReplicas}}
		manager := NewManager(fake.NewClientset(sts), dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), vm), nil)
		session := controllerSession(domain.WorkloadSpec{
			Adapter:          domain.WorkloadVMCluster,
			Pod:              domain.ObjectReference{Namespace: "vm", Name: "vmstorage-metrics-1"},
			Controller:       domain.ObjectReference{Namespace: "vm", Name: sts.Name, UID: sts.UID},
			OriginalReplicas: &originalReplicas,
			Ordinal:          &ordinal,
			VMCluster:        &domain.VMClusterSpec{APIVersion: vmClusterAPIVersion, Name: "metrics", UID: "vm-uid", Component: "vmstorage"},
		})
		if err := manager.Resume(context.Background(), session); domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("Grafana", func(t *testing.T) {
		currentReplicas, originalReplicas := int32(2), int32(1)
		grafana := grafanaObject("grafana-uid", true)
		deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "vm", Name: "grafana", UID: "deployment-uid"}, Spec: appsv1.DeploymentSpec{Replicas: &currentReplicas}}
		manager := NewManager(fake.NewClientset(deployment), dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana), nil)
		session := controllerSession(domain.WorkloadSpec{
			Adapter:          domain.WorkloadGrafana,
			Pod:              domain.ObjectReference{Namespace: "vm", Name: "grafana-pod"},
			Controller:       domain.ObjectReference{Namespace: "vm", Name: deployment.Name, UID: deployment.UID},
			OriginalReplicas: &originalReplicas,
			Grafana:          &domain.GrafanaSpec{APIVersion: grafanaAPIVersion, Name: "grafana", UID: "grafana-uid", OriginalReplicas: originalReplicas},
		})
		if err := manager.Resume(context.Background(), session); domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
}

func TestVMClusterInvalidSessionDoesNotPauseCR(t *testing.T) {
	vm := vmClusterObject("vm-uid", false)
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), vm)
	manager := NewManager(fake.NewClientset(), dynamicClient, nil)
	session := controllerSession(domain.WorkloadSpec{
		Adapter:   domain.WorkloadVMCluster,
		Pod:       domain.ObjectReference{Namespace: "vm"},
		VMCluster: &domain.VMClusterSpec{APIVersion: vmClusterAPIVersion, Name: "metrics", UID: "vm-uid", Component: "vmstorage"},
	})
	if err := manager.Pause(context.Background(), session); domain.CategoryOf(err) != domain.ErrorInternal {
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
		Pod:     domain.ObjectReference{Namespace: "vm"},
		Grafana: &domain.GrafanaSpec{APIVersion: grafanaAPIVersion, Name: "grafana", UID: "grafana-uid", OriginalSuspend: true},
	})
	if err := manager.setGrafanaPaused(context.Background(), session, true); err != nil {
		t.Fatal(err)
	}
	updates := countDynamicActions(dynamicClient.Actions(), "update", grafanaResource)
	if err := manager.setGrafanaPaused(context.Background(), session, true); err != nil {
		t.Fatal(err)
	}
	if countDynamicActions(dynamicClient.Actions(), "update", grafanaResource) != updates {
		t.Fatal("idempotent Grafana pause issued an update")
	}
}

func TestGrafanaSuspendRestoresOmittedField(t *testing.T) {
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafanaObject("grafana-uid", false))
	object, err := dynamicClient.Resource(mustGVR(grafanaAPIVersion, grafanaResource)).Namespace("vm").Get(context.Background(), "grafana", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	unstructured.RemoveNestedField(object.Object, "spec", "suspend")
	if _, err := dynamicClient.Resource(mustGVR(grafanaAPIVersion, grafanaResource)).Namespace("vm").Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	manager := NewManager(fake.NewClientset(), dynamicClient, nil)
	session := controllerSession(domain.WorkloadSpec{
		Pod:     domain.ObjectReference{Namespace: "vm"},
		Grafana: &domain.GrafanaSpec{APIVersion: grafanaAPIVersion, Name: "grafana", UID: "grafana-uid", OriginalSuspend: false, OriginalSuspendConfigured: false},
	})
	if err := manager.setGrafanaPaused(context.Background(), session, true); err != nil {
		t.Fatal(err)
	}
	if err := manager.restoreGrafanaPause(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	current, err := dynamicClient.Resource(mustGVR(grafanaAPIVersion, grafanaResource)).Namespace("vm").Get(context.Background(), "grafana", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, found, nestedErr := unstructured.NestedFieldNoCopy(current.Object, "spec", "suspend"); nestedErr != nil || found {
		t.Fatalf("suspend field was added after restore: found=%t err=%v", found, nestedErr)
	}
}

func TestRestoreVMClusterPauseClearsOwnerAndAllowsNextSession(t *testing.T) {
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), vmClusterObject("vm-uid", true))
	manager := NewManager(fake.NewClientset(), dynamicClient, nil)
	session := controllerSession(domain.WorkloadSpec{
		Pod:       domain.ObjectReference{Namespace: "vm"},
		VMCluster: &domain.VMClusterSpec{APIVersion: vmClusterAPIVersion, Name: "metrics", UID: "vm-uid", Component: "vmstorage", OriginalPaused: true},
	})
	if err := manager.setVMClusterPaused(context.Background(), session, true); err != nil {
		t.Fatal(err)
	}
	if err := manager.restoreVMClusterPause(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	current, err := dynamicClient.Resource(mustGVR(vmClusterAPIVersion, vmClusterResource)).Namespace("vm").Get(context.Background(), "metrics", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if current.GetAnnotations()[pauseSessionAnnotation] != "" {
		t.Fatalf("pause owner=%q", current.GetAnnotations()[pauseSessionAnnotation])
	}

	next := controllerSession(domain.WorkloadSpec{
		Pod:       domain.ObjectReference{Namespace: "vm"},
		VMCluster: &domain.VMClusterSpec{APIVersion: vmClusterAPIVersion, Name: "metrics", UID: "vm-uid", Component: "vmstorage", OriginalPaused: true},
	})
	next.ID = "next-session"
	if err := manager.setVMClusterPaused(context.Background(), next, true); err != nil {
		t.Fatal(err)
	}
	current, err = dynamicClient.Resource(mustGVR(vmClusterAPIVersion, vmClusterResource)).Namespace("vm").Get(context.Background(), "metrics", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if current.GetAnnotations()[pauseSessionAnnotation] != next.ID {
		t.Fatalf("next pause owner=%q want=%q", current.GetAnnotations()[pauseSessionAnnotation], next.ID)
	}
}

func TestRestoreGrafanaPauseClearsOwnerAndAllowsNextSession(t *testing.T) {
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafanaObject("grafana-uid", true))
	manager := NewManager(fake.NewClientset(), dynamicClient, nil)
	session := controllerSession(domain.WorkloadSpec{
		Pod:     domain.ObjectReference{Namespace: "vm"},
		Grafana: &domain.GrafanaSpec{APIVersion: grafanaAPIVersion, Name: "grafana", UID: "grafana-uid", OriginalSuspend: true},
	})
	if err := manager.setGrafanaPaused(context.Background(), session, true); err != nil {
		t.Fatal(err)
	}
	if err := manager.restoreGrafanaPause(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	current, err := dynamicClient.Resource(mustGVR(grafanaAPIVersion, grafanaResource)).Namespace("vm").Get(context.Background(), "grafana", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if current.GetAnnotations()[pauseSessionAnnotation] != "" {
		t.Fatalf("pause owner=%q", current.GetAnnotations()[pauseSessionAnnotation])
	}

	next := controllerSession(domain.WorkloadSpec{
		Pod:     domain.ObjectReference{Namespace: "vm"},
		Grafana: &domain.GrafanaSpec{APIVersion: grafanaAPIVersion, Name: "grafana", UID: "grafana-uid", OriginalSuspend: true},
	})
	next.ID = "next-session"
	if err := manager.setGrafanaPaused(context.Background(), next, true); err != nil {
		t.Fatal(err)
	}
	current, err = dynamicClient.Resource(mustGVR(grafanaAPIVersion, grafanaResource)).Namespace("vm").Get(context.Background(), "grafana", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if current.GetAnnotations()[pauseSessionAnnotation] != next.ID {
		t.Fatalf("next pause owner=%q want=%q", current.GetAnnotations()[pauseSessionAnnotation], next.ID)
	}
}

func TestControllerPauseDetectsExternalCRPauseState(t *testing.T) {
	t.Run("VMCluster", func(t *testing.T) {
		vm := vmClusterObject("vm-uid", true)
		dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), vm)
		manager := NewManager(fake.NewClientset(), dynamicClient, nil)
		session := controllerSession(domain.WorkloadSpec{Pod: domain.ObjectReference{Namespace: "vm"}, VMCluster: &domain.VMClusterSpec{APIVersion: vmClusterAPIVersion, Name: "metrics", UID: "vm-uid", Component: "vmstorage", OriginalPaused: false}})
		if err := manager.setVMClusterPaused(context.Background(), session, true); domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("Grafana", func(t *testing.T) {
		grafana := grafanaObject("grafana-uid", true)
		dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana)
		manager := NewManager(fake.NewClientset(), dynamicClient, nil)
		session := controllerSession(domain.WorkloadSpec{Pod: domain.ObjectReference{Namespace: "vm"}, Grafana: &domain.GrafanaSpec{APIVersion: grafanaAPIVersion, Name: "grafana", UID: "grafana-uid", OriginalSuspend: false}})
		if err := manager.setGrafanaPaused(context.Background(), session, true); domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
}

func TestGrafanaSuspendOwnershipConflictUsesSuspendTerminology(t *testing.T) {
	grafana := grafanaObject("grafana-uid", false)
	grafana.SetAnnotations(map[string]string{pauseSessionAnnotation: "other-session"})
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana)
	manager := NewManager(fake.NewClientset(), dynamicClient, nil)
	session := controllerSession(domain.WorkloadSpec{Pod: domain.ObjectReference{Namespace: "vm"}, Grafana: &domain.GrafanaSpec{APIVersion: grafanaAPIVersion, Name: "grafana", UID: "grafana-uid", OriginalSuspend: false}})

	err := manager.setGrafanaPaused(context.Background(), session, true)
	if domain.CategoryOf(err) != domain.ErrorConflict || !strings.Contains(err.Error(), "suspend is owned") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestGrafanaResumeUsesCompleteDeploymentSelector(t *testing.T) {
	replicas := int32(0)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "vm", Name: "grafana", UID: "deployment-uid"},
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
	grafana := grafanaObject("grafana-uid", true)
	grafana.SetAnnotations(map[string]string{pauseSessionAnnotation: "session"})
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana)
	manager := NewManager(fake.NewClientset(deployment, wrong, right), dynamicClient, nil)
	manager.poll = time.Millisecond
	originalReplicas := int32(1)
	session := controllerSession(domain.WorkloadSpec{
		Adapter:          domain.WorkloadGrafana,
		Pod:              domain.ObjectReference{Namespace: "vm", Name: "old"},
		Controller:       domain.ObjectReference{Namespace: "vm", Name: deployment.Name, UID: deployment.UID},
		OriginalReplicas: &originalReplicas,
		Grafana:          &domain.GrafanaSpec{APIVersion: grafanaAPIVersion, Name: "grafana", UID: "grafana-uid", OriginalReplicas: originalReplicas},
	})
	if err := manager.Resume(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if session.Spec.Workload().Pod.Name != right.Name {
		t.Fatalf("resumed Pod=%s want=%s", session.Spec.Workload().Pod.Name, right.Name)
	}
}

func TestKubeBlocksStopDriftReturnsConflict(t *testing.T) {
	t.Run("before pause", func(t *testing.T) {
		cluster := kubeBlocksClusterObject(true)
		dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster)
		manager := NewManager(fake.NewClientset(), dynamicClient, nil)
		session := kubeBlocksRecoverySession(false)
		if err := manager.setKubeBlocksPaused(context.Background(), session, true); domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("after pause", func(t *testing.T) {
		ctx := context.Background()
		cluster := kubeBlocksClusterObject(false)
		dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster)
		manager := NewManager(fake.NewClientset(), dynamicClient, nil)
		session := kubeBlocksRecoverySession(false)
		if err := manager.setKubeBlocksPaused(ctx, session, true); err != nil {
			t.Fatal(err)
		}
		resource := dynamicClient.Resource(mustGVR(kubeBlocksClusterAPIVersion, clusterResource)).Namespace("db")
		current, _ := resource.Get(ctx, "cluster", metav1.GetOptions{})
		components, _, _ := unstructured.NestedSlice(current.Object, "spec", "componentSpecs")
		_ = unstructured.SetNestedField(components[0].(map[string]any), false, "stop")
		_ = unstructured.SetNestedField(current.Object, components, "spec", "componentSpecs")
		if _, err := resource.Update(ctx, current, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := manager.setKubeBlocksPaused(ctx, session, false); domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
}

func TestKubeBlocksPauseRetriesAPIServerConflictWithDriftCheck(t *testing.T) {
	cluster := kubeBlocksClusterObject(false)
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster)
	updates := 0
	dynamicClient.PrependReactor("update", "clusters", func(clienttesting.Action) (bool, runtime.Object, error) {
		updates++
		if updates == 1 {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "apps.kubeblocks.io", Resource: "clusters"}, "cluster", nil)
		}
		return false, nil, nil
	})
	manager := NewManager(fake.NewClientset(), dynamicClient, nil)
	session := kubeBlocksRecoverySession(false)
	if err := manager.setKubeBlocksPaused(context.Background(), session, true); err != nil {
		t.Fatal(err)
	}
	if updates != 2 {
		t.Fatalf("updates=%d want=2", updates)
	}
	current, _ := dynamicClient.Resource(mustGVR(kubeBlocksClusterAPIVersion, clusterResource)).Namespace("db").Get(context.Background(), "cluster", metav1.GetOptions{})
	if current.GetAnnotations()[pauseSessionAnnotation] != session.ID {
		t.Fatalf("annotations=%v", current.GetAnnotations())
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

func kubeBlocksClusterObject(stopped bool) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": kubeBlocksClusterAPIVersion,
		"kind":       "Cluster",
		"metadata":   map[string]any{"name": "cluster", "namespace": "db", "uid": "cluster-uid"},
		"spec": map[string]any{"componentSpecs": []any{
			map[string]any{"name": "postgresql", "stop": stopped},
		}},
	}}
}

func kubeBlocksRecoverySession(originalStopped bool) *domain.Session {
	session := kubeBlocksSession()
	session.Spec.Workload().KubeBlocks.Component = "postgresql"
	session.Spec.Workload().KubeBlocks.ClusterAPIVersion = kubeBlocksClusterAPIVersion
	session.Spec.Workload().KubeBlocks.ClusterUID = "cluster-uid"
	session.Spec.Workload().KubeBlocks.OriginalStops = map[string]bool{"postgresql": originalStopped}
	return session
}
