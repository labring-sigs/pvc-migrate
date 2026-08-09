package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	discoveryfake "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func mustGVR(apiVersion, resource string) schema.GroupVersionResource {
	gvr, err := kube.ParseGroupVersionResource(apiVersion, resource)
	if err != nil {
		panic(err)
	}
	return gvr
}

func TestVMClusterUsesComponentPauseAndStatefulSetScale(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	replicas := int32(2)
	vm := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": vmClusterAPIVersion,
		"kind":       "VMCluster",
		"metadata":   map[string]any{"name": "metrics", "namespace": "vm", "uid": "vm-uid"},
		"spec":       map[string]any{"vmstorage": map[string]any{"replicaCount": int64(2), "paused": false}},
	}}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "vm", Name: "vmstorage-metrics", UID: types.UID("sts-uid"), OwnerReferences: []metav1.OwnerReference{{APIVersion: vmClusterAPIVersion, Kind: "VMCluster", Name: "metrics", UID: "vm-uid", Controller: boolPointer(true)}}},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
	}
	pod := readyPod("vm", "vmstorage-metrics-1", "node-a")
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: sts.Name, UID: sts.UID, Controller: boolPointer(true)}}
	client := fake.NewClientset(sts, pod)
	podsResource := corev1.SchemeGroupVersion.WithResource("pods")
	client.PrependReactor("update", "statefulsets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		updated := action.(clienttesting.UpdateAction).GetObject().(*appsv1.StatefulSet)
		if *updated.Spec.Replicas == 1 {
			_ = client.Tracker().Delete(podsResource, "vm", pod.Name)
		} else {
			_ = client.Tracker().Create(podsResource, readyPod("vm", pod.Name, "node-b"), "vm")
		}
		return false, nil, nil
	})
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), vm)
	manager := NewManager(client, dynamicClient, client.Discovery())
	manager.poll = time.Millisecond
	workload, err := manager.Discover(ctx, DiscoverOptions{Namespace: "vm", PodName: pod.Name, AllowLeaderDowntime: true})
	if err != nil {
		t.Fatal(err)
	}
	if workload.Adapter != domain.WorkloadVMCluster || workload.VMCluster == nil || workload.VMCluster.Component != "vmstorage" {
		t.Fatalf("workload=%#v", workload)
	}
	session := controllerSession(workload)
	session.Spec.WorkflowOptionsPtr().TargetNode = "node-b"
	session.Status.Phase = domain.PhasePausing
	if err := manager.Pause(ctx, session); err != nil {
		t.Fatal(err)
	}
	paused, err := dynamicClient.Resource(mustGVR(vmClusterAPIVersion, vmClusterResource)).Namespace("vm").Get(ctx, "metrics", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, _, _ := unstructured.NestedBool(paused.Object, "spec", "vmstorage", "paused"); !got {
		t.Fatal("vmstorage was not paused")
	}
	if got, found, nestedErr := unstructured.NestedInt64(paused.Object, "spec", "vmstorage", "replicaCount"); nestedErr != nil || !found || got != 1 {
		t.Fatalf("paused vmstorage replicaCount=%d found=%t err=%v, want ordinal 1", got, found, nestedErr)
	}
	pausedSTS, err := client.AppsV1().StatefulSets("vm").Get(ctx, sts.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := statefulSetReplicas(pausedSTS); got != 1 {
		t.Fatalf("paused StatefulSet replicas=%d, want ordinal 1", got)
	}
	if err := manager.Resume(ctx, session); err != nil {
		t.Fatal(err)
	}
	resumed, err := dynamicClient.Resource(mustGVR(vmClusterAPIVersion, vmClusterResource)).Namespace("vm").Get(ctx, "metrics", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, _, _ := unstructured.NestedBool(resumed.Object, "spec", "vmstorage", "paused"); got {
		t.Fatal("vmstorage remained paused")
	}
	if got, found, nestedErr := unstructured.NestedInt64(resumed.Object, "spec", "vmstorage", "replicaCount"); nestedErr != nil || !found || got != 2 {
		t.Fatalf("resumed vmstorage replicaCount=%d found=%t err=%v, want 2", got, found, nestedErr)
	}
	if resumed.GetAnnotations()[pauseSessionAnnotation] != "" {
		t.Fatalf("vmstorage pause owner=%q", resumed.GetAnnotations()[pauseSessionAnnotation])
	}
}

func TestGrafanaUsesCRSuspendAndDeploymentScale(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	replicas := int32(1)
	grafana := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": grafanaAPIVersion,
		"kind":       "Grafana",
		"metadata":   map[string]any{"name": "grafana", "namespace": "vm", "uid": "grafana-uid"},
		"spec":       map[string]any{"suspend": false, "deployment": map[string]any{"spec": map[string]any{"replicas": int64(1)}}},
	}}
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "vm", Name: "grafana-deployment", UID: types.UID("deployment-uid"), OwnerReferences: []metav1.OwnerReference{{APIVersion: grafanaAPIVersion, Kind: "Grafana", Name: "grafana", UID: "grafana-uid", Controller: boolPointer(true)}}},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "grafana"}}},
	}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: "vm", Name: "grafana-rs", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: deployment.Name, UID: deployment.UID, Controller: boolPointer(true)}}}}
	pod := readyPod("vm", "grafana-pod", "node-a")
	pod.Labels = map[string]string{"app": "grafana"}
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, Controller: boolPointer(true)}}
	client := fake.NewClientset(deployment, rs, pod)
	podsResource := corev1.SchemeGroupVersion.WithResource("pods")
	client.PrependReactor("update", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
		updated := action.(clienttesting.UpdateAction).GetObject().(*appsv1.Deployment)
		if *updated.Spec.Replicas == 0 {
			_ = client.Tracker().Delete(podsResource, "vm", pod.Name)
		} else {
			resumed := readyPod("vm", pod.Name, "node-b")
			resumed.Labels = map[string]string{"app": "grafana"}
			_ = client.Tracker().Create(podsResource, resumed, "vm")
		}
		return false, nil, nil
	})
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), grafana)
	manager := NewManager(client, dynamicClient, client.Discovery())
	manager.poll = time.Millisecond
	workload, err := manager.Discover(ctx, DiscoverOptions{Namespace: "vm", PodName: pod.Name})
	if err != nil {
		t.Fatal(err)
	}
	if workload.Adapter != domain.WorkloadGrafana || workload.Grafana == nil {
		t.Fatalf("workload=%#v", workload)
	}
	session := controllerSession(workload)
	session.Status.Phase = domain.PhasePausing
	if err := manager.Pause(ctx, session); err != nil {
		t.Fatal(err)
	}
	pausedCR, err := dynamicClient.Resource(mustGVR(grafanaAPIVersion, grafanaResource)).Namespace("vm").Get(ctx, "grafana", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if suspended, found, nestedErr := unstructured.NestedBool(pausedCR.Object, "spec", "suspend"); nestedErr != nil || !found || !suspended {
		t.Fatalf("paused Grafana suspend=%t found=%t err=%v, want true", suspended, found, nestedErr)
	}
	if err := manager.Resume(ctx, session); err != nil {
		t.Fatal(err)
	}
	if session.Spec.Workload().Pod.UID == "" {
		t.Fatal("resumed Grafana Pod identity was not recorded")
	}
	resumed, err := dynamicClient.Resource(mustGVR(grafanaAPIVersion, grafanaResource)).Namespace("vm").Get(ctx, "grafana", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.GetAnnotations()[pauseSessionAnnotation] != "" {
		t.Fatalf("Grafana pause owner=%q", resumed.GetAnnotations()[pauseSessionAnnotation])
	}
	if suspended, found, nestedErr := unstructured.NestedBool(resumed.Object, "spec", "suspend"); nestedErr != nil || !found || suspended {
		t.Fatalf("resumed Grafana suspend=%t found=%t err=%v, want false", suspended, found, nestedErr)
	}
}

func TestDiscoverRejectsControllerSpecificUnsafeWorkloads(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		parent *metav1.OwnerReference
		want   string
	}{
		{name: "cockroach", labels: map[string]string{"app.kubernetes.io/name": "cockroachdb"}, want: "CockroachDB"},
		{name: "backup helper", parent: &metav1.OwnerReference{APIVersion: "dataprotection.kubeblocks.io/v1alpha1", Kind: "Backup", Name: "archive"}, want: "backup helper"},
		{name: "minio tenant", parent: &metav1.OwnerReference{APIVersion: "minio.min.io/v2", Kind: "Tenant", Name: "object-storage"}, want: "MinIO"},
		{name: "minio helm", labels: map[string]string{"app.kubernetes.io/name": "minio"}, want: "MinIO"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replicas := int32(1)
			sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: types.UID("sts-uid"), Labels: tt.labels}, Spec: appsv1.StatefulSetSpec{Replicas: &replicas}}
			if tt.parent != nil {
				parent := *tt.parent
				parent.Controller = boolPointer(true)
				sts.OwnerReferences = []metav1.OwnerReference{parent}
			}
			pod := readyPod("app", "data-0", "node-a")
			pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: sts.Name, UID: sts.UID, Controller: boolPointer(true)}}
			client := fake.NewClientset(sts, pod)
			manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
			_, err := manager.Discover(context.Background(), DiscoverOptions{Namespace: "app", PodName: pod.Name, AllowLeaderDowntime: true})
			if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestDiscoverRejectsBackupHelperBeforeReadiness(t *testing.T) {
	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "postgres-archive-wal",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "dataprotection.kubeblocks.io/v1alpha1",
				Kind:       "Backup",
				Name:       "postgres-backup",
				Controller: boolPointer(true),
			}},
		},
		Spec: appsv1.StatefulSetSpec{Replicas: &replicas},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "postgres-archive-wal-0",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       sts.Name,
				UID:        sts.UID,
				Controller: boolPointer(true),
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodFailed},
	}
	manager := NewManager(fake.NewClientset(sts, pod), dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), nil)
	_, err := manager.Discover(context.Background(), DiscoverOptions{Namespace: "app", PodName: pod.Name})
	if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "backup helper") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestDiscoverRejectsBackupOwnedJobBeforeReadiness(t *testing.T) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Namespace: "app",
		Name:      "postgres-archive-wal",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "dataprotection.kubeblocks.io/v1alpha1",
			Kind:       "Backup",
			Name:       "postgres-backup",
			Controller: boolPointer(true),
		}},
	}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "postgres-archive-wal-failed",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "batch/v1",
				Kind:       "Job",
				Name:       job.Name,
				UID:        job.UID,
				Controller: boolPointer(true),
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodFailed},
	}
	manager := NewManager(fake.NewClientset(job, pod), dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), nil)
	_, err := manager.Discover(context.Background(), DiscoverOptions{Namespace: "app", PodName: pod.Name})
	if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "archive-WAL Job") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestVictoriaLogsHelmStatefulSetUsesOrdinalAdapter(t *testing.T) {
	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "logs",
			Name:      "victoria-logs-vlstorage",
			Annotations: map[string]string{
				"meta.helm.sh/release-name": "victoria-logs",
			},
			Labels: map[string]string{
				"app.kubernetes.io/name": "victoria-logs-cluster",
				"app":                    "vlstorage",
			},
		},
		Spec: appsv1.StatefulSetSpec{Replicas: &replicas},
	}
	pod := readyPod("logs", "victoria-logs-vlstorage-0", "node-a")
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: sts.Name, UID: sts.UID, Controller: boolPointer(true)}}
	manager := NewManager(fake.NewClientset(sts, pod), dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), nil)
	workload, err := manager.Discover(context.Background(), DiscoverOptions{Namespace: "logs", PodName: pod.Name, AllowLeaderDowntime: true})
	if err != nil {
		t.Fatal(err)
	}
	if workload.Adapter != domain.WorkloadVictoriaLogs || workload.Controller.Name != sts.Name {
		t.Fatalf("workload=%#v", workload)
	}
}

func TestVictoriaLogsPauseUsesFullReplicaLock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	replicas := int32(2)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "logs",
			Name:      "victoria-logs-vlstorage",
			UID:       types.UID("sts-uid"),
			Annotations: map[string]string{
				"meta.helm.sh/release-name": "victoria-logs",
			},
			Labels: map[string]string{
				"app.kubernetes.io/name": "victoria-logs-cluster",
				"app":                    "vlstorage",
			},
		},
		Spec: appsv1.StatefulSetSpec{Replicas: &replicas},
	}
	pods := []*corev1.Pod{
		readyPod("logs", "victoria-logs-vlstorage-0", "node-a"),
		readyPod("logs", "victoria-logs-vlstorage-1", "node-a"),
	}
	for _, pod := range pods {
		pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: sts.Name, UID: sts.UID, Controller: boolPointer(true)}}
	}
	client := fake.NewClientset(sts, pods[0], pods[1])
	podsResource := corev1.SchemeGroupVersion.WithResource("pods")
	var replicaUpdates []int32
	client.PrependReactor("update", "statefulsets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		updated := action.(clienttesting.UpdateAction).GetObject().(*appsv1.StatefulSet)
		replicaUpdates = append(replicaUpdates, statefulSetReplicas(updated))
		if statefulSetReplicas(updated) == 0 {
			for _, pod := range pods {
				_ = client.Tracker().Delete(podsResource, "logs", pod.Name)
			}
		} else {
			for _, pod := range pods {
				_ = client.Tracker().Create(podsResource, pod.DeepCopy(), "logs")
			}
		}
		return false, nil, nil
	})
	manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
	manager.poll = time.Millisecond
	workload, err := manager.Discover(ctx, DiscoverOptions{Namespace: "logs", PodName: pods[1].Name})
	if err != nil {
		t.Fatal(err)
	}
	if workload.Adapter != domain.WorkloadVictoriaLogs || workload.Ordinal == nil || *workload.Ordinal != 0 || len(workload.AffectedPods) != 2 {
		t.Fatalf("workload=%#v", workload)
	}
	session := controllerSession(workload)
	session.Status.Phase = domain.PhasePausing
	if err := manager.Pause(ctx, session); err != nil {
		t.Fatal(err)
	}
	paused, err := client.AppsV1().StatefulSets("logs").Get(ctx, sts.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if statefulSetReplicas(paused) != 0 || paused.Annotations[pauseSessionAnnotation] != session.ID {
		t.Fatalf("paused StatefulSet replicas=%d annotations=%v", statefulSetReplicas(paused), paused.Annotations)
	}
	if err := manager.Resume(ctx, session); err != nil {
		t.Fatal(err)
	}
	resumed, err := client.AppsV1().StatefulSets("logs").Get(ctx, sts.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if statefulSetReplicas(resumed) != 2 || resumed.Annotations[pauseSessionAnnotation] != "" {
		t.Fatalf("resumed StatefulSet replicas=%d annotations=%v", statefulSetReplicas(resumed), resumed.Annotations)
	}
	if strings.Contains(strings.Trim(strings.Trim(fmt.Sprint(replicaUpdates), "[]"), " "), "1") {
		t.Fatalf("Victoria Logs used ordinal scaling: replicas=%v", replicaUpdates)
	}
}

func TestDiscoverRejectsUnsafeKubeBlocksInstanceSetComponents(t *testing.T) {
	tests := []struct {
		name        string
		component   string
		definition  string
		wantMessage string
	}{
		{name: "minio", component: "minio", definition: "minio", wantMessage: "MinIO"},
		{name: "cockroach", component: "cockroachdb", definition: "cockroachdb", wantMessage: "CockroachDB"},
		{name: "archive wal", component: "archive-wal", definition: "postgresql", wantMessage: "archive-WAL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selected := readyPod("db", "cluster-"+tt.component+"-0", "node-a")
			selected.Labels = map[string]string{
				"app.kubernetes.io/instance":        "cluster",
				"apps.kubeblocks.io/component-name": tt.component,
			}
			selected.OwnerReferences = []metav1.OwnerReference{{APIVersion: "workloads.kubeblocks.io/v1alpha1", Kind: "InstanceSet", Name: "cluster-" + tt.component, Controller: boolPointer(true)}}
			typed := fake.NewClientset(selected)
			discovery := typed.Discovery().(*discoveryfake.FakeDiscovery)
			discovery.Resources = []*metav1.APIResourceList{{
				GroupVersion: "apps.kubeblocks.io/v1alpha1",
				APIResources: []metav1.APIResource{{Name: "opsrequests", Kind: "OpsRequest", Namespaced: true}},
			}}
			cluster := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "apps.kubeblocks.io/v1alpha1",
				"kind":       "Cluster",
				"metadata": map[string]any{
					"name":      "cluster",
					"namespace": "db",
					"uid":       "cluster-uid",
				},
				"spec": map[string]any{"componentSpecs": []any{map[string]any{
					"name":            tt.component,
					"componentDefRef": tt.definition,
					"stop":            false,
				}}},
			}}
			dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster)
			manager := NewManager(typed, dynamicClient, discovery)
			_, err := manager.Discover(context.Background(), DiscoverOptions{Namespace: "db", PodName: selected.Name})
			if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), tt.wantMessage) {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}
