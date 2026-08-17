package controller

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestManagerWaitLogsBeforePolling(t *testing.T) {
	var logs bytes.Buffer
	manager := NewManager(kubernetesfake.NewClientset(), nil, nil).WithLogger(slog.New(slog.NewTextHandler(&logs, nil)))
	manager.poll = time.Millisecond
	if err := manager.waitFor(context.Background(), "StatefulSet readiness", func(context.Context) (bool, error) { return true, nil }); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "waiting for workload controller") || !strings.Contains(logs.String(), "StatefulSet readiness") {
		t.Fatalf("logs=%q", logs.String())
	}
}

func readyPod(namespace, name, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: types.UID(name + "-uid")},
		Spec:       corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func boolPointer(value bool) *bool { return &value }

func TestDiscoverStatefulSetArbitraryOrdinal(t *testing.T) {
	ctx := context.Background()
	replicas := int32(3)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "db", UID: types.UID("sts-uid")},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
		},
	}
	pods := []*corev1.Pod{readyPod("app", "db-0", "node-a"), readyPod("app", "db-1", "node-a"), readyPod("app", "db-2", "node-a")}
	for _, pod := range pods {
		pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "db", UID: sts.UID, Controller: boolPointer(true)}}
	}
	client := kubernetesfake.NewClientset(sts, pods[0], pods[1], pods[2])
	manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
	workload, err := manager.Discover(ctx, DiscoverOptions{Namespace: "app", PodName: "db-1", AllowLeaderDowntime: true})
	if err != nil {
		t.Fatal(err)
	}
	if workload.Adapter != domain.WorkloadStatefulSet || workload.Ordinal == nil || *workload.Ordinal != 1 {
		t.Fatalf("unexpected workload: %#v", workload)
	}
	if workload.OriginalReplicas == nil || *workload.OriginalReplicas != 3 {
		t.Fatalf("original replicas: %#v", workload.OriginalReplicas)
	}
	if len(workload.AffectedPods) != 2 || workload.AffectedPods[0].Name != "db-1" || workload.AffectedPods[1].Name != "db-2" {
		t.Fatalf("affected Pods: %#v", workload.AffectedPods)
	}
}

func TestDiscoverHelmManagedStatefulSetUsesNativeAdapter(t *testing.T) {
	ctx := context.Background()
	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "redis",
			UID:       types.UID("sts-uid"),
			Annotations: map[string]string{
				"meta.helm.sh/release-name":      "redis",
				"meta.helm.sh/release-namespace": "app",
			},
			Labels: map[string]string{"app.kubernetes.io/managed-by": "Helm"},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
		},
	}
	pod := readyPod("app", "redis-0", "node-a")
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1",
		Kind:       "StatefulSet",
		Name:       sts.Name,
		UID:        sts.UID,
		Controller: boolPointer(true),
	}}
	client := kubernetesfake.NewClientset(sts, pod)
	manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
	workload, err := manager.Discover(ctx, DiscoverOptions{Namespace: "app", PodName: pod.Name, AllowLeaderDowntime: true})
	if err != nil {
		t.Fatal(err)
	}
	if workload.Adapter != domain.WorkloadStatefulSet || workload.Ordinal == nil || *workload.Ordinal != 0 {
		t.Fatalf("unexpected workload: %#v", workload)
	}
}

func TestStatefulSetApplicationDetectionIgnoresNameSubstrings(t *testing.T) {
	for _, name := range []string{"cockroach-cache", "minio-cache"} {
		t.Run(name, func(t *testing.T) {
			sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{kube.AppNameLabel: "redis"}}}
			if reason := unsupportedStatefulSetReason(sts); reason != "" {
				t.Fatalf("unsupported reason=%q", reason)
			}
		})
	}
}

func TestStatefulSetApplicationDetectionUsesApplicationLabels(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{name: "CockroachDB", labels: map[string]string{kube.AppNameLabel: "cockroachdb"}, want: "CockroachDB"},
		{name: "MinIO component", labels: map[string]string{kube.AppComponentLabel: "minio"}, want: "MinIO"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "data", Labels: test.labels}}
			if reason := unsupportedStatefulSetReason(sts); !strings.Contains(reason, test.want) {
				t.Fatalf("unsupported reason=%q, want %q", reason, test.want)
			}
		})
	}
}

func TestPauseStatefulSetRejectsExternalReplicaChange(t *testing.T) {
	ctx := context.Background()
	replicas := int32(4)
	client := kubernetesfake.NewClientset(&appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "db", UID: types.UID("sts-uid")},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
	})
	manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
	original := int32(3)
	ordinal := int32(1)
	session := domain.NewSession("session", domain.NewSessionSpec(domain.OperationMigrate, domain.SessionCommon{
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		SessionNamespace:   "system",
		Volumes:            []domain.VolumeSpec{{SourcePVC: domain.ObjectReference{Name: "data"}}},
	}, domain.WorkloadSpec{
		Adapter:          domain.WorkloadStatefulSet,
		Controller:       domain.ObjectReference{Namespace: "app", Name: "db", UID: types.UID("sts-uid")},
		OriginalReplicas: &original,
		Ordinal:          &ordinal,
	}, false, domain.SessionWorkflowOptions{}), time.Now())
	err := manager.Pause(ctx, session)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestStandalonePauseResumeRebuildsPodOnTargetNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	pod := readyPod("app", "worker", "source")
	pod.Spec.Containers = []corev1.Container{{Name: "app", Image: "example/app:v1"}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "target", Labels: map[string]string{corev1.LabelHostname: "target-host"}}}
	client := kubernetesfake.NewClientset(pod, node)
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		created := action.(clienttesting.CreateAction).GetObject().(*corev1.Pod)
		created.UID = types.UID("recreated-uid")
		created.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
		return false, nil, nil
	})
	manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
	manager.poll = time.Millisecond
	workload, err := manager.Discover(ctx, DiscoverOptions{Namespace: "app", PodName: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	session := domain.NewSession("session", domain.NewSessionSpec(domain.OperationMigrate, domain.SessionCommon{
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		SessionNamespace:   "system",
		Volumes:            []domain.VolumeSpec{{SourcePVC: domain.ObjectReference{Name: "data"}}},
	}, workload, false, domain.SessionWorkflowOptions{TargetNode: "target"}), time.Now())
	if err := manager.Pause(ctx, session); err != nil {
		t.Fatal(err)
	}
	if err := manager.Resume(ctx, session); err != nil {
		t.Fatal(err)
	}
	recreated, err := client.CoreV1().Pods("app").Get(ctx, "worker", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if recreated.Spec.NodeSelector[corev1.LabelHostname] != "target-host" || recreated.Spec.NodeName != "" {
		t.Fatalf("recreated scheduling: nodeName=%q selector=%v", recreated.Spec.NodeName, recreated.Spec.NodeSelector)
	}
	if recreated.Spec.Containers[0].Image != "example/app:v1" {
		t.Fatalf("container spec changed: %#v", recreated.Spec.Containers)
	}
	if session.Spec.Workload().Pod.UID != recreated.UID {
		t.Fatalf("session Pod UID=%s want=%s", session.Spec.Workload().Pod.UID, recreated.UID)
	}
	if err := manager.Pause(ctx, session); err != nil {
		t.Fatalf("pause recreated Pod: %v", err)
	}
}

func TestStandaloneRollbackRebuildsPodOnSourceNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	pod := readyPod("app", "worker", "source")
	pod.Spec.Containers = []corev1.Container{{Name: "app", Image: "example/app:v1"}}
	sourceNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "source", Labels: map[string]string{corev1.LabelHostname: "source-host"}}}
	targetNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "target", Labels: map[string]string{corev1.LabelHostname: "target-host"}}}
	client := kubernetesfake.NewClientset(pod, sourceNode, targetNode)
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		created := action.(clienttesting.CreateAction).GetObject().(*corev1.Pod)
		created.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
		return false, nil, nil
	})
	manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
	manager.poll = time.Millisecond
	workload, err := manager.Discover(ctx, DiscoverOptions{Namespace: "app", PodName: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	session := domain.NewSession("session", domain.NewSessionSpec(domain.OperationMigrate, domain.SessionCommon{
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		SessionNamespace:   "system",
		Volumes:            []domain.VolumeSpec{{SourcePVC: domain.ObjectReference{Name: "data"}}},
	}, workload, false, domain.SessionWorkflowOptions{SourceNode: "source", TargetNode: "target"}), time.Now())
	session.Status.Phase = domain.PhaseRollingBack
	if err := manager.Pause(ctx, session); err != nil {
		t.Fatal(err)
	}
	if err := manager.Resume(ctx, session); err != nil {
		t.Fatal(err)
	}
	recreated, err := client.CoreV1().Pods("app").Get(ctx, "worker", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if recreated.Spec.NodeSelector[corev1.LabelHostname] != "source-host" {
		t.Fatalf("rollback scheduling: selector=%v", recreated.Spec.NodeSelector)
	}
}

func TestKubeBlocksUsesDiscoveredCurrentOpsAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	selected := readyPod("db", "cluster-postgresql-0", "node-a")
	selected.OwnerReferences = []metav1.OwnerReference{{APIVersion: "workloads.kubeblocks.io/v1alpha1", Kind: "InstanceSet", Name: "cluster-postgresql", UID: types.UID("is-uid"), Controller: boolPointer(true)}}
	selected.Labels = map[string]string{
		"app.kubernetes.io/instance":        "cluster",
		"apps.kubeblocks.io/component-name": "postgresql",
		"kubeblocks.io/role":                "primary",
	}
	candidate := readyPod("db", "cluster-postgresql-1", "node-b")
	candidate.Labels = map[string]string{
		"app.kubernetes.io/instance":        "cluster",
		"apps.kubeblocks.io/component-name": "postgresql",
		"kubeblocks.io/role":                "secondary",
	}
	typed := kubernetesfake.NewClientset(selected, candidate)
	discovery := typed.Discovery().(*fake.FakeDiscovery)
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
		"spec": map[string]any{"componentSpecs": []any{map[string]any{"name": "postgresql", "stop": false}}},
	}}
	instanceSet := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "workloads.kubeblocks.io/v1alpha1",
		"kind":       "InstanceSet",
		"metadata": map[string]any{
			"name":      "cluster-postgresql",
			"namespace": "db",
			"uid":       "is-uid",
		},
		"spec": map[string]any{"paused": false},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster, instanceSet)
	var switchoverCandidate string
	var operationTypes []string
	dynamicClient.PrependReactor("create", "opsrequests", func(action clienttesting.Action) (bool, runtime.Object, error) {
		object := action.(clienttesting.CreateAction).GetObject().(*unstructured.Unstructured)
		createOptions := action.(interface{ GetCreateOptions() metav1.CreateOptions }).GetCreateOptions()
		operationType, _, _ := unstructured.NestedString(object.Object, "spec", "type")
		if len(createOptions.DryRun) > 0 {
			return false, nil, nil
		}
		operationTypes = append(operationTypes, operationType)
		if operationType == "Switchover" {
			items, _, _ := unstructured.NestedSlice(object.Object, "spec", "switchover")
			switchoverCandidate, _, _ = unstructured.NestedString(items[0].(map[string]any), "instanceName")
			pod, _ := typed.CoreV1().Pods("db").Get(ctx, selected.Name, metav1.GetOptions{})
			pod.Labels["kubeblocks.io/role"] = "secondary"
			_, _ = typed.CoreV1().Pods("db").Update(ctx, pod, metav1.UpdateOptions{})
		}
		_ = unstructured.SetNestedField(object.Object, "Succeed", "status", "phase")
		return false, nil, nil
	})
	clusterUpdates := 0
	dynamicClient.PrependReactor("update", "clusters", func(action clienttesting.Action) (bool, runtime.Object, error) {
		clusterUpdates++
		return false, nil, nil
	})
	dynamicClient.PrependReactor("update", "instancesets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		object := action.(clienttesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		paused, _, _ := unstructured.NestedBool(object.Object, "spec", "paused")
		if !paused {
			restored := readyPod("db", selected.Name, "node-b")
			restored.Labels = selected.Labels
			restored.OwnerReferences = selected.OwnerReferences
			_, _ = typed.CoreV1().Pods("db").Create(ctx, restored, metav1.CreateOptions{})
		}
		return false, nil, nil
	})
	manager := NewManager(typed, dynamicClient, discovery)
	manager.poll = time.Millisecond
	workload, err := manager.Discover(ctx, DiscoverOptions{
		Namespace:           "db",
		PodName:             selected.Name,
		SwitchoverCandidate: candidate.Name,
	})
	if err != nil {
		t.Fatal(err)
	}
	if workload.KubeBlocks == nil || workload.KubeBlocks.OpsAPIVersion != "apps.kubeblocks.io/v1alpha1" {
		t.Fatalf("KubeBlocks discovery: %#v", workload.KubeBlocks)
	}
	if workload.KubeBlocks.ClusterUID != "cluster-uid" || workload.KubeBlocks.OriginalStops["postgresql"] {
		t.Fatalf("KubeBlocks recovery state: %#v", workload.KubeBlocks)
	}
	session := domain.NewSession("session", domain.NewSessionSpec(domain.OperationMigrate, domain.SessionCommon{
		SourceNamespace:    "db",
		TemporaryNamespace: "system",
		SessionNamespace:   "system",
		Volumes:            []domain.VolumeSpec{{SourcePVC: domain.ObjectReference{Name: "data"}}},
	}, workload, false, domain.SessionWorkflowOptions{}), time.Now())
	session.Status.Phase = domain.PhasePausing
	if err := manager.Pause(ctx, session); err != nil {
		t.Fatal(err)
	}
	if _, err := typed.CoreV1().Pods("db").Get(ctx, candidate.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("candidate Pod did not remain available: %v", err)
	}
	if clusterUpdates != 0 {
		t.Fatalf("InstanceSet migration updated Cluster %d times", clusterUpdates)
	}
	if switchoverCandidate != candidate.Name {
		t.Fatalf("switchover instanceName=%q want=%q", switchoverCandidate, candidate.Name)
	}
	if err := manager.Resume(ctx, session); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(operationTypes, ","), "Switchover"; got != want {
		t.Fatalf("KubeBlocks operations=%q want=%q", got, want)
	}
}
