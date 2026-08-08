package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestDiscoverRejectsUnsafePodAndControllerStates(t *testing.T) {
	tests := []struct {
		name    string
		objects []runtime.Object
		pod     *corev1.Pod
		want    string
	}{
		{
			name: "Pod is unready",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "worker"},
				Status:     corev1.PodStatus{Phase: corev1.PodRunning},
			},
			want: "must be Running and Ready",
		},
		{
			name: "static mirror Pod",
			pod: func() *corev1.Pod {
				pod := readyPod("app", "worker", "node-a")
				pod.Annotations = map[string]string{corev1.MirrorPodAnnotationKey: "hash"}
				return pod
			}(),
			want: "static mirror Pods are unsupported",
		},
		{
			name: "unsupported controller",
			pod: func() *corev1.Pod {
				pod := readyPod("app", "worker", "node-a")
				pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "worker", Controller: boolPointer(true)}}
				return pod
			}(),
			want: "has no safe pause adapter",
		},
		{
			name: "malformed controller version",
			pod: func() *corev1.Pod {
				pod := readyPod("app", "worker", "node-a")
				pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "bad/version/extra", Kind: "Controller", Name: "worker", Controller: boolPointer(true)}}
				return pod
			}(),
			want: "parse controller apiVersion",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objects := append([]runtime.Object(nil), tt.objects...)
			objects = append(objects, tt.pod)
			client := kubernetesfake.NewClientset(objects...)
			manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
			_, err := manager.Discover(context.Background(), DiscoverOptions{Namespace: tt.pod.Namespace, PodName: tt.pod.Name})
			if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestDiscoverStatefulSetSafetyChecks(t *testing.T) {
	tests := []struct {
		name          string
		selected      string
		retention     appsv1.PersistentVolumeClaimRetentionPolicyType
		role          string
		allowDowntime bool
		want          string
	}{
		{name: "retention deletes claims", selected: "db-1", retention: appsv1.DeletePersistentVolumeClaimRetentionPolicyType, allowDowntime: true, want: "PVC retention whenScaled is Delete"},
		{name: "leader affected by scale down", selected: "db-1", retention: appsv1.RetainPersistentVolumeClaimRetentionPolicyType, role: "primary", want: "allow-leader-downtime"},
		{name: "ordinal outside replicas", selected: "db-3", retention: appsv1.RetainPersistentVolumeClaimRetentionPolicyType, allowDowntime: true, want: "outside replicas"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replicas := int32(3)
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "db", UID: types.UID("sts-uid")},
				Spec: appsv1.StatefulSetSpec{
					Replicas:                             &replicas,
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{WhenScaled: tt.retention},
				},
			}
			objects := []runtime.Object{sts}
			for ordinal := 0; ordinal <= 3; ordinal++ {
				pod := readyPod("app", "db-"+string(rune('0'+ordinal)), "node-a")
				pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "db", UID: sts.UID, Controller: boolPointer(true)}}
				if pod.Name == tt.selected && tt.role != "" {
					pod.Labels = map[string]string{"role": tt.role}
				}
				objects = append(objects, pod)
			}
			client := kubernetesfake.NewClientset(objects...)
			manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
			_, err := manager.Discover(context.Background(), DiscoverOptions{Namespace: "app", PodName: tt.selected, AllowLeaderDowntime: tt.allowDowntime})
			if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestDiscoverKubeBlocksValidatesIdentityAPIAndCandidate(t *testing.T) {
	tests := []struct {
		name          string
		labels        map[string]string
		serveAPI      bool
		candidate     *corev1.Pod
		candidateName string
		allowLeader   bool
		want          string
	}{
		{name: "missing identity", labels: map[string]string{}, serveAPI: true, want: "lacks cluster or component"},
		{name: "missing OpsRequest API", labels: map[string]string{"app.kubernetes.io/instance": "cluster", "apps.kubeblocks.io/component-name": "db"}, want: "no served OpsRequest API"},
		{name: "leader needs candidate", labels: map[string]string{"app.kubernetes.io/instance": "cluster", "apps.kubeblocks.io/component-name": "db", "kubeblocks.io/role": "primary"}, serveAPI: true, want: "use --kubeblocks-candidate"},
		{name: "candidate from another component", labels: map[string]string{"app.kubernetes.io/instance": "cluster", "apps.kubeblocks.io/component-name": "db", "kubeblocks.io/role": "primary"}, serveAPI: true, candidate: kubeBlocksCandidate("other"), candidateName: "cluster-other-1", want: "same component"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selected := readyPod("db", "cluster-db-0", "node-a")
			selected.OwnerReferences = []metav1.OwnerReference{{APIVersion: "workloads.kubeblocks.io/v1alpha1", Kind: "InstanceSet", Name: "cluster-db", Controller: boolPointer(true)}}
			selected.Labels = tt.labels
			objects := []runtime.Object{selected}
			if tt.candidate != nil {
				objects = append(objects, tt.candidate)
			}
			client := kubernetesfake.NewClientset(objects...)
			if tt.serveAPI {
				discovery := client.Discovery().(*fake.FakeDiscovery)
				discovery.Resources = []*metav1.APIResourceList{{GroupVersion: "apps.kubeblocks.io/v1alpha1", APIResources: []metav1.APIResource{{Name: "opsrequests"}}}}
			}
			manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
			_, err := manager.Discover(context.Background(), DiscoverOptions{Namespace: "db", PodName: selected.Name, SwitchoverCandidate: tt.candidateName, AllowLeaderDowntime: tt.allowLeader})
			if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestDiscoverRejectsKubeBlocksMinIOAndCockroachComponents(t *testing.T) {
	for _, test := range []struct {
		name       string
		component  string
		definition string
		want       string
	}{
		{name: "minio", component: "minio", definition: "minio", want: "MinIO"},
		{name: "cockroach", component: "crdb", definition: "cockroachdb", want: "CockroachDB"},
	} {
		t.Run(test.name, func(t *testing.T) {
			selected := readyPod("db", "cluster-"+test.component+"-0", "node-a")
			selected.OwnerReferences = []metav1.OwnerReference{{APIVersion: "workloads.kubeblocks.io/v1alpha1", Kind: "InstanceSet", Name: "cluster-" + test.component, Controller: boolPointer(true)}}
			selected.Labels = map[string]string{
				"app.kubernetes.io/instance":        "cluster",
				"apps.kubeblocks.io/component-name": test.component,
			}
			cluster := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "apps.kubeblocks.io/v1alpha1",
				"kind":       "Cluster",
				"metadata":   map[string]any{"name": "cluster", "namespace": "db", "uid": "cluster-uid"},
				"spec": map[string]any{"componentSpecs": []any{map[string]any{
					"name": test.component, "componentDefRef": test.definition, "stop": false,
				}}},
			}}
			client := kubernetesfake.NewClientset(selected)
			discovery := client.Discovery().(*fake.FakeDiscovery)
			discovery.Resources = []*metav1.APIResourceList{{GroupVersion: "apps.kubeblocks.io/v1alpha1", APIResources: []metav1.APIResource{{Name: "opsrequests"}}}}
			manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster), discovery)
			_, err := manager.Discover(context.Background(), DiscoverOptions{Namespace: "db", PodName: selected.Name, AllowLeaderDowntime: true})
			if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestDiscoverRejectsKubeBlocksSwitchoverRejectedByAdmission(t *testing.T) {
	selected := readyPod("db", "cluster-db-0", "node-a")
	selected.OwnerReferences = []metav1.OwnerReference{{APIVersion: "workloads.kubeblocks.io/v1alpha1", Kind: "InstanceSet", Name: "cluster-db", Controller: boolPointer(true)}}
	selected.Labels = map[string]string{"app.kubernetes.io/instance": "cluster", "apps.kubeblocks.io/component-name": "db", "kubeblocks.io/role": "primary"}
	candidate := readyPod("db", "cluster-db-1", "node-b")
	candidate.Labels = map[string]string{"app.kubernetes.io/instance": "cluster", "apps.kubeblocks.io/component-name": "db", "kubeblocks.io/role": "secondary"}
	typed := kubernetesfake.NewClientset(selected, candidate)
	discovery := typed.Discovery().(*fake.FakeDiscovery)
	discovery.Resources = []*metav1.APIResourceList{{GroupVersion: "apps.kubeblocks.io/v1alpha1", APIResources: []metav1.APIResource{{Name: "opsrequests"}}}}
	cluster := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps.kubeblocks.io/v1alpha1",
		"kind":       "Cluster",
		"metadata":   map[string]any{"name": "cluster", "namespace": "db", "uid": "cluster-uid"},
		"spec":       map[string]any{"componentSpecs": []any{map[string]any{"name": "db", "stop": false}}},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster)
	dynamicClient.PrependReactor("create", "opsrequests", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "apps.kubeblocks.io", Resource: "opsrequests"}, "preflight", errors.New("component does not support switchover"))
	})
	manager := NewManager(typed, dynamicClient, discovery)
	_, err := manager.Discover(context.Background(), DiscoverOptions{Namespace: "db", PodName: selected.Name, SwitchoverCandidate: candidate.Name})
	if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "automatic switchover") || !strings.Contains(err.Error(), "native switchover") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func kubeBlocksCandidate(component string) *corev1.Pod {
	pod := readyPod("db", "cluster-other-1", "node-b")
	pod.Labels = map[string]string{"app.kubernetes.io/instance": "cluster", "apps.kubeblocks.io/component-name": component}
	return pod
}

func TestKubeBlocksSwitchoverCommandMatchesServedAPI(t *testing.T) {
	appsCommand := kubeBlocksSwitchoverCommand("db", "cluster", "postgresql", "cluster-postgresql-0", "cluster-postgresql-1", "apps.kubeblocks.io/v1alpha1")
	if !strings.Contains(appsCommand, "instanceName: cluster-postgresql-1") || strings.Contains(appsCommand, "candidateName:") {
		t.Fatalf("apps OpsRequest command=%q", appsCommand)
	}
	operationsCommand := kubeBlocksSwitchoverCommand("db", "cluster", "postgresql", "cluster-postgresql-0", "cluster-postgresql-1", "operations.kubeblocks.io/v1alpha1")
	if !strings.Contains(operationsCommand, "instanceName: cluster-postgresql-0") || !strings.Contains(operationsCommand, "candidateName: cluster-postgresql-1") {
		t.Fatalf("operations OpsRequest command=%q", operationsCommand)
	}
}

func TestStatefulSetPauseResumeAreIdempotentAtDesiredReplicaCounts(t *testing.T) {
	replicas := int32(1)
	client := kubernetesfake.NewClientset(&appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "db", UID: types.UID("sts-uid")},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
	})
	manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
	original, ordinal := int32(3), int32(1)
	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadStatefulSet, Controller: domain.ObjectReference{Namespace: "app", Name: "db", UID: types.UID("sts-uid")}, OriginalReplicas: &original, Ordinal: &ordinal,
	})
	if err := manager.Pause(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	sts, _ := client.AppsV1().StatefulSets("app").Get(context.Background(), "db", metav1.GetOptions{})
	sts.Spec.Replicas = &original
	if _, err := client.AppsV1().StatefulSets("app").Update(context.Background(), sts, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Resume(context.Background(), session); err != nil {
		t.Fatal(err)
	}
}

func TestStatefulSetPauseResumeWaitsForAffectedPods(t *testing.T) {
	replicas := int32(3)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "db", UID: types.UID("sts-uid")},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
	}
	pod1, pod2 := readyPod("app", "db-1", "node-a"), readyPod("app", "db-2", "node-a")
	client := kubernetesfake.NewClientset(sts, pod1, pod2)
	podsResource := corev1.SchemeGroupVersion.WithResource("pods")
	client.PrependReactor("update", "statefulsets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		updated := action.(clienttesting.UpdateAction).GetObject().(*appsv1.StatefulSet)
		if *updated.Spec.Replicas == 1 {
			for _, name := range []string{"db-1", "db-2"} {
				if err := client.Tracker().Delete(podsResource, "app", name); err != nil && !apierrors.IsNotFound(err) {
					return true, nil, err
				}
			}
		} else {
			for _, pod := range []*corev1.Pod{readyPod("app", "db-1", "node-b"), readyPod("app", "db-2", "node-b")} {
				if err := client.Tracker().Create(podsResource, pod, "app"); err != nil && !apierrors.IsAlreadyExists(err) {
					return true, nil, err
				}
			}
		}
		return false, nil, nil
	})
	manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
	manager.poll = time.Millisecond
	original, ordinal := int32(3), int32(1)
	session := controllerSession(domain.WorkloadSpec{
		Adapter:          domain.WorkloadStatefulSet,
		Pod:              domain.ObjectReference{Namespace: "app", Name: "db-1"},
		Controller:       domain.ObjectReference{Namespace: "app", Name: "db", UID: sts.UID},
		OriginalReplicas: &original,
		Ordinal:          &ordinal,
		AffectedPods:     []domain.ObjectReference{{Namespace: "app", Name: "db-1"}, {Namespace: "app", Name: "db-2"}},
	})
	if err := manager.Pause(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if err := manager.VerifyPaused(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if err := manager.Resume(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"db-1", "db-2"} {
		pod, err := client.CoreV1().Pods("app").Get(context.Background(), name, metav1.GetOptions{})
		if err != nil || !podReady(pod) {
			t.Fatalf("Pod %s readiness: pod=%#v error=%v", name, pod, err)
		}
	}
}

func TestVerifyPausedChecksEveryAffectedPod(t *testing.T) {
	stale := readyPod("app", "db-2", "node-a")
	client := kubernetesfake.NewClientset(stale)
	manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadStatefulSet,
		Pod:     domain.ObjectReference{Namespace: "app", Name: "db-1"},
		AffectedPods: []domain.ObjectReference{
			{Namespace: "app", Name: "db-1"},
			{Namespace: "app", Name: "db-2"},
		},
	})
	if err := manager.VerifyPaused(context.Background(), session); domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "db-2") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestStatefulSetSessionRequiresCompleteReplicaState(t *testing.T) {
	manager := NewManager(kubernetesfake.NewClientset(), dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), nil)
	session := controllerSession(domain.WorkloadSpec{Adapter: domain.WorkloadStatefulSet})
	if err := manager.Pause(context.Background(), session); domain.CategoryOf(err) != domain.ErrorInternal {
		t.Fatalf("pause category=%s error=%v", domain.CategoryOf(err), err)
	}
	if err := manager.Resume(context.Background(), session); domain.CategoryOf(err) != domain.ErrorInternal {
		t.Fatalf("resume category=%s error=%v", domain.CategoryOf(err), err)
	}
	original := int32(3)
	session.Spec.WorkloadPtr().OriginalReplicas = &original
	if err := manager.Resume(context.Background(), session); domain.CategoryOf(err) != domain.ErrorInternal {
		t.Fatalf("resume missing ordinal category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestStandalonePauseIsIdempotentAndProtectsUID(t *testing.T) {
	missingClient := kubernetesfake.NewClientset()
	manager := NewManager(missingClient, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), missingClient.Discovery())
	session := controllerSession(domain.WorkloadSpec{Adapter: domain.WorkloadStandalone, Pod: domain.ObjectReference{Namespace: "app", Name: "worker", UID: types.UID("old")}})
	if err := manager.Pause(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	pod := readyPod("app", "worker", "node-a")
	client := kubernetesfake.NewClientset(pod)
	manager = NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
	err := manager.Pause(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestStandaloneResumeHandlesExistingPodsAndNodeValidation(t *testing.T) {
	owned := readyPod("app", "worker", "node-a")
	owned.Annotations = map[string]string{kube.SessionAnnotation: "session"}
	client := kubernetesfake.NewClientset(owned)
	manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
	session := controllerSession(domain.WorkloadSpec{Adapter: domain.WorkloadStandalone, Pod: domain.ObjectReference{Namespace: "app", Name: "worker"}, OriginalObject: []byte("invalid")})
	if err := manager.Resume(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if session.Spec.Workload().Pod.UID != owned.UID {
		t.Fatalf("session Pod UID=%s want=%s", session.Spec.Workload().Pod.UID, owned.UID)
	}

	foreign := owned.DeepCopy()
	foreign.Annotations[kube.SessionAnnotation] = "another-session"
	foreignClient := kubernetesfake.NewClientset(foreign)
	manager = NewManager(foreignClient, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), foreignClient.Discovery())
	err := manager.Resume(context.Background(), controllerSession(domain.WorkloadSpec{Adapter: domain.WorkloadStandalone, Pod: domain.ObjectReference{Namespace: "app", Name: "worker"}}))
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	saved := readyPod("app", "worker", "old-node")
	raw, err := json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}
	nodeClient := kubernetesfake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "target"}})
	manager = NewManager(nodeClient, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), nodeClient.Discovery())
	nodeSession := controllerSession(domain.WorkloadSpec{Adapter: domain.WorkloadStandalone, Pod: domain.ObjectReference{Namespace: "app", Name: "worker"}, OriginalObject: raw})
	nodeSession.Spec.WorkflowOptionsPtr().TargetNode = "target"
	err = manager.Resume(context.Background(), nodeSession)
	if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "lacks kubernetes.io/hostname") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestStandaloneAbortResumeUsesSourceNode(t *testing.T) {
	saved := readyPod("app", "worker", "old-node")
	raw, err := json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}
	client := kubernetesfake.NewClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "source", Labels: map[string]string{corev1.LabelHostname: "source-host"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "target", Labels: map[string]string{corev1.LabelHostname: "target-host"}}},
	)
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		pod := action.(clienttesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		pod.UID = types.UID("resumed-uid")
		pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
		if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace); err != nil {
			t.Fatal(err)
		}
		return true, pod, nil
	})
	manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
	session := controllerSession(domain.WorkloadSpec{
		Adapter:        domain.WorkloadStandalone,
		Pod:            domain.ObjectReference{Namespace: "app", Name: "worker"},
		OriginalObject: raw,
	})
	session.Status.Phase = domain.PhaseAborting
	session.Spec.WorkflowOptionsPtr().SourceNode = "source"
	session.Spec.WorkflowOptionsPtr().TargetNode = "target"
	if err := manager.Resume(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	resumed, err := client.CoreV1().Pods("app").Get(context.Background(), "worker", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Spec.NodeSelector[corev1.LabelHostname] != "source-host" {
		t.Fatalf("abort resumed Pod selector=%v, want source-host", resumed.Spec.NodeSelector)
	}
}

func TestStandaloneResumeRejectsConcurrentForeignPod(t *testing.T) {
	saved := readyPod("app", "worker", "node-a")
	raw, err := json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}
	client := kubernetesfake.NewClientset()
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		created := action.(clienttesting.CreateAction).GetObject().(*corev1.Pod)
		foreign := readyPod(created.Namespace, created.Name, "node-b")
		foreign.Annotations = map[string]string{kube.SessionAnnotation: "foreign-session"}
		if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"), foreign, created.Namespace); err != nil {
			t.Fatalf("create concurrent Pod: %v", err)
		}
		return true, nil, apierrors.NewAlreadyExists(corev1.Resource("pods"), created.Name)
	})
	manager := NewManager(client, dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), client.Discovery())
	session := controllerSession(domain.WorkloadSpec{
		Adapter:        domain.WorkloadStandalone,
		Pod:            domain.ObjectReference{Namespace: "app", Name: "worker"},
		OriginalObject: raw,
	})
	if err := manager.Resume(context.Background(), session); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func controllerSession(workload domain.WorkloadSpec) *domain.Session {
	return domain.NewSession("session", domain.NewSessionSpec(domain.OperationMigrate, domain.SessionCommon{
		SourceNamespace: "app", TemporaryNamespace: "system", SessionNamespace: "system",
		Volumes: []domain.VolumeSpec{{SourcePVC: domain.ObjectReference{Name: "data"}}},
	}, workload, false), time.Now())
}

func TestCreateAndWaitOpsReusesSucceededAndRetriesFailedRequests(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "apps.kubeblocks.io", Version: "v1alpha1", Resource: "opsrequests"}
	succeeded := opsRequest("session-offline", "Succeed")
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), succeeded)
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)
	manager.poll = time.Millisecond
	session := kubeBlocksSession()
	if err := manager.createAndWaitOps(context.Background(), session, "offline", map[string]any{"type": "HorizontalScaling"}); err != nil {
		t.Fatal(err)
	}
	if countDynamicActions(dynamicClient.Actions(), "create", gvr.Resource) != 0 {
		t.Fatal("succeeded request should be reused")
	}

	failed := opsRequest("session-online", "Failed")
	dynamicClient = dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), failed)
	dynamicClient.PrependReactor("create", "opsrequests", func(action clienttesting.Action) (bool, runtime.Object, error) {
		object := action.(clienttesting.CreateAction).GetObject().(*unstructured.Unstructured)
		_ = unstructured.SetNestedField(object.Object, "Succeed", "status", "phase")
		return false, nil, nil
	})
	manager = NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)
	manager.poll = time.Millisecond
	if err := manager.createAndWaitOps(context.Background(), session, "online", map[string]any{"type": "HorizontalScaling"}); err != nil {
		t.Fatal(err)
	}
	if countDynamicActions(dynamicClient.Actions(), "delete", gvr.Resource) != 1 || countDynamicActions(dynamicClient.Actions(), "create", gvr.Resource) != 1 {
		t.Fatalf("actions=%#v", dynamicClient.Actions())
	}
}

func TestCreateAndWaitOpsReturnsTerminalFailure(t *testing.T) {
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	dynamicClient.PrependReactor("create", "opsrequests", func(action clienttesting.Action) (bool, runtime.Object, error) {
		object := action.(clienttesting.CreateAction).GetObject().(*unstructured.Unstructured)
		_ = unstructured.SetNestedField(object.Object, "Cancelled", "status", "phase")
		return false, nil, nil
	})
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)
	manager.poll = time.Millisecond
	err := manager.createAndWaitOps(context.Background(), kubeBlocksSession(), "offline", map[string]any{"type": "HorizontalScaling"})
	if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "Cancelled") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestKubeBlocksAdaptersRequireState(t *testing.T) {
	manager := NewManager(kubernetesfake.NewClientset(), dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), nil)
	session := controllerSession(domain.WorkloadSpec{Adapter: domain.WorkloadKubeBlocks})
	if err := manager.Pause(context.Background(), session); domain.CategoryOf(err) != domain.ErrorInternal {
		t.Fatalf("pause category=%s error=%v", domain.CategoryOf(err), err)
	}
	if err := manager.Resume(context.Background(), session); domain.CategoryOf(err) != domain.ErrorInternal {
		t.Fatalf("resume category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestOperationNameIsStableAndDNSLengthBounded(t *testing.T) {
	name := operationName(strings.Repeat("a", 80), "offline")
	if len(name) != 63 || !strings.HasPrefix(name, "pvc-migrate-") {
		t.Fatalf("operation name=%q length=%d", name, len(name))
	}
	if got := operationName("short", "online"); got != "pvc-migrate-short-online" {
		t.Fatalf("operation name=%q", got)
	}
}

func opsRequest(action, phase string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps.kubeblocks.io/v1alpha1",
		"kind":       "OpsRequest",
		"metadata": map[string]any{
			"name":      "pvc-migrate-" + action,
			"namespace": "db",
			"uid":       "request-uid",
		},
		"status": map[string]any{"phase": phase},
	}}
}

func kubeBlocksSession() *domain.Session {
	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadKubeBlocks,
		Pod:     domain.ObjectReference{Namespace: "db", Name: "cluster-db-0"},
		KubeBlocks: &domain.KubeBlocksSpec{
			Cluster: "cluster", Component: "db", Instance: "cluster-db-0", OpsAPIVersion: "apps.kubeblocks.io/v1alpha1",
		},
	})
	return session
}

func countDynamicActions(actions []clienttesting.Action, verb, resource string) int {
	count := 0
	for _, action := range actions {
		if action.GetVerb() == verb && action.GetResource().Resource == resource {
			count++
		}
	}
	return count
}

func TestPauseKubeBlocksReturnsPodReadErrorsBeforeOffline(t *testing.T) {
	client := kubernetesfake.NewClientset()
	client.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "cluster-db-0", errors.New("denied"))
	})
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	manager := NewManager(client, dynamicClient, nil)
	err := manager.Pause(context.Background(), kubeBlocksSession())
	if domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	if countDynamicActions(dynamicClient.Actions(), "create", "opsrequests") != 0 {
		t.Fatal("offline operation started after Pod read failure")
	}
}

func TestKubeBlocksPausePreservesOriginalStopsAcrossComponents(t *testing.T) {
	ctx := context.Background()
	cluster := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps.kubeblocks.io/v1alpha1",
		"kind":       "Cluster",
		"metadata": map[string]any{
			"name":      "cluster",
			"namespace": "db",
			"uid":       "cluster-uid",
		},
		"spec": map[string]any{"componentSpecs": []any{
			map[string]any{"name": "postgresql", "stop": false},
			map[string]any{"name": "etcd", "stop": true},
		}},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster)
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)
	session := kubeBlocksSession()
	session.Spec.Workload().KubeBlocks.ClusterAPIVersion = "apps.kubeblocks.io/v1alpha1"
	if err := manager.setKubeBlocksPaused(ctx, session, true); err != nil {
		t.Fatal(err)
	}
	paused, err := dynamicClient.Resource(mustGVR(kubeBlocksClusterAPIVersion, clusterResource)).Namespace("db").Get(ctx, "cluster", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	components, _, _ := unstructured.NestedSlice(paused.Object, "spec", "componentSpecs")
	for index, expected := range []bool{true, true} {
		stopped, _, _ := unstructured.NestedBool(components[index].(map[string]any), "stop")
		if stopped != expected {
			t.Fatalf("pause component[%d] stop=%v want=%v", index, stopped, expected)
		}
	}
	if err := manager.setKubeBlocksPaused(ctx, session, false); err != nil {
		t.Fatal(err)
	}
	resumed, err := dynamicClient.Resource(mustGVR(kubeBlocksClusterAPIVersion, clusterResource)).Namespace("db").Get(ctx, "cluster", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	components, _, _ = unstructured.NestedSlice(resumed.Object, "spec", "componentSpecs")
	for index, expected := range []bool{false, true} {
		stopped, _, _ := unstructured.NestedBool(components[index].(map[string]any), "stop")
		if stopped != expected {
			t.Fatalf("resume component[%d] stop=%v want=%v", index, stopped, expected)
		}
	}
}

func TestKubeBlocksPauseRejectsClusterUIDChange(t *testing.T) {
	cluster := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps.kubeblocks.io/v1alpha1",
		"kind":       "Cluster",
		"metadata":   map[string]any{"name": "cluster", "namespace": "db", "uid": "new-uid"},
		"spec":       map[string]any{"componentSpecs": []any{map[string]any{"name": "postgresql", "stop": false}}},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster)
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)
	session := kubeBlocksSession()
	session.Spec.Workload().KubeBlocks.ClusterAPIVersion = "apps.kubeblocks.io/v1alpha1"
	session.Spec.Workload().KubeBlocks.ClusterUID = "old-uid"
	if err := manager.setKubeBlocksPaused(context.Background(), session, true); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}
