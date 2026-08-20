package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
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
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "app",
					Name:      "worker",
					UID:       types.UID("worker-uid"),
				},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
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
				pod.OwnerReferences = []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "Deployment",
						Name:       "worker",
						UID:        types.UID("deployment-uid"),
						Controller: new(true),
					},
				}

				return pod
			}(),
			want: "has no safe pause adapter",
		},
		{
			name: "malformed controller version",
			pod: func() *corev1.Pod {
				pod := readyPod("app", "worker", "node-a")
				pod.OwnerReferences = []metav1.OwnerReference{
					{
						APIVersion: "bad/version/extra",
						Kind:       "Controller",
						Name:       "worker",
						UID:        types.UID("controller-uid"),
						Controller: new(true),
					},
				}

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
			manager := NewManager(
				client,
				dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
				client.Discovery(),
			)

			_, err := manager.Discover(
				context.Background(),
				DiscoverOptions{Namespace: tt.pod.Namespace, PodName: tt.pod.Name},
			)
			if domain.CategoryOf(err) != domain.ErrorPrecondition ||
				!strings.Contains(err.Error(), tt.want) {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestDiscoverRejectsIncompleteKubernetesIdentity(t *testing.T) {
	t.Run("Pod UID", func(t *testing.T) {
		pod := readyPod("app", "worker", "node-a")
		pod.UID = ""
		manager := NewManager(
			kubernetesfake.NewClientset(),
			dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
			nil,
		)

		_, err := manager.DiscoverPod(
			context.Background(),
			pod,
			DiscoverOptions{Namespace: pod.Namespace, PodName: pod.Name},
		)
		if domain.CategoryOf(err) != domain.ErrorKubernetes {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("owner UID", func(t *testing.T) {
		pod := readyPod("app", "worker", "node-a")
		pod.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: domain.AppsAPIVersion,
				Kind:       domain.KindStatefulSet,
				Name:       "worker",
				Controller: new(true),
			},
		}
		manager := NewManager(
			kubernetesfake.NewClientset(),
			dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
			nil,
		)

		_, err := manager.DiscoverPod(
			context.Background(),
			pod,
			DiscoverOptions{Namespace: pod.Namespace, PodName: pod.Name},
		)
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
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
		{
			name:          "retention deletes claims",
			selected:      "db-1",
			retention:     appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
			allowDowntime: true,
			want:          "PVC retention whenScaled is Delete",
		},
		{
			name:      "leader affected by scale down",
			selected:  "db-1",
			retention: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			role:      "primary",
			want:      "allow-leader-downtime",
		},
		{
			name:          "ordinal outside replicas",
			selected:      "db-3",
			retention:     appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			allowDowntime: true,
			want:          "outside replicas",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replicas := int32(3)
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "app",
					Name:      "db",
					UID:       types.UID("sts-uid"),
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &replicas,
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenScaled: tt.retention,
					},
				},
			}

			objects := make([]runtime.Object, 1, 5)
			objects[0] = sts

			for ordinal := range 4 {
				pod := readyPod("app", "db-"+string(rune('0'+ordinal)), "node-a")

				pod.OwnerReferences = []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "StatefulSet",
						Name:       "db",
						UID:        sts.UID,
						Controller: new(true),
					},
				}
				if pod.Name == tt.selected && tt.role != "" {
					pod.Labels = map[string]string{"role": tt.role}
				}

				objects = append(objects, pod)
			}

			client := kubernetesfake.NewClientset(objects...)
			manager := NewManager(
				client,
				dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
				client.Discovery(),
			)

			_, err := manager.Discover(
				context.Background(),
				DiscoverOptions{
					Namespace:           "app",
					PodName:             tt.selected,
					AllowLeaderDowntime: tt.allowDowntime,
				},
			)
			if domain.CategoryOf(err) != domain.ErrorPrecondition ||
				!strings.Contains(err.Error(), tt.want) {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestDiscoverRejectsReplacedControllerChain(t *testing.T) {
	t.Run("StatefulSet", func(t *testing.T) {
		replicas := int32(1)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "db", UID: "replacement-sts-uid"},
			Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		}
		pod := readyPod("app", "db-0", "node-a")
		pod.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: domain.AppsAPIVersion,
				Kind:       domain.KindStatefulSet,
				Name:       sts.Name,
				UID:        "original-sts-uid",
				Controller: new(true),
			},
		}
		client := kubernetesfake.NewClientset(sts, pod)
		manager := NewManager(
			client,
			dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
			client.Discovery(),
		)

		_, err := manager.Discover(
			context.Background(),
			DiscoverOptions{Namespace: pod.Namespace, PodName: pod.Name},
		)
		if domain.CategoryOf(err) != domain.ErrorConflict ||
			!strings.Contains(err.Error(), "StatefulSet owner UID changed") {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("ReplicaSet", func(t *testing.T) {
		replicaSet := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "app",
				Name:      "grafana-rs",
				UID:       "replacement-rs-uid",
			},
		}
		pod := readyPod("app", "grafana", "node-a")
		pod.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: domain.AppsAPIVersion,
				Kind:       domain.KindReplicaSet,
				Name:       replicaSet.Name,
				UID:        "original-rs-uid",
				Controller: new(true),
			},
		}
		client := kubernetesfake.NewClientset(replicaSet, pod)
		manager := NewManager(
			client,
			dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
			client.Discovery(),
		)

		_, err := manager.Discover(
			context.Background(),
			DiscoverOptions{Namespace: pod.Namespace, PodName: pod.Name},
		)
		if domain.CategoryOf(err) != domain.ErrorConflict ||
			!strings.Contains(err.Error(), "ReplicaSet owner UID changed") {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
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
		{
			name:     "missing identity",
			labels:   map[string]string{},
			serveAPI: true,
			want:     "lacks cluster or component",
		},
		{
			name: "missing OpsRequest API",
			labels: map[string]string{
				"app.kubernetes.io/instance":        "cluster",
				"apps.kubeblocks.io/component-name": "db",
			},
			want: "no served OpsRequest API",
		},
		{
			name: "leader needs candidate",
			labels: map[string]string{
				"app.kubernetes.io/instance":        "cluster",
				"apps.kubeblocks.io/component-name": "db",
				"kubeblocks.io/role":                "primary",
			},
			serveAPI: true,
			want:     "use --kubeblocks-candidate",
		},
		{
			name: "candidate does not exist",
			labels: map[string]string{
				"app.kubernetes.io/instance":        "cluster",
				"apps.kubeblocks.io/component-name": "db",
				"kubeblocks.io/role":                "primary",
			},
			serveAPI:      true,
			candidateName: "cluster-db-missing",
			want:          "candidate Pod db/cluster-db-missing does not exist",
		},
		{
			name: "candidate is selected Pod",
			labels: map[string]string{
				"app.kubernetes.io/instance":        "cluster",
				"apps.kubeblocks.io/component-name": "db",
				"kubeblocks.io/role":                "primary",
			},
			serveAPI:      true,
			candidateName: "cluster-db-0",
			want:          "refers to the selected source Pod",
		},
		{
			name: "candidate from another component",
			labels: map[string]string{
				"app.kubernetes.io/instance":        "cluster",
				"apps.kubeblocks.io/component-name": "db",
				"kubeblocks.io/role":                "primary",
			},
			serveAPI:      true,
			candidate:     kubeBlocksCandidate("other"),
			candidateName: "cluster-other-1",
			want:          "expected cluster cluster component db",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selected := readyPod("db", "cluster-db-0", "node-a")
			selected.OwnerReferences = []metav1.OwnerReference{
				{
					APIVersion: "workloads.kubeblocks.io/v1alpha1",
					Kind:       "InstanceSet",
					Name:       "cluster-db",
					UID:        types.UID("instanceset-uid"),
					Controller: new(true),
				},
			}
			selected.Labels = tt.labels

			objects := []runtime.Object{selected}
			if tt.candidate != nil {
				objects = append(objects, tt.candidate)
			}

			client := kubernetesfake.NewClientset(objects...)
			if tt.serveAPI {
				discovery := testutil.MustType[*fake.FakeDiscovery](t, client.Discovery())
				discovery.Resources = []*metav1.APIResourceList{
					{
						GroupVersion: "apps.kubeblocks.io/v1alpha1",
						APIResources: []metav1.APIResource{{Name: "opsrequests"}},
					},
				}
			}

			manager := NewManager(
				client,
				dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
				client.Discovery(),
			)

			_, err := manager.Discover(
				context.Background(),
				DiscoverOptions{
					Namespace:           "db",
					PodName:             selected.Name,
					SwitchoverCandidate: tt.candidateName,
					AllowLeaderDowntime: tt.allowLeader,
				},
			)
			if domain.CategoryOf(err) != domain.ErrorPrecondition ||
				!strings.Contains(err.Error(), tt.want) {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestDiscoverKubeBlocksMissingCandidateSuggestsReadySibling(t *testing.T) {
	selected := readyPod("db", "cluster-db-0", "node-a")
	selected.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: "workloads.kubeblocks.io/v1alpha1",
			Kind:       "InstanceSet",
			Name:       "cluster-db",
			UID:        types.UID("instanceset-uid"),
			Controller: new(true),
		},
	}
	selected.Labels = map[string]string{
		kube.AppInstanceLabel:    "cluster",
		kubeBlocksComponentLabel: "db",
		kubeBlocksRoleLabel:      "primary",
	}
	candidate := readyPod("db", "cluster-db-1", "node-b")
	candidate.OwnerReferences = selected.OwnerReferences
	candidate.Labels = map[string]string{
		kube.AppInstanceLabel:    "cluster",
		kubeBlocksComponentLabel: "db",
		kubeBlocksRoleLabel:      "secondary",
	}
	client := kubernetesfake.NewClientset(selected, candidate)
	discovery := testutil.MustType[*fake.FakeDiscovery](t, client.Discovery())
	discovery.Resources = []*metav1.APIResourceList{
		{
			GroupVersion: kubeBlocksClusterAPIVersion,
			APIResources: []metav1.APIResource{{Name: "opsrequests"}},
		},
	}
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		discovery,
	)

	_, err := manager.Discover(
		context.Background(),
		DiscoverOptions{
			Namespace:           "db",
			PodName:             selected.Name,
			SwitchoverCandidate: "cluster-db-missing",
		},
	)
	if domain.CategoryOf(err) != domain.ErrorPrecondition || !apierrors.IsNotFound(err) ||
		!strings.Contains(err.Error(), "does not exist") ||
		!strings.Contains(err.Error(), "--kubeblocks-candidate "+candidate.Name) {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
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
			selected.OwnerReferences = []metav1.OwnerReference{
				{
					APIVersion: "workloads.kubeblocks.io/v1alpha1",
					Kind:       "InstanceSet",
					Name:       "cluster-" + test.component,
					UID:        types.UID("instanceset-uid"),
					Controller: new(true),
				},
			}
			selected.Labels = map[string]string{
				"app.kubernetes.io/instance":        "cluster",
				"apps.kubeblocks.io/component-name": test.component,
			}
			cluster := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "apps.kubeblocks.io/v1alpha1",
				"kind":       "Cluster",
				"metadata": map[string]any{
					"name":      "cluster",
					"namespace": "db",
					"uid":       "cluster-uid",
				},
				"spec": map[string]any{"componentSpecs": []any{map[string]any{
					"name": test.component, "componentDefRef": test.definition, "stop": false,
				}}},
			}}
			client := kubernetesfake.NewClientset(selected)
			discovery := testutil.MustType[*fake.FakeDiscovery](t, client.Discovery())
			discovery.Resources = []*metav1.APIResourceList{
				{
					GroupVersion: "apps.kubeblocks.io/v1alpha1",
					APIResources: []metav1.APIResource{{Name: "opsrequests"}},
				},
			}
			manager := NewManager(
				client,
				dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster),
				discovery,
			)

			_, err := manager.Discover(
				context.Background(),
				DiscoverOptions{Namespace: "db", PodName: selected.Name, AllowLeaderDowntime: true},
			)
			if domain.CategoryOf(err) != domain.ErrorPrecondition ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestDiscoverRejectsKubeBlocksSwitchoverRejectedByAdmission(t *testing.T) {
	selected := readyPod("db", "cluster-db-0", "node-a")
	selected.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: "workloads.kubeblocks.io/v1alpha1",
			Kind:       "InstanceSet",
			Name:       "cluster-db",
			UID:        types.UID("instanceset-uid"),
			Controller: new(true),
		},
	}
	selected.Labels = map[string]string{
		"app.kubernetes.io/instance":        "cluster",
		"apps.kubeblocks.io/component-name": "db",
		"kubeblocks.io/role":                "primary",
	}
	candidate := readyPod("db", "cluster-db-1", "node-b")
	candidate.OwnerReferences = selected.OwnerReferences
	candidate.Labels = map[string]string{
		"app.kubernetes.io/instance":        "cluster",
		"apps.kubeblocks.io/component-name": "db",
		"kubeblocks.io/role":                "secondary",
	}
	typed := kubernetesfake.NewClientset(selected, candidate)
	discovery := testutil.MustType[*fake.FakeDiscovery](t, typed.Discovery())
	discovery.Resources = []*metav1.APIResourceList{
		{
			GroupVersion: "apps.kubeblocks.io/v1alpha1",
			APIResources: []metav1.APIResource{{Name: "opsrequests"}},
		},
	}
	cluster := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps.kubeblocks.io/v1alpha1",
		"kind":       "Cluster",
		"metadata":   map[string]any{"name": "cluster", "namespace": "db", "uid": "cluster-uid"},
		"spec": map[string]any{
			"componentSpecs": []any{map[string]any{"name": "db", "stop": false}},
		},
	}}
	instanceSet := kubeBlocksInstanceSetObject("workloads.kubeblocks.io/v1alpha1", new(false))
	_ = unstructured.SetNestedSlice(instanceSet.Object, []any{
		map[string]any{"name": "primary", "isLeader": true},
		map[string]any{"name": "secondary", "isLeader": false},
	}, "spec", "roles")
	_ = unstructured.SetNestedSlice(instanceSet.Object, []any{
		map[string]any{"podName": selected.Name, "role": map[string]any{"name": "primary"}},
		map[string]any{"podName": candidate.Name, "role": map[string]any{"name": "secondary"}},
	}, "status", "membersStatus")
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster, instanceSet)
	dynamicClient.PrependReactor(
		"create",
		"opsrequests",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				schema.GroupResource{Group: "apps.kubeblocks.io", Resource: "opsrequests"},
				"preflight",
				errors.New("component does not support switchover"),
			)
		},
	)
	manager := NewManager(typed, dynamicClient, discovery)

	_, err := manager.Discover(
		context.Background(),
		DiscoverOptions{
			Namespace:           "db",
			PodName:             selected.Name,
			SwitchoverCandidate: candidate.Name,
		},
	)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "automatic switchover") ||
		!strings.Contains(err.Error(), "native switchover") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestDiscoverKubeBlocksMongoDBFallsBackToNativeSwitchover(t *testing.T) {
	var logs bytes.Buffer

	selected := readyPod("db", "cluster-mongodb-0", "node-a")
	selected.Spec.Containers = []corev1.Container{{Name: "mongodb"}}
	selected.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: "workloads.kubeblocks.io/v1alpha1",
			Kind:       "InstanceSet",
			Name:       "cluster-mongodb",
			UID:        "instanceset-uid",
			Controller: new(true),
		},
	}
	selected.Labels = map[string]string{
		kube.AppInstanceLabel:    "cluster",
		kube.AppNameLabel:        "mongodb",
		kubeBlocksComponentLabel: "mongodb",
		kubeBlocksRoleLabel:      "primary",
	}
	candidate := readyPod("db", "cluster-mongodb-1", "node-b")
	candidate.OwnerReferences = selected.OwnerReferences
	candidate.Labels = map[string]string{
		kube.AppInstanceLabel:    "cluster",
		kube.AppNameLabel:        "mongodb",
		kubeBlocksComponentLabel: "mongodb",
		kubeBlocksRoleLabel:      "secondary",
	}
	typed := kubernetesfake.NewClientset(selected, candidate)
	discovery := testutil.MustType[*fake.FakeDiscovery](t, typed.Discovery())
	discovery.Resources = []*metav1.APIResourceList{
		{
			GroupVersion: kubeBlocksClusterAPIVersion,
			APIResources: []metav1.APIResource{{Name: "opsrequests"}},
		},
	}
	cluster := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": kubeBlocksClusterAPIVersion,
		"kind":       domain.KindCluster,
		"metadata":   map[string]any{"name": "cluster", "namespace": "db", "uid": "cluster-uid"},
		"spec": map[string]any{
			"componentSpecs": []any{map[string]any{"name": "mongodb", "stop": false}},
		},
	}}
	instanceSet := kubeBlocksInstanceSetObject("workloads.kubeblocks.io/v1alpha1", new(false))
	instanceSet.SetName("cluster-mongodb")
	_ = unstructured.SetNestedSlice(instanceSet.Object, []any{
		map[string]any{"name": "primary", "isLeader": true},
		map[string]any{"name": "secondary", "isLeader": false},
	}, "spec", "roles")
	_ = unstructured.SetNestedSlice(instanceSet.Object, []any{
		map[string]any{"podName": selected.Name, "role": map[string]any{"name": "primary"}},
		map[string]any{"podName": candidate.Name, "role": map[string]any{"name": "secondary"}},
	}, "status", "membersStatus")
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster, instanceSet)
	dynamicClient.PrependReactor(
		"create",
		"opsrequests",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				schema.GroupResource{Group: domain.KubeBlocksAppsGroup, Resource: "opsrequests"},
				"preflight",
				errors.New("this cluster component mongodb does not support switchover"),
			)
		},
	)

	var command podCommandRequest

	manager := NewManager(
		typed,
		dynamicClient,
		discovery,
	).WithLogger(slog.New(slog.NewTextHandler(&logs, nil)))
	manager.commandExecutor = podCommandExecutorFunc(
		func(_ context.Context, request podCommandRequest) (podCommandResult, error) {
			command = request
			return podCommandResult{}, nil
		},
	)

	workload, err := manager.Discover(
		context.Background(),
		DiscoverOptions{
			Namespace:           "db",
			PodName:             selected.Name,
			SwitchoverCandidate: candidate.Name,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if workload.KubeBlocks == nil ||
		workload.KubeBlocks.SwitchoverStrategy != domain.KubeBlocksSwitchoverMongoDBNative ||
		workload.KubeBlocks.SwitchoverContainer != "mongodb" {
		t.Fatalf("KubeBlocks state=%#v", workload.KubeBlocks)
	}

	if command.Namespace != "db" || command.Pod != selected.Name ||
		command.Container != "mongodb" ||
		strings.Join(
			command.Command,
			" ",
		) != "sh -c test -x /scripts/switchover-with-candidate.sh" {
		t.Fatalf("native script preflight=%#v", command)
	}

	output := logs.String()
	if !strings.Contains(output, "checking KubeBlocks automatic switchover") ||
		!strings.Contains(output, "checking MongoDB native switchover script") {
		t.Fatalf("logs=%q", output)
	}
}

func TestKubeBlocksLeaderGuidanceProvidesMongoDBNativeCommand(t *testing.T) {
	selected := readyPod("db", "cluster-mongodb-0", "node-a")
	owner := []metav1.OwnerReference{
		{
			APIVersion: "workloads.kubeblocks.io/v1alpha1",
			Kind:       domain.KindInstanceSet,
			Name:       "cluster-mongodb",
			UID:        "instanceset-uid",
			Controller: new(true),
		},
	}
	selected.OwnerReferences = owner
	selected.Labels = map[string]string{
		kube.AppInstanceLabel:    "cluster",
		kube.AppNameLabel:        "mongodb",
		kubeBlocksComponentLabel: "mongodb",
		kubeBlocksRoleLabel:      "primary",
	}
	candidate := readyPod("db", "cluster-mongodb-1", "node-b")
	candidate.OwnerReferences = owner
	candidate.Labels = map[string]string{
		kube.AppInstanceLabel:    "cluster",
		kube.AppNameLabel:        "mongodb",
		kubeBlocksComponentLabel: "mongodb",
		kubeBlocksRoleLabel:      "secondary",
	}
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	dynamicClient.PrependReactor(
		"create",
		"opsrequests",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				schema.GroupResource{Group: domain.KubeBlocksAppsGroup, Resource: "opsrequests"},
				"preflight",
				errors.New("component mongodb does not support switchover"),
			)
		},
	)
	manager := NewManager(kubernetesfake.NewClientset(selected, candidate), dynamicClient, nil)

	guidance := manager.kubeBlocksLeaderGuidance(
		context.Background(),
		selected,
		"cluster",
		"mongodb",
		"primary",
		kubeBlocksClusterAPIVersion,
	)
	if !strings.Contains(
		guidance,
		kubeBlocksMongoDBNativeSwitchoverCommand(
			"db",
			"cluster",
			"mongodb",
			selected.Name,
			candidate.Name,
		),
	) {
		t.Fatalf("guidance=%q", guidance)
	}
}

func TestKubeBlocksMongoDBPreflightFailureProvidesRecoveryGuidance(t *testing.T) {
	selected := readyPod("db", "cluster-mongodb-0", "node-a")
	selected.Spec.Containers = []corev1.Container{{Name: "mongodb"}}
	selected.Labels = map[string]string{kube.AppNameLabel: "mongodb"}
	candidate := "cluster-mongodb-1"
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	dynamicClient.PrependReactor(
		"create",
		"opsrequests",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				schema.GroupResource{Group: domain.KubeBlocksAppsGroup, Resource: "opsrequests"},
				"preflight",
				errors.New("component mongodb does not support switchover"),
			)
		},
	)
	manager := NewManager(kubernetesfake.NewClientset(selected), dynamicClient, nil)
	manager.commandExecutor = podCommandExecutorFunc(
		func(context.Context, podCommandRequest) (podCommandResult, error) {
			return podCommandResult{}, errors.New("script unavailable")
		},
	)

	_, _, err := manager.kubeBlocksSwitchoverStrategy(
		context.Background(),
		selected,
		"cluster",
		"mongodb",
		candidate,
		kubeBlocksClusterAPIVersion,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"verify /scripts/switchover-with-candidate.sh is executable in the mongodb container",
		) ||
		strings.Contains(err.Error(), "kubectl --namespace") {
		t.Fatalf("error=%v", err)
	}
}

func TestRunMongoDBNativeSwitchoverExecutesScriptAndWaitsForRoleLabels(t *testing.T) {
	var logs bytes.Buffer

	selected := readyPod("db", "cluster-mongodb-0", "node-a")
	owner := []metav1.OwnerReference{
		{
			APIVersion: "workloads.kubeblocks.io/v1alpha1",
			Kind:       domain.KindInstanceSet,
			Name:       "cluster-mongodb",
			UID:        "instanceset-uid",
			Controller: new(true),
		},
	}
	selected.OwnerReferences = owner
	selected.Labels = map[string]string{kubeBlocksRoleLabel: "primary"}
	candidate := readyPod("db", "cluster-mongodb-1", "node-b")
	candidate.OwnerReferences = owner
	candidate.Labels = map[string]string{kubeBlocksRoleLabel: "secondary"}
	typed := kubernetesfake.NewClientset(selected, candidate)
	manager := NewManager(
		typed,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		nil,
	).WithLogger(slog.New(slog.NewTextHandler(&logs, nil)))
	manager.poll = time.Millisecond

	var command podCommandRequest

	manager.commandExecutor = podCommandExecutorFunc(
		func(ctx context.Context, request podCommandRequest) (podCommandResult, error) {
			command = request

			current, err := typed.CoreV1().Pods("db").Get(ctx, selected.Name, metav1.GetOptions{})
			if err != nil {
				return podCommandResult{}, err
			}

			current.Labels[kubeBlocksRoleLabel] = "secondary"
			if _, err := typed.CoreV1().
				Pods("db").
				Update(ctx, current, metav1.UpdateOptions{}); err != nil {
				return podCommandResult{}, err
			}

			current, err = typed.CoreV1().Pods("db").Get(ctx, candidate.Name, metav1.GetOptions{})
			if err != nil {
				return podCommandResult{}, err
			}

			current.Labels[kubeBlocksRoleLabel] = "primary"
			_, err = typed.CoreV1().Pods("db").Update(ctx, current, metav1.UpdateOptions{})

			return podCommandResult{Stdout: "Switchover complete"}, err
		},
	)
	session := kubeBlocksSession()
	session.Spec.WorkloadPtr().Controller = objectReference(
		owner[0].APIVersion,
		owner[0].Kind,
		"db",
		owner[0].Name,
		owner[0].UID,
		"",
	)
	session.Spec.WorkloadPtr().Pod.UID = selected.UID
	kb := session.Spec.Workload().KubeBlocks
	kb.Component = "mongodb"
	kb.Instance = selected.Name
	kb.SwitchoverCandidate = candidate.Name
	kb.SwitchoverStrategy = domain.KubeBlocksSwitchoverMongoDBNative
	kb.SwitchoverContainer = "mongodb"

	if err := manager.runMongoDBNativeSwitchover(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"env",
		"KB_CONSENSUS_LEADER_POD_FQDN=cluster-mongodb-0.cluster-mongodb-headless",
		"KB_SWITCHOVER_CANDIDATE_FQDN=cluster-mongodb-1.cluster-mongodb-headless",
		"/scripts/switchover-with-candidate.sh",
	}
	if command.Namespace != "db" || command.Pod != selected.Name ||
		command.Container != "mongodb" ||
		strings.Join(command.Command, "|") != strings.Join(want, "|") {
		t.Fatalf("native switchover command=%#v", command)
	}

	output := logs.String()
	if !strings.Contains(output, "starting KubeBlocks MongoDB native switchover") ||
		!strings.Contains(output, "waiting for workload controller") ||
		!strings.Contains(
			output,
			"KubeBlocks MongoDB switchover from "+selected.Name+" to "+candidate.Name,
		) {
		t.Fatalf("logs=%q", output)
	}
}

func TestRunMongoDBNativeSwitchoverRequiresRoleConvergence(t *testing.T) {
	selected := readyPod("db", "cluster-mongodb-0", "node-a")
	owner := []metav1.OwnerReference{
		{
			APIVersion: "workloads.kubeblocks.io/v1alpha1",
			Kind:       domain.KindInstanceSet,
			Name:       "cluster-mongodb",
			UID:        "instanceset-uid",
			Controller: new(true),
		},
	}
	selected.OwnerReferences = owner
	selected.Labels = map[string]string{kubeBlocksRoleLabel: "primary"}
	candidate := readyPod("db", "cluster-mongodb-1", "node-b")
	candidate.OwnerReferences = owner
	candidate.Labels = map[string]string{kubeBlocksRoleLabel: "secondary"}
	manager := NewManager(
		kubernetesfake.NewClientset(selected, candidate),
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		nil,
	)
	manager.poll = time.Millisecond
	manager.commandExecutor = podCommandExecutorFunc(
		func(context.Context, podCommandRequest) (podCommandResult, error) {
			return podCommandResult{Stdout: "Switchover complete"}, nil
		},
	)
	session := kubeBlocksSession()
	session.Spec.WorkloadPtr().Controller = objectReference(
		owner[0].APIVersion,
		owner[0].Kind,
		"db",
		owner[0].Name,
		owner[0].UID,
		"",
	)
	session.Spec.WorkloadPtr().Pod.UID = selected.UID
	kb := session.Spec.Workload().KubeBlocks
	kb.Component = "mongodb"
	kb.Instance = selected.Name
	kb.SwitchoverCandidate = candidate.Name
	kb.SwitchoverStrategy = domain.KubeBlocksSwitchoverMongoDBNative
	kb.SwitchoverContainer = "mongodb"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := manager.runMongoDBNativeSwitchover(ctx, session)
	if domain.CategoryOf(err) != domain.ErrorTimeout ||
		!strings.Contains(err.Error(), "MongoDB switchover") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRunMongoDBNativeSwitchoverProvidesManualCommandAfterScriptFailure(t *testing.T) {
	selected := readyPod("db", "cluster-mongodb-0", "node-a")
	owner := []metav1.OwnerReference{
		{
			APIVersion: "workloads.kubeblocks.io/v1alpha1",
			Kind:       domain.KindInstanceSet,
			Name:       "cluster-mongodb",
			UID:        "instanceset-uid",
			Controller: new(true),
		},
	}
	selected.OwnerReferences = owner
	selected.Labels = map[string]string{kubeBlocksRoleLabel: "primary"}
	candidate := readyPod("db", "cluster-mongodb-1", "node-b")
	candidate.OwnerReferences = owner
	candidate.Labels = map[string]string{kubeBlocksRoleLabel: "secondary"}
	manager := NewManager(
		kubernetesfake.NewClientset(selected, candidate),
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		nil,
	)
	commandErr := errors.New("script exited with status 1")
	manager.commandExecutor = podCommandExecutorFunc(
		func(context.Context, podCommandRequest) (podCommandResult, error) {
			return podCommandResult{Stderr: "candidate is not caught up"}, commandErr
		},
	)
	session := kubeBlocksSession()
	session.Spec.WorkloadPtr().Controller = objectReference(
		owner[0].APIVersion,
		owner[0].Kind,
		"db",
		owner[0].Name,
		owner[0].UID,
		"",
	)
	session.Spec.WorkloadPtr().Pod.UID = selected.UID
	kb := session.Spec.Workload().KubeBlocks
	kb.Component = "mongodb"
	kb.Instance = selected.Name
	kb.SwitchoverCandidate = candidate.Name
	kb.SwitchoverStrategy = domain.KubeBlocksSwitchoverMongoDBNative
	kb.SwitchoverContainer = "mongodb"

	err := manager.runMongoDBNativeSwitchover(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(
			err.Error(),
			kubeBlocksMongoDBNativeSwitchoverCommand(
				"db",
				"cluster",
				"mongodb",
				selected.Name,
				candidate.Name,
			),
		) {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func kubeBlocksCandidate(component string) *corev1.Pod {
	pod := readyPod("db", "cluster-other-1", "node-b")
	pod.Labels = map[string]string{
		"app.kubernetes.io/instance":        "cluster",
		"apps.kubeblocks.io/component-name": component,
	}

	return pod
}

func TestKubeBlocksSwitchoverCommandMatchesServedAPI(t *testing.T) {
	appsCommand := kubeBlocksSwitchoverCommand(
		"db",
		"cluster",
		"postgresql",
		"cluster-postgresql-0",
		"cluster-postgresql-1",
		"apps.kubeblocks.io/v1alpha1",
	)
	if !strings.Contains(appsCommand, "instanceName: cluster-postgresql-1") ||
		strings.Contains(appsCommand, "candidateName:") {
		t.Fatalf("apps OpsRequest command=%q", appsCommand)
	}

	operationsCommand := kubeBlocksSwitchoverCommand(
		"db",
		"cluster",
		"postgresql",
		"cluster-postgresql-0",
		"cluster-postgresql-1",
		"operations.kubeblocks.io/v1alpha1",
	)
	if !strings.Contains(operationsCommand, "instanceName: cluster-postgresql-0") ||
		!strings.Contains(operationsCommand, "candidateName: cluster-postgresql-1") {
		t.Fatalf("operations OpsRequest command=%q", operationsCommand)
	}
}

func TestKubeBlocksMongoDBNativeSwitchoverCommand(t *testing.T) {
	command := kubeBlocksMongoDBNativeSwitchoverCommand(
		"db",
		"cluster",
		"mongodb",
		"cluster-mongodb-0",
		"cluster-mongodb-1",
	)
	for _, fragment := range []string{
		"kubectl --namespace db exec cluster-mongodb-0 -c mongodb -- env",
		"KB_CONSENSUS_LEADER_POD_FQDN=cluster-mongodb-0.cluster-mongodb-headless",
		"KB_SWITCHOVER_CANDIDATE_FQDN=cluster-mongodb-1.cluster-mongodb-headless",
		"/scripts/switchover-with-candidate.sh",
	} {
		if !strings.Contains(command, fragment) {
			t.Fatalf("command=%q does not contain %q", command, fragment)
		}
	}
}

func TestStatefulSetPauseResumeAreIdempotentAtDesiredReplicaCounts(t *testing.T) {
	replicas := int32(1)
	client := kubernetesfake.NewClientset(&appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "db", UID: types.UID("sts-uid")},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
	})
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	original, ordinal := int32(3), int32(1)

	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadStatefulSet,
		Controller: domain.ObjectReference{
			APIVersion: domain.AppsAPIVersion,
			Kind:       domain.KindStatefulSet,
			Namespace:  "app",
			Name:       "db",
			UID:        types.UID("sts-uid"),
		},
		OriginalReplicas: &original,
		Ordinal:          &ordinal,
	})
	if err := manager.Pause(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	sts, _ := client.AppsV1().
		StatefulSets("app").
		Get(context.Background(), "db", metav1.GetOptions{})

	sts.Spec.Replicas = &original
	if _, err := client.AppsV1().
		StatefulSets("app").
		Update(context.Background(), sts, metav1.UpdateOptions{}); err != nil {
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
	client.PrependReactor(
		"update",
		"statefulsets",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			updated := testutil.MustActionObject[*appsv1.StatefulSet](t, action)
			if *updated.Spec.Replicas == 1 {
				for _, name := range []string{"db-1", "db-2"} {
					if err := client.Tracker().
						Delete(podsResource, "app", name); err != nil &&
						!apierrors.IsNotFound(err) {
						return true, nil, err
					}
				}
			} else {
				for _, pod := range []*corev1.Pod{readyPod("app", "db-1", "node-b"), readyPod("app", "db-2", "node-b")} {
					pod.UID = types.UID(pod.Name + "-resumed-uid")

					pod.OwnerReferences = []metav1.OwnerReference{
						{
							APIVersion: domain.AppsAPIVersion,
							Kind:       domain.KindStatefulSet,
							Name:       sts.Name,
							UID:        sts.UID,
							Controller: new(true),
						},
					}
					if err := client.Tracker().
						Create(podsResource, pod, "app"); err != nil &&
						!apierrors.IsAlreadyExists(err) {
						return true, nil, err
					}
				}
			}

			return false, nil, nil
		},
	)
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	manager.poll = time.Millisecond
	original, ordinal := int32(3), int32(1)

	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadStatefulSet,
		Pod:     domain.ObjectReference{Namespace: "app", Name: "db-1", UID: pod1.UID},
		Controller: domain.ObjectReference{
			APIVersion: domain.AppsAPIVersion,
			Kind:       domain.KindStatefulSet,
			Namespace:  "app",
			Name:       "db",
			UID:        sts.UID,
		},
		OriginalReplicas: &original,
		Ordinal:          &ordinal,
		AffectedPods: []domain.ObjectReference{
			{Namespace: "app", Name: "db-1", UID: pod1.UID},
			{Namespace: "app", Name: "db-2", UID: pod2.UID},
		},
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

		var recorded domain.ObjectReference
		for _, ref := range session.Spec.Workload().AffectedPods {
			if ref.Name == name {
				recorded = ref
				break
			}
		}

		if recorded.UID != pod.UID {
			t.Fatalf("Pod %s session UID=%s, current UID=%s", name, recorded.UID, pod.UID)
		}
	}

	if session.Spec.Workload().Pod.UID != session.Spec.Workload().AffectedPods[0].UID {
		t.Fatalf(
			"workload Pod UID=%s, affected Pod UID=%s",
			session.Spec.Workload().Pod.UID,
			session.Spec.Workload().AffectedPods[0].UID,
		)
	}
}

func TestWaitForPodDeletionRejectsSameNameReplacement(t *testing.T) {
	original := readyPod("app", "db-1", "node-a")
	original.UID = types.UID("original-pod-uid")
	replacement := original.DeepCopy()
	replacement.UID = types.UID("replacement-pod-uid")
	client := kubernetesfake.NewClientset(original)
	getCount := 0
	client.PrependReactor(
		"get",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			getCount++
			if getCount == 1 {
				return true, original.DeepCopy(), nil
			}

			return true, replacement, nil
		},
	)
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	manager.poll = time.Millisecond

	err := manager.waitForPodDeletion(context.Background(), domain.ObjectReference{
		Namespace: "app",
		Name:      "db-1",
		UID:       original.UID,
	}, "pause StatefulSet")
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "replaced while waiting") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestWaitForPodDeletionRequiresExpectedUID(t *testing.T) {
	manager := NewManager(kubernetesfake.NewClientset(), nil, nil)

	err := manager.waitForPodDeletion(
		context.Background(),
		domain.ObjectReference{Namespace: "app", Name: "db-1"},
		"pause StatefulSet",
	)
	if domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestVerifyPausedChecksEveryAffectedPod(t *testing.T) {
	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "db", UID: "sts-uid"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
	}
	stale := readyPod("app", "db-2", "node-a")
	client := kubernetesfake.NewClientset(sts, stale)
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	original, ordinal := int32(3), int32(1)

	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadStatefulSet,
		Pod:     domain.ObjectReference{Namespace: "app", Name: "db-1"},
		Controller: domain.ObjectReference{
			APIVersion: domain.AppsAPIVersion,
			Kind:       domain.KindStatefulSet,
			Namespace:  "app",
			Name:       "db",
			UID:        sts.UID,
		},
		OriginalReplicas: &original,
		Ordinal:          &ordinal,
		AffectedPods: []domain.ObjectReference{
			{Namespace: "app", Name: "db-1"},
			{Namespace: "app", Name: "db-2"},
		},
	})
	if err := manager.VerifyPaused(
		context.Background(),
		session,
	); domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "db-2") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestVerifyPausedRejectsStatefulSetControlDrift(t *testing.T) {
	for _, test := range []struct {
		name        string
		uid         types.UID
		replicas    int32
		category    domain.ErrorCategory
		messagePart string
	}{
		{name: "controller replaced", uid: "replacement-uid", replicas: 1, category: domain.ErrorConflict, messagePart: "UID changed"},
		{name: "replicas changed", uid: "sts-uid", replicas: 2, category: domain.ErrorPrecondition, messagePart: "replicas=2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "db", UID: test.uid},
				Spec:       appsv1.StatefulSetSpec{Replicas: &test.replicas},
			}
			client := kubernetesfake.NewClientset(sts)
			manager := NewManager(
				client,
				dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
				client.Discovery(),
			)
			original, ordinal := int32(3), int32(1)
			session := controllerSession(domain.WorkloadSpec{
				Adapter: domain.WorkloadStatefulSet,
				Pod:     domain.ObjectReference{Namespace: "app", Name: "db-1"},
				Controller: domain.ObjectReference{
					APIVersion: domain.AppsAPIVersion,
					Kind:       domain.KindStatefulSet,
					Namespace:  "app",
					Name:       "db",
					UID:        "sts-uid",
				},
				OriginalReplicas: &original,
				Ordinal:          &ordinal,
				AffectedPods:     []domain.ObjectReference{{Namespace: "app", Name: "db-1"}},
			})

			err := manager.VerifyPaused(context.Background(), session)
			if domain.CategoryOf(err) != test.category ||
				!strings.Contains(err.Error(), test.messagePart) {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestResumeStatefulSetRejectsForeignReadyPod(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "db", UID: "sts-uid"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
	}
	client := kubernetesfake.NewClientset(sts)
	client.PrependReactor(
		"update",
		"statefulsets",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			foreign := readyPod("app", "db-1", "node-b")

			foreign.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: domain.AppsAPIVersion,
				Kind:       domain.KindStatefulSet,
				Name:       "other-db",
				UID:        "other-sts-uid",
				Controller: new(true),
			}}
			if err := client.Tracker().
				Create(corev1.SchemeGroupVersion.WithResource("pods"), foreign, "app"); err != nil &&
				!apierrors.IsAlreadyExists(err) {
				return true, nil, err
			}

			return false, nil, nil
		},
	)
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	manager.poll = time.Millisecond
	original, ordinal := int32(2), int32(1)
	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadStatefulSet,
		Controller: objectReference(
			domain.AppsAPIVersion,
			domain.KindStatefulSet,
			"app",
			"db",
			sts.UID,
			sts.ResourceVersion,
		),
		OriginalReplicas: &original,
		Ordinal:          &ordinal,
		AffectedPods:     []domain.ObjectReference{{Namespace: "app", Name: "db-1"}},
	})

	err := manager.Resume(ctx, session)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "not controlled by the expected StatefulSet") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestPodControlledByDeploymentRejectsMatchingForeignPod(t *testing.T) {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "grafana", UID: "deployment-uid"},
	}
	replicaSet := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: "app",
		Name:      "foreign-rs",
		UID:       "foreign-rs-uid",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: domain.AppsAPIVersion,
			Kind:       domain.KindDeployment,
			Name:       "other-deployment",
			UID:        "other-deployment-uid",
			Controller: new(true),
		}},
	}}
	pod := readyPod("app", "matching-labels", "node-a")
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: domain.AppsAPIVersion,
		Kind:       domain.KindReplicaSet,
		Name:       replicaSet.Name,
		UID:        replicaSet.UID,
		Controller: new(true),
	}}
	manager := NewManager(
		kubernetesfake.NewClientset(replicaSet),
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		nil,
	)

	owned, err := manager.podControlledByDeployment(context.Background(), pod, deployment)
	if err != nil {
		t.Fatal(err)
	}

	if owned {
		t.Fatal("foreign Pod with matching labels was accepted as a Deployment Pod")
	}
}

func TestStatefulSetSessionRequiresCompleteReplicaState(t *testing.T) {
	manager := NewManager(
		kubernetesfake.NewClientset(),
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		nil,
	)

	session := controllerSession(domain.WorkloadSpec{Adapter: domain.WorkloadStatefulSet})
	if err := manager.Pause(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorInternal {
		t.Fatalf("pause category=%s error=%v", domain.CategoryOf(err), err)
	}

	if err := manager.Resume(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorInternal {
		t.Fatalf("resume category=%s error=%v", domain.CategoryOf(err), err)
	}

	original := int32(3)

	session.Spec.WorkloadPtr().OriginalReplicas = &original
	if err := manager.Resume(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorInternal {
		t.Fatalf("resume missing ordinal category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestStandalonePauseIsIdempotentAndProtectsUID(t *testing.T) {
	missingClient := kubernetesfake.NewClientset()
	manager := NewManager(
		missingClient,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		missingClient.Discovery(),
	)

	session := controllerSession(
		domain.WorkloadSpec{
			Adapter: domain.WorkloadStandalone,
			Pod: domain.ObjectReference{
				Namespace: "app",
				Name:      "worker",
				UID:       types.UID("old"),
			},
		},
	)
	if err := manager.Pause(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	pod := readyPod("app", "worker", "node-a")
	client := kubernetesfake.NewClientset(pod)
	manager = NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)

	err := manager.Pause(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestStandalonePauseRejectsReplacementWhileWaitingForDeletion(t *testing.T) {
	pod := readyPod("app", "worker", "node-a")
	pod.UID = "original-uid"
	client := kubernetesfake.NewClientset(pod)
	deleted := false
	client.PrependReactor(
		"delete",
		"pods",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			deleted = true
			return true, nil, nil
		},
	)
	client.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		if !deleted {
			return false, nil, nil
		}

		replacement := pod.DeepCopy()
		replacement.UID = "replacement-uid"

		return true, replacement, nil
	})
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	manager.poll = time.Millisecond

	session := controllerSession(
		domain.WorkloadSpec{
			Adapter: domain.WorkloadStandalone,
			Pod:     domain.ObjectReference{Namespace: "app", Name: "worker", UID: pod.UID},
		},
	)
	if err := manager.Pause(
		context.Background(),
		session,
	); domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "replaced while waiting for deletion") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestStandaloneResumeHandlesExistingPodsAndNodeValidation(t *testing.T) {
	owned := readyPod("app", "worker", "node-a")
	owned.Annotations = map[string]string{kube.SessionKey: "session"}
	client := kubernetesfake.NewClientset(owned)
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)

	session := controllerSession(
		domain.WorkloadSpec{
			Adapter:        domain.WorkloadStandalone,
			Pod:            domain.ObjectReference{Namespace: "app", Name: "worker"},
			OriginalObject: []byte("invalid"),
		},
	)
	if err := manager.Resume(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Spec.Workload().Pod.UID != owned.UID {
		t.Fatalf("session Pod UID=%s want=%s", session.Spec.Workload().Pod.UID, owned.UID)
	}

	foreign := owned.DeepCopy()
	foreign.Annotations[kube.SessionKey] = "another-session"
	foreignClient := kubernetesfake.NewClientset(foreign)
	manager = NewManager(
		foreignClient,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		foreignClient.Discovery(),
	)

	err := manager.Resume(
		context.Background(),
		controllerSession(
			domain.WorkloadSpec{
				Adapter: domain.WorkloadStandalone,
				Pod:     domain.ObjectReference{Namespace: "app", Name: "worker"},
			},
		),
	)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	saved := readyPod("app", "worker", "old-node")

	raw, err := json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}

	nodeClient := kubernetesfake.NewClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "target"}},
	)
	manager = NewManager(
		nodeClient,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		nodeClient.Discovery(),
	)
	nodeSession := controllerSession(
		domain.WorkloadSpec{
			Adapter:        domain.WorkloadStandalone,
			Pod:            domain.ObjectReference{Namespace: "app", Name: "worker"},
			OriginalObject: raw,
		},
	)
	nodeSession.Spec.WorkflowOptionsPtr().TargetNode = "target"

	err = manager.Resume(context.Background(), nodeSession)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "lacks kubernetes.io/hostname") {
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
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "source",
				Labels: map[string]string{corev1.LabelHostname: "source-host"},
			},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "target",
				Labels: map[string]string{corev1.LabelHostname: "target-host"},
			},
		},
	)
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action).DeepCopy()
			pod.UID = types.UID("resumed-uid")

			pod.Status = corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			}
			if err := client.Tracker().
				Create(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace); err != nil {
				t.Fatal(err)
			}

			return true, pod, nil
		},
	)
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
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

	resumed, err := client.CoreV1().
		Pods("app").
		Get(context.Background(), "worker", metav1.GetOptions{})
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
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			created := testutil.MustActionObject[*corev1.Pod](t, action)
			foreign := readyPod(created.Namespace, created.Name, "node-b")

			foreign.Annotations = map[string]string{kube.SessionKey: "foreign-session"}
			if err := client.Tracker().
				Create(corev1.SchemeGroupVersion.WithResource("pods"), foreign, created.Namespace); err != nil {
				t.Fatalf("create concurrent Pod: %v", err)
			}

			return true, nil, apierrors.NewAlreadyExists(corev1.Resource("pods"), created.Name)
		},
	)
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)

	session := controllerSession(domain.WorkloadSpec{
		Adapter:        domain.WorkloadStandalone,
		Pod:            domain.ObjectReference{Namespace: "app", Name: "worker"},
		OriginalObject: raw,
	})
	if err := manager.Resume(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestStandaloneResumeRejectsReplacementWhileWaiting(t *testing.T) {
	saved := readyPod("app", "worker", "node-a")

	raw, err := json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}

	client := kubernetesfake.NewClientset()

	var created *corev1.Pod
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			created = testutil.MustActionObject[*corev1.Pod](t, action)
			created.UID = "created-uid"
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"get",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			if created == nil {
				return false, nil, nil
			}

			replacement := created.DeepCopy()
			replacement.UID = "replacement-uid"
			replacement.Status = corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			}

			return true, replacement, nil
		},
	)
	manager := NewManager(
		client,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		client.Discovery(),
	)
	manager.poll = time.Millisecond
	session := controllerSession(
		domain.WorkloadSpec{
			Adapter:        domain.WorkloadStandalone,
			Pod:            domain.ObjectReference{Namespace: "app", Name: "worker"},
			OriginalObject: raw,
		},
	)

	err = manager.Resume(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "replaced while waiting") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func controllerSession(workload domain.WorkloadSpec) *domain.Session {
	return domain.NewSession(
		"session",
		domain.NewSessionSpec(domain.OperationMigrate, domain.SessionCommon{
			SourceNamespace: "app", TemporaryNamespace: "system", SessionNamespace: "system",
			Volumes: []domain.VolumeSpec{{SourcePVC: domain.ObjectReference{Name: "data"}}},
		}, workload, false, domain.SessionWorkflowOptions{}),
		time.Now(),
	)
}

func TestCreateAndWaitOpsReusesSucceededAndRetriesFailedRequests(t *testing.T) {
	gvr := schema.GroupVersionResource{
		Group:    "apps.kubeblocks.io",
		Version:  "v1alpha1",
		Resource: "opsrequests",
	}
	succeeded := opsRequest("session-offline", "Succeed")
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), succeeded)
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)
	manager.poll = time.Millisecond

	session := kubeBlocksSession()
	if err := manager.createAndWaitOps(
		context.Background(),
		session,
		"offline",
		map[string]any{"type": "HorizontalScaling"},
	); err != nil {
		t.Fatal(err)
	}

	if countDynamicActions(dynamicClient.Actions(), "create", gvr.Resource) != 0 {
		t.Fatal("succeeded request should be reused")
	}

	failed := opsRequest("session-online", "Failed")
	dynamicClient = dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), failed)
	dynamicClient.PrependReactor(
		"create",
		"opsrequests",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			object := testutil.MustActionObject[*unstructured.Unstructured](t, action)
			object.SetUID("created-request-uid")
			_ = unstructured.SetNestedField(object.Object, "Succeed", "status", "phase")
			return false, nil, nil
		},
	)
	manager = NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)

	manager.poll = time.Millisecond
	if err := manager.createAndWaitOps(
		context.Background(),
		session,
		"online",
		map[string]any{"type": "HorizontalScaling"},
	); err != nil {
		t.Fatal(err)
	}

	if countDynamicActions(dynamicClient.Actions(), "delete", gvr.Resource) != 1 ||
		countDynamicActions(dynamicClient.Actions(), "create", gvr.Resource) != 1 {
		t.Fatalf("actions=%#v", dynamicClient.Actions())
	}
}

func TestCreateAndWaitOpsReturnsTerminalFailure(t *testing.T) {
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	dynamicClient.PrependReactor(
		"create",
		"opsrequests",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			object := testutil.MustActionObject[*unstructured.Unstructured](t, action)
			object.SetUID("created-request-uid")
			_ = unstructured.SetNestedField(object.Object, "Cancelled", "status", "phase")
			return false, nil, nil
		},
	)
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)
	manager.poll = time.Millisecond

	err := manager.createAndWaitOps(
		context.Background(),
		kubeBlocksSession(),
		"offline",
		map[string]any{"type": "HorizontalScaling"},
	)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "Cancelled") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestCreateAndWaitOpsRejectsMissingCreatedUID(t *testing.T) {
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)

	err := manager.createAndWaitOps(
		context.Background(),
		kubeBlocksSession(),
		"offline",
		map[string]any{"type": "HorizontalScaling"},
	)
	if domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestCreateAndWaitOpsRejectsForeignExistingRequest(t *testing.T) {
	foreign := opsRequest("session-offline", "Failed")
	labels := foreign.GetLabels()
	labels[kube.SessionKey] = "foreign-session"
	foreign.SetLabels(labels)
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), foreign)
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)
	manager.poll = time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := manager.createAndWaitOps(
		ctx,
		kubeBlocksSession(),
		"offline",
		map[string]any{"type": "HorizontalScaling"},
	)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if countDynamicActions(dynamicClient.Actions(), "delete", "opsrequests") != 0 {
		t.Fatal("foreign OpsRequest was deleted")
	}
}

func TestCreateAndWaitOpsRejectsReplacementWhileWaiting(t *testing.T) {
	original := opsRequest("session-offline", "Running")
	replacement := original.DeepCopy()
	replacement.SetUID("replacement-uid")
	_ = unstructured.SetNestedField(replacement.Object, "Succeed", "status", "phase")
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), original)
	reads := 0
	dynamicClient.PrependReactor(
		"get",
		"opsrequests",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			reads++
			if reads == 1 {
				return true, original.DeepCopy(), nil
			}

			return true, replacement.DeepCopy(), nil
		},
	)
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)
	manager.poll = time.Millisecond

	err := manager.createAndWaitOps(
		context.Background(),
		kubeBlocksSession(),
		"offline",
		map[string]any{"type": "HorizontalScaling"},
	)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "replaced while waiting") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestKubeBlocksAdaptersRequireState(t *testing.T) {
	manager := NewManager(
		kubernetesfake.NewClientset(),
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		nil,
	)

	session := controllerSession(domain.WorkloadSpec{Adapter: domain.WorkloadKubeBlocks})
	if err := manager.Pause(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorInternal {
		t.Fatalf("pause category=%s error=%v", domain.CategoryOf(err), err)
	}

	if err := manager.Resume(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorInternal {
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
			"labels": map[string]any{
				kube.ManagedByLabel: kube.ManagedByValue,
				kube.SessionKey:     "session",
			},
		},
		"status": map[string]any{"phase": phase},
	}}
}

func kubeBlocksSession() *domain.Session {
	session := controllerSession(domain.WorkloadSpec{
		Adapter: domain.WorkloadKubeBlocks,
		Pod:     domain.ObjectReference{Namespace: "db", Name: "cluster-db-0"},
		KubeBlocks: &domain.KubeBlocksSpec{
			Cluster: "cluster", Component: "db", Instance: "cluster-db-0",
			SwitchoverStrategy: domain.KubeBlocksSwitchoverOpsRequest,
			OpsAPIVersion:      "apps.kubeblocks.io/v1alpha1",
			ClusterUID:         "cluster-uid",
			OriginalStops:      map[string]bool{"db": false},
		},
	})

	return session
}

func kubeBlocksInstanceSetObject(apiVersion string, paused *bool) *unstructured.Unstructured {
	spec := map[string]any{}
	if paused != nil {
		spec["paused"] = *paused
	}

	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion,
		"kind":       "InstanceSet",
		"metadata": map[string]any{
			"name":      "cluster-db",
			"namespace": "db",
			"uid":       "instanceset-uid",
		},
		"spec": spec,
	}}
}

func kubeBlocksInstanceSetSession(
	apiVersion string,
	originalPaused, configured bool,
) *domain.Session {
	session := kubeBlocksSession()
	session.Spec.WorkloadPtr().Controller = domain.ObjectReference{
		APIVersion: apiVersion,
		Kind:       "InstanceSet",
		Namespace:  "db",
		Name:       "cluster-db",
		UID:        "instanceset-uid",
	}
	session.Spec.Workload().KubeBlocks.OriginalPaused = originalPaused
	session.Spec.Workload().KubeBlocks.OriginalPausedConfigured = configured

	return session
}

func TestDiscoverKubeBlocksInstanceSetProbesOmittedPausedField(t *testing.T) {
	for _, apiVersion := range []string{"workloads.kubeblocks.io/v1alpha1", "workloads.kubeblocks.io/v1"} {
		t.Run(apiVersion, func(t *testing.T) {
			object := kubeBlocksInstanceSetObject(apiVersion, nil)
			dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
			manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)
			owner := &metav1.OwnerReference{
				APIVersion: apiVersion,
				Kind:       "InstanceSet",
				Name:       object.GetName(),
				UID:        object.GetUID(),
			}

			state, err := manager.discoverKubeBlocksInstanceSet(
				context.Background(),
				"db",
				owner,
				"cluster-db-0",
			)
			if err != nil {
				t.Fatal(err)
			}

			if state.Paused || state.PausedConfigured || state.UID != object.GetUID() {
				t.Fatalf("state=%+v", state)
			}

			foundDryRun := false
			for _, action := range dynamicClient.Actions() {
				if action.GetVerb() != "update" ||
					action.GetResource().Resource != instanceSetResource {
					continue
				}

				options := testutil.MustType[interface {
					GetUpdateOptions() metav1.UpdateOptions
				}](t, action).GetUpdateOptions()
				foundDryRun = len(options.DryRun) > 0
			}

			if !foundDryRun {
				t.Fatal("InstanceSet paused support was not probed with dry-run")
			}
		})
	}
}

func TestDiscoverKubeBlocksInstanceSetRejectsPrunedPausedField(t *testing.T) {
	apiVersion := "workloads.kubeblocks.io/v1alpha1"
	object := kubeBlocksInstanceSetObject(apiVersion, nil)
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
	dynamicClient.PrependReactor(
		"update",
		instanceSetResource,
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			updated := testutil.MustActionObject[*unstructured.Unstructured](t, action).DeepCopy()
			unstructured.RemoveNestedField(updated.Object, "spec", "paused")
			return true, updated, nil
		},
	)
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)
	owner := &metav1.OwnerReference{
		APIVersion: apiVersion,
		Kind:       "InstanceSet",
		Name:       object.GetName(),
		UID:        object.GetUID(),
	}

	_, err := manager.discoverKubeBlocksInstanceSet(
		context.Background(),
		"db",
		owner,
		"cluster-db-0",
	)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "does not support spec.paused") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestDiscoverKubeBlocksUsesInstanceSetRoleStatus(t *testing.T) {
	for _, test := range []struct {
		name       string
		labelRole  string
		memberRole string
		allow      bool
		wantRole   string
		wantError  string
	}{
		{name: "missing role is blocked", wantError: "role is unavailable"},
		{name: "missing role accepts downtime", allow: true, wantRole: "unknown"},
		{name: "member status resolves role", memberRole: "secondary", wantRole: "secondary"},
		{name: "label and status conflict", labelRole: "primary", memberRole: "secondary", allow: true, wantError: "role changed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			const apiVersion = "workloads.kubeblocks.io/v1alpha1"

			selected := readyPod("db", "cluster-db-0", "node-a")
			selected.OwnerReferences = []metav1.OwnerReference{
				{
					APIVersion: apiVersion,
					Kind:       domain.KindInstanceSet,
					Name:       "cluster-db",
					UID:        "instanceset-uid",
					Controller: new(true),
				},
			}

			selected.Labels = map[string]string{
				kube.AppInstanceLabel:    "cluster",
				kubeBlocksComponentLabel: "db",
			}
			if test.labelRole != "" {
				selected.Labels[kubeBlocksRoleLabel] = test.labelRole
			}

			typed := kubernetesfake.NewClientset(selected)
			discovery := testutil.MustType[*fake.FakeDiscovery](t, typed.Discovery())
			discovery.Resources = []*metav1.APIResourceList{
				{
					GroupVersion: kubeBlocksClusterAPIVersion,
					APIResources: []metav1.APIResource{{Name: "opsrequests"}},
				},
			}
			cluster := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": kubeBlocksClusterAPIVersion,
				"kind":       domain.KindCluster,
				"metadata": map[string]any{
					"name":      "cluster",
					"namespace": "db",
					"uid":       "cluster-uid",
				},
				"spec": map[string]any{
					"componentSpecs": []any{map[string]any{"name": "db", "stop": false}},
				},
			}}
			instanceSet := kubeBlocksInstanceSetObject(apiVersion, new(false))

			_ = unstructured.SetNestedSlice(instanceSet.Object, []any{
				map[string]any{"name": "primary", "isLeader": true},
				map[string]any{"name": "secondary", "isLeader": false},
			}, "spec", "roles")
			if test.memberRole != "" {
				_ = unstructured.SetNestedSlice(instanceSet.Object, []any{
					map[string]any{
						"podName": selected.Name,
						"role":    map[string]any{"name": test.memberRole},
					},
				}, "status", "membersStatus")
			}

			manager := NewManager(
				typed,
				dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster, instanceSet),
				discovery,
			)

			workload, err := manager.Discover(
				context.Background(),
				DiscoverOptions{
					Namespace:           "db",
					PodName:             selected.Name,
					AllowLeaderDowntime: test.allow,
				},
			)
			if test.wantError != "" {
				if domain.CategoryOf(err) != domain.ErrorPrecondition &&
					domain.CategoryOf(err) != domain.ErrorConflict {
					t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
				}

				if !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("error=%v want=%q", err, test.wantError)
				}

				return
			}

			if err != nil {
				t.Fatal(err)
			}

			if workload.KubeBlocks == nil || workload.KubeBlocks.Role != test.wantRole {
				t.Fatalf("workload=%#v", workload.KubeBlocks)
			}
		})
	}
}

func TestDiscoverKubeBlocksInstanceSetRejectsInitiallyPausedState(t *testing.T) {
	const apiVersion = "workloads.kubeblocks.io/v1alpha1"

	selected := readyPod("db", "cluster-db-0", "node-a")
	selected.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: apiVersion,
			Kind:       domain.KindInstanceSet,
			Name:       "cluster-db",
			UID:        "instanceset-uid",
			Controller: new(true),
		},
	}
	selected.Labels = map[string]string{
		kube.AppInstanceLabel:    "cluster",
		kubeBlocksComponentLabel: "db",
		kubeBlocksRoleLabel:      "secondary",
	}
	typed := kubernetesfake.NewClientset(selected)
	discovery := testutil.MustType[*fake.FakeDiscovery](t, typed.Discovery())
	discovery.Resources = []*metav1.APIResourceList{
		{
			GroupVersion: kubeBlocksClusterAPIVersion,
			APIResources: []metav1.APIResource{{Name: "opsrequests"}},
		},
	}
	cluster := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": kubeBlocksClusterAPIVersion,
		"kind":       domain.KindCluster,
		"metadata":   map[string]any{"name": "cluster", "namespace": "db", "uid": "cluster-uid"},
		"spec": map[string]any{
			"componentSpecs": []any{map[string]any{"name": "db", "stop": false}},
		},
	}}
	originalPaused := true
	instanceSet := kubeBlocksInstanceSetObject(apiVersion, &originalPaused)
	manager := NewManager(
		typed,
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster, instanceSet),
		discovery,
	)

	_, err := manager.Discover(
		context.Background(),
		DiscoverOptions{Namespace: "db", PodName: selected.Name},
	)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "already paused") ||
		!strings.Contains(err.Error(), "spec.paused=false") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestKubeBlocksInstanceSetPauseRestoresOmittedState(t *testing.T) {
	ctx := context.Background()
	apiVersion := "workloads.kubeblocks.io/v1"
	object := kubeBlocksInstanceSetObject(apiVersion, nil)
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)

	session := kubeBlocksInstanceSetSession(apiVersion, false, false)
	if err := manager.setKubeBlocksPaused(ctx, session, true); err != nil {
		t.Fatal(err)
	}

	resource := dynamicClient.Resource(mustGVR(apiVersion, instanceSetResource)).Namespace("db")

	paused, err := resource.Get(ctx, object.GetName(), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if current, found, nestedErr := unstructured.NestedBool(
		paused.Object,
		"spec",
		"paused",
	); nestedErr != nil || !found ||
		!current {
		t.Fatalf("paused=%t found=%t err=%v", current, found, nestedErr)
	}

	if err := manager.setKubeBlocksPaused(ctx, session, false); err != nil {
		t.Fatal(err)
	}

	resumed, err := resource.Get(ctx, object.GetName(), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, found, nestedErr := unstructured.NestedBool(
		resumed.Object,
		"spec",
		"paused",
	); nestedErr != nil ||
		found {
		t.Fatalf("restored paused field found=%t err=%v", found, nestedErr)
	}

	if resumed.GetAnnotations()[pauseSessionAnnotation] != "" {
		t.Fatalf("pause owner=%q", resumed.GetAnnotations()[pauseSessionAnnotation])
	}
}

func TestKubeBlocksInstanceSetResumeRejectsInitiallyPausedState(t *testing.T) {
	ctx := context.Background()
	apiVersion := "workloads.kubeblocks.io/v1alpha1"
	originalPaused := true
	object := kubeBlocksInstanceSetObject(apiVersion, &originalPaused)
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)

	session := kubeBlocksInstanceSetSession(apiVersion, true, true)
	if err := manager.setKubeBlocksPaused(ctx, session, true); err != nil {
		t.Fatal(err)
	}

	if err := manager.resumeKubeBlocks(
		ctx,
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	resumed, err := dynamicClient.Resource(mustGVR(apiVersion, instanceSetResource)).
		Namespace("db").
		Get(ctx, object.GetName(), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if current, found, nestedErr := unstructured.NestedBool(
		resumed.Object,
		"spec",
		"paused",
	); nestedErr != nil || !found ||
		!current {
		t.Fatalf("paused=%t found=%t err=%v", current, found, nestedErr)
	}

	if resumed.GetAnnotations()[pauseSessionAnnotation] != session.ID {
		t.Fatalf("pause owner=%q", resumed.GetAnnotations()[pauseSessionAnnotation])
	}
}

func TestKubeBlocksInstanceSetPauseRejectsForeignOwnership(t *testing.T) {
	apiVersion := "workloads.kubeblocks.io/v1alpha1"
	originalPaused := false
	object := kubeBlocksInstanceSetObject(apiVersion, &originalPaused)
	object.SetAnnotations(map[string]string{pauseSessionAnnotation: "other-session"})
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)

	err := manager.setKubeBlocksPaused(
		context.Background(),
		kubeBlocksInstanceSetSession(apiVersion, false, true),
		true,
	)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "other-session") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
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
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Resource: "pods"},
			"cluster-db-0",
			errors.New("denied"),
		)
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

func TestKubeBlocksPauseOnlyStopsSelectedComponent(t *testing.T) {
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
			map[string]any{"name": "etcd", "stop": false},
		}},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster)
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)
	session := kubeBlocksSession()
	session.Spec.Workload().KubeBlocks.Component = "postgresql"

	session.Spec.Workload().KubeBlocks.OriginalStops = map[string]bool{"postgresql": false}
	if err := manager.setKubeBlocksPaused(ctx, session, true); err != nil {
		t.Fatal(err)
	}

	paused, err := dynamicClient.Resource(mustGVR(kubeBlocksClusterAPIVersion, clusterResource)).
		Namespace("db").
		Get(ctx, "cluster", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	components, _, _ := unstructured.NestedSlice(paused.Object, "spec", "componentSpecs")
	for index, expected := range []bool{true, false} {
		component := testutil.MustType[map[string]any](t, components[index])

		stopped, _, err := unstructured.NestedBool(component, "stop")
		if err != nil {
			t.Fatal(err)
		}

		if stopped != expected {
			t.Fatalf("pause component[%d] stop=%v want=%v", index, stopped, expected)
		}
	}

	if err := manager.setKubeBlocksPaused(ctx, session, false); err != nil {
		t.Fatal(err)
	}

	resumed, err := dynamicClient.Resource(mustGVR(kubeBlocksClusterAPIVersion, clusterResource)).
		Namespace("db").
		Get(ctx, "cluster", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	components, _, _ = unstructured.NestedSlice(resumed.Object, "spec", "componentSpecs")
	for index, expected := range []bool{false, false} {
		component := testutil.MustType[map[string]any](t, components[index])

		stopped, _, err := unstructured.NestedBool(component, "stop")
		if err != nil {
			t.Fatal(err)
		}

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
		"spec": map[string]any{
			"componentSpecs": []any{map[string]any{"name": "postgresql", "stop": false}},
		},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster)
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)
	session := kubeBlocksSession()

	session.Spec.Workload().KubeBlocks.ClusterUID = "old-uid"
	if err := manager.setKubeBlocksPaused(
		context.Background(),
		session,
		true,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}
