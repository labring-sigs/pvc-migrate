package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func TestValidateResumeRejectsOperatorControlDriftWithoutUpdates(t *testing.T) {
	tests := []struct {
		name       string
		typed      []runtime.Object
		dynamic    []runtime.Object
		workload   domain.WorkloadSpec
		wantDetail string
	}{
		{
			name: "Victoria Logs pause owner",
			typed: []runtime.Object{&appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "logs",
					Name:      "logs",
					UID:       "logs-sts-uid",
					Annotations: map[string]string{
						pauseSessionAnnotation: "other-session",
					},
				},
				Spec: appsv1.StatefulSetSpec{Replicas: new(int32)},
			}},
			workload: domain.WorkloadSpec{
				Adapter: domain.WorkloadVictoriaLogs,
				Pod:     domain.ObjectReference{Namespace: "logs", Name: "logs-0"},
				Controller: domain.ObjectReference{
					APIVersion: domain.AppsAPIVersion,
					Kind:       domain.KindStatefulSet,
					Namespace:  "logs",
					Name:       "logs",
					UID:        "logs-sts-uid",
				},
				OriginalReplicas: new(int32(1)),
			},
			wantDetail: "pause ownership changed",
		},
		{
			name:  "Grafana suspend state",
			typed: []runtime.Object{grafanaDeploymentForResume()},
			dynamic: []runtime.Object{&unstructured.Unstructured{Object: map[string]any{
				"apiVersion": grafanaAPIVersion,
				"kind":       domain.KindGrafana,
				"metadata": map[string]any{
					"name": "grafana", "namespace": "monitoring", "uid": "grafana-uid",
					"annotations": map[string]any{pauseSessionAnnotation: "session"},
				},
				"spec": map[string]any{"suspend": false},
			}}},
			workload:   grafanaResumeWorkload(),
			wantDetail: "suspend state changed",
		},
		{
			name: "VMCluster top-level pause",
			typed: []runtime.Object{operatorStatefulSet(
				"metrics", "vmstorage-metrics", "vm-sts-uid", 1,
			)},
			dynamic: []runtime.Object{&unstructured.Unstructured{Object: map[string]any{
				"apiVersion": vmClusterAPIVersion,
				"kind":       domain.KindVMCluster,
				"metadata": map[string]any{
					"name": "metrics", "namespace": "metrics", "uid": "vm-uid",
					"annotations": map[string]any{pauseSessionAnnotation: "session"},
				},
				"spec": map[string]any{
					"paused":    true,
					"vmstorage": map[string]any{"paused": true, "replicaCount": int64(1)},
				},
			}}},
			workload: domain.WorkloadSpec{
				Adapter: domain.WorkloadVMCluster,
				Pod:     domain.ObjectReference{Namespace: "metrics", Name: "vmstorage-metrics-1"},
				Controller: domain.ObjectReference{
					APIVersion: domain.AppsAPIVersion,
					Kind:       domain.KindStatefulSet,
					Namespace:  "metrics",
					Name:       "vmstorage-metrics",
					UID:        "vm-sts-uid",
				},
				OriginalReplicas: new(int32(2)),
				Ordinal:          new(int32(1)),
				VMCluster: &domain.VMClusterSpec{
					APIVersion:                 vmClusterAPIVersion,
					Name:                       "metrics",
					UID:                        "vm-uid",
					Component:                  "vmstorage",
					OriginalReplicas:           2,
					OriginalReplicasConfigured: true,
				},
			},
			wantDetail: "paused externally",
		},
		{
			name: "KubeBlocks component stop",
			dynamic: []runtime.Object{&unstructured.Unstructured{Object: map[string]any{
				"apiVersion": kubeBlocksClusterAPIVersion,
				"kind":       "Cluster",
				"metadata": map[string]any{
					"name": "database", "namespace": "database", "uid": "cluster-uid",
					"annotations": map[string]any{pauseSessionAnnotation: "session"},
				},
				"spec": map[string]any{"componentSpecs": []any{
					map[string]any{"name": "postgresql", "stop": false},
				}},
			}}},
			workload: domain.WorkloadSpec{
				Adapter: domain.WorkloadKubeBlocks,
				Pod: domain.ObjectReference{
					Namespace: "database",
					Name:      "database-postgresql-0",
				},
				Controller: domain.ObjectReference{
					APIVersion: "apps.kubeblocks.io/v1alpha1",
					Kind:       domain.KindComponent,
					Namespace:  "database",
					Name:       "database-postgresql",
					UID:        "component-uid",
				},
				KubeBlocks: &domain.KubeBlocksSpec{
					Cluster:       "database",
					ClusterUID:    "cluster-uid",
					Component:     "postgresql",
					OriginalStops: map[string]bool{"postgresql": false},
				},
			},
			wantDetail: "stop changed",
		},
		{
			name: "KubeBlocks InstanceSet pause",
			dynamic: []runtime.Object{&unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "workloads.kubeblocks.io/v1alpha1",
				"kind":       domain.KindInstanceSet,
				"metadata": map[string]any{
					"name":      "database-postgresql",
					"namespace": "database",
					"uid":       "instanceset-uid",
					"annotations": map[string]any{
						pauseSessionAnnotation: "session",
					},
				},
				"spec": map[string]any{"paused": false},
			}}},
			workload: domain.WorkloadSpec{
				Adapter: domain.WorkloadKubeBlocks,
				Pod: domain.ObjectReference{
					Namespace: "database",
					Name:      "database-postgresql-0",
				},
				Controller: domain.ObjectReference{
					APIVersion: "workloads.kubeblocks.io/v1alpha1",
					Kind:       domain.KindInstanceSet,
					Namespace:  "database",
					Name:       "database-postgresql",
					UID:        "instanceset-uid",
				},
				KubeBlocks: &domain.KubeBlocksSpec{
					Cluster:                  "database",
					Component:                "postgresql",
					OriginalPausedConfigured: true,
				},
			},
			wantDetail: "paused state changed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			typed := kubernetesfake.NewClientset(test.typed...)
			dynamic := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), test.dynamic...)
			manager := NewManager(typed, dynamic, typed.Discovery())
			session := controllerSession(test.workload)

			err := manager.ValidateResume(context.Background(), session)
			if domain.CategoryOf(err) != domain.ErrorConflict ||
				!strings.Contains(err.Error(), test.wantDetail) {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}

			for _, action := range typed.Actions() {
				if action.GetVerb() == "update" || action.GetVerb() == "patch" {
					t.Fatalf(
						"typed dry-run mutation: %s %s",
						action.GetVerb(),
						action.GetResource().Resource,
					)
				}
			}

			for _, action := range dynamic.Actions() {
				if action.GetVerb() == "update" || action.GetVerb() == "patch" {
					t.Fatalf(
						"dynamic dry-run mutation: %s %s",
						action.GetVerb(),
						action.GetResource().Resource,
					)
				}
			}
		})
	}
}

func grafanaDeploymentForResume() *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "monitoring",
			Name:      "grafana",
			UID:       "grafana-deployment-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: grafanaAPIVersion,
				Kind:       domain.KindGrafana,
				Name:       "grafana",
				UID:        "grafana-uid",
				Controller: new(true),
			}},
		},
		Spec: appsv1.DeploymentSpec{Replicas: new(int32)},
	}
}

func grafanaResumeWorkload() domain.WorkloadSpec {
	return domain.WorkloadSpec{
		Adapter: domain.WorkloadGrafana,
		Pod:     domain.ObjectReference{Namespace: "monitoring", Name: "grafana-old"},
		Controller: domain.ObjectReference{
			APIVersion: domain.AppsAPIVersion,
			Kind:       domain.KindDeployment,
			Namespace:  "monitoring",
			Name:       "grafana",
			UID:        "grafana-deployment-uid",
		},
		OriginalReplicas: new(int32(1)),
		Grafana: &domain.GrafanaSpec{
			APIVersion: grafanaAPIVersion,
			Name:       "grafana",
			UID:        "grafana-uid",
		},
	}
}

func operatorStatefulSet(
	namespace string,
	name string,
	uid types.UID,
	replicas int32,
) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: uid},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
	}
}
