package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
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

func TestValidateKubeBlocksLegacyResumeAcceptsStoppedReplicas(t *testing.T) {
	cluster := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": kubeBlocksClusterAPIVersion,
		"kind":       "Cluster",
		"metadata": map[string]any{
			"name": "cluster", "namespace": "db", "uid": "cluster-uid",
			"annotations": map[string]any{pauseSessionAnnotation: "session"},
		},
		"spec": map[string]any{"componentSpecs": []any{
			map[string]any{"name": "db", "replicas": int64(0)},
		}},
		"status": map[string]any{"phase": "Stopped"},
	}}

	dynamicClient := dynamicfake.NewSimpleDynamicClient(
		runtime.NewScheme(),
		cluster,
		opsRequest("session-pause", "Succeed"),
	)
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)

	if err := manager.ValidateResume(context.Background(), kubeBlocksSession()); err != nil {
		t.Fatalf("ValidateResume() error=%v", err)
	}
}

func TestValidateKubeBlocksLegacyResumeAcceptsConvergedFailedStart(t *testing.T) {
	cluster := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": kubeBlocksClusterAPIVersion,
		"kind":       "Cluster",
		"metadata": map[string]any{
			"name": "cluster", "namespace": "db", "uid": "cluster-uid",
			"annotations": map[string]any{pauseSessionAnnotation: "session"},
		},
		"spec": map[string]any{"componentSpecs": []any{
			map[string]any{"name": "db", "replicas": int64(1)},
		}},
		"status": map[string]any{
			"phase": "Running",
			"components": map[string]any{
				"db": map[string]any{"phase": "Running"},
			},
		},
	}}

	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), cluster)
	manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)
	session := kubeBlocksSession()

	if err := manager.ValidateResume(context.Background(), session); err != nil {
		t.Fatalf("ValidateResume() error=%v", err)
	}

	if err := manager.setKubeBlocksPaused(context.Background(), session, false); err != nil {
		t.Fatalf("setKubeBlocksPaused() error=%v", err)
	}

	if got := countDynamicActions(dynamicClient.Actions(), "create", "opsrequests"); got != 0 {
		t.Fatalf("created %d redundant Start OpsRequest(s)", got)
	}
}

func TestKubeBlocksLegacyResumeConvergenceChecksOperationsComponent(t *testing.T) {
	cluster := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": kubeBlocksClusterAPIVersion,
		"kind":       "Cluster",
		"metadata": map[string]any{
			"name": "cluster", "namespace": "db", "uid": "cluster-uid",
		},
		"spec": map[string]any{"componentSpecs": []any{
			map[string]any{"name": "db", "replicas": int64(1)},
		}},
		"status": map[string]any{
			"phase": "Running",
			"components": map[string]any{
				"db": map[string]any{"phase": "Stopped"},
			},
		},
	}}
	kb := &domain.KubeBlocksSpec{
		Component:     "db",
		OpsAPIVersion: kubeBlocksOpsAPIVersion,
	}

	converged, err := kubeBlocksLegacyResumeConverged(cluster, kb)
	if err != nil {
		t.Fatal(err)
	}

	if converged {
		t.Fatal("reported convergence while the operations component was Stopped")
	}

	if err := unstructured.SetNestedField(
		cluster.Object,
		"Running",
		"status", "components", "db", "phase",
	); err != nil {
		t.Fatal(err)
	}

	converged, err = kubeBlocksLegacyResumeConverged(cluster, kb)
	if err != nil {
		t.Fatal(err)
	}

	if !converged {
		t.Fatal("did not report convergence after the operations component became Running")
	}
}

func TestLegacyResumeValidatesExistingStartWhileClusterConverges(t *testing.T) {
	for _, test := range []struct {
		name      string
		phase     string
		missing   bool
		foreign   bool
		wantError bool
	}{
		{name: "pending", phase: ""},
		{name: "running", phase: "Running"},
		{name: "succeeded", phase: "Succeed"},
		{name: "missing", missing: true, wantError: true},
		{name: "failed", phase: "Failed", wantError: true},
		{name: "foreign", phase: "Running", foreign: true, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := kubeBlocksSession()
			session.Status.Phase = domain.PhaseResuming
			cluster := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": kubeBlocksClusterAPIVersion,
				"kind":       "Cluster",
				"metadata": map[string]any{
					"name":        "cluster",
					"namespace":   "db",
					"uid":         "cluster-uid",
					"annotations": map[string]any{pauseSessionAnnotation: session.ID},
				},
				"spec": map[string]any{
					"componentSpecs": []any{map[string]any{"name": "db", "replicas": int64(1)}},
				},
				"status": map[string]any{"phase": "Updating"},
			}}

			objects := []runtime.Object{cluster, opsRequest("session-pause", "Succeed")}
			if !test.missing {
				start := opsRequest("session-resume", test.phase)
				if test.foreign {
					start.SetLabels(map[string]string{})
				}

				objects = append(objects, start)
			}

			dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), objects...)
			manager := NewManager(kubernetesfake.NewClientset(), dynamicClient, nil)

			err := manager.ValidateResume(t.Context(), session)
			if (err != nil) != test.wantError {
				t.Fatalf("error=%v wantError=%t", err, test.wantError)
			}

			if test.phase == "Succeed" {
				if err := manager.setKubeBlocksPaused(t.Context(), session, false); err != nil {
					t.Fatal(err)
				}
			}

			for _, action := range dynamicClient.Actions() {
				if action.GetVerb() != "get" {
					t.Fatalf(
						"existing Start was mutated: %s %s",
						action.GetVerb(),
						action.GetResource().Resource,
					)
				}
			}
		})
	}
}

func TestKubeBlocksFailedRecoveryKeepsOperationName(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhaseResuming, domain.PhaseAborting, domain.PhaseRollingBack} {
		session := kubeBlocksSession()
		session.Status.Phase = phase
		want := kubeBlocksOperationName(session, "resume")
		session.Status.ResumeFrom = phase

		session.Status.Phase = domain.PhaseFailed
		if got := kubeBlocksOperationName(session, "resume"); got != want {
			t.Fatalf("phase=%s operation=%s want=%s", phase, got, want)
		}
	}
}

func TestKubeBlocksInstanceSetResumeWaitsForClusterRunning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	apiVersion := "workloads.kubeblocks.io/v1alpha1"
	instanceSet := kubeBlocksInstanceSetObject(apiVersion, new(true))
	instanceSet.SetAnnotations(map[string]string{pauseSessionAnnotation: "session"})

	cluster := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": kubeBlocksClusterAPIVersion,
		"kind":       "Cluster",
		"metadata": map[string]any{
			"name": "cluster", "namespace": "db", "uid": "cluster-uid",
		},
		"status": map[string]any{
			"phase": "Updating",
			"components": map[string]any{
				"db": map[string]any{"phase": "Updating"},
			},
		},
	}}

	session := kubeBlocksInstanceSetSession(apiVersion, false, true)
	pod := readyPod("db", "cluster-db-0", "node-a")
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: apiVersion,
		Kind:       domain.KindInstanceSet,
		Name:       "cluster-db",
		UID:        "instanceset-uid",
		Controller: new(true),
	}}
	session.Spec.WorkloadPtr().Pod = podReference(pod)

	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), instanceSet, cluster)
	clusterReads := 0
	dynamicClient.PrependReactor(
		"get",
		clusterResource,
		func(clienttesting.Action) (bool, runtime.Object, error) {
			clusterReads++

			current := cluster.DeepCopy()
			if clusterReads > 1 {
				_ = unstructured.SetNestedField(current.Object, "Running", "status", "phase")
				_ = unstructured.SetNestedField(
					current.Object,
					"Running",
					"status",
					"components",
					"db",
					"phase",
				)
			}

			return true, current, nil
		},
	)
	manager := NewManager(kubernetesfake.NewClientset(pod), dynamicClient, nil)
	manager.poll = time.Millisecond

	if err := manager.resumeKubeBlocks(ctx, session); err != nil {
		t.Fatal(err)
	}

	if clusterReads < 2 {
		t.Fatalf("Cluster convergence reads=%d want at least 2", clusterReads)
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
