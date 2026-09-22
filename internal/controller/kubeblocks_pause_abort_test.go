package controller

import (
	"context"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

func legacyKubeBlocksClusterObject(uid, phase, pauseOwner string) *unstructured.Unstructured {
	cluster := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": kubeBlocksClusterAPIVersion,
			"kind":       "Cluster",
			"metadata": map[string]any{
				"name":      "legacy-mg",
				"namespace": "tenants",
				"uid":       uid,
			},
			"spec": map[string]any{
				"componentSpecs": []any{map[string]any{"name": "mongodb"}},
			},
			"status": map[string]any{"phase": phase},
		},
	}

	if pauseOwner != "" {
		cluster.SetAnnotations(map[string]string{pauseSessionAnnotation: pauseOwner})
	}

	return cluster
}

// TestLegacyAbortSkipsResumeWhenPauseNeverStarted pins the abort recovery for
// a failed pause: the pause flow writes its ownership annotation before any
// stop operation, so a cluster without the annotation was never paused by
// this session — abort must skip the workload resume instead of demanding the
// missing ownership and wedging the workflow forever.
func TestLegacyAbortSkipsResumeWhenPauseNeverStarted(t *testing.T) {
	cases := []struct {
		name  string
		phase string
	}{
		{name: "failed cluster", phase: "Failed"},
		{name: "stopped cluster", phase: "Stopped"},
		{name: "running cluster", phase: "Running"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			cluster := legacyKubeBlocksClusterObject("cluster-uid", testCase.phase, "")
			dynamicClient := dynamicfake.NewSimpleDynamicClient(scheme, cluster)

			manager := NewManager(fake.NewClientset(), dynamicClient, nil)

			kb := &v1alpha1.KubeBlocksSpec{
				Cluster:       "legacy-mg",
				Component:     "mongodb",
				Instance:      "legacy-mg-mongodb-0",
				ClusterUID:    "cluster-uid",
				OpsAPIVersion: "apps.kubeblocks.io/v1alpha1",
			}

			notStarted, err := manager.legacyKubeBlocksPauseNotStarted(
				context.Background(),
				v1alpha1.ObjectReference{
					Namespace: "tenants",
					Name:      "legacy-mg-mongodb-0",
					UID:       "pod-uid",
				},
				v1alpha1.ObjectReference{Kind: "StatefulSet", Name: "legacy-mg-mongodb"},
				kb,
			)
			if err != nil {
				t.Fatal(err)
			}

			if !notStarted {
				t.Fatalf(
					"cluster phase %s without any pause annotation must not block abort",
					testCase.phase,
				)
			}
		})
	}
}

// TestLegacyAbortStillRespectsForeignPauseOwner keeps the ownership conflict:
// another session's annotation means the pause is real and actively owned, so
// this session's abort must not silently skip the resume.
func TestLegacyAbortStillRespectsForeignPauseOwner(t *testing.T) {
	scheme := runtime.NewScheme()
	cluster := legacyKubeBlocksClusterObject("cluster-uid", "Stopped", "other-session")
	dynamicClient := dynamicfake.NewSimpleDynamicClient(scheme, cluster)

	manager := NewManager(fake.NewClientset(), dynamicClient, nil)

	kb := &v1alpha1.KubeBlocksSpec{
		Cluster:       "legacy-mg",
		Component:     "mongodb",
		ClusterUID:    "cluster-uid",
		OpsAPIVersion: "apps.kubeblocks.io/v1alpha1",
	}

	notStarted, err := manager.legacyKubeBlocksPauseNotStarted(
		context.Background(),
		v1alpha1.ObjectReference{Namespace: "tenants", Name: "legacy-mg-mongodb-0"},
		v1alpha1.ObjectReference{Kind: "StatefulSet", Name: "legacy-mg-mongodb"},
		kb,
	)
	if err != nil {
		t.Fatal(err)
	}

	if notStarted {
		t.Fatal("foreign pause owner must keep the pause-started verdict")
	}
}

// TestLegacyAbortRejectsReplacedCluster keeps the UID fence on the recovery
// read even after the ownership short-circuit.
func TestLegacyAbortRejectsReplacedCluster(t *testing.T) {
	scheme := runtime.NewScheme()
	cluster := legacyKubeBlocksClusterObject("new-cluster-uid", "Failed", "")
	dynamicClient := dynamicfake.NewSimpleDynamicClient(scheme, cluster)

	manager := NewManager(fake.NewClientset(), dynamicClient, nil)

	kb := &v1alpha1.KubeBlocksSpec{
		Cluster:       "legacy-mg",
		Component:     "mongodb",
		ClusterUID:    "original-cluster-uid",
		OpsAPIVersion: "apps.kubeblocks.io/v1alpha1",
	}

	_, err := manager.legacyKubeBlocksPauseNotStarted(
		context.Background(),
		v1alpha1.ObjectReference{Namespace: "tenants", Name: "legacy-mg-mongodb-0"},
		v1alpha1.ObjectReference{Kind: "StatefulSet", Name: "legacy-mg-mongodb"},
		kb,
	)
	if err == nil || domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("replaced cluster error=%v, want conflict", err)
	}
}

var (
	_ = corev1.ResourcePods
	_ = metav1.NamespaceDefault
)
