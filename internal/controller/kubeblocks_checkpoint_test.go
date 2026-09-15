package controller

import (
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func TestKubeBlocksResumeReturnsPodIdentityOnConvergenceFailure(t *testing.T) {
	const apiVersion = "workloads.kubeblocks.io/v1alpha1"

	instanceSet := kubeBlocksInstanceSetObject(apiVersion, new(true))
	instanceSet.SetAnnotations(map[string]string{pauseSessionAnnotation: "migration"})
	controller := v1alpha1.ObjectReference{
		APIVersion: apiVersion,
		Kind:       domain.KindInstanceSet,
		Namespace:  "db",
		Name:       instanceSet.GetName(),
		UID:        instanceSet.GetUID(),
	}
	pod := readyPod("db", "cluster-db-0", "node-a")
	pod.UID = "replacement-uid"
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: controller.APIVersion,
		Kind:       controller.Kind,
		Name:       controller.Name,
		UID:        controller.UID,
		Controller: new(true),
	}}
	previous := podReference(pod)
	previous.UID = "original-uid"
	kb := &v1alpha1.KubeBlocksSpec{
		Cluster:                  "cluster",
		ClusterUID:               "cluster-uid",
		Component:                "db",
		Instance:                 pod.Name,
		OriginalPausedConfigured: true,
	}
	// The InstanceSet can resume, but its Cluster cannot be read afterward.
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), instanceSet)
	manager := NewManager(kubernetesfake.NewClientset(pod), dynamicClient, nil)

	updated, err := manager.resumeKubeBlocks(t.Context(), "migration", previous,
		controller, kb, domain.PhaseResuming, "", false)
	if domain.CategoryOf(err) != domain.ErrorKubernetes ||
		!strings.Contains(err.Error(), "read Cluster convergence state") {
		t.Fatalf("expected convergence failure after Pod recovery, got %v", err)
	}

	if updated != podReference(pod) {
		t.Fatalf("recovered Pod identity = %+v, want %+v", updated, podReference(pod))
	}

	if previous.UID != "original-uid" {
		t.Fatalf("input identity changed: %+v", previous)
	}
}
