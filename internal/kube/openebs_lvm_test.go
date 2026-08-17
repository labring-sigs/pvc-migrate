package kube

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func TestOpenEBSLVMSharedVolumeManager(t *testing.T) {
	newManager := func(shared string) (OpenEBSLVMSharedVolumeManager, *dynamicfake.FakeDynamicClient) {
		t.Helper()
		volume := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "local.openebs.io/v1alpha1", "kind": "LVMVolume",
			"metadata": map[string]any{"name": "pvc-123", "namespace": "openebs"},
			"spec":     map[string]any{"shared": shared},
		}}
		dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{openEBSLVMVolumeGVR: "LVMVolumeList"}, volume)
		typed := kubernetesfake.NewClientset(&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-source"},
			Spec:       corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: "local.csi.openebs.io", VolumeHandle: "PVC-123"}}},
		})
		return NewOpenEBSLVMSharedVolumeManager(typed, dynamicClient), dynamicClient
	}

	t.Run("enables an existing unshared volume", func(t *testing.T) {
		manager, dynamicClient := newManager("no")
		result, err := manager.EnsureShared(context.Background(), "pv-source")
		if err != nil {
			t.Fatal(err)
		}
		if !result.Changed || result.PreviousShared != "no" || result.Reference != "LVMVolume openebs/pvc-123" {
			t.Fatalf("result=%#v", result)
		}
		volume, err := dynamicClient.Resource(openEBSLVMVolumeGVR).Namespace("openebs").Get(context.Background(), "pvc-123", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		shared, _, err := unstructured.NestedString(volume.Object, "spec", "shared")
		if err != nil || shared != "yes" {
			t.Fatalf("spec.shared=%q error=%v", shared, err)
		}
		isShared, err := manager.Shared(context.Background(), "pv-source")
		if err != nil || !isShared {
			t.Fatalf("shared=%t error=%v", isShared, err)
		}
	})

	t.Run("does not patch an already shared volume", func(t *testing.T) {
		manager, dynamicClient := newManager("yes")
		result, err := manager.EnsureShared(context.Background(), "pv-source")
		if err != nil {
			t.Fatal(err)
		}
		if result.Changed {
			t.Fatalf("result=%#v", result)
		}
		for _, action := range dynamicClient.Actions() {
			if action.GetVerb() == "patch" {
				t.Fatalf("unexpected patch: %#v", action)
			}
		}
	})
}
