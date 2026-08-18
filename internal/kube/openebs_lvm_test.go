package kube

import (
	"context"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func TestOpenEBSLVMSharedVolumeManager(t *testing.T) {
	sourcePV := domain.ObjectReference{Name: "pv-source", UID: "pv-uid"}
	newManager := func(shared string) (OpenEBSLVMSharedVolumeManager, *dynamicfake.FakeDynamicClient) {
		t.Helper()
		spec := map[string]any{}
		if shared != "" {
			spec["shared"] = shared
		}
		volume := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "local.openebs.io/v1alpha1", "kind": "LVMVolume",
			"metadata": map[string]any{"name": "pvc-123", "namespace": "openebs", "uid": "lvm-uid", "resourceVersion": "1"},
			"spec":     spec,
		}}
		dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{openEBSLVMVolumeGVR: "LVMVolumeList"}, volume)
		typed := kubernetesfake.NewClientset(&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: sourcePV.Name, UID: sourcePV.UID},
			Spec:       corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: "local.csi.openebs.io", VolumeHandle: "PVC-123"}}},
		})
		return NewOpenEBSLVMSharedVolumeManager(typed, dynamicClient), dynamicClient
	}
	prepareAndEnable := func(t *testing.T, manager OpenEBSLVMSharedVolumeManager) (domain.OpenEBSLVMSharedMount, OpenEBSLVMSharedResult) {
		t.Helper()
		result, err := manager.PrepareShared(context.Background(), sourcePV)
		if err != nil {
			t.Fatal(err)
		}
		state := domain.OpenEBSLVMSharedMount{
			SourcePV: sourcePV, LVMVolume: result.LVMVolume,
			PreviousShared: result.PreviousShared, PreviousSharedSet: result.PreviousSharedSet,
		}
		if result.NeedsChange {
			if err := manager.EnableShared(context.Background(), "session-1", state); err != nil {
				t.Fatal(err)
			}
		}
		return state, result
	}

	t.Run("enables an existing unshared volume", func(t *testing.T) {
		manager, dynamicClient := newManager("no")
		state, result := prepareAndEnable(t, manager)
		if !result.NeedsChange || result.PreviousShared != "no" || result.Reference != "LVMVolume openebs/pvc-123" || result.LVMVolume.UID != "lvm-uid" {
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
		isShared, err := manager.Shared(context.Background(), sourcePV, state.LVMVolume, "session-1")
		if err != nil || !isShared {
			t.Fatalf("shared=%t error=%v", isShared, err)
		}
		if err := manager.ValidateRestoreShared(context.Background(), "session-1", state); err != nil {
			t.Fatal(err)
		}
		if err := manager.RestoreShared(context.Background(), "session-1", state); err != nil {
			t.Fatal(err)
		}
		volume, err = dynamicClient.Resource(openEBSLVMVolumeGVR).Namespace("openebs").Get(context.Background(), "pvc-123", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		shared, _, err = unstructured.NestedString(volume.Object, "spec", "shared")
		if err != nil || shared != "no" {
			t.Fatalf("restored spec.shared=%q error=%v", shared, err)
		}
		if _, exists := volume.GetAnnotations()[openEBSLVMSharedSessionAnnotation]; exists {
			t.Fatalf("temporary shared-mount annotation remains: %#v", volume.GetAnnotations())
		}
	})

	t.Run("restores an absent shared field", func(t *testing.T) {
		manager, dynamicClient := newManager("")
		state, result := prepareAndEnable(t, manager)
		if result.PreviousSharedSet {
			t.Fatalf("result=%#v", result)
		}
		if err := manager.RestoreShared(context.Background(), "session-1", state); err != nil {
			t.Fatal(err)
		}
		volume, err := dynamicClient.Resource(openEBSLVMVolumeGVR).Namespace("openebs").Get(context.Background(), "pvc-123", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_, found, err := unstructured.NestedString(volume.Object, "spec", "shared")
		if err != nil || found {
			t.Fatalf("restored spec.shared present=%t error=%v", found, err)
		}
	})

	t.Run("does not patch an already shared volume", func(t *testing.T) {
		manager, dynamicClient := newManager("yes")
		result, err := manager.PrepareShared(context.Background(), sourcePV)
		if err != nil {
			t.Fatal(err)
		}
		if result.NeedsChange {
			t.Fatalf("result=%#v", result)
		}
		shared, err := manager.Shared(context.Background(), sourcePV, domain.ObjectReference{}, "")
		if err != nil || !shared {
			t.Fatalf("preconfigured shared=%t error=%v", shared, err)
		}
		if _, err := manager.Shared(context.Background(), sourcePV, domain.ObjectReference{}, "session-1"); domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("session-owned read category=%s error=%v", domain.CategoryOf(err), err)
		}
		for _, action := range dynamicClient.Actions() {
			if action.GetVerb() == "patch" {
				t.Fatalf("unexpected patch: %#v", action)
			}
		}
	})

	t.Run("rejects a managed shared volume after its ownership marker is removed", func(t *testing.T) {
		manager, dynamicClient := newManager("no")
		_, _ = prepareAndEnable(t, manager)
		resource := dynamicClient.Resource(openEBSLVMVolumeGVR).Namespace("openebs")
		volume, err := resource.Get(context.Background(), "pvc-123", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		volume.SetAnnotations(nil)
		if _, err := resource.Update(context.Background(), volume, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := manager.Shared(context.Background(), sourcePV, domain.ObjectReference{}, "session-1"); domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("rejects restore after an external shared change", func(t *testing.T) {
		manager, dynamicClient := newManager("no")
		state, _ := prepareAndEnable(t, manager)
		volume, err := dynamicClient.Resource(openEBSLVMVolumeGVR).Namespace("openebs").Get(context.Background(), "pvc-123", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := unstructured.SetNestedField(volume.Object, "external", "spec", "shared"); err != nil {
			t.Fatal(err)
		}
		if _, err := dynamicClient.Resource(openEBSLVMVolumeGVR).Namespace("openebs").Update(context.Background(), volume, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		err = manager.ValidateRestoreShared(context.Background(), "session-1", state)
		if domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("prepared state is harmless before the patch", func(t *testing.T) {
		manager, dynamicClient := newManager("no")
		result, err := manager.PrepareShared(context.Background(), sourcePV)
		if err != nil {
			t.Fatal(err)
		}
		state := domain.OpenEBSLVMSharedMount{SourcePV: sourcePV, LVMVolume: result.LVMVolume, PreviousShared: result.PreviousShared, PreviousSharedSet: result.PreviousSharedSet}
		if err := manager.RestoreShared(context.Background(), "session-1", state); err != nil {
			t.Fatal(err)
		}
		for _, action := range dynamicClient.Actions() {
			if action.GetVerb() == "patch" {
				t.Fatalf("unexpected patch: %#v", action)
			}
		}
	})

	t.Run("rejects a replacement LVMVolume during restore", func(t *testing.T) {
		manager, dynamicClient := newManager("no")
		state, _ := prepareAndEnable(t, manager)
		resource := dynamicClient.Resource(openEBSLVMVolumeGVR).Namespace("openebs")
		if err := resource.Delete(context.Background(), "pvc-123", metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		replacement := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "local.openebs.io/v1alpha1", "kind": "LVMVolume",
			"metadata": map[string]any{"name": "pvc-123", "namespace": "openebs", "uid": "replacement-lvm-uid", "resourceVersion": "2"},
			"spec":     map[string]any{"shared": "yes"},
		}}
		if _, err := resource.Create(context.Background(), replacement, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		err := manager.RestoreShared(context.Background(), "session-1", state)
		if domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
		current, err := resource.Get(context.Background(), "pvc-123", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		shared, _, err := unstructured.NestedString(current.Object, "spec", "shared")
		if err != nil || shared != "yes" {
			t.Fatalf("replacement spec.shared=%q error=%v", shared, err)
		}
	})

	t.Run("rejects a replacement LVMVolume during shared verification", func(t *testing.T) {
		manager, dynamicClient := newManager("no")
		state, _ := prepareAndEnable(t, manager)
		resource := dynamicClient.Resource(openEBSLVMVolumeGVR).Namespace("openebs")
		if err := resource.Delete(context.Background(), "pvc-123", metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		replacement := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "local.openebs.io/v1alpha1", "kind": "LVMVolume",
			"metadata": map[string]any{
				"name": "pvc-123", "namespace": "openebs", "uid": "replacement-lvm-uid", "resourceVersion": "2",
				"annotations": map[string]any{openEBSLVMSharedSessionAnnotation: "session-1"},
			},
			"spec": map[string]any{"shared": "yes"},
		}}
		if _, err := resource.Create(context.Background(), replacement, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		_, err := manager.Shared(context.Background(), state.SourcePV, state.LVMVolume, "session-1")
		if domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("requires source PV identity", func(t *testing.T) {
		manager, _ := newManager("no")
		if _, err := manager.PrepareShared(context.Background(), domain.ObjectReference{Name: sourcePV.Name}); domain.CategoryOf(err) != domain.ErrorValidation {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
}

func TestOpenEBSLVMSharedVolumeManagerRejectsReplacedSourcePV(t *testing.T) {
	volume := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "local.openebs.io/v1alpha1", "kind": "LVMVolume",
		"metadata": map[string]any{"name": "pvc-123", "namespace": "openebs"},
		"spec":     map[string]any{"shared": "no"},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{openEBSLVMVolumeGVR: "LVMVolumeList"}, volume)
	typed := kubernetesfake.NewClientset(&corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-source", UID: "current-pv-uid"},
		Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
			Driver: OpenEBSLVMCSIDriver, VolumeHandle: "pvc-123",
		}}},
	})
	manager := NewOpenEBSLVMSharedVolumeManager(typed, dynamicClient)
	_, err := manager.PrepareShared(context.Background(), domain.ObjectReference{Name: "pv-source", UID: "replaced-pv-uid"})
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}
