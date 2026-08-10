package kube

import (
	"context"
	"fmt"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestAcquirePVCHandlesOwnershipAndRetriesConflicts(t *testing.T) {
	ctx := context.Background()
	ref := domain.ObjectReference{Namespace: "app", Name: "data", UID: types.UID("pvc-uid")}
	client := fake.NewClientset(&corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name, UID: ref.UID},
	})
	updates := 0
	client.PrependReactor("update", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
		updates++
		if updates == 1 {
			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Resource: "persistentvolumeclaims"},
				ref.Name,
				fmt.Errorf("injected concurrent update"),
			)
		}
		return false, nil, nil
	})

	if err := AcquirePVC(ctx, client, ref, "session"); err != nil {
		t.Fatal(err)
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pvc.Annotations[SessionKey] != "session" || updates != 2 {
		t.Fatalf("PVC ownership=%q updates=%d", pvc.Annotations[SessionKey], updates)
	}
	if err := AcquirePVC(ctx, client, ref, "session"); err != nil {
		t.Fatalf("idempotent acquire: %v", err)
	}
	if updates != 2 {
		t.Fatalf("idempotent acquire issued an update: updates=%d", updates)
	}
}

func TestAcquirePVCRejectsUIDAndOwnerConflicts(t *testing.T) {
	tests := []struct {
		name string
		ref  domain.ObjectReference
		pvc  *corev1.PersistentVolumeClaim
	}{
		{
			name: "uid changed",
			ref:  domain.ObjectReference{Namespace: "app", Name: "data", UID: types.UID("expected")},
			pvc:  &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: types.UID("replacement")}},
		},
		{
			name: "foreign owner",
			ref:  domain.ObjectReference{Namespace: "app", Name: "data", UID: types.UID("expected")},
			pvc: &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
				Namespace: "app", Name: "data", UID: types.UID("expected"),
				Annotations: map[string]string{SessionKey: "other-session"},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := fake.NewClientset(test.pvc)
			err := AcquirePVC(context.Background(), client, test.ref, "session")
			if domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestReleasePVCIsOwnershipSafeAndIdempotent(t *testing.T) {
	ctx := context.Background()
	uid := types.UID("pvc-uid")
	ref := domain.ObjectReference{Namespace: "app", Name: "data", UID: uid}
	client := fake.NewClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "app", Name: "data", UID: uid,
		Annotations: map[string]string{SessionKey: "session", "example.com/keep": "value"},
	}})
	if err := ReleasePVC(ctx, client, ref, "session"); err != nil {
		t.Fatal(err)
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims("app").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := pvc.Annotations[SessionKey]; exists || pvc.Annotations["example.com/keep"] != "value" {
		t.Fatalf("annotations after release: %#v", pvc.Annotations)
	}
	if err := ReleasePVC(ctx, client, ref, "session"); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}

	pvc.Annotations[SessionKey] = "other-session"
	if _, err := client.CoreV1().PersistentVolumeClaims("app").Update(ctx, pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := ReleasePVC(ctx, client, ref, "session"); err != nil {
		t.Fatalf("release of foreign ownership: %v", err)
	}
	pvc, _ = client.CoreV1().PersistentVolumeClaims("app").Get(ctx, "data", metav1.GetOptions{})
	if pvc.Annotations[SessionKey] != "other-session" {
		t.Fatalf("foreign ownership changed: %#v", pvc.Annotations)
	}
	if err := ReleasePVC(ctx, client, domain.ObjectReference{Namespace: "app", Name: "missing"}, "session"); err != nil {
		t.Fatalf("release missing PVC: %v", err)
	}
}

func TestReleasePVCRejectsReusedName(t *testing.T) {
	client := fake.NewClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "app", Name: "data", UID: types.UID("replacement"),
		Annotations: map[string]string{SessionKey: "session"},
	}})
	err := ReleasePVC(context.Background(), client, domain.ObjectReference{
		Namespace: "app", Name: "data", UID: types.UID("original"),
	}, "session")
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestFinalizePVCRestoresOriginalMetadataAndPreservesBindingAnnotations(t *testing.T) {
	ctx := context.Background()
	ref := domain.ObjectReference{Namespace: "app", Name: "data", UID: types.UID("pvc-uid")}
	originalOwner := metav1.OwnerReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "database", UID: types.UID("owner-uid")}
	client := fake.NewClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: ref.Namespace, Name: ref.Name, UID: ref.UID,
		Labels: map[string]string{
			ManagedByLabel: "pvc-migrate", SessionKey: "session", "external.example/keep": "value",
		},
		Annotations: map[string]string{
			SessionKey: "session", RollbackPVAnnotation: "old-pv", SourcePVAnnotation: "source-pv", SourcePVCUIDAnnotation: "source-pvc", "pv.kubernetes.io/bind-completed": "yes",
		},
	}})
	original := domain.PVCMetadata{
		Labels:          map[string]string{ManagedByLabel: "database-operator", "original.example/label": "value"},
		Annotations:     map[string]string{"original.example/annotation": "value"},
		OwnerReferences: []metav1.OwnerReference{originalOwner},
	}
	if err := FinalizePVC(ctx, client, ref, "session", original); err != nil {
		t.Fatal(err)
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pvc.Labels[ManagedByLabel] != "database-operator" || pvc.Labels[SessionKey] != "" || pvc.Labels["external.example/keep"] != "value" || pvc.Labels["original.example/label"] != "value" {
		t.Fatalf("labels=%v", pvc.Labels)
	}
	if pvc.Annotations[SessionKey] != "" || pvc.Annotations[RollbackPVAnnotation] != "" || pvc.Annotations[SourcePVAnnotation] != "" || pvc.Annotations[SourcePVCUIDAnnotation] != "" || pvc.Annotations["pv.kubernetes.io/bind-completed"] != "yes" || pvc.Annotations["original.example/annotation"] != "value" {
		t.Fatalf("annotations=%v", pvc.Annotations)
	}
	if len(pvc.OwnerReferences) != 1 || pvc.OwnerReferences[0].UID != originalOwner.UID {
		t.Fatalf("ownerReferences=%v", pvc.OwnerReferences)
	}
	if err := FinalizePVC(ctx, client, ref, "session", original); err != nil {
		t.Fatalf("idempotent finalize: %v", err)
	}
}

func TestFinalizePVCRejectsForeignOwnership(t *testing.T) {
	ref := domain.ObjectReference{Namespace: "app", Name: "data", UID: types.UID("pvc-uid")}
	client := fake.NewClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: ref.Namespace, Name: ref.Name, UID: ref.UID,
		Labels: map[string]string{SessionKey: "foreign-session"},
	}})
	err := FinalizePVC(context.Background(), client, ref, "session", domain.PVCMetadata{})
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}
