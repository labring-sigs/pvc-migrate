package kube

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestServiceAccountIdentityFingerprintStableAcrossMapOrder(t *testing.T) {
	automount := true
	first := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Namespace: "tenant", Name: "s3-transfer", UID: types.UID("uid-1"),
		Labels:      map[string]string{"b": "2", "a": "1"},
		Annotations: map[string]string{"role": "arn:aws:iam::123:role/transfer", "audience": "s3"},
	}, AutomountServiceAccountToken: &automount}
	second := first.DeepCopy()
	second.Labels = map[string]string{"a": "1", "b": "2"}
	second.Annotations = map[string]string{"audience": "s3", "role": "arn:aws:iam::123:role/transfer"}
	if got, want := ServiceAccountIdentityFingerprint(first), ServiceAccountIdentityFingerprint(second); got != want || got == "" {
		t.Fatalf("fingerprint differs for equivalent identities: %q vs %q", got, want)
	}
}

func TestServiceAccountIdentityFingerprintIgnoresRotatingFields(t *testing.T) {
	base := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Namespace: "tenant", Name: "s3-transfer", UID: types.UID("uid-1"),
		ResourceVersion: "1",
		Labels:          map[string]string{"role": "transfer"},
		Annotations:     map[string]string{"cloud.google.com/iam-service-account": "transfer@example"},
		ManagedFields:   []metav1.ManagedFieldsEntry{{Manager: "controller"}},
	}, Secrets: []corev1.ObjectReference{{Name: "token-a"}}, ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull-a"}}}
	changed := base.DeepCopy()
	changed.ResourceVersion = "2"
	changed.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "different"}}
	changed.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "pull-b"}}
	changed.Secrets = []corev1.ObjectReference{{Name: "token-b"}}
	if ServiceAccountIdentityFingerprint(base) != ServiceAccountIdentityFingerprint(changed) {
		t.Fatal("rotating ServiceAccount fields changed identity fingerprint")
	}
}

func TestServiceAccountIdentityFingerprintChangesIdentityInputs(t *testing.T) {
	base := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Namespace: "tenant", Name: "s3-transfer", UID: types.UID("uid-1"),
		Labels:      map[string]string{"role": "transfer"},
		Annotations: map[string]string{"cloud.google.com/iam-service-account": "transfer@example"},
	}}
	want := ServiceAccountIdentityFingerprint(base)
	mutations := []func(*corev1.ServiceAccount){
		func(sa *corev1.ServiceAccount) { sa.UID = types.UID("uid-2") },
		func(sa *corev1.ServiceAccount) { sa.Labels["role"] = "other" },
		func(sa *corev1.ServiceAccount) {
			sa.Annotations["cloud.google.com/iam-service-account"] = "other@example"
		},
		func(sa *corev1.ServiceAccount) { sa.Namespace = "other" },
		func(sa *corev1.ServiceAccount) { value := true; sa.AutomountServiceAccountToken = &value },
	}
	for i, mutate := range mutations {
		got := base.DeepCopy()
		mutate(got)
		if fingerprint := ServiceAccountIdentityFingerprint(got); fingerprint == want {
			t.Fatalf("mutation %d did not change fingerprint", i)
		}
	}
}

func TestServiceAccountIdentityFingerprintNil(t *testing.T) {
	if got := ServiceAccountIdentityFingerprint(nil); got != "" {
		t.Fatalf("nil ServiceAccount fingerprint=%q", got)
	}
}
