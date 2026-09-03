package kube

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func TestIdentityReadsStableKubeSystemUID(t *testing.T) {
	client := kubernetesfake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: metav1.NamespaceSystem,
			UID:  types.UID("cluster-uid"),
		},
	})

	identity, err := Identity(context.Background(), &Clients{Kubernetes: client})
	if err != nil {
		t.Fatalf("read cluster identity: %v", err)
	}

	if identity.ID != "cluster-uid" {
		t.Fatalf("identity=%q, want cluster-uid", identity.ID)
	}
}

func TestIdentityFailsClosedWithoutKubeSystemUID(t *testing.T) {
	client := kubernetesfake.NewSimpleClientset()
	if _, err := Identity(context.Background(), &Clients{Kubernetes: client}); err == nil {
		t.Fatal("missing kube-system namespace was accepted")
	}

	client = kubernetesfake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: metav1.NamespaceSystem},
	})
	if _, err := Identity(context.Background(), &Clients{Kubernetes: client}); err == nil {
		t.Fatal("kube-system namespace without UID was accepted")
	}
}
