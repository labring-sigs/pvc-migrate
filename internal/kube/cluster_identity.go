package kube

import (
	"context"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterIdentity is a stable, non-secret identifier for an API endpoint.
// It deliberately does not use a kubeconfig context name: context names are
// routinely reused across files and do not identify a cluster.
type ClusterIdentity struct {
	ID string `json:"id" yaml:"id"`
}

func Identity(ctx context.Context, clients *Clients) (ClusterIdentity, error) {
	if clients == nil || clients.Kubernetes == nil {
		return ClusterIdentity{}, errors.New("kubernetes client is unavailable")
	}

	namespace, err := clients.Kubernetes.CoreV1().
		Namespaces().
		Get(ctx, metav1.NamespaceSystem, metav1.GetOptions{})
	if err != nil {
		return ClusterIdentity{}, fmt.Errorf("read kube-system cluster identity: %w", err)
	}

	if namespace.UID == "" {
		return ClusterIdentity{}, errors.New("kube-system namespace has no UID")
	}

	return ClusterIdentity{ID: string(namespace.UID)}, nil
}
