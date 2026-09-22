package kube

import (
	"context"
	"errors"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"k8s.io/client-go/kubernetes"
)

// CRDWorkflowLocker protects workflow ownership independently of object storage.
type CRDWorkflowLocker struct {
	client kubernetes.Interface
}

// ConfigMapWorkflowLocker uses the same Lease protocol without rejecting the
// ConfigMap that owns the workflow. It has no dependency on a session model.
type ConfigMapWorkflowLocker struct {
	client kubernetes.Interface
}

func NewConfigMapWorkflowLocker(client kubernetes.Interface) *ConfigMapWorkflowLocker {
	return &ConfigMapWorkflowLocker{client: client}
}

func (l *ConfigMapWorkflowLocker) AcquireSessionLock(
	ctx context.Context,
	namespace, id string,
) (SessionLock, error) {
	if l == nil || l.client == nil {
		return nil, errors.New("workflow lease client is not configured")
	}
	return acquireWorkflowLease(ctx, l.client, namespace, id, id, 0, 0)
}

func NewCRDWorkflowLocker(client kubernetes.Interface) *CRDWorkflowLocker {
	return &CRDWorkflowLocker{client: client}
}

func (l *CRDWorkflowLocker) AcquireSessionLock(
	ctx context.Context,
	namespace, id string,
) (SessionLock, error) {
	if l == nil || l.client == nil {
		return nil, domain.NewError(
			domain.ErrorKubernetes,
			"acquire session lock",
			"CRD workflow lease client is not configured",
		)
	}

	if err := checkConfigMapNameCollision(
		ctx, l.client, id, []string{namespace}, true,
	); err != nil {
		return nil, err
	}

	lock, err := acquireWorkflowLease(ctx, l.client, namespace, id, id, 0, 0)
	if err != nil {
		return nil, err
	}

	// A ConfigMap may have claimed the name while this worker waited for the Lease.
	if err := checkConfigMapNameCollision(
		ctx, l.client, id, []string{namespace}, true,
	); err != nil {
		return nil, errors.Join(err, lock.Release(ctx))
	}

	return lock, nil
}
