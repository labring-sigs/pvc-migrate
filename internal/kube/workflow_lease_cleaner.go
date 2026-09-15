package kube

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// CRDWorkflowLeaseCleaner removes abandoned workflow Leases. Cleanup holds the
// same lock while deleting the workflow, so a concurrent mutator cannot race
// this operation. The resourceVersion precondition also protects the small
// window where an expired holder is replaced before its cleanup call runs.
type CRDWorkflowLeaseCleaner struct {
	client kubernetes.Interface
}

func NewCRDWorkflowLeaseCleaner(client kubernetes.Interface) *CRDWorkflowLeaseCleaner {
	return &CRDWorkflowLeaseCleaner{client: client}
}

func (c *CRDWorkflowLeaseCleaner) DeleteSessionLease(
	ctx context.Context,
	namespace, id string,
) error {
	if namespace == "" || id == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"delete session lock",
			"session namespace and ID are required",
		)
	}

	leases := c.client.CoordinationV1().Leases(namespace)
	name := SessionLockName(id)

	lease, err := leases.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"delete session lock",
			fmt.Sprintf("read Lease %s/%s", namespace, name),
			err,
		)
	}

	if lease.Labels[ManagedByLabel] != ManagedByValue || lease.Labels[SessionKey] != id {
		return domain.NewError(
			domain.ErrorConflict,
			"delete session lock",
			fmt.Sprintf("Lease %s/%s is owned by another resource", namespace, name),
		)
	}

	if lease.UID == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			"delete session lock",
			fmt.Sprintf("Lease %s/%s has an incomplete identity", namespace, name),
		)
	}

	preconditions := &metav1.Preconditions{UID: &lease.UID, ResourceVersion: &lease.ResourceVersion}

	err = leases.Delete(ctx, name, metav1.DeleteOptions{Preconditions: preconditions})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if apierrors.IsConflict(err) {
		return domain.WrapError(
			domain.ErrorConflict,
			"delete session lock",
			fmt.Sprintf("Lease %s/%s changed while deleting", namespace, name),
			err,
		)
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"delete session lock",
			fmt.Sprintf("delete Lease %s/%s", namespace, name),
			err,
		)
	}

	return nil
}
