package kube

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
)

func TestCRDWorkflowLockerRejectsCollisionAfterAcquisition(t *testing.T) {
	client := newSessionLeaseTestClient()
	reads := 0
	client.PrependReactor(
		"get",
		"configmaps",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			reads++
			if reads == 1 {
				return false, nil, nil
			}

			return true, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
				Name: SessionConfigMapName("rename"), Namespace: action.GetNamespace(),
			}}, nil
		},
	)

	lock, err := NewCRDWorkflowLocker(client).AcquireSessionLock(t.Context(), "workflows", "rename")
	if lock != nil || domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("lock=%v error=%v", lock, err)
	}

	lease, err := client.CoordinationV1().
		Leases("workflows").
		Get(t.Context(), SessionLockName("rename"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "" {
		t.Fatal("collision left the workflow Lease held")
	}
}

func TestCRDWorkflowLockerFencesCompetingOperations(t *testing.T) {
	locker := NewCRDWorkflowLocker(newSessionLeaseTestClient())

	first, err := locker.AcquireSessionLock(t.Context(), "workflows", "copy")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = first.Release(t.Context()) })

	second, err := locker.AcquireSessionLock(t.Context(), "workflows", "copy")
	if second != nil || !IsSessionLockContention(err) {
		t.Fatalf("competing lock=%v error=%v", second, err)
	}

	if err := first.Release(t.Context()); err != nil {
		t.Fatal(err)
	}

	second, err = locker.AcquireSessionLock(t.Context(), "workflows", "copy")
	if err != nil {
		t.Fatal(err)
	}

	if err := second.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}
