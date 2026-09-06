package kube

import (
	"errors"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestCRDSessionRejectsConfigMapSessionName(t *testing.T) {
	for _, namespace := range []string{"system", "source"} {
		t.Run(namespace, func(t *testing.T) {
			ctx := t.Context()
			client := kubernetesfake.NewClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
				Name: SessionConfigMapName("alpha"), Namespace: namespace,
			}})
			store := NewCRDSessionStore(newCRDTestClient()).WithLeaseClient(client)
			session := storeTestSession()
			if namespace == "source" {
				session.Spec.SourceNamespace = namespace
				session.Spec.Volumes[0].SourcePVC.Namespace = namespace
			}
			err := store.CheckWorkflowNameCollision(ctx, session)
			if domain.CategoryOf(err) != domain.ErrorConflict || !strings.Contains(err.Error(), "ConfigMap session") {
				t.Fatalf("collision error=%v", err)
			}
			if namespace == "system" {
				if err := store.Create(ctx, session); domain.CategoryOf(err) != domain.ErrorConflict {
					t.Fatalf("create error=%v", err)
				}
			}
			lock, err := store.AcquireSessionLock(ctx, namespace, session.ID)
			if lock != nil || domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("lock=%v error=%v", lock, err)
			}
			leases, err := client.CoordinationV1().Leases(namespace).List(ctx, metav1.ListOptions{})
			if err != nil || len(leases.Items) != 0 {
				t.Fatalf("collision created a Lease: %v error=%v", leases, err)
			}
		})
	}
}

func TestCRDSessionCollisionChecksFailClosedForExecution(t *testing.T) {
	client := kubernetesfake.NewClientset()
	client.PrependReactor("get", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "alpha", errors.New("denied"))
	})
	store := NewCRDSessionStore(newCRDTestClient()).WithLeaseClient(client)
	session := storeTestSession()
	if err := store.Create(t.Context(), session); err != nil {
		t.Fatalf("tenant submission should defer unreadable collision checks to controller: %v", err)
	}
	if err := store.CheckWorkflowNameCollision(t.Context(), session); domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("controller collision check error=%v", err)
	}
	if _, err := store.AcquireSessionLock(t.Context(), "system", "alpha"); domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("execution lock error=%v", err)
	}
}

func TestCRDSessionRechecksConfigMapAfterAcquiringLease(t *testing.T) {
	client := kubernetesfake.NewClientset()
	client.PrependReactor("create", "leases", func(action clienttesting.Action) (bool, runtime.Object, error) {
		lease := action.(clienttesting.CreateAction).GetObject().(*coordinationv1.Lease)
		lease.UID = types.UID("lease-uid")
		if err := client.Tracker().Add(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: SessionConfigMapName("alpha"), Namespace: "system",
		}}); err != nil {
			t.Fatal(err)
		}
		return false, nil, nil
	})
	store := NewCRDSessionStore(newCRDTestClient()).WithLeaseClient(client)
	lock, err := store.AcquireSessionLock(t.Context(), "system", "alpha")
	if lock != nil || domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("lock=%v error=%v", lock, err)
	}
	lease, err := client.CoordinationV1().Leases("system").Get(t.Context(), SessionLockName("alpha"), metav1.GetOptions{})
	if err != nil || lease.Spec.HolderIdentity != nil {
		t.Fatalf("blocked worker retained Lease: %v error=%v", lease, err)
	}
}
