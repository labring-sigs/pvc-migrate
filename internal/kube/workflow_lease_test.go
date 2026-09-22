package kube

import (
	"context"
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type workflowLeaseTestLock struct {
	bound, released      bool
	fenceErr, releaseErr error
}

func (l *workflowLeaseTestLock) Bind(ctx context.Context) (context.Context, context.CancelFunc) {
	l.bound = true
	return context.WithCancel(ctx)
}

func (l *workflowLeaseTestLock) Err() error { return l.fenceErr }

func (l *workflowLeaseTestLock) Release(context.Context) error {
	l.released = true
	return l.releaseErr
}

func (*workflowLeaseTestLock) Delete(context.Context) error { return nil }

type workflowLeaseTestLocker struct{ lock *workflowLeaseTestLock }

func (l workflowLeaseTestLocker) AcquireSessionLock(
	context.Context,
	string,
	string,
) (SessionLock, error) {
	return l.lock, nil
}

type workflowLeaseBackupStore struct {
	WorkflowStore[*v1alpha1.Backup]
	object *v1alpha1.Backup
}

func (s workflowLeaseBackupStore) Load(
	context.Context,
	crclient.ObjectKey,
) (*v1alpha1.Backup, error) {
	return s.object.DeepCopy(), nil
}

func TestWorkflowLeaseProtectsRepositoryCredentialsAndPreservesAllFailures(t *testing.T) {
	client, repository, owner := repositoryStoreFixture()
	writeErr := errors.New("credential write failed")
	fenceErr := errors.New("workflow lease lost")
	releaseErr := errors.New("lease release failed")
	lock := &workflowLeaseTestLock{releaseErr: releaseErr}
	wroteRepository, attemptedCredentials := false, false

	client.PrependReactor(
		"create",
		"configmaps",
		func(ktesting.Action) (bool, runtime.Object, error) {
			if !lock.bound || lock.released {
				t.Fatal("repository created outside workflow lease")
			}

			wroteRepository = true

			return false, nil, nil
		},
	)
	client.PrependReactor("create", "secrets", func(ktesting.Action) (bool, runtime.Object, error) {
		if !lock.bound || lock.released || !wroteRepository {
			t.Fatal("credentials created outside repository lease sequence")
		}

		attemptedCredentials = true
		lock.fenceErr = fenceErr

		return true, nil, writeErr
	})

	object := &v1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{
		Name: owner.Name, Namespace: repository.Namespace, UID: owner.UID, ResourceVersion: "1",
	}}
	store := workflowLeaseBackupStore{object: object.DeepCopy()}
	repositories := NewConfigMapRepositoryStore(client)

	err := WithWorkflowLease(
		t.Context(),
		store,
		workflowLeaseTestLocker{lock},
		owner.Namespace,
		object,
		false,
		func(ctx context.Context, _ SessionLock) error {
			if err := repositories.Create(ctx, repository, owner); err != nil {
				return err
			}

			return repositories.CreateCredentials(
				ctx,
				repository.Namespace,
				owner,
				map[string][]byte{
					BackupAccessKeyDataKey: []byte(
						"access",
					),
					BackupSecretKeyDataKey: []byte("secret"),
				},
			)
		},
	)
	for _, expected := range []error{writeErr, fenceErr, releaseErr} {
		if !errors.Is(err, expected) {
			t.Fatalf("lost failure %v: %v", expected, err)
		}
	}

	if !attemptedCredentials || !lock.released {
		t.Fatalf(
			"incomplete lease sequence: credentials=%v released=%v",
			attemptedCredentials,
			lock.released,
		)
	}
}
