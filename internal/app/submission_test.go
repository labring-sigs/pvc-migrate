package app

import (
	"context"
	"errors"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"k8s.io/client-go/kubernetes/fake"
)

type submissionStore struct {
	memoryStore
	creates     int
	validations int
}

func (*submissionStore) StorageBackend() string { return kube.SessionBackendCRD }

func (s *submissionStore) Create(ctx context.Context, session *domain.Session) error {
	s.creates++
	return s.memoryStore.Create(ctx, session)
}

func (s *submissionStore) ValidateCreate(context.Context, *domain.Session) error {
	s.validations++
	return nil
}

func (*submissionStore) AcquireSessionLock(
	context.Context,
	string,
	string,
) (kube.SessionLock, error) {
	return nil, errors.New("submitter cannot acquire execution leases")
}

func TestControllerSubmissionLeavesExecutionToController(t *testing.T) {
	for _, dryRun := range []bool{false, true} {
		t.Run(map[bool]string{false: "create", true: "dry-run"}[dryRun], func(t *testing.T) {
			store := &submissionStore{}
			client := fake.NewClientset()
			service := NewService(client, store, nil, nil, nil, nil, Config{})
			session := appTestSession()

			plan := &domain.MigrationPlan{
				Ready:       true,
				SessionID:   session.ID,
				SessionSpec: session.Spec,
			}
			if _, err := service.CreateSession(context.Background(), plan, dryRun); err != nil {
				t.Fatal(err)
			}

			if len(client.Actions()) != 0 || store.updates != 0 || store.deletes != 0 {
				t.Fatalf(
					"submission touched execution resources: actions=%v store=%+v",
					client.Actions(),
					store,
				)
			}

			wantCreates := 1
			if dryRun {
				wantCreates = 0
			}

			if store.creates != wantCreates {
				t.Fatalf("creates=%d, want %d", store.creates, wantCreates)
			}

			if store.validations != 1-wantCreates {
				t.Fatalf("validations=%d, want %d", store.validations, 1-wantCreates)
			}
		})
	}
}

func TestControllerCancellationPreservesExecutionCheckpoint(t *testing.T) {
	store := &submissionStore{}
	service := NewService(fake.NewClientset(), store, nil, nil, nil, nil, Config{})
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserving
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := service.failContext(
		ctx,
		session,
		context.Canceled,
	); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf("error=%v", err)
	}

	if session.Status.Phase != domain.PhaseReserving || store.updates != 0 {
		t.Fatalf("phase=%s updates=%d", session.Status.Phase, store.updates)
	}
}
