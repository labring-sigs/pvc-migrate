package app

import (
	"context"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type deletionStore struct {
	memoryStore
	latest *domain.Session
}

func (*deletionStore) StorageBackend() string { return kube.SessionBackendCRD }

func (s *deletionStore) GetByKind(
	context.Context,
	string,
	string,
	domain.ControllerKind,
) (*domain.Session, error) {
	return s.latest, nil
}

func (*deletionStore) CheckWorkflowNameCollision(
	context.Context,
	*domain.Session,
) error {
	return nil
}

func (*deletionStore) EnsureSessionProtection(context.Context, *domain.Session) error { return nil }

func TestDeleteBeforeCopyStartsPreservesSource(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhasePlanned, domain.PhaseFailed, domain.PhaseAborted} {
		t.Run(string(phase), func(t *testing.T) {
			session := appTestSession()
			setSessionOperation(session, domain.OperationCopy)
			session.Status.Phase = phase
			session.Status.ResumeFrom = domain.PhasePlanned
			session.Deleting = true
			session.BackendUID = "workflow-uid"
			volume := session.Spec.Volumes[0]
			client := fake.NewClientset(
				&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
					Namespace: volume.SourcePVC.Namespace,
					Name:      volume.SourcePVC.Name,
					UID:       volume.SourcePVC.UID,
				}},
				&corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: volume.SourcePV.Name,
						UID:  volume.SourcePV.UID,
					},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
						ClaimRef: &corev1.ObjectReference{
							Namespace: volume.SourcePVC.Namespace,
							Name:      volume.SourcePVC.Name,
							UID:       volume.SourcePVC.UID,
						},
					},
				},
			)
			store := &deletionStore{latest: session}

			service := NewService(client, store, nil, nil, nil, nil, Config{})
			if err := service.FinalizeDeletedWorkflow(t.Context(), session); err != nil {
				t.Fatal(err)
			}

			if session.Status.Phase != domain.PhaseAborted || store.deletes != 1 {
				t.Fatalf("phase=%s deletes=%d", session.Status.Phase, store.deletes)
			}

			for _, action := range client.Actions() {
				if action.GetVerb() == "delete" || action.GetVerb() == "update" ||
					action.GetVerb() == "patch" {
					t.Fatalf("source mutated: %v", action)
				}
			}
		})
	}
}

func TestDeletionRejectsReplacedWorkflowOrWithdrawnIntent(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		session := appTestSession()
		session.BackendUID = "original"
		latest := appTestSession()
		latest.BackendUID = "replacement"

		latest.Deleting = replaced
		if !replaced {
			latest.BackendUID = session.BackendUID
		}

		store := &deletionStore{latest: latest}

		service := NewService(fake.NewClientset(), store, nil, nil, nil, nil, Config{})
		if err := service.FinalizeDeletedWorkflow(
			t.Context(),
			session,
		); domain.CategoryOf(
			err,
		) != domain.ErrorConflict {
			t.Fatalf("error=%v", err)
		}

		if store.updates != 0 || store.deletes != 0 {
			t.Fatal("changed workflow was mutated")
		}
	}
}

func TestAbortRejectsInProgressActivation(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhaseActivating, domain.PhaseResuming} {
		session := appTestSession()
		session.Status.Phase = phase
		session.Status.ResumeFrom = ""

		service := &Service{now: time.Now}
		if err := service.abort(
			t.Context(),
			session,
		); domain.CategoryOf(
			err,
		) != domain.ErrorPrecondition {
			t.Fatalf("phase=%s error=%v", phase, err)
		}

		if session.Status.Phase != phase {
			t.Fatal("abort changed activation checkpoint")
		}
	}
}

func TestAbortRejectsInterruptedPVCIdentity(t *testing.T) {
	for _, operation := range []domain.Operation{domain.OperationRename, domain.OperationMove} {
		for _, failed := range []bool{false, true} {
			session := appTestSession()
			setSessionOperation(session, operation)

			phase := domain.PhaseRenaming
			if operation == domain.OperationMove {
				phase = domain.PhaseMoving
			}

			session.Status.Phase = phase
			if failed {
				session.Status.Phase = domain.PhaseFailed
				session.Status.ResumeFrom = phase
			}

			service := &Service{now: time.Now}
			if err := service.validateAbort(
				t.Context(),
				session,
			); domain.CategoryOf(
				err,
			) != domain.ErrorPrecondition {
				t.Fatalf("operation=%s failed=%v error=%v", operation, failed, err)
			}

			if !deletionRequiresConvergence(phase) {
				t.Fatalf("deletion would abort %s", phase)
			}
		}
	}
}
