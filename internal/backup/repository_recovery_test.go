package backup

import (
	"strings"
	"context"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type publishedRecoveryRepository struct {
	*backupExecutorRepository
	publications int
}

func (r *publishedRecoveryRepository) PutManifest(context.Context, objectstore.Manifest) error {
	r.publications++
	return nil
}

func TestBackupPublishedRecoveryCleansOnlyOwnedProbes(t *testing.T) {
	object := plannedBackupObject()
	object.Status.Phase = domain.PhaseWarmCopied
	store := &backupCheckpointStore{object: object.DeepCopy()}
	repository := &publishedRecoveryRepository{
		backupExecutorRepository: publishedBackupRepository(object),
	}
	probe := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: object.Namespace, Name: "stale-probe", UID: "probe",
		Labels: map[string]string{
			kube.ManagedByLabel:    kube.ManagedByValue,
			kube.SessionKey:        object.Name,
			kube.ResourceRoleLabel: kube.ResourceRoleToolProbe,
		},
	}}
	foreign := probe.DeepCopy()
	foreign.Name, foreign.UID = "foreign-probe", "foreign"
	foreign.Labels[kube.SessionKey] = "another-backup"
	client := fake.NewClientset(probe, foreign)
	executor := NewBackupExecutor(client, store,
		backupExecutorLocker{&recordingBackupSessionLock{}}, "app",
		BackupExecutorConfig{Repository: backupExecutorResolver{repository}})

	if err := executor.Validate(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().
		Pods(object.Namespace).
		Get(t.Context(), probe.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("validation removed probe: %v", err)
	}

	if store.writes != 0 {
		t.Fatal("validation persisted state")
	}

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().
		Pods(object.Namespace).
		Get(t.Context(), probe.Name, metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("owned probe remains: %v", err)
	}

	if _, err := client.CoreV1().
		Pods(object.Namespace).
		Get(t.Context(), foreign.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("foreign probe removed: %v", err)
	}

	if object.Status.Phase != domain.PhaseCompleted || repository.publications != 0 {
		t.Fatalf(
			"published recovery phase=%s publications=%d",
			object.Status.Phase,
			repository.publications,
		)
	}

	// Completion is durable even after repository credentials are removed.
	client.ClearActions()

	writes := store.writes

	executor = NewBackupExecutor(client, store,
		backupExecutorLocker{&recordingBackupSessionLock{}}, "app", BackupExecutorConfig{})
	if err := executor.Validate(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.writes != writes || len(client.Actions()) != 0 {
		t.Fatal("completed workflow recreated resources or rewrote its checkpoint")
	}
}

func TestBackupRecoveryRejectsForeignPublishedManifest(t *testing.T) {
	object := plannedBackupObject()
	store := &backupCheckpointStore{object: object.DeepCopy()}
	repository := &publishedRecoveryRepository{
		backupExecutorRepository: publishedBackupRepository(object),
	}
	repository.manifest.SessionID = "another-backup"
	client := fake.NewClientset()
	executor := NewBackupExecutor(client, store,
		backupExecutorLocker{&recordingBackupSessionLock{}}, "app",
		BackupExecutorConfig{Repository: backupExecutorResolver{repository}})

	before := object.DeepCopy()
	if err := executor.Validate(
		t.Context(),
		object,
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("foreign manifest accepted during validation: %v", err)
	}

	if store.writes != 0 || !reflect.DeepEqual(before, object) {
		t.Fatal("validation mutated workflow")
	}

	err := executor.Run(t.Context(), object)
	if domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("foreign manifest accepted during execution: %v", err)
	}

	// The collision error must tell the user exactly what to change.
	if err == nil || !strings.Contains(err.Error(), "already exists") ||
		!strings.Contains(err.Error(), "different recovery point name") {
		t.Fatalf("collision error lacks user guidance: %v", err)
	}

	if object.Status.Phase != domain.PhaseFailed ||
		object.Status.ResumeFrom != domain.PhasePlanned ||
		repository.publications != 0 {
		t.Fatalf("foreign manifest advanced recovery: %+v", object.Status)
	}

	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("foreign manifest mutated resources: %v", action)
		}
	}
}

type leasedRestoreCheckpointStore struct {
	restoreCheckpointStore
	lock   *recordingBackupSessionLock
	t      *testing.T
	writes int
}

func (s *leasedRestoreCheckpointStore) Save(ctx context.Context, object *v1alpha1.Restore) error {
	if !s.lock.bound || s.lock.released {
		s.t.Fatal("restore checkpoint written outside its lease")
	}

	s.writes++

	return s.restoreCheckpointStore.Save(ctx, object)
}

func TestRestoreRecoveryFailureCheckpointRemainsInsideLease(t *testing.T) {
	object := plannedRestoreObject()
	lock := &recordingBackupSessionLock{}
	store := &leasedRestoreCheckpointStore{
		restoreCheckpointStore: restoreCheckpointStore{object: object.DeepCopy()},
		lock:                   lock, t: t,
	}

	executor := NewRestoreExecutor(fake.NewClientset(), store,
		backupExecutorLocker{lock}, "app", RestoreExecutorConfig{})
	if err := executor.Run(t.Context(), object); err == nil {
		t.Fatal("unresolved repository was accepted")
	}

	if !lock.released || store.writes != 1 || store.object.Status.Phase != domain.PhaseFailed ||
		store.object.Status.ResumeFrom != domain.PhasePlanned {
		t.Fatalf(
			"recovery lost failure checkpoint: writes=%d released=%v status=%+v",
			store.writes,
			lock.released,
			store.object.Status,
		)
	}
}
