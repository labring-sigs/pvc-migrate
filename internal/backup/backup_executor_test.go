package backup

import (
	"context"
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type backupCheckpointStore struct {
	kube.WorkflowStore[*v1alpha1.Backup]
	object    *v1alpha1.Backup
	err       error
	failPhase v1alpha1.WorkflowPhase
	writes    int
	deleted   bool
}

func (s *backupCheckpointStore) Load(
	context.Context,
	crclient.ObjectKey,
) (*v1alpha1.Backup, error) {
	return s.object.DeepCopy(), nil
}

func (s *backupCheckpointStore) Save(ctx context.Context, object *v1alpha1.Backup) error {
	if err := errors.Join(ctx.Err(), kube.LeaseFenceError(ctx)); err != nil {
		return err
	}

	s.writes++
	if s.err != nil && (s.failPhase == "" || object.Status.Phase == s.failPhase) {
		return s.err
	}

	s.object = object.DeepCopy()

	return nil
}

func (s *backupCheckpointStore) Delete(context.Context, *v1alpha1.Backup) error {
	s.deleted = true
	return nil
}

type backupExecutorLocker struct{ lock *recordingBackupSessionLock }

func (l backupExecutorLocker) AcquireSessionLock(
	context.Context,
	string,
	string,
) (kube.SessionLock, error) {
	return l.lock, nil
}

type backupExecutorRepository struct {
	s3RepositoryStoreStub
	manifest       *objectstore.Manifest
	beforeManifest func()
}

func (s *backupExecutorRepository) Manifest(context.Context) (*objectstore.Manifest, error) {
	if s.beforeManifest != nil {
		s.beforeManifest()
	}
	return s.manifest, nil
}

type backupExecutorResolver struct{ repository S3RepositoryStore }

func (r backupExecutorResolver) Resolve(
	context.Context,
	crclient.ObjectKey,
	string,
) (S3RepositoryStore, *v1alpha1.BackupRepositoryBindingStatus, error) {
	return r.repository, &v1alpha1.BackupRepositoryBindingStatus{
		Type: v1alpha1.BackupRepositoryTypeS3, UID: "repository", Generation: 1,
		S3: &v1alpha1.S3BackupRepositoryBindingStatus{CredentialsSecretUID: "credentials"},
	}, nil
}

func plannedBackupObject() *v1alpha1.Backup {
	return &v1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "backup",
			Namespace:       "app",
			UID:             "workflow",
			ResourceVersion: "1",
		},
		Spec: v1alpha1.BackupSpec{
			SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}, Name: "daily", Path: "/data",
			RepositoryRef: v1alpha1.LocalObjectReference{Name: "archive"},
		},
		Status: v1alpha1.BackupStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned},
			Plan: &v1alpha1.BackupPlan{
				SourcePVC: v1alpha1.LocalResourceReference{Name: "data", UID: "pvc"},
				SourcePV:  v1alpha1.LocalResourceReference{Name: "pv", UID: "pv"},
				RepositoryRef: v1alpha1.LocalObjectReference{
					Name: "archive",
				},
				Name:      "daily",
				Path:      "/data",
				ToolImage: "example/tool:v1",
			},
		},
	}
}

func publishedBackupRepository(object *v1alpha1.Backup) *backupExecutorRepository {
	return &backupExecutorRepository{
		s3RepositoryStoreStub: s3RepositoryStoreStub{repositoryStoreStub{backend: "s3"}},
		manifest: &objectstore.Manifest{
			SessionID:       object.Name,
			SourceNamespace: object.Namespace,
			SourcePVC:       "data",
			SourcePVCUID:    "pvc",
			SourcePV:        "pv",
			SourcePVUID:     "pv",
			Path:            "/data",
			Consistency:     backupConsistency(false),
		},
	}
}

func TestBackupExecutorRecoversPublishedManifestWithoutSource(t *testing.T) {
	object := plannedBackupObject()
	object.Status.Phase, object.Status.ResumeFrom = domain.PhaseFailed, domain.PhaseWarmCopied
	store := &backupCheckpointStore{object: object.DeepCopy()}
	lock := &recordingBackupSessionLock{}
	repository := publishedBackupRepository(object)

	executor := NewBackupExecutor(
		fake.NewClientset(),
		store,
		backupExecutorLocker{lock},
		"storage",
		BackupExecutorConfig{Repository: backupExecutorResolver{repository}},
	)
	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseCompleted ||
		store.object.Status.Phase != domain.PhaseCompleted ||
		object.Status.Repository == nil ||
		!lock.released {
		t.Fatalf("published recovery did not complete: %+v", object.Status)
	}
}

func TestBackupExecutorAbortingFailureDoesNotRestartTransfer(t *testing.T) {
	object := plannedBackupObject()
	object.Status.Phase, object.Status.ResumeFrom = domain.PhaseFailed, domain.PhaseAborting
	store := &backupCheckpointStore{object: object.DeepCopy()}

	executor := NewBackupExecutor(
		fake.NewClientset(),
		store,
		backupExecutorLocker{&recordingBackupSessionLock{}},
		"app",
		BackupExecutorConfig{},
	)
	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseAborted ||
		object.Status.Message != "backup aborted; any published recovery point is retained" {
		t.Fatalf("phase = %s, message = %q", object.Status.Phase, object.Status.Message)
	}
}

func TestBackupExecutorAbortRetriesFailedCheckpoint(t *testing.T) {
	object := plannedBackupObject()
	failure := errors.New("checkpoint unavailable")
	store := &backupCheckpointStore{
		object:    object.DeepCopy(),
		err:       failure,
		failPhase: domain.PhaseAborted,
	}

	executor := NewBackupExecutor(
		fake.NewClientset(),
		store,
		backupExecutorLocker{&recordingBackupSessionLock{}},
		"app",
		BackupExecutorConfig{},
	)
	if err := executor.Abort(t.Context(), object); !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}

	if object.Status.Phase != domain.PhaseAborting ||
		store.object.Status.Phase != domain.PhaseAborting {
		t.Fatal("failed checkpoint lost resumable abort")
	}

	store.err = nil
	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseAborted {
		t.Fatal("abort did not converge")
	}
}

func TestBackupExecutorOrphanToolPreventsAbortAndDeletion(t *testing.T) {
	object := plannedBackupObject()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orphan",
			Namespace: object.Namespace,
			Labels: map[string]string{
				kube.AppInstanceLabel:  "pv-migrate-attempt",
				kube.AppComponentLabel: kube.ToolComponentRclone,
			},
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "data",
						},
					},
				},
			},
		},
	}
	store := &backupCheckpointStore{object: object.DeepCopy()}
	executor := NewBackupExecutor(
		fake.NewClientset(pod),
		store,
		backupExecutorLocker{&recordingBackupSessionLock{}},
		"app",
		BackupExecutorConfig{},
	)

	before := object.DeepCopy()
	if err := executor.Abort(
		t.Context(),
		object,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("orphan accepted: %v", err)
	}

	if store.writes != 0 || !reflect.DeepEqual(before, object) {
		t.Fatal("orphan discarded recovery state")
	}

	object.Status.Phase = domain.PhaseCompleted

	store.object = object.DeepCopy()
	if err := executor.Cleanup(
		t.Context(),
		object,
		BackupCleanupOptions{Finalize: true, DeleteSession: true},
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("orphan cleanup accepted: %v", err)
	}

	if store.deleted {
		t.Fatal("orphan lost workflow protection")
	}
}

func TestBackupExecutorLostLeasePreventsCompletionAndFailureWrite(t *testing.T) {
	object := plannedBackupObject()
	store := &backupCheckpointStore{object: object.DeepCopy()}
	lock := &recordingBackupSessionLock{}
	failure := errors.New("lease replaced")
	repository := publishedBackupRepository(object)
	repository.beforeManifest = func() { lock.err = failure }

	executor := NewBackupExecutor(
		fake.NewClientset(),
		store,
		backupExecutorLocker{lock},
		"app",
		BackupExecutorConfig{Repository: backupExecutorResolver{repository}},
	)
	if err := executor.Run(t.Context(), object); !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}

	if store.writes != 1 || object.Status.Phase != domain.PhasePlanned || !lock.released {
		t.Fatal("lost lease allowed a lifecycle write")
	}
}

func TestBackupExecutorValidationDoesNotPinOrMutate(t *testing.T) {
	object := plannedBackupObject()
	before := object.DeepCopy()
	store := &backupCheckpointStore{object: object.DeepCopy()}

	executor := NewBackupExecutor(
		fake.NewClientset(),
		store,
		nil,
		"app",
		BackupExecutorConfig{Repository: backupExecutorResolver{publishedBackupRepository(object)}},
	)
	if err := executor.Validate(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.writes != 0 || !reflect.DeepEqual(before, object) {
		t.Fatal("validation mutated workflow")
	}
}

type backupBindingResolver struct {
	store   S3RepositoryStore
	binding *v1alpha1.BackupRepositoryBindingStatus
}

func (r backupBindingResolver) Resolve(
	context.Context,
	crclient.ObjectKey,
	string,
) (S3RepositoryStore, *v1alpha1.BackupRepositoryBindingStatus, error) {
	return r.store, r.binding, nil
}

func TestBackupPlanPinsRepositoryBeforeExecution(t *testing.T) {
	object := plannedBackupObject()
	binding := &v1alpha1.BackupRepositoryBindingStatus{
		Type: v1alpha1.BackupRepositoryTypeS3, UID: "repository", Generation: 1,
		S3: &v1alpha1.S3BackupRepositoryBindingStatus{CredentialsSecretUID: "credentials"},
	}
	store := &backupCheckpointStore{object: object.DeepCopy()}

	executor := NewBackupExecutor(
		fake.NewClientset(),
		store,
		backupExecutorLocker{&recordingBackupSessionLock{}},
		"app",
		BackupExecutorConfig{
			Repository: backupBindingResolver{publishedBackupRepository(object), binding},
		},
	)
	if err := executor.Prepare(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.writes != 0 || !reflect.DeepEqual(binding, object.Status.Repository) {
		t.Fatal("plan binding must be available for the caller's atomic checkpoint")
	}

	store.object = object.DeepCopy()

	binding.Generation++
	if object.Status.Repository.Generation != 1 {
		t.Fatal("plan retained mutable repository ownership")
	}

	if err := executor.Run(t.Context(), object); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("changed repository was accepted at execution: %v", err)
	}

	if object.Status.Phase != domain.PhaseFailed || object.Status.Repository.Generation != 1 {
		t.Fatal("repository replacement changed the captured binding")
	}
}

func TestBackupRejectsUnrelatedOrInconsistentLifecycle(t *testing.T) {
	for _, status := range []v1alpha1.WorkflowStatus{
		{Phase: domain.PhaseRenaming},
		{Phase: domain.PhaseAborting, ResumeFrom: domain.PhaseCompleted},
		{Phase: domain.PhaseFailed, ResumeFrom: domain.PhaseCompleted},
		{Phase: domain.PhaseFailed, ResumeFrom: domain.PhaseWarmCopying, FailureReason: "DestinationCapacityInsufficient"},
	} {
		object := plannedBackupObject()

		object.Status.WorkflowStatus = status
		if err := validateBackupObject(object); err == nil {
			t.Fatalf("invalid repository lifecycle accepted: %+v", status)
		}
	}
}
