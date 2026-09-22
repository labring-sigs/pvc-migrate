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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type restoreCheckpointStore struct {
	kube.WorkflowStore[*v1alpha1.Restore]
	object    *v1alpha1.Restore
	err       error
	failPhase v1alpha1.WorkflowPhase
	deleted   bool
}

func (s *restoreCheckpointStore) Load(
	context.Context,
	crclient.ObjectKey,
) (*v1alpha1.Restore, error) {
	return s.object.DeepCopy(), nil
}

func (s *restoreCheckpointStore) Save(ctx context.Context, object *v1alpha1.Restore) error {
	if err := errors.Join(ctx.Err(), kube.LeaseFenceError(ctx)); err != nil {
		return err
	}

	if s.err != nil && (s.failPhase == "" || object.Status.Phase == s.failPhase) {
		return s.err
	}

	s.object = object.DeepCopy()

	return nil
}

func (s *restoreCheckpointStore) Delete(context.Context, *v1alpha1.Restore) error {
	s.deleted = true
	return nil
}

func plannedRestoreObject() *v1alpha1.Restore {
	return &v1alpha1.Restore{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "restore",
			Namespace:       "app",
			UID:             "workflow",
			ResourceVersion: "1",
		},
		Spec: v1alpha1.RestoreSpec{
			DestinationPVC: v1alpha1.LocalResourceReference{Name: "data"},
			Name:           "daily",
			RepositoryRef:  v1alpha1.LocalObjectReference{Name: "archive"},
		},
		Status: v1alpha1.RestoreStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned},
			Plan: &v1alpha1.RestorePlan{
				DestinationPVC: v1alpha1.LocalResourceReference{Name: "data", UID: "pvc"},
				Name:           "daily",
				RepositoryRef:  v1alpha1.LocalObjectReference{Name: "archive"},
				ToolImage:      "example/tool:v1",
			},
		},
	}
}

func restoreDestinationFixture() (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	mode := corev1.PersistentVolumeFilesystem
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: "pvc"},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:  "pv",
			VolumeMode:  &mode,
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv", UID: "pv"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			ClaimRef: &corev1.ObjectReference{
				Namespace: pvc.Namespace,
				Name:      pvc.Name,
				UID:       pvc.UID,
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}

	return pvc, pv
}

func TestRestoreExecutorRecoversTransferredCheckpointWithoutRepository(t *testing.T) {
	object := plannedRestoreObject()
	pvc, pv := restoreDestinationFixture()
	pvcRef, pvRef := kube.PVCReference(pvc), kube.PVReference(pv)
	object.Status.DestinationPVC, object.Status.DestinationPV = &pvcRef, &pvRef
	object.Status.Phase, object.Status.ResumeFrom = domain.PhaseFailed, domain.PhaseWarmCopied
	store := &restoreCheckpointStore{object: object.DeepCopy()}
	client := fake.NewClientset(pvc, pv)

	executor := NewRestoreExecutor(
		client,
		store,
		backupExecutorLocker{&recordingBackupSessionLock{}},
		"app",
		RestoreExecutorConfig{},
	)
	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.object.Status.Phase != domain.PhaseCompleted {
		t.Fatalf("not completed: %+v", store.object.Status)
	}

	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("recovery mutated resources: %v", action)
		}
	}
}

func TestRestoreExecutorAbortCheckpointFailureRetriesWithoutTransfer(t *testing.T) {
	object := plannedRestoreObject()
	object.Status.Phase, object.Status.ResumeFrom = domain.PhaseFailed, domain.PhaseAborting
	store := &restoreCheckpointStore{
		object:    object.DeepCopy(),
		err:       errors.New("save failed"),
		failPhase: domain.PhaseAborted,
	}

	executor := NewRestoreExecutor(
		fake.NewClientset(),
		store,
		backupExecutorLocker{&recordingBackupSessionLock{}},
		"app",
		RestoreExecutorConfig{},
	)
	if err := executor.Run(t.Context(), object); !errors.Is(err, store.err) {
		t.Fatalf("error=%v", err)
	}

	if object.Status.Phase != domain.PhaseAborting ||
		store.object.Status.Phase != domain.PhaseAborting {
		t.Fatal("failed checkpoint advanced abort")
	}

	store.err = nil
	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseAborted ||
		object.Status.Message != "restore aborted; destination data is retained" {
		t.Fatalf("abort did not resume: %+v", object.Status.WorkflowStatus)
	}
}

func TestRestoreDestinationCheckpointPrecedesBindingAndHonorsFence(t *testing.T) {
	for _, scenario := range []string{"save failure", "lost fence", "success"} {
		t.Run(scenario, func(t *testing.T) {
			object := plannedRestoreObject()
			object.Status.Plan.CreatePVC = true
			object.Status.Plan.DestinationPVC.UID = ""
			pvc, _ := restoreDestinationFixture()
			pvc.Status.Phase, pvc.Spec.VolumeName = corev1.ClaimPending, ""
			store := &restoreCheckpointStore{object: object.DeepCopy()}
			lock := &recordingBackupSessionLock{}
			ctx := kube.WithLeaseFence(t.Context(), lock)

			if scenario == "save failure" {
				store.err = errors.New("save unavailable")
			}

			executor := NewRestoreExecutor(
				fake.NewClientset(pvc),
				store,
				nil,
				"app",
				RestoreExecutorConfig{},
			)
			probed := false
			stopProbe := errors.New("probe started")

			err := checkpointAndBindRestorePVC(ctx, executor.client,
				func(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
					err := executor.checkpointPVC(ctx, object, pvc)
					if scenario == "lost fence" {
						lock.err = errors.New("lease lost")
					}

					return err
				},
				func(context.Context, *corev1.PersistentVolumeClaim) error {
					probed = true

					if store.object.Status.DestinationPVC == nil ||
						store.object.Status.DestinationPVC.UID != pvc.UID {
						t.Fatal("binding started before durable identity")
					}

					return stopProbe
				}, pvc)
			if scenario == "success" {
				if !probed || !errors.Is(err, stopProbe) {
					t.Fatalf("probe=%v error=%v", probed, err)
				}
			} else if probed || err == nil {
				t.Fatalf("unsafe binding: probe=%v error=%v", probed, err)
			}

			if scenario == "save failure" && object.Status.DestinationPVC != nil {
				t.Fatal("failed identity save mutated status")
			}
		})
	}
}

func TestRestoreExecutorRejectsMissingAndReplacedDestination(t *testing.T) {
	for _, missing := range []bool{true, false} {
		object := plannedRestoreObject()
		pvc, _ := restoreDestinationFixture()
		pvc.UID = "replacement"

		client := fake.NewClientset()
		if !missing {
			if _, err := client.CoreV1().
				PersistentVolumeClaims(pvc.Namespace).
				Create(t.Context(), pvc, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
		}

		executor := NewRestoreExecutor(client, nil, nil, "app", RestoreExecutorConfig{})
		plan := *object.Status.Plan
		plan.CreatePVC = true

		_, err := executor.validateDestination(
			t.Context(),
			object.Namespace,
			object.Name,
			plan,
			nil,
			objectstore.Config{},
			objectstore.Manifest{Capacity: "1Gi", VolumeMode: "Filesystem"},
		)
		if domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("missing=%v error=%v", missing, err)
		}
	}
}

func TestRestoreCleanupRetainsDestinationAndCredentialsAndRetries(t *testing.T) {
	object := plannedRestoreObject()
	object.Spec.CreatePVC, object.Status.Plan.CreatePVC = true, true
	object.Spec.DestinationAccessMode, object.Status.Plan.DestinationAccessMode = "ReadWriteOnce", "ReadWriteOnce"
	object.Status.Plan.DestinationPVC.UID = ""
	object.Status.Phase = domain.PhaseAborted
	pvc, pv := restoreDestinationFixture()
	pvc.Labels = map[string]string{
		kube.SessionKey:     object.Name,
		kube.ManagedByLabel: kube.ManagedByValue,
	}
	pvc.Annotations = map[string]string{restoreNameAnnotation: "daily"}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "credentials", UID: "credentials"},
	}
	client := fake.NewClientset(pvc, pv, secret)
	store := &restoreCheckpointStore{object: object.DeepCopy()}

	executor := NewRestoreExecutor(
		client,
		store,
		backupExecutorLocker{&recordingBackupSessionLock{}},
		"app",
		RestoreExecutorConfig{},
	)
	for range 2 {
		if err := executor.Cleanup(
			t.Context(),
			object,
			RestoreCleanupOptions{Finalize: true},
		); err != nil {
			t.Fatal(err)
		}
	}

	retained, err := client.CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Get(t.Context(), pvc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if retained.UID != pvc.UID || retained.Labels[kube.SessionKey] != "" ||
		retained.Annotations[restoreNameAnnotation] != "daily" {
		t.Fatal("cleanup changed data identity or retained ownership")
	}

	if _, err := client.CoreV1().
		Secrets("app").
		Get(t.Context(), "credentials", metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}

	if store.object.Status.DestinationPVC == nil {
		t.Fatal("cleanup did not record recovered destination before finalization")
	}
}

func TestRestoreCreationCannotAdoptAnotherWorkflowPVC(t *testing.T) {
	pvc, _ := restoreDestinationFixture()
	pvc.Labels = map[string]string{kube.SessionKey: "first"}
	config := objectstore.Config{Bucket: "backups", Name: "daily"}

	pvc.Annotations = map[string]string{
		restoreBucketAnnotation: config.Bucket,
		restoreNameAnnotation:   config.Name,
	}
	if err := validateRestorePVCOwnership(
		pvc,
		"second",
		config,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("error=%v", err)
	}
}

func TestRestorePreparePinsRepositoryAndDestinationWithoutWrites(t *testing.T) {
	object := plannedRestoreObject()
	pvc, pv := restoreDestinationFixture()
	client := fake.NewClientset(
		pvc,
		pv,
	)
	repository := &backupExecutorRepository{
		s3RepositoryStoreStub: s3RepositoryStoreStub{repositoryStoreStub{backend: "s3"}},
		manifest:              &objectstore.Manifest{Capacity: "1Gi", VolumeMode: "Filesystem"},
	}
	executor := NewRestoreExecutor(
		client,
		nil,
		nil,
		"app",
		RestoreExecutorConfig{Repository: backupExecutorResolver{repository}},
	)

	before := object.DeepCopy()
	if err := executor.Validate(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(before, object) {
		t.Fatal("validation mutated workflow")
	}

	if err := executor.Prepare(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Repository == nil || object.Status.DestinationPVC == nil ||
		object.Status.DestinationPV == nil {
		t.Fatalf("missing checkpoint: %+v", object.Status)
	}

	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("preparation wrote resource: %v", action)
		}
	}
}

func TestRestoreExecutorLostLeasePreventsLifecycleWrites(t *testing.T) {
	object := plannedRestoreObject()
	pvc, pv := restoreDestinationFixture()
	store := &restoreCheckpointStore{object: object.DeepCopy()}
	lock := &recordingBackupSessionLock{}
	failure := errors.New("lease replaced")
	repository := &backupExecutorRepository{
		s3RepositoryStoreStub: s3RepositoryStoreStub{repositoryStoreStub{backend: "s3"}},
		manifest:              &objectstore.Manifest{Capacity: "1Gi", VolumeMode: "Filesystem"},
		beforeManifest:        func() { lock.err = failure },
	}

	executor := NewRestoreExecutor(
		fake.NewClientset(pvc, pv),
		store,
		backupExecutorLocker{lock},
		"app",
		RestoreExecutorConfig{Repository: backupExecutorResolver{repository}},
	)
	if err := executor.Run(t.Context(), object); !errors.Is(err, failure) {
		t.Fatalf("error=%v", err)
	}

	if object.Status.Phase != domain.PhasePlanned ||
		store.object.Status.Phase != domain.PhasePlanned ||
		!lock.released {
		t.Fatal("lost lease advanced lifecycle")
	}
}

func TestRestoreExecutorOrphanToolPreventsAbortAndDeletion(t *testing.T) {
	object := plannedRestoreObject()
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
	store := &restoreCheckpointStore{object: object.DeepCopy()}
	executor := NewRestoreExecutor(
		fake.NewClientset(pod),
		store,
		backupExecutorLocker{&recordingBackupSessionLock{}},
		"app",
		RestoreExecutorConfig{},
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

	if !reflect.DeepEqual(before, object) {
		t.Fatal("orphan discarded recovery state")
	}

	object.Status.Phase = domain.PhaseAborted

	store.object = object.DeepCopy()
	if err := executor.Cleanup(
		t.Context(),
		object,
		RestoreCleanupOptions{Finalize: true, DeleteSession: true},
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("orphan cleanup accepted: %v", err)
	}

	if store.deleted {
		t.Fatal("orphan lost workflow protection")
	}
}

func TestRestoreValidationRejectsUnrelatedOrIncompleteCheckpoints(t *testing.T) {
	for _, mutate := range []func(*v1alpha1.Restore){
		func(o *v1alpha1.Restore) { o.Status.Phase = domain.PhaseWarmCopied },
		func(o *v1alpha1.Restore) {
			o.Status.DestinationPVC = &v1alpha1.ObjectReference{Namespace: o.Namespace, Name: "other", UID: "pvc"}
		},
		func(o *v1alpha1.Restore) { o.Status.DestinationPV = &v1alpha1.ObjectReference{Name: "pv", UID: "pv"} },
		func(o *v1alpha1.Restore) {
			o.Status.Phase = domain.PhaseFailed
			o.Status.ResumeFrom = domain.PhaseCompleted
		},
		func(o *v1alpha1.Restore) { o.Spec.AllowMounted = true },
	} {
		object := plannedRestoreObject()
		mutate(object)

		if err := validateRestoreObject(object); err == nil {
			t.Fatalf("invalid workflow accepted: %+v", object.Status)
		}
	}
}
