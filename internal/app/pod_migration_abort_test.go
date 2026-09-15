package app

import (
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func installPodAbortSources(
	t *testing.T,
	client kubernetes.Interface,
	namespace string,
	volumes []v1alpha1.VolumeSpec,
) {
	t.Helper()

	for _, volume := range volumes {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      volume.SourcePVC.Name,
				Namespace: namespace,
				UID:       volume.SourcePVC.UID,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName: volume.SourcePV.Name,
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}
		if _, err := client.CoreV1().
			PersistentVolumeClaims(namespace).
			Create(t.Context(), pvc, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}

		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: volume.SourcePV.Name, UID: volume.SourcePV.UID},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{
					Name:      pvc.Name,
					Namespace: namespace,
					UID:       pvc.UID,
				},
			},
		}
		if _, err := client.CoreV1().
			PersistentVolumes().
			Create(t.Context(), pv, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPodMigrationAbortRecoversFailedWorkloadResume(t *testing.T) {
	executor, object, store, _ := podMigrationExecutorFixture(t)

	executor.workloads = &fakeController{}
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Pause(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	installPodAbortSources(t, executor.client, "source", object.Status.Plan.Volumes)
	original := object.Status.Plan.DeepCopy()
	cause := errors.New("workload readiness interrupted")
	controller := &resumingPodController{
		cause: cause,
		checkpoint: &v1alpha1.PodMigrationWorkloadStatus{
			Pod: &v1alpha1.LocalResourceReference{
				Name:            "workload",
				UID:             "recreated",
				ResourceVersion: "9",
			},
		},
	}

	executor.workloads = controller
	if err := executor.Abort(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error=%v", err)
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhaseFailed ||
		loaded.Status.ResumeFrom != domain.PhaseAborting ||
		loaded.Status.Workload.Pod.UID != "recreated" {
		t.Fatalf("lost abort recovery checkpoint: %+v", loaded.Status)
	}

	controller.cause = nil

	if err := executor.Run(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhaseAborted || controller.resumed != 2 ||
		controller.seen[1].Pod.UID != "recreated" {
		t.Fatal("abort recovery did not restore workload")
	}

	if !reflect.DeepEqual(original, loaded.Status.Plan) {
		t.Fatal("abort mutated execution plan")
	}

	if err := executor.Abort(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if controller.resumed != 2 {
		t.Fatal("completed abort resumed workload again")
	}
}

func TestPodMigrationAbortSaveFailureRetainsRecoveryPhase(t *testing.T) {
	executor, object, store, _ := podMigrationExecutorFixture(t)

	executor.workloads = &fakeController{}
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Pause(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	installPodAbortSources(t, executor.client, "source", object.Status.Plan.Volumes)

	cause := errors.New("checkpoint save failed")
	store.err, store.failAt = cause, store.writes+2
	controller := &resumingPodController{
		checkpoint: &v1alpha1.PodMigrationWorkloadStatus{
			Pod: &v1alpha1.LocalResourceReference{Name: "workload", UID: "recreated"},
		},
	}

	executor.workloads = controller
	if err := executor.Abort(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error=%v", err)
	}

	if object.Status.Workload != nil || object.Status.Phase != domain.PhaseAborting {
		t.Fatal("failed save advanced abort state")
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if err := executor.Abort(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhaseAborted || controller.resumed != 2 {
		t.Fatal("retry skipped workload recovery")
	}
}

func TestPodMigrationAbortStopsToolsBeforeWorkloadResume(t *testing.T) {
	executor, object, _, _ := podMigrationExecutorFixture(t)
	controller := &fakeController{}
	executor.workloads = controller
	executor.switcher = &scriptedSwitcher{client: executor.client}
	engine := &concreteCopyEngine{
		copy: func(copyengine.Request) error { return errors.New("copy interrupted") },
	}
	executor.transfer.copier = engine

	executor.transfer.config.Retries = 1
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Pause(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	installPodAbortSources(t, executor.client, "source", object.Status.Plan.Volumes)

	if err := executor.FinalSync(t.Context(), object); err == nil {
		t.Fatal("expected copy failure")
	}

	cause := errors.New("tool cleanup failed")

	engine.cleanupErr = cause
	if err := executor.Abort(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error=%v", err)
	}

	if controller.resumed != 0 || object.Status.Phase != domain.PhaseFailed ||
		object.Status.ResumeFrom != domain.PhaseAborting {
		t.Fatal("workload resumed before cleanup")
	}

	engine.cleanupErr = nil

	if err := executor.Abort(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if controller.resumed != 1 || object.Status.Phase != domain.PhaseAborted {
		t.Fatal("abort retry failed")
	}

	cleanups := len(engine.cleanups)

	if err := executor.Abort(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if len(engine.cleanups) != cleanups || controller.resumed != 1 {
		t.Fatal("completed abort repeated operations")
	}
}

func TestNamespacedPodMigrationAbortRecoversFailedWorkloadResume(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationFixture(t)

	executor.workloads = &fakeController{}
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Pause(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	installPodAbortSources(t, executor.client, object.Namespace, object.Status.Plan.Volumes)
	original := object.Status.Plan.DeepCopy()
	cause := errors.New("workload readiness interrupted")
	controller := &resumingPodController{
		cause: cause,
		checkpoint: &v1alpha1.PodMigrationWorkloadStatus{
			Pod: &v1alpha1.LocalResourceReference{
				Name:            "workload",
				UID:             "recreated",
				ResourceVersion: "9",
			},
		},
	}

	executor.workloads = controller
	if err := executor.Abort(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error=%v", err)
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhaseFailed ||
		loaded.Status.ResumeFrom != domain.PhaseAborting ||
		loaded.Status.Workload.Pod.UID != "recreated" {
		t.Fatalf("lost abort recovery checkpoint: %+v", loaded.Status)
	}

	controller.cause = nil

	if err := executor.Run(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhaseAborted || controller.resumed != 2 ||
		controller.seen[1].Pod.UID != "recreated" {
		t.Fatal("abort recovery did not restore workload")
	}

	if !reflect.DeepEqual(original, loaded.Status.Plan) {
		t.Fatal("abort mutated execution plan")
	}

	if err := executor.Abort(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if controller.resumed != 2 {
		t.Fatal("completed abort resumed workload again")
	}
}

func TestNamespacedPodMigrationAbortSaveFailureRetainsRecoveryPhase(t *testing.T) {
	executor, object, store, _ := namespacedPodMigrationFixture(t)

	executor.workloads = &fakeController{}
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Pause(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	installPodAbortSources(t, executor.client, object.Namespace, object.Status.Plan.Volumes)

	cause := errors.New("checkpoint save failed")
	store.err, store.failAt = cause, store.writes+2
	controller := &resumingPodController{
		checkpoint: &v1alpha1.PodMigrationWorkloadStatus{
			Pod: &v1alpha1.LocalResourceReference{Name: "workload", UID: "recreated"},
		},
	}

	executor.workloads = controller
	if err := executor.Abort(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error=%v", err)
	}

	if object.Status.Workload != nil || object.Status.Phase != domain.PhaseAborting {
		t.Fatal("failed save advanced abort state")
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if err := executor.Abort(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhaseAborted || controller.resumed != 2 {
		t.Fatal("retry skipped workload recovery")
	}
}

func TestNamespacedPodMigrationAbortStopsToolsBeforeWorkloadResume(t *testing.T) {
	executor, object, _, _ := namespacedPodMigrationFixture(t)
	controller := &fakeController{}
	executor.workloads = controller
	executor.switcher = &scriptedSwitcher{client: executor.client}
	engine := &concreteCopyEngine{
		copy: func(copyengine.Request) error { return errors.New("copy interrupted") },
	}
	executor.transfer.copier = engine

	executor.transfer.config.Retries = 1
	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Pause(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	installPodAbortSources(t, executor.client, object.Namespace, object.Status.Plan.Volumes)

	if err := executor.FinalSync(t.Context(), object); err == nil {
		t.Fatal("expected copy failure")
	}

	cause := errors.New("tool cleanup failed")

	engine.cleanupErr = cause
	if err := executor.Abort(t.Context(), object); !errors.Is(err, cause) {
		t.Fatalf("error=%v", err)
	}

	if controller.resumed != 0 || object.Status.Phase != domain.PhaseFailed ||
		object.Status.ResumeFrom != domain.PhaseAborting {
		t.Fatal("workload resumed before cleanup")
	}

	engine.cleanupErr = nil

	if err := executor.Abort(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if controller.resumed != 1 || object.Status.Phase != domain.PhaseAborted {
		t.Fatal("abort retry failed")
	}

	cleanups := len(engine.cleanups)

	if err := executor.Abort(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if len(engine.cleanups) != cleanups || controller.resumed != 1 {
		t.Fatal("completed abort repeated operations")
	}
}
