package app

import (
	"context"
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func podMigrationSharedWarmFixture(t *testing.T) (
	*ClusterPodMigrationExecutor, *v1alpha1.ClusterPodMigration, *podMigrationCheckpointStore,
	*concreteCopyEngine, *recordingOpenEBSLVMSharedVolumeManager,
) {
	t.Helper()
	executor, object, store, _ := podMigrationExecutorFixture(
		t,
		func(o *v1alpha1.ClusterPodMigration) {
			o.Spec.OpenEBSLVMEnableShared = true
			o.Status.Plan.OpenEBSLVMEnableShared = true
		},
	)
	engine := &concreteCopyEngine{}
	executor.transfer.copier = engine
	executor.transfer.config.Retries = 1
	manager := &recordingOpenEBSLVMSharedVolumeManager{}
	executor.sharedVolumes = manager

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "source", Name: "workload", UID: "workload-uid"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, volume := range object.Status.Plan.Volumes {
		source := volume.SourcePVC
		if _, err := executor.client.CoreV1().
			PersistentVolumeClaims("source").
			Create(t.Context(), &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "source",
					Name:      source.Name,
					UID:       source.UID,
				},
				Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: volume.SourcePV.Name},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
			}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}

		if _, err := executor.client.CoreV1().
			PersistentVolumes().
			Create(t.Context(), &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: volume.SourcePV.Name, UID: volume.SourcePV.UID},
				Spec: corev1.PersistentVolumeSpec{
					ClaimRef: &corev1.ObjectReference{
						Namespace: "source",
						Name:      source.Name,
						UID:       source.UID,
					},
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{Driver: kube.OpenEBSLVMCSIDriver},
					},
				},
			}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}

		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: source.Name,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: source.Name,
				},
			},
		})
	}

	if _, err := executor.client.CoreV1().
		Pods("source").
		Create(t.Context(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	return executor, object, store, engine, manager
}

func TestPodMigrationSharedPreparationSavesBeforeMutation(t *testing.T) {
	executor, object, store, engine, manager := podMigrationSharedWarmFixture(t)
	manager.onEnable = func() {
		loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
		if err != nil {
			t.Fatal(err)
		}

		if len(loaded.Status.OpenEBSLVMSharedMounts) != len(manager.enablePVs) {
			t.Fatal("shared setting changed before recording compensation")
		}
	}
	engine.copy = func(request copyengine.CopyRequest) error {
		if !request.Source.MountReadWrite || len(object.Status.OpenEBSLVMSharedMounts) != 2 {
			t.Fatal("copy did not use checkpointed writable source mounts")
		}
		return nil
	}

	before := object.Status.Plan.DeepCopy()
	if err := executor.WarmCopy(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.WarmPassesCompleted != 1 || len(object.Status.OpenEBSLVMSharedMounts) != 0 ||
		!reflect.DeepEqual(
			manager.restorePVs,
			[]string{"pv-b", "pv-a"},
		) || !reflect.DeepEqual(object.Status.Plan, before) {
		t.Fatal("warm pass did not restore shared settings without changing the plan")
	}
}

func TestPodMigrationWarmProbesMatchVolumesByIdentity(t *testing.T) {
	executor, object, _, _, manager := podMigrationSharedWarmFixture(t)
	prober := &recordingToolImageProber{}
	executor.config.ToolImageProber = prober
	manager.shared = true

	_, err := executor.probeWarmCopy(t.Context(), object.Name, object.Status.Plan.ToolImage,
		[]string{domain.StrategyLocal}, object.Status.Plan.Volumes,
		[]kube.ToolProbeTarget{
			{Namespace: "source", PVCName: "b"},
			{Namespace: "source", PVCName: "a"},
		}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(manager.sharedPVs, []string{"pv-b", "pv-a"}) ||
		len(prober.calls) != 1 || len(prober.calls[0].Targets) != 2 {
		t.Fatal("source probes depended on volume slice order")
	}

	for _, target := range prober.calls[0].Targets {
		if !target.WritablePVCMount {
			t.Fatal("shared source probe must mount read-write")
		}
	}
}

func TestPodMigrationSharedPreparationSaveFailureCompensatesEarlierVolumes(t *testing.T) {
	executor, object, store, engine, manager := podMigrationSharedWarmFixture(t)
	failure := errors.New("second preparation checkpoint rejected")

	store.failAt, store.err = store.writes+2, failure
	if err := executor.WarmCopy(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(manager.enablePVs, []string{"pv-a"}) ||
		!reflect.DeepEqual(manager.restorePVs, []string{"pv-a"}) ||
		len(object.Status.OpenEBSLVMSharedMounts) != 0 || len(engine.requests) != 0 {
		t.Fatal("failed preparation mutated an uncheckpointed volume or lost earlier compensation")
	}

	if err := executor.WarmCopy(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.WarmPassesCompleted != 1 {
		t.Fatal("preparation retry did not complete")
	}
}

func TestPodMigrationCanceledProbeRestoresSharedMounts(t *testing.T) {
	executor, object, _, engine, manager := podMigrationSharedWarmFixture(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	executor.config.ToolImageProber = &recordingToolImageProber{
		onProbe: func(context.Context) { cancel() }, err: context.Canceled,
	}
	if err := executor.WarmCopy(ctx, object); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	if len(object.Status.OpenEBSLVMSharedMounts) != 0 || len(engine.requests) != 0 ||
		!reflect.DeepEqual(manager.restorePVs, []string{"pv-b", "pv-a"}) {
		t.Fatal("canceled probe leaked temporary shared settings")
	}

	for _, err := range manager.restoreContextErrs {
		if err != nil {
			t.Fatal("compensation used the canceled execution context")
		}
	}
}

func TestPodMigrationSharedPreparationStopsOnFenceLoss(t *testing.T) {
	executor, object, store, engine, manager := podMigrationSharedWarmFixture(t)
	lost := errors.New("lease lost after enabling shared mount")
	lock := &fakeSessionLock{}
	executor.locker = &fakeSessionLocker{lock: lock}
	manager.onEnable = func() { lock.err = lost }

	if err := executor.WarmCopy(t.Context(), object); !errors.Is(err, lost) {
		t.Fatal(err)
	}

	if len(manager.enablePVs) != 1 || len(manager.restorePVs) != 0 || len(engine.requests) != 0 ||
		len(object.Status.OpenEBSLVMSharedMounts) != 1 {
		t.Fatal("lost fence changed more resources or discarded recovery data")
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	if len(loaded.Status.OpenEBSLVMSharedMounts) != 1 {
		t.Fatal("recovery checkpoint was not durable")
	}
}

func TestPodMigrationSharedRestoreFailurePreservesCapacityFailure(t *testing.T) {
	executor, object, _, engine, manager := podMigrationSharedWarmFixture(t)
	engine.copy = func(copyengine.CopyRequest) error { return errors.New("No space left on device") }
	restoreFailure := errors.New("shared mount restoration unavailable")

	manager.restoreErr = restoreFailure
	if err := executor.WarmCopy(t.Context(), object); !errors.Is(err, restoreFailure) {
		t.Fatal(err)
	}

	if object.Status.FailureReason != domain.FailureDestinationCapacityExhausted ||
		len(object.Status.OpenEBSLVMSharedMounts) != 2 {
		t.Fatalf("compensation failure erased the nonretryable copy failure: %+v", object.Status)
	}

	if err := executor.WarmCopy(t.Context(), object); err == nil || len(engine.requests) != 1 {
		t.Fatal("capacity failure became retryable after compensation failed")
	}
}

func TestPodMigrationWarmPreflightRejectsMovedSourceConsumer(t *testing.T) {
	executor, object, store, _ := podMigrationExecutorFixture(
		t,
		func(o *v1alpha1.ClusterPodMigration) {
			o.Status.Plan.SourceNode = "planned-node"
		},
	)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "source", Name: "moved", UID: "moved"},
		Spec: corev1.PodSpec{NodeName: "other-node", Volumes: []corev1.Volume{
			{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "a",
					},
				},
			},
		}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if _, err := executor.client.CoreV1().
		Pods("source").
		Create(t.Context(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := executor.ValidateWarmCopy(
		t.Context(),
		object,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatal(err)
	}

	if store.writes != 0 {
		t.Fatal("read-only preflight saved state")
	}
}
