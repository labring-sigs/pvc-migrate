package app

import (
	"context"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func TestPodMigrationCutoverChecksShrinkBeforeAndAfterPause(t *testing.T) {
	for _, stage := range []string{"before", "after"} {
		t.Run(stage, func(t *testing.T) {
			executor, object, _, _ := podMigrationExecutorFixture(
				t,
				func(object *v1alpha1.ClusterPodMigration) {
					object.Status.Plan.Volumes[0].SourceCapacity = "2Gi"
				},
			)
			controller := &fakeController{}
			executor.workloads = controller
			executor.switcher = &scriptedSwitcher{client: executor.client}
			engine := &concreteCopyEngine{}
			executor.transfer.copier = engine

			executor.config.VolumeUsageReader = &staticVolumeUsageReader{
				result: kube.VolumeUsageReadResult{UsedBytes: 512 << 20, Source: "test"},
			}
			if err := executor.Reserve(t.Context(), object); err != nil {
				t.Fatal(err)
			}

			checked := false

			executor.config.VolumeUsageReader = volumeUsageReaderFunc(
				func(context.Context, kube.VolumeUsageReadOptions) (kube.VolumeUsageReadResult, error) {
					overflow := stage == "before" || controller.paused > 0

					used := int64(512 << 20)
					if overflow {
						used = 2 << 30
						checked = true
					}

					return kube.VolumeUsageReadResult{UsedBytes: used, Source: "test"}, nil
				},
			)
			if err := executor.PauseAndFinalSync(
				t.Context(),
				object,
			); err == nil ||
				!strings.Contains(err.Error(), "above destination capacity") {
				t.Fatalf("error = %v", err)
			}

			if !checked || len(engine.requests) != 0 {
				t.Fatal("unsafe copy started")
			}

			if stage == "before" &&
				(controller.paused != 0 || object.Status.Phase != domain.PhaseReserved) {
				t.Fatal("known overflow interrupted workload")
			}

			if stage == "after" &&
				(controller.paused != 1 || object.Status.Phase != domain.PhaseFailed || object.Status.ResumeFrom != domain.PhasePaused) {
				t.Fatalf("post-pause overflow lost recoverable phase: %+v", object.Status)
			}
		})
	}
}

func TestNamespacedPodMigrationCutoverChecksShrinkBeforeAndAfterPause(t *testing.T) {
	for _, stage := range []string{"before", "after"} {
		t.Run(stage, func(t *testing.T) {
			executor, object, _, _ := namespacedPodMigrationFixture(
				t,
				func(object *v1alpha1.PodMigration) {
					object.Status.Plan.Volumes[0].SourceCapacity = "2Gi"
				},
			)
			controller := &fakeController{}
			executor.workloads = controller
			executor.switcher = &scriptedSwitcher{client: executor.client}
			engine := &concreteCopyEngine{}
			executor.transfer.copier = engine

			executor.config.VolumeUsageReader = &staticVolumeUsageReader{
				result: kube.VolumeUsageReadResult{UsedBytes: 512 << 20, Source: "test"},
			}
			if err := executor.Reserve(t.Context(), object); err != nil {
				t.Fatal(err)
			}

			checked := false

			executor.config.VolumeUsageReader = volumeUsageReaderFunc(
				func(context.Context, kube.VolumeUsageReadOptions) (kube.VolumeUsageReadResult, error) {
					overflow := stage == "before" || controller.paused > 0

					used := int64(512 << 20)
					if overflow {
						used = 2 << 30
						checked = true
					}

					return kube.VolumeUsageReadResult{UsedBytes: used, Source: "test"}, nil
				},
			)
			if err := executor.PauseAndFinalSync(
				t.Context(),
				object,
			); err == nil ||
				!strings.Contains(err.Error(), "above destination capacity") {
				t.Fatalf("error = %v", err)
			}

			if !checked || len(engine.requests) != 0 {
				t.Fatal("unsafe copy started")
			}

			if stage == "before" &&
				(controller.paused != 0 || object.Status.Phase != domain.PhaseReserved) {
				t.Fatal("known overflow interrupted workload")
			}

			if stage == "after" &&
				(controller.paused != 1 || object.Status.Phase != domain.PhaseFailed || object.Status.ResumeFrom != domain.PhasePaused) {
				t.Fatalf("post-pause overflow lost recoverable phase: %+v", object.Status)
			}
		})
	}
}
