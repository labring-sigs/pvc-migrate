package app

import (
	"strconv"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestNamespacedPodMigrationRunHonorsPrecopyPasses(t *testing.T) {
	for _, passes := range []int{0, 1, 3} {
		t.Run(strconv.Itoa(passes), func(t *testing.T) {
			executor, object, _, _ := namespacedPodMigrationFixture(
				t,
				func(object *v1alpha1.PodMigration) {
					object.Spec.PrecopyPasses = passes
					object.Status.Plan.PrecopyPasses = passes
				},
			)
			engine := &concreteCopyEngine{}
			executor.transfer.copier = engine
			executor.transfer.config.Retries = 1
			executor.workloads = &fakeController{}
			executor.switcher = &scriptedSwitcher{client: executor.client}
			installPodWarmSourcePVs(t, executor.client, object.Status.Plan.Volumes)

			if err := executor.Run(t.Context(), object); err != nil {
				t.Fatal(err)
			}

			warm, final := 0, 0
			for _, request := range engine.requests {
				switch request.Mode {
				case copyengine.ModeWarm:
					warm++
				case copyengine.ModeFinal:
					final++
				default:
					t.Fatalf("unexpected mode %s", request.Mode)
				}
			}

			if warm != passes*2 || final != 2 || object.Status.Phase != domain.PhaseCompleted ||
				object.Status.WarmPassesCompleted != passes {
				t.Fatalf(
					"warm=%d final=%d phase=%s passes=%d",
					warm,
					final,
					object.Status.Phase,
					object.Status.WarmPassesCompleted,
				)
			}

			count := len(engine.requests)

			if err := executor.Run(t.Context(), object); err != nil {
				t.Fatal(err)
			}

			if len(engine.requests) != count {
				t.Fatal("completed run repeated transfers")
			}
		})
	}
}
