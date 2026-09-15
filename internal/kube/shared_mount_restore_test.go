package kube

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

type sharedMountRestoreManager struct {
	restore func(context.Context, string, v1alpha1.SharedMountStatus) error
}

func (m sharedMountRestoreManager) RestoreShared(
	ctx context.Context,
	id string,
	mount v1alpha1.SharedMountStatus,
) error {
	return m.restore(ctx, id, mount)
}

func TestRestoreSharedMountsCheckpointsOnlyFailedCompensations(t *testing.T) {
	mounts := []v1alpha1.SharedMountStatus{
		{SourcePV: v1alpha1.LocalResourceReference{Name: "first"}},
		{SourcePV: v1alpha1.LocalResourceReference{Name: "second"}},
		{SourcePV: v1alpha1.LocalResourceReference{Name: "third"}},
	}
	firstErr := errors.New("first mount unavailable")
	thirdErr := errors.New("third mount unavailable")

	var restored []string

	manager := sharedMountRestoreManager{
		restore: func(_ context.Context, id string, mount v1alpha1.SharedMountStatus) error {
			if id != "operation" {
				t.Fatalf("restore owner=%q", id)
			}

			restored = append(restored, mount.SourcePV.Name)
			switch mount.SourcePV.Name {
			case "first":
				return firstErr
			case "third":
				return thirdErr
			default:
				return nil
			}
		},
	}
	saves := 0

	err := RestoreSharedMounts(t.Context(), manager, "operation", &mounts,
		func(ctx context.Context) error {
			saves++

			if _, bounded := ctx.Deadline(); !bounded {
				t.Fatal("checkpoint has no deadline")
			}

			return nil
		}, nil,
	)
	if !errors.Is(err, firstErr) || !errors.Is(err, thirdErr) {
		t.Fatalf("missing compensation failures: %v", err)
	}

	if !slices.Equal(restored, []string{"third", "second", "first"}) || saves != 1 {
		t.Fatalf("compensation order=%v saves=%d", restored, saves)
	}

	if len(mounts) != 2 || mounts[0].SourcePV.Name != "first" ||
		mounts[1].SourcePV.Name != "third" {
		t.Fatalf("pending compensation order=%+v", mounts)
	}
}

func TestRestoreSharedMountsPreservesCheckpointOnInterruptedRecovery(t *testing.T) {
	for _, failure := range []string{"save", "cancel", "lease", "lease during save"} {
		t.Run(failure, func(t *testing.T) {
			mounts := []v1alpha1.SharedMountStatus{
				{SourcePV: v1alpha1.LocalResourceReference{Name: "first"}},
				{SourcePV: v1alpha1.LocalResourceReference{Name: "second"}},
			}
			previous := slices.Clone(mounts)
			failureErr := errors.New("recovery interrupted")
			fence := &testLeaseFence{}

			ctx, cancel := context.WithCancel(WithLeaseFence(t.Context(), fence))
			defer cancel()

			restores, saves := 0, 0
			manager := sharedMountRestoreManager{
				restore: func(context.Context, string, v1alpha1.SharedMountStatus) error {
					restores++

					switch failure {
					case "cancel":
						cancel()
					case "lease":
						fence.err = failureErr
					}

					return nil
				},
			}
			err := RestoreSharedMounts(ctx, manager, "operation", &mounts,
				func(context.Context) error {
					saves++

					if failure == "lease during save" {
						fence.err = failureErr
						return nil
					}

					return failureErr
				}, nil,
			)

			if failure == "cancel" {
				failureErr = context.Canceled
			}

			if !errors.Is(err, failureErr) || !reflect.DeepEqual(mounts, previous) {
				t.Fatalf("interrupted recovery lost checkpoint: mounts=%+v err=%v", mounts, err)
			}

			if failure == "cancel" || failure == "lease" {
				if restores != 1 || saves != 0 {
					t.Fatalf(
						"recovery advanced after interruption: restores=%d saves=%d",
						restores,
						saves,
					)
				}
			} else if restores != 2 || saves != 1 {
				t.Fatalf("checkpoint was not attempted: restores=%d saves=%d", restores, saves)
			}
		})
	}
}
