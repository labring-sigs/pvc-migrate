package app

import (
	"context"
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
)

type activationSteps func(*v1alpha1.ClusterVolumeActivationStatus, kube.ProgressFunc) error

func (f activationSteps) ActivatePVC(
	_ context.Context,
	_ string,
	_ string,
	_ kube.PVCTransferBindings,
	_ *corev1.PersistentVolumeClaim,
	status *v1alpha1.ClusterVolumeActivationStatus,
	save kube.ProgressFunc,
) error {
	return f(status, save)
}

func TestMigrationActivationRetainsOnlySavedCheckpoints(t *testing.T) {
	writeErr := errors.New("checkpoint rejected")
	status := v1alpha1.ClusterVolumeActivationStatus{}
	writes := 0

	var durable v1alpha1.ClusterVolumeActivationStatus

	steps := activationSteps(
		func(checkpoint *v1alpha1.ClusterVolumeActivationStatus, save kube.ProgressFunc) error {
			checkpoint.TemporaryPVCDeleted = true

			if err := save(); err != nil {
				return err
			}

			checkpoint.SourcePVCDeleted = true
			checkpoint.ActivePVC = &v1alpha1.ObjectReference{
				Name: "replacement",
				UID:  "replacement-uid",
			}
			err := save()
			// A resource implementation retaining its working checkpoint must not
			// mutate the object that the operation will persist on failure.
			checkpoint.ActivePVC.Name = "changed-after-save"

			return err
		},
	)

	err := activateMigrationVolume(t.Context(), nil, steps, "migration", "app",
		kube.PVCTransferBindings{}, nil, &status, func(context.Context) error {
			writes++
			if writes == 2 {
				return writeErr
			}

			durable = *status.DeepCopy()

			return nil
		})
	if !errors.Is(err, writeErr) || writes != 2 {
		t.Fatalf("error=%v writes=%d", err, writes)
	}

	if !status.TemporaryPVCDeleted || status.SourcePVCDeleted || status.ActivePVC != nil {
		t.Fatalf("failed checkpoint leaked into workflow: %#v", status)
	}

	if !durable.TemporaryPVCDeleted || durable.SourcePVCDeleted || durable.ActivePVC != nil {
		t.Fatalf("unexpected durable checkpoint: %#v", durable)
	}
}
