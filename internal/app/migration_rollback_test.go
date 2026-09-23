package app

import (
	"context"
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type rollbackSteps func(*v1alpha1.ClusterVolumeActivationStatus, kube.ProgressFunc) error

func (f rollbackSteps) RollbackPVC(
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

func TestMigrationRollbackRejectsUnsavedProgressAndRetries(t *testing.T) {
	writeErr := errors.New("checkpoint rejected")
	status := v1alpha1.ClusterVolumeActivationStatus{
		ActivePVC:   &v1alpha1.ObjectReference{Name: "data", UID: "destination-uid"},
		ActivatedAt: &metav1.Time{},
	}
	before := status.DeepCopy()
	attempts := 0
	steps := rollbackSteps(
		func(checkpoint *v1alpha1.ClusterVolumeActivationStatus, save kube.ProgressFunc) error {
			attempts++

			if !reflect.DeepEqual(checkpoint, before) {
				t.Fatalf("retry received uncommitted progress: %#v", checkpoint)
			}

			checkpoint.ActivePVC.UID = "restored-uid"
			checkpoint.RolledBackAt = &metav1.Time{}
			err := save()
			checkpoint.ActivePVC.UID = "mutated-after-save"

			return err
		},
	)

	var durable *v1alpha1.ClusterVolumeActivationStatus

	save := func(context.Context) error {
		if attempts == 1 {
			return writeErr
		}

		durable = status.DeepCopy()

		return nil
	}

	err := rollbackMigrationVolume(
		t.Context(),
		steps,
		"migration",
		"app",
		kube.PVCTransferBindings{},
		nil,
		&status,
		save,
	)
	if !errors.Is(err, writeErr) || !reflect.DeepEqual(&status, before) {
		t.Fatalf("failed checkpoint leaked: error=%v status=%#v", err, status)
	}

	if err := rollbackMigrationVolume(
		t.Context(),
		steps,
		"migration",
		"app",
		kube.PVCTransferBindings{},
		nil,
		&status,
		save,
	); err != nil {
		t.Fatal(err)
	}

	if attempts != 2 || durable == nil || durable.ActivePVC.UID != "restored-uid" ||
		status.ActivePVC.UID != "restored-uid" || status.RolledBackAt == nil {
		t.Fatalf(
			"rollback retry did not preserve saved identity: durable=%#v status=%#v",
			durable,
			status,
		)
	}
}
