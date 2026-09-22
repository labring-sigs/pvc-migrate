package app

import (
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
)

func TestReservationAbortPreservesStorageAndRecoversFailedCheckpoint(t *testing.T) {
	executor, object, store, reserver := reservationExecutorFixture(t)
	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	before := object.DeepCopy()
	// Aborting must not depend on data-plane availability or reprovision storage.
	executor.client = nil
	reserver.validate = func(*corev1.PersistentVolumeClaim, v1alpha1.ClusterVolumeReservationStatus) error {
		return errors.New("abort must not validate provisioning")
	}
	writeErr := errors.New("status write failed")
	store.err, store.failAt = writeErr, store.writes+2

	if err := executor.Abort(t.Context(), object); !errors.Is(err, writeErr) {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseAborting ||
		object.Status.ResumeFrom != domain.PhaseReserved ||
		!reflect.DeepEqual(object, store.object) {
		t.Fatalf("lost abort recovery state: %+v", object.Status)
	}

	store.failAt = 0

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseAborted || object.Status.CompletedAt == nil ||
		!reflect.DeepEqual(object.Status.Volumes, before.Status.Volumes) ||
		!reflect.DeepEqual(object.Status.Plan, before.Status.Plan) ||
		!reflect.DeepEqual(object.Spec, before.Spec) || len(reserver.calls) != 2 {
		t.Fatalf("abort changed reserved storage: %+v", object)
	}

	writes := store.writes

	if err := executor.Abort(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.writes != writes {
		t.Fatal("terminal abort rewrote state")
	}
}

func TestReservationAbortBeforePlanning(t *testing.T) {
	for _, phase := range []v1alpha1.WorkflowPhase{"", domain.PhasePlanned, domain.PhaseFailed} {
		t.Run(string(phase), func(t *testing.T) {
			executor, object, store, reserver := reservationExecutorFixture(t)
			object.Status.Plan = nil
			object.Status.Phase = phase

			if phase == domain.PhaseFailed {
				object.Status.ResumeFrom = domain.PhasePlanned
			}

			store.object = object.DeepCopy()
			if err := executor.Abort(t.Context(), object); err != nil {
				t.Fatal(err)
			}

			if object.Status.Phase != domain.PhaseAborted || object.Status.Plan != nil ||
				len(reserver.calls) != 0 || store.writes != 1 {
				t.Fatalf("unplanned abort touched execution: %+v", object)
			}

			if err := validateClusterReservationObject(object); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReservationResumePlanningFailureIsTransactional(t *testing.T) {
	executor, object, store, reserver := reservationExecutorFixture(t)
	object.Status.Plan = nil
	object.Status.Phase = domain.PhaseFailed
	object.Status.ResumeFrom = domain.PhasePlanned
	object.Status.Message = "planning failed"
	object.Status.ErrorCategory = string(domain.ErrorPrecondition)
	store.object = object.DeepCopy()
	before := object.DeepCopy()
	writeErr := errors.New("status unavailable")
	store.err, store.failAt = writeErr, 1

	if err := executor.RequestResume(t.Context(), object); !errors.Is(err, writeErr) {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(object, before) || !reflect.DeepEqual(store.object, before) {
		t.Fatal("failed resume write changed retry state")
	}

	store.failAt = 0

	if err := executor.RequestResume(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhasePlanned || object.Status.Plan != nil ||
		object.Status.ErrorCategory != "" ||
		len(reserver.calls) != 0 ||
		!reflect.DeepEqual(object, store.object) {
		t.Fatalf("planning retry created execution state: %+v", object)
	}
}

func TestReservationResumeExecutionFailureKeepsPartialReferences(t *testing.T) {
	executor, object, store, reserver := reservationExecutorFixture(t)
	provisionErr := errors.New("provisioning interrupted")
	reserver.reserve = func(_ string, _ *v1alpha1.ClusterVolumeReservationStatus) error {
		return provisionErr
	}

	if err := executor.Run(t.Context(), object); !errors.Is(err, provisionErr) {
		t.Fatal(err)
	}

	before := object.DeepCopy()
	if err := executor.RequestResume(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseReserving ||
		!reflect.DeepEqual(object.Status.Volumes, before.Status.Volumes) ||
		!reflect.DeepEqual(object.Status.Plan, before.Status.Plan) ||
		!reflect.DeepEqual(object.Spec, before.Spec) || len(reserver.calls) != 1 ||
		!reflect.DeepEqual(store.object, object) {
		t.Fatalf("resume changed partial reservation: %+v", object)
	}
}

func TestReservationLifecycleRejectsOtherOperationResumeCheckpoint(t *testing.T) {
	executor, object, store, _ := reservationExecutorFixture(t)
	object.Status.Phase = domain.PhaseFailed
	object.Status.ResumeFrom = domain.PhaseActivating

	if err := executor.Abort(
		t.Context(),
		object,
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("foreign abort accepted: %v", err)
	}

	if err := executor.RequestResume(
		t.Context(),
		object,
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("foreign resume accepted: %v", err)
	}

	if store.writes != 0 {
		t.Fatal("foreign lifecycle wrote state")
	}
}

func TestReservationToolFailureDoesNotAcquireCopyFailurePolicy(t *testing.T) {
	executor, object, _, reserver := reservationExecutorFixture(t)
	reserver.reserve = func(_ string, _ *v1alpha1.ClusterVolumeReservationStatus) error {
		return kube.ErrToolPodNoSpace
	}

	if err := executor.Run(t.Context(), object); !errors.Is(err, kube.ErrToolPodNoSpace) {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseFailed || object.Status.FailureReason != "" {
		t.Fatalf("reservation inherited data-copy capacity policy: %+v", object.Status)
	}

	if err := executor.RequestResume(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Abort(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseAborted {
		t.Fatalf("tool failure prevented abort: %+v", object.Status)
	}
}
