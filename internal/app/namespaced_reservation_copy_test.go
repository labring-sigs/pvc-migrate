package app

import (
	"context"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestNamespacedReservedCopyPreservesStorageAndOwnsItsInput(t *testing.T) {
	executor, reservation, _ := namespacedReservationFixture(t, interceptor.Funcs{})
	if err := executor.Run(t.Context(), reservation); err != nil {
		t.Fatal(err)
	}

	before := reservation.DeepCopy()
	spec := NamespacedCopySpecFromReservation(reservation.Spec)
	spec.Online = true
	spec.VerifyChecksum = true
	spec.DeleteExtraneous = true
	spec.SourceNode = "copy-source"
	spec.Strategies = []string{domain.StrategyClusterIP}
	spec.UnusedStoragePolicy = "Delete"

	object, err := NamespacedCopyFromReservation(reservation, spec)
	if err != nil {
		t.Fatal(err)
	}

	if object.UID != "" || object.ResourceVersion != "" || object.Name != reservation.Name ||
		object.Status.Phase != domain.PhaseReserved || !object.Status.Plan.Online ||
		!object.Status.Plan.VerifyChecksum || !object.Status.Plan.DeleteExtraneous ||
		object.Status.Plan.SourceNode != "copy-source" || object.Status.Plan.UnusedStoragePolicy != "Delete" {
		t.Fatalf("unexpected copy: %+v", object)
	}

	if !reflect.DeepEqual(object.Status.Plan.Volumes, reservation.Status.Plan.Volumes) {
		t.Fatal("handoff changed reserved storage")
	}

	for i, checkpoint := range reservation.Status.Volumes {
		if !reflect.DeepEqual(
			object.Status.Volumes[i].VolumeReservationStatus,
			checkpoint.VolumeReservationStatus,
		) {
			t.Fatal("handoff lost a reservation checkpoint")
		}
	}

	object.Spec.Volumes[0].SourcePVC.Name = "changed"
	object.Status.Plan.Volumes[0].SourcePVC.Name = "changed"
	object.Status.Volumes[0].DestinationPVC.Name = "changed"

	object.Status.Plan.Strategies[0] = "changed"
	if !reflect.DeepEqual(reservation, before) || spec.Volumes[0].SourcePVC.Name == "changed" ||
		spec.Strategies[0] == "changed" {
		t.Fatal("copy aliases reservation or supplied input")
	}
}

func TestNamespacedReservedCopyRejectsStorageRetargeting(t *testing.T) {
	executor, reservation, _ := namespacedReservationFixture(t, interceptor.Funcs{})
	if err := executor.Run(t.Context(), reservation); err != nil {
		t.Fatal(err)
	}

	for _, change := range []func(*v1alpha1.CopySpec){
		func(spec *v1alpha1.CopySpec) { spec.TargetNode = "other-node" },
		func(spec *v1alpha1.CopySpec) { spec.DestinationCapacity = "100Gi" },
		func(spec *v1alpha1.CopySpec) { spec.SourcePath = "other-path" },
		func(spec *v1alpha1.CopySpec) { spec.Volumes[0].SourcePVC.Name = "other-volume" },
	} {
		spec := NamespacedCopySpecFromReservation(reservation.Spec)
		change(&spec)

		if _, err := NamespacedCopyFromReservation(
			reservation,
			spec,
		); domain.CategoryOf(
			err,
		) != domain.ErrorPrecondition {
			t.Fatalf("retargeting error = %v", err)
		}
	}
}

func TestNamespacedReservedCopyRejectsUnfinishedReservation(t *testing.T) {
	_, reservation, _ := namespacedReservationFixture(t, interceptor.Funcs{})
	if _, err := NamespacedCopyFromReservation(
		reservation,
		NamespacedCopySpecFromReservation(reservation.Spec),
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("error = %v", err)
	}
}

func TestNamespacedReservedCopyAdoptionChecksConsumersBeforeStorageCommit(t *testing.T) {
	reserver, reservation, _ := namespacedReservationFixture(t, interceptor.Funcs{})

	sourceStore := reserver.store
	if err := reserver.Run(t.Context(), reservation); err != nil {
		t.Fatal(err)
	}

	executor, _, engine := namespacedCopyFixture(t, interceptor.Funcs{})

	_, err := executor.client.CoreV1().Pods("data").Create(t.Context(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "data", Name: "consumer"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
			Name: "data", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "a"},
			},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	commits := 0
	handoff := func(_ context.Context, _ *v1alpha1.Reservation, object *v1alpha1.Copy) error {
		commits++
		object.UID = "copy-record"
		object.ResourceVersion = "2"
		return nil
	}

	spec := NamespacedCopySpecFromReservation(reservation.Spec)
	if _, err := executor.AdoptReservation(
		t.Context(),
		sourceStore,
		reservation,
		spec,
		handoff,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("error = %v", err)
	}

	if commits != 0 {
		t.Fatal("offline consumer precondition committed the handoff")
	}

	if err := executor.client.CoreV1().
		Pods("data").
		Delete(t.Context(), "consumer", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	object, err := executor.AdoptReservation(t.Context(), sourceStore, reservation, spec, handoff)
	if err != nil {
		t.Fatal(err)
	}

	if commits != 1 || object.UID != "copy-record" || len(engine.requests) != 0 {
		t.Fatal("handoff failed to persist the new identity or started transfer prematurely")
	}

	reservation.ResourceVersion = "stale"
	if _, err := executor.AdoptReservation(
		t.Context(),
		sourceStore,
		reservation,
		spec,
		handoff,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("stale reservation error = %v", err)
	}

	if commits != 1 {
		t.Fatal("stale reservation committed another handoff")
	}
}
