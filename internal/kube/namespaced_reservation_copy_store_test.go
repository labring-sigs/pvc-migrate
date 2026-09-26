package kube

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func namespacedConfigMapHandoffFixture(
	t *testing.T,
) (*fake.Clientset, *v1alpha1.Reservation, *v1alpha1.Copy) {
	t.Helper()

	source := &v1alpha1.Reservation{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.GroupVersion.String(),
			Kind:       "Reservation",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "data",
			Name:            "workflow",
			UID:             "storage-uid",
			ResourceVersion: "1",
		},
		Spec: v1alpha1.ReservationSpec{},
		Status: v1alpha1.ReservationStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: "Reserved"},
			Volumes: []v1alpha1.ReservationVolumeStatus{
				{
					SourcePVCName: "data",
					Reserved:      true,
					DestinationPVC: &v1alpha1.LocalResourceReference{
						Name: "data",
						UID:  "pvc",
					},
					DestinationPV: &v1alpha1.LocalResourceReference{Name: "reserved-pv", UID: "pv"},
				},
			},
		},
	}

	data, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}

	client := fake.NewClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			// The record lives in the session storage namespace while the
			// Reservation object carries its tenant namespace in metadata.
			Namespace: "sessions", Name: SessionConfigMapName(source.Name), UID: source.UID,
			ResourceVersion: source.ResourceVersion, Labels: sessionLabels(source.Name),
		},
		Data: map[string]string{SessionDataKey: string(data)},
	})
	destination := &v1alpha1.Copy{
		ObjectMeta: metav1.ObjectMeta{Namespace: source.Namespace, Name: source.Name},
		Spec:       v1alpha1.CopySpec{},
		Status: v1alpha1.CopyStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: "Reserved"},
			Volumes: []v1alpha1.CopyVolumeStatus{
				{
					VolumeReservationStatus: *source.Status.Volumes[0].VolumeReservationStatus.DeepCopy(),
				},
			},
		},
	}

	return client, source, destination
}

func TestNamespacedConfigMapReservationHandoffCommitsCompleteCopyAtomically(t *testing.T) {
	client, source, destination := namespacedConfigMapHandoffFixture(t)

	before := source.DeepCopy()
	if err := NamespacedHandoffConfigMapReservationToCopy(
		t.Context(),
		client,
		"sessions",
		source,
		destination,
	); err != nil {
		t.Fatal(err)
	}

	store, err := NewConfigMapWorkflowStore(
		client,
		"sessions",
		func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Load(
		t.Context(),
		crclient.ObjectKey{Namespace: source.Namespace, Name: source.Name},
	)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(destination, loaded) || destination.UID != source.UID ||
		destination.Status.ExecutionIntentHash == "" || !reflect.DeepEqual(source, before) {
		t.Fatal("handoff lost identity, progress or modified the source snapshot")
	}

	// The routing label must follow the graduated payload: the bare status
	// lists select records by workflow-kind, so a Copy record that still
	// carries the reservation label vanishes from every list.
	record, err := client.CoreV1().
		ConfigMaps("sessions").
		Get(t.Context(), SessionConfigMapName(source.Name), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if record.Labels[WorkflowKindLabel] != "Copy" {
		t.Fatalf("record workflow-kind label = %q, want Copy", record.Labels[WorkflowKindLabel])
	}

	listed, err := store.List(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}

	if len(listed) != 1 || listed[0].Name != source.Name {
		t.Fatalf("graduated record not listed by the copy store: %#v", listed)
	}

	writes := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "update" {
			writes++
		}

		if action.GetVerb() == "delete" || action.GetVerb() == "create" {
			t.Fatal("handoff used a non-atomic replacement")
		}
	}

	if writes != 1 {
		t.Fatalf("writes = %d", writes)
	}
}

func TestNamespacedConfigMapReservationHandoffRejectsLeaseLossAfterUpdate(t *testing.T) {
	client, source, destination := namespacedConfigMapHandoffFixture(t)
	lost := errors.New("lease lost after handoff update")
	fence := &testLeaseFence{}
	client.PrependReactor(
		"update",
		"configmaps",
		func(ktesting.Action) (bool, runtime.Object, error) {
			fence.err = lost
			return false, nil, nil
		},
	)

	err := NamespacedHandoffConfigMapReservationToCopy(
		WithLeaseFence(t.Context(), fence),
		client,
		"sessions",
		source,
		destination,
	)
	if !errors.Is(err, lost) {
		t.Fatalf("error = %v, want lease loss after update", err)
	}

	store, err := NewConfigMapWorkflowStore(
		client,
		"sessions",
		func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := store.Load(
		t.Context(),
		crclient.ObjectKey{Namespace: source.Namespace, Name: source.Name},
	); err != nil {
		t.Fatalf("successful update was not durable: %v", err)
	}
}

func TestNamespacedConfigMapReservationHandoffFailurePreservesBothInputs(t *testing.T) {
	for _, mode := range []string{"save", "fence", "stale"} {
		t.Run(mode, func(t *testing.T) {
			client, source, destination := namespacedConfigMapHandoffFixture(t)
			failure := errors.New("handoff unavailable")

			fence := &testLeaseFence{}
			switch mode {
			case "save":
				client.PrependReactor(
					"update",
					"configmaps",
					func(ktesting.Action) (bool, runtime.Object, error) {
						return true, nil, failure
					},
				)
			case "fence":
				client.PrependReactor(
					"get",
					"configmaps",
					func(ktesting.Action) (bool, runtime.Object, error) {
						fence.err = failure
						return false, nil, nil
					},
				)
			case "stale":
				source.ResourceVersion = "old"
			}

			beforeSource, beforeDestination := source.DeepCopy(), destination.DeepCopy()

			err := NamespacedHandoffConfigMapReservationToCopy(
				WithLeaseFence(t.Context(), fence),
				client,
				"sessions",
				source,
				destination,
			)
			if err == nil || (mode != "stale" && !errors.Is(err, failure)) {
				t.Fatalf("error = %v", err)
			}

			if !reflect.DeepEqual(source, beforeSource) ||
				!reflect.DeepEqual(destination, beforeDestination) {
				t.Fatal("failed handoff mutated its inputs")
			}

			store, err := NewConfigMapWorkflowStore(
				client,
				"sessions",
				func() *v1alpha1.Reservation { return &v1alpha1.Reservation{} },
			)
			if err != nil {
				t.Fatal(err)
			}

			if _, err := store.Load(
				t.Context(),
				crclient.ObjectKey{Namespace: source.Namespace, Name: source.Name},
			); err != nil {
				t.Fatalf("failed handoff lost the reservation: %v", err)
			}
		})
	}
}
