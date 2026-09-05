package kube

import (
	"context"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestSessionRecordsFindsWorkflowDespiteMissingConfigMap(t *testing.T) {
	session := newControllerWaitTestSession(domain.PhaseReserving, "reserving")
	client := newControllerWaitDynamicClient(controllerWaitTestObject(t, session))
	records := NewSessionRecords(kubernetesfake.NewClientset(), client)

	got, err := records.Find(context.Background(), session.ID, "system", "app", "app")
	if err != nil || got == nil || got.Backend != SessionBackendCRD ||
		got.Status.Phase != domain.PhaseReserving {
		t.Fatalf("session=%#v error=%v", got, err)
	}
}

func TestSessionRecordsCannotProveAbsenceWithoutReadAccess(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	client.PrependReactor(
		"get",
		"migrations",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				schema.GroupResource{Group: MetadataDomain, Resource: "migrations"},
				"owned",
				nil,
			)
		},
	)
	records := NewSessionRecords(kubernetesfake.NewClientset(), client)

	got, err := records.Find(context.Background(), "owned", "app")
	if got != nil || err == nil || !strings.Contains(err.Error(), "forbidden") {
		t.Fatalf("session=%#v error=%v", got, err)
	}
}

func TestSessionRecordsAcceptsAbsentRecords(t *testing.T) {
	records := NewSessionRecords(
		kubernetesfake.NewClientset(),
		dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
	)

	got, err := records.Find(context.Background(), "absent", "app")
	if got != nil || err != nil {
		t.Fatalf("session=%#v error=%v", got, err)
	}
}
