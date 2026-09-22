package crosscluster_test

import (
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	. "github.com/labring-sigs/pvc-migrate/internal/crosscluster"
	"k8s.io/client-go/kubernetes/fake"
)

func reservationOptionsFromCopy(options CopyOptions) ReservationOptions {
	return ReservationOptions{
		UnusedStoragePolicy:     options.UnusedStoragePolicy,
		SessionID:               options.SessionID,
		SessionNamespace:        options.SessionNamespace,
		SourceNamespace:         options.SourceNamespace,
		DestinationNamespace:    options.DestinationNamespace,
		SourcePVCs:              options.SourcePVCs,
		DestinationPVCs:         options.DestinationPVCs,
		DestinationCapacities:   options.DestinationCapacities,
		SourcePaths:             options.SourcePaths,
		DestinationPaths:        options.DestinationPaths,
		DestinationStorageClass: options.DestinationStorageClass,
		AllowVolumeShrink:       options.AllowVolumeShrink,
		SkipSourceUsageCheck:    options.SkipSourceUsageCheck,
		TargetNode:              options.TargetNode,
		ToolImage:               options.ToolImage,
		Strategies:              options.Strategies,
	}
}

func reservationFixture(
	t *testing.T,
) (*Service, CopyOptions, *ReservationSession) {
	t.Helper()

	service, options, _ := crossFixture()

	source, ok := service.SourceClientForTest().(*fake.Clientset)
	if !ok {
		t.Fatalf("source client type=%T", service.SourceClientForTest())
	}

	assignConfigMapResourceVersions(source)

	reservationOptions := reservationOptionsFromCopy(options)

	plan, err := service.PlanReservation(t.Context(), reservationOptions)
	if err != nil || !plan.Ready {
		t.Fatalf("reservation plan ready=%v err=%v checks=%#v", plan.Ready, err, plan.Checks)
	}

	reservation, err := service.CreateReservationSession(t.Context(), reservationOptions, plan)
	if err != nil {
		t.Fatal(err)
	}

	return service, options, reservation
}

// TestPlanReservationBindsKindAndEveryPlanningInput pins the reservation plan
// kind and every planning input the session creation guard compares.
func TestPlanReservationBindsKindAndEveryPlanningInput(t *testing.T) {
	service, options, _ := crossFixture()
	reservationOptions := reservationOptionsFromCopy(options)

	plan, err := service.PlanReservation(t.Context(), reservationOptions)
	if err != nil || !plan.Ready {
		t.Fatalf("reservation plan ready=%v err=%v checks=%#v", plan.Ready, err, plan.Checks)
	}

	if plan.Kind != ReservationKind {
		t.Fatalf("reservation plan kind=%q, want %q", plan.Kind, ReservationKind)
	}

	// A reservation plan must not inherit copy-only transfer controls.
	if plan.Online || plan.VerifyChecksum || plan.DeleteExtraneous {
		t.Fatalf("reservation plan carries copy-only controls: %+v", plan)
	}

	cases := []struct {
		name   string
		mutate func(*ReservationOptions)
	}{
		{
			name:   "session id",
			mutate: func(value *ReservationOptions) { value.SessionID = "another-session" },
		},
		{
			name:   "session namespace",
			mutate: func(value *ReservationOptions) { value.SessionNamespace = "another-namespace" },
		},
		{
			name:   "source namespace",
			mutate: func(value *ReservationOptions) { value.SourceNamespace = "another-source" },
		},
		{
			name: "destination namespace",
			mutate: func(value *ReservationOptions) {
				value.DestinationNamespace = "another-destination"
			},
		},
		{name: "unused storage policy", mutate: func(value *ReservationOptions) {
			value.UnusedStoragePolicy = v1alpha1.UnusedStorageDelete
		}},
		{name: "volume shrink", mutate: func(value *ReservationOptions) {
			value.AllowVolumeShrink = !value.AllowVolumeShrink
		}},
		{name: "source usage check", mutate: func(value *ReservationOptions) {
			value.SkipSourceUsageCheck = !value.SkipSourceUsageCheck
		}},
		{
			name:   "target node",
			mutate: func(value *ReservationOptions) { value.TargetNode = "destination-node" },
		},
		{
			name:   "tool image",
			mutate: func(value *ReservationOptions) { value.ToolImage = "example/tool:v2" },
		},
		{
			name:   "strategies",
			mutate: func(value *ReservationOptions) { value.Strategies = []string{"remote"} },
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			changed := reservationOptions
			testCase.mutate(&changed)

			if _, err := service.CreateReservationSession(t.Context(), changed, plan); err == nil ||
				!strings.Contains(err.Error(), "options changed after planning") {
				t.Fatalf("CreateReservationSession error=%v, want changed-options rejection", err)
			}
		})
	}
}

// TestSessionCreationRejectsCrossKindPlans pins that a reservation plan can
// never create a copy session and a copy plan can never create a reservation.
func TestSessionCreationRejectsCrossKindPlans(t *testing.T) {
	service, options, _ := crossFixture()
	reservationOptions := reservationOptionsFromCopy(options)

	copyPlan, err := service.PlanCopy(t.Context(), options)
	if err != nil || !copyPlan.Ready {
		t.Fatalf("copy plan ready=%v err=%v checks=%#v", copyPlan.Ready, err, copyPlan.Checks)
	}

	if _, err := service.CreateReservationSession(
		t.Context(), reservationOptions, copyPlan,
	); err == nil || !strings.Contains(err.Error(), "options changed after planning") {
		t.Fatalf("reservation creation accepted a copy plan: err=%v", err)
	}

	reservationPlan, err := service.PlanReservation(t.Context(), reservationOptions)
	if err != nil || !reservationPlan.Ready {
		t.Fatalf(
			"reservation plan ready=%v err=%v checks=%#v",
			reservationPlan.Ready,
			err,
			reservationPlan.Checks,
		)
	}

	if _, err := service.CreateCopySession(
		t.Context(), options, reservationPlan,
	); err == nil || !strings.Contains(err.Error(), "options changed after planning") {
		t.Fatalf("copy creation accepted a reservation plan: err=%v", err)
	}
}
