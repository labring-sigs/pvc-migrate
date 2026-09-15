package planner

import (
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestStructuredVolumeOverridesPreserveDefaults(t *testing.T) {
	state := planState{
		pvcNames: []string{"data", "logs", "cache"}, destinationPVCs: make([]string, 3),
		requestedCapacities: make([]string, 3), transferScopes: make([]*v1alpha1.TransferScope, 3),
	}

	err := applyWorkflowVolumes(&state, []v1alpha1.VolumeRequest{
		{
			SourcePVC: v1alpha1.LocalResourceReference{Name: "logs"}, Capacity: "4Gi",
			DestinationPVC: &v1alpha1.LocalResourceReference{Name: "logs-saved"},
			TransferScope:  &v1alpha1.TransferScope{SourcePath: "current", DestinationPath: "."},
		},
	}, v1alpha1.TransferOptions{DestinationCapacity: "3Gi", DestinationPath: "restored"})
	if err != nil {
		t.Fatal(err)
	}

	if strings.Join(state.requestedCapacities, ",") != "3Gi,4Gi,3Gi" ||
		strings.Join(state.destinationPVCs, ",") != ",logs-saved," {
		t.Fatalf(
			"volume defaults lost: capacities=%v destinations=%v",
			state.requestedCapacities,
			state.destinationPVCs,
		)
	}

	for i, expected := range []v1alpha1.TransferScope{
		{SourcePath: ".", DestinationPath: "restored"},
		{SourcePath: "current", DestinationPath: "."},
		{SourcePath: ".", DestinationPath: "restored"},
	} {
		if state.transferScopes[i] == nil || *state.transferScopes[i] != expected {
			t.Fatalf("path defaults lost at %d: %+v", i, state.transferScopes[i])
		}
	}
}

func TestStructuredVolumeOverridesRejectInvalidInputs(t *testing.T) {
	for _, test := range []struct {
		name     string
		volumes  []v1alpha1.VolumeRequest
		defaults v1alpha1.TransferOptions
		message  string
	}{
		{name: "duplicate", volumes: []v1alpha1.VolumeRequest{
			{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
			{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
		}, message: "duplicate source PVC"},
		{name: "unknown", volumes: []v1alpha1.VolumeRequest{
			{SourcePVC: v1alpha1.LocalResourceReference{Name: "unknown"}},
		}, message: "not part of the selected workload"},
		{name: "unsafe override", volumes: []v1alpha1.VolumeRequest{
			{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}, TransferScope: &v1alpha1.TransferScope{SourcePath: "../secret"}},
		}, message: "invalid transfer paths"},
		{name: "unsafe default", defaults: v1alpha1.TransferOptions{DestinationPath: "/etc"}, message: "invalid transfer paths"},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := planState{
				pvcNames:        []string{"data"},
				destinationPVCs: make([]string, 1),
				requestedCapacities: make(
					[]string,
					1,
				),
				transferScopes: make([]*v1alpha1.TransferScope, 1),
			}

			err := applyWorkflowVolumes(&state, test.volumes, test.defaults)
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("error=%v, want %q", err, test.message)
			}
		})
	}
}

func TestStructuredCapacityRejectsCLIExpressions(t *testing.T) {
	for _, capacity := range []string{"data=3Gi", "0", "-1Gi"} {
		plan := planWithDestinationCapacity(t, capacity, false)
		if plan.Ready || !hasFailedCheck(
			plan.Checks, domain.CheckNameDestinationCapacity,
		) {
			t.Fatalf("accepted CRD capacity %q: %+v", capacity, plan.Checks)
		}
	}
}
