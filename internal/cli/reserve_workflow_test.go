package cli

import "testing"

func TestReservationWorkflowPreservesFutureCopyMappings(t *testing.T) {
	flags := &reserveFlags{
		sessionID:             "reserve",
		sourceNamespace:       "app",
		destinationNamespace:  "archive",
		podName:               "database",
		destinationCapacities: []string{"logs=3Gi"},
		destinationPVCs:       []string{"logs=saved"},
		sourcePaths:           []string{"logs=current"},
		strategies:            []string{"mount"},
		verifyChecksum:        true,
	}

	object, err := flags.workflow(&rootState{}, &commandRuntime{}, false)
	if err != nil {
		t.Fatal(err)
	}

	if object.Spec.Pod == nil || object.Spec.Pod.Name != "database" ||
		len(object.Spec.Volumes) != 1 {
		t.Fatalf("lost Pod volume selection: %+v", object.Spec)
	}

	volume := object.Spec.Volumes[0]
	if volume.SourcePVC.Name != "logs" || volume.DestinationPVC == nil ||
		volume.DestinationPVC.Name != "saved" || volume.Capacity != "3Gi" {
		t.Fatalf("lost reservation volume mapping: %+v", volume)
	}

	if volume.TransferScope == nil || volume.TransferScope.SourcePath != "current" ||
		volume.TransferScope.DestinationPath != "." || !object.Spec.VerifyChecksum {
		t.Fatalf("lost future copy settings: %+v", object.Spec)
	}

	flags.strategies[0] = "changed"

	if object.Spec.Strategies[0] != "mount" {
		t.Fatal("CRD input aliases flags")
	}
}
