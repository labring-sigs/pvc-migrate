package cli

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

func TestCopyWorkflowRejectsAmbiguousMappings(t *testing.T) {
	for _, capacities := range [][]string{{"1Gi", "2Gi"}, {"1Gi", "data=2Gi"}, {"data=1Gi", "data=2Gi"}, {"data="}} {
		flags := &copyFlags{
			sessionID:             "copy",
			sourceNamespace:       "app",
			sourcePVCs:            []string{"data"},
			destinationCapacities: capacities,
		}

		_, err := flags.workflow(&rootState{}, &commandRuntime{}, false)
		if err == nil {
			t.Fatalf("accepted ambiguous mapping %v", capacities)
		}
	}
}

func TestCopyWorkflowOwnsStructuredInputs(t *testing.T) {
	flags := &copyFlags{
		sessionID: "copy", sourceNamespace: "app", destinationNamespace: "archive",
		sourcePVCs: []string{"data"}, destinationPVCs: []string{"data=saved"},
		destinationCapacities: []string{"data=3Gi"}, sourcePaths: []string{"data=logs"},
		strategies: []string{"mount"}, online: true,
	}

	object, err := flags.workflow(
		&rootState{global: globals{sessionNamespace: "sessions"}},
		&commandRuntime{},
		false,
	)
	if err != nil {
		t.Fatal(err)
	}

	volume := object.Spec.Volumes[0]
	if volume.SourcePVC.Name != "data" || volume.DestinationPVC.Name != "saved" ||
		volume.Capacity != "3Gi" ||
		volume.TransferScope.SourcePath != "logs" ||
		volume.TransferScope.DestinationPath != "." {
		t.Fatalf("mapping lost: %+v", volume)
	}

	flags.sourcePVCs[0], flags.strategies[0] = "changed", "changed"

	if object.Spec.Volumes[0].SourcePVC.Name != "data" || object.Spec.Strategies[0] != "mount" {
		t.Fatal("CRD input aliases CLI flag slices")
	}
}

func TestCopyPodMappingsBecomePartialVolumeOverrides(t *testing.T) {
	flags := &copyFlags{
		sessionID: "copy", sourceNamespace: "app", podName: "database",
		destinationCapacities: []string{"logs=3Gi"}, destinationPaths: []string{"logs=saved"},
	}

	object, err := flags.workflow(&rootState{}, &commandRuntime{}, false)
	if err != nil {
		t.Fatal(err)
	}

	if object.Spec.Pod == nil || object.Spec.Pod.Name != "database" ||
		len(object.Spec.Volumes) != 1 {
		t.Fatalf("Pod selector changed: %+v", object.Spec)
	}

	if object.Spec.Volumes[0].TransferScope == nil ||
		*object.Spec.Volumes[0].TransferScope != (v1alpha1.TransferScope{SourcePath: ".", DestinationPath: "saved"}) {
		t.Fatalf("partial paths changed: %+v", object.Spec.Volumes[0])
	}
}
