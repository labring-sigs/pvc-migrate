package v1alpha1_test

import (
	"encoding/json"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

func TestExplicitZeroPrecopySurvivesSerialization(t *testing.T) {
	for _, request := range []any{
		v1alpha1.PodMigrationSpec{Pod: v1alpha1.LocalResourceReference{Name: "database-0"}, PrecopyPasses: 0},
		v1alpha1.ClusterPodMigrationSpec{SourceNamespace: "app", PodMigrationSpec: v1alpha1.PodMigrationSpec{Pod: v1alpha1.LocalResourceReference{Name: "database-0"}, PrecopyPasses: 0}},
	} {
		encoded, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}

		var object map[string]any
		if err := json.Unmarshal(encoded, &object); err != nil {
			t.Fatal(err)
		}

		if object["precopyPasses"] != float64(0) {
			t.Fatalf("%T lost explicit zero: %s", request, encoded)
		}
	}
}

func TestTransferOptionsDeleteExtraneousFalseIsSerialized(t *testing.T) {
	encoded, err := json.Marshal(v1alpha1.TransferOptions{DeleteExtraneous: new(false)})
	if err != nil {
		t.Fatal(err)
	}

	var object map[string]any
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatal(err)
	}

	value, ok := object["deleteExtraneous"]
	if !ok {
		t.Fatal("deleteExtraneous was omitted from typed request")
	}

	if value != false {
		t.Fatalf("deleteExtraneous = %#v, want false", value)
	}
}

func TestTransferOptionsDeleteExtraneousOmissionKeepsAPIDefault(t *testing.T) {
	encoded, err := json.Marshal(v1alpha1.TransferOptions{})
	if err != nil {
		t.Fatal(err)
	}

	var object map[string]any
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatal(err)
	}

	if _, ok := object["deleteExtraneous"]; ok {
		t.Fatalf("omitted deleteExtraneous was serialized: %s", encoded)
	}

	if got := (v1alpha1.TransferOptions{}).DeleteExtraneousValue(); !got {
		t.Fatal("omitted deleteExtraneous did not resolve to the API default")
	}
}

func TestTransferOptionsDeepCopyPreservesOptionalDeleteExtraneous(t *testing.T) {
	original := v1alpha1.TransferOptions{DeleteExtraneous: new(false)}

	clone := original.DeepCopy()
	if clone == nil || clone.DeleteExtraneous == nil || *clone.DeleteExtraneous {
		t.Fatalf("deep copy lost explicit false: %#v", clone)
	}

	*clone.DeleteExtraneous = true
	if *original.DeleteExtraneous {
		t.Fatal("deep copy aliased optional bool")
	}
}
