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
