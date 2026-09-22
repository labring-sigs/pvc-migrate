package planner

import (
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

func TestTransferPlannerOptionsStayOperationSpecific(t *testing.T) {
	if _, exists := reflect.TypeFor[v1alpha1.ReservationSpec]().FieldByName("Online"); exists {
		t.Fatal("reserve options expose copy-only Online")
	}

	if _, exists := reflect.TypeFor[v1alpha1.ReservationSpec]().FieldByName("PrecopyPasses"); exists {
		t.Fatal("reserve options expose pod-migration PrecopyPasses")
	}

	if _, exists := reflect.TypeFor[v1alpha1.CopySpec]().FieldByName("SwitchoverCandidate"); exists {
		t.Fatal("copy options expose workload cutover SwitchoverCandidate")
	}

	if _, exists := reflect.TypeFor[v1alpha1.CopySpec]().FieldByName("AllowLeaderDowntime"); exists {
		t.Fatal("copy options expose workload cutover AllowLeaderDowntime")
	}
}
