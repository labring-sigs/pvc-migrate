package planner

import (
	"reflect"
	"testing"
)

func TestTransferPlannerOptionsStayOperationSpecific(t *testing.T) {
	if _, exists := reflect.TypeOf(ReserveOptions{}).FieldByName("Online"); exists {
		t.Fatal("reserve options expose copy-only Online")
	}
	if _, exists := reflect.TypeOf(ReserveOptions{}).FieldByName("PrecopyPasses"); exists {
		t.Fatal("reserve options expose pod-migration PrecopyPasses")
	}
	if _, exists := reflect.TypeOf(CopyOptions{}).FieldByName("SwitchoverCandidate"); exists {
		t.Fatal("copy options expose workload cutover SwitchoverCandidate")
	}
	if _, exists := reflect.TypeOf(CopyOptions{}).FieldByName("AllowLeaderDowntime"); exists {
		t.Fatal("copy options expose workload cutover AllowLeaderDowntime")
	}
}
