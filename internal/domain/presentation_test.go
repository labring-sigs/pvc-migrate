package domain_test

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

func TestPresentationFieldRef(t *testing.T) {
	cases := []struct {
		field string
		cli   string
		crd   string
	}{
		{"allowLeaderDowntime", "--allow-leader-downtime", "allowLeaderDowntime"},
		{"switchoverCandidate", "--switchover-candidate", "switchoverCandidate"},
		{"skipSourceUsageCheck", "--skip-source-usage-check", "skipSourceUsageCheck"},
		{"forceReprovision", "--force-reprovision", "forceReprovision"},
		{"online", "--online", "online"},
	}

	for _, testCase := range cases {
		if got := domain.PresentationCLI.FieldRef(testCase.field); got != testCase.cli {
			t.Fatalf("CLI FieldRef(%s) = %q want %q", testCase.field, got, testCase.cli)
		}

		if got := domain.PresentationController.FieldRef(testCase.field); got != testCase.crd {
			t.Fatalf("controller FieldRef(%s) = %q want %q", testCase.field, got, testCase.crd)
		}
	}
}

func TestPresentationFieldUse(t *testing.T) {
	if got := domain.PresentationCLI.FieldUse(
		"allowLeaderDowntime",
	); got != "use --allow-leader-downtime" {
		t.Fatalf("CLI FieldUse = %q", got)
	}

	if got := domain.PresentationController.FieldUse(
		"allowLeaderDowntime",
	); got != "set allowLeaderDowntime" {
		t.Fatalf("controller FieldUse = %q", got)
	}
}
