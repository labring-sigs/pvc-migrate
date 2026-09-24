package controller

import (
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestKubeBlocksGuidanceRendersPerAudience pins the dual rendering of
// field-spelling guidance: CLI planning quotes flags, controller planning
// records spec fields because its messages land verbatim on workflow CRs.
func TestKubeBlocksGuidanceRendersPerAudience(t *testing.T) {
	owner := &metav1.OwnerReference{Kind: domain.KindStatefulSet}

	cliErr := validateKubeBlocksSwitchoverCandidate(owner, "pod-1", domain.PresentationCLI)
	if cliErr == nil ||
		!strings.Contains(cliErr.Error(), "--switchover-candidate is supported only for") {
		t.Fatalf("CLI rendering must quote the flag: %v", cliErr)
	}

	controllerErr := validateKubeBlocksSwitchoverCandidate(
		owner,
		"pod-1",
		domain.PresentationController,
	)
	if controllerErr == nil ||
		!strings.Contains(controllerErr.Error(), "switchoverCandidate is supported only for") {
		t.Fatalf("controller rendering must use the spec field: %v", controllerErr)
	}

	if strings.Contains(controllerErr.Error(), "--") {
		t.Fatalf("controller rendering must not quote flags: %v", controllerErr)
	}
}
