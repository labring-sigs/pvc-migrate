package cli_test

import (
	"strings"
	"testing"

	. "github.com/labring-sigs/pvc-migrate/internal/cli"
)

func TestCrossClusterCleanupGuidanceIsExecutable(t *testing.T) {
	command := CrossClusterCleanupGuidanceForTest(CrossClusterFlagsForTest{
		SourceKubeconfig: "/tmp/source config", SourceContext: "source",
		DestinationKubeconfig: "/tmp/destination config", DestinationContext: "destination",
		SessionNamespace: "migration-control",
	}, "cross-test")
	for _, required := range []string{
		"pvc-migrate session cleanup cross-cluster cross-test",
		"--source-kubeconfig '/tmp/source config'",
		"--source-context source",
		"--destination-kubeconfig '/tmp/destination config'",
		"--destination-context destination",
		"--session-namespace migration-control",
		"--delete-destination --delete-session --yes --dry-run=false",
	} {
		if !strings.Contains(command, required) {
			t.Fatalf("cleanup guidance %q does not contain %q", command, required)
		}
	}
}
