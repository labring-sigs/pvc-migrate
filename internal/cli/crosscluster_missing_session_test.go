package cli

import (
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/crosscluster"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

// An explicit --session must never fall back to planning a fresh session: a
// typo in a resume would provision new destination volumes under a name the
// caller believes is already tracked.
func TestCrossClusterRunRejectsMissingExplicitSession(t *testing.T) {
	clients := &kube.Clients{
		Kubernetes: kubernetesfake.NewSimpleClientset(
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "control"}},
		),
		RESTConfig: &rest.Config{Host: "https://source"},
	}
	service := crosscluster.NewService(clients, clients, nil)

	t.Run("copy", func(t *testing.T) {
		_, err := loadExistingCopySession(t.Context(), service, "control", "missing-id")
		if err == nil {
			t.Fatal("missing session fell through to fresh planning")
		}

		if domain.CategoryOf(err) != domain.ErrorValidation {
			t.Fatalf("category = %v, want validation: %v", domain.CategoryOf(err), err)
		}

		for _, want := range []string{"missing-id", "control", "omit --session"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error %q does not mention %q", err, want)
			}
		}
	})

	t.Run("reserve", func(t *testing.T) {
		_, err := loadExistingReservationSession(t.Context(), service, "control", "missing-id")
		if err == nil {
			t.Fatal("missing reservation session fell through to fresh planning")
		}

		if domain.CategoryOf(err) != domain.ErrorValidation {
			t.Fatalf("category = %v, want validation: %v", domain.CategoryOf(err), err)
		}

		if !strings.Contains(err.Error(), "omit --session") {
			t.Fatalf("error %q does not offer creating a new session", err)
		}
	})
}

// The run commands decide resume from whether the caller passed --session,
// not from the flag struct's value: options() writes the generated ID of a
// fresh session back into that struct, and treating a generated ID as a
// resume attempt breaks creation entirely.
func TestCrossClusterRunDistinguishesGeneratedFromExplicitSession(t *testing.T) {
	flags := &crossClusterCopyFlags{}
	command := newCrossClusterCopyRunCommandForTest(t, flags)

	// options() wrote back a generated session ID.
	flags.sessionID = "mig-generated"

	if command.Flags().Changed("session") {
		t.Fatal("generated session ID read as an explicit --session resume")
	}

	if err := command.Flags().Set("session", "explicit"); err != nil {
		t.Fatal(err)
	}

	if !command.Flags().Changed("session") {
		t.Fatal("explicit --session not detected")
	}
}

func newCrossClusterCopyRunCommandForTest(
	t *testing.T,
	flags *crossClusterCopyFlags,
) *cobra.Command {
	t.Helper()

	command := &cobra.Command{Use: "run-test"}
	flags.bindConnections(command, &rootState{})
	command.Flags().StringVar(&flags.sessionID, "session", "", "Cross-cluster session ID")

	return command
}
