package cli

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// TestControllerPlanNamespacesCollapsesOnlySingleTenantSubmissions pins the
// guard contract: controller submissions collapse the session and temporary
// roles into the source namespace only when the destination stays inside the
// same tenant. Cross-namespace submissions must keep the configured session
// namespace, and session runs never collapse.
func TestControllerPlanNamespacesCollapsesOnlySingleTenantSubmissions(t *testing.T) {
	runtime := &commandRuntime{
		controllerKinds: []domain.ControllerKind{domain.ControllerKindMigration},
	}
	state := &rootState{global: globals{sessionNamespace: "pvc-migrate-system"}}

	session, temporary := state.controllerPlanNamespaces(
		runtime,
		domain.SessionTypeMigrate,
		"app", "app", "pvc-migrate-system", false, true,
	)
	if session != "app" || temporary != "app" {
		t.Fatalf("single-tenant submission = %q/%q, want collapsed into app", session, temporary)
	}

	session, temporary = state.controllerPlanNamespaces(
		runtime,
		domain.SessionTypeMigrate,
		"app", "other", "pvc-migrate-system", false, true,
	)
	if session != "pvc-migrate-system" || temporary != "pvc-migrate-system" {
		t.Fatalf(
			"cross-namespace submission = %q/%q, want the configured session and temporary roles",
			session,
			temporary,
		)
	}

	session, temporary = state.controllerPlanNamespaces(
		runtime,
		domain.SessionTypeMigrate,
		"app", "app", "staging", true, true,
	)
	if session != "pvc-migrate-system" || temporary != "staging" {
		t.Fatalf("explicit temporary role = %q/%q, want the typed staging role", session, temporary)
	}

	session, temporary = state.controllerPlanNamespaces(
		runtime,
		domain.SessionTypeMigrate,
		"app", "app", "pvc-migrate-system", false, false,
	)
	if session != "pvc-migrate-system" || temporary != "pvc-migrate-system" {
		t.Fatalf("session run = %q/%q, want the typed roles untouched", session, temporary)
	}
}
