package cli

import (
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

// workflowSource pins the persistence backend a lifecycle command addresses.
// Session commands only ever touch ConfigMap session records; cr commands only
// ever touch workflow CRs. The modes never probe each other's storage.
type workflowSource int

const (
	// sourceSession addresses ConfigMap-backed session records created by the
	// in-process run commands.
	sourceSession workflowSource = iota
	// sourceController addresses namespaced workflow CRs in the tenant
	// namespace selected by -n.
	sourceController
	// sourceClusterController addresses cluster-scoped workflow CRs
	// (ClusterMigration, ClusterCopy, ClusterReservation, Move).
	sourceClusterController
)

// isControllerCommand reports whether a command belongs to the cr group, so
// shared guidance helpers can render controller-shaped follow-ups.
func isControllerCommand(command *cobra.Command) bool {
	for current := command; current != nil; current = current.Parent() {
		if current.Name() == "cr" && current != command.Root() {
			return true
		}
	}

	return false
}

// crNamespaceForCommand resolves the tenant namespace a cr command addresses.
// Namespaced workflow CRs live in the tenant namespace, so cr commands take an
// explicit -n/--namespace flag instead of probing.
func crNamespaceForCommand(command *cobra.Command) string {
	if command == nil {
		return ""
	}

	value, err := command.Flags().GetString("namespace")
	if err != nil {
		return ""
	}

	return strings.TrimSpace(value)
}

// bindCRNamespace registers the addressing flag shared by every cr subcommand.
// Cluster-scoped kinds ignore it; namespaced kinds require it.
func bindCRNamespace(command *cobra.Command, namespace *string) {
	command.Flags().
		StringVarP(namespace, "namespace", "n", "", "Tenant namespace of the workflow CR")
}

// workflowArgLabel names the positional workflow argument per mode: session
// commands address a session ID, cr commands address a CR name.
func workflowArgLabel(source workflowSource) string {
	if source != sourceSession {
		return "NAME"
	}

	return "SESSION"
}

// workflowCommandPath renders the command path a hint should print for one
// workflow family: plain under the session root, "cr "-prefixed inside the cr
// group.
func workflowCommandPath(cmd *cobra.Command, family string) string {
	if isControllerCommand(cmd) {
		return "cr " + family
	}

	return family
}

// workflowHintAddress renders the workflow argument a suggested command
// carries: session mode quotes the session ID, controller mode appends the
// -n tenant namespace that namespaced CR addressing requires.
func workflowHintAddress(cmd *cobra.Command, namespace, name string) string {
	address := shellQuote(name)
	if isControllerCommand(cmd) && namespace != "" {
		address += " -n " + namespace
	}

	return address
}

// workflowScopeName renders "<namespace>/<name>" for namespaced workflows and
// the bare name for cluster-scoped ones.
func workflowScopeName(namespace, name string) string {
	if namespace != "" {
		return namespace + "/" + name
	}

	return name
}

// crFamilyPathForResource maps a workflow CRD resource name to its cr command
// family path. Fully-qualified resource names are normalized first.
func crFamilyPathForResource(resource string) string {
	if group, _, found := strings.Cut(resource, "."); found {
		resource = group
	}

	switch resource {
	case domain.PodMigrationResource:
		return "cr migrate-pod"
	case domain.MigrationResource:
		return "cr migrate"
	case domain.ClusterMigrationResource:
		return "cr cluster-migrate"
	case domain.CopyResource:
		return "cr copy"
	case domain.ClusterCopyResource:
		return "cr cluster-copy"
	case domain.ReservationResource:
		return "cr reserve"
	case domain.ClusterReservationResource:
		return "cr cluster-reserve"
	case domain.RenameResource:
		return "cr rename"
	case domain.MoveResource:
		return "cr move"
	case domain.BackupResource:
		return "cr backup"
	case domain.RestoreResource:
		return "cr restore"
	default:
		return "cr"
	}
}
