package planner

import (
	"context"
	"slices"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// CopyOptions describes a finite PVC copy. Workload cutover fields belong to
// PodMigrationOptions and are intentionally absent here.
type CopyOptions struct {
	SessionID             string
	SourceNamespace       string
	TemporaryNamespace    string
	DestinationNamespace  string
	SessionNamespace      string
	StagingNamespace      string
	ToolImage             string
	CapacityAwareness     domain.CapacityAwareness
	SourcePVCs            []string
	DestinationPVCs       []string
	DestinationCapacities []string
	SourcePaths           []string
	DestinationPaths      []string
	AllowVolumeShrink     bool
	SkipSourceUsageCheck  bool
	PodName               string
	SourceNode            string
	TargetNode            string
	DestinationClass      string
	Strategies            []string
	Online                bool
	VerifyChecksum        bool
	DeleteExtraneous      bool
}

// PlanCopy builds a plan for a finite offline or warm copy.
func (p *Planner) PlanCopy(ctx context.Context, options CopyOptions) (*domain.MigrationPlan, error) {
	return p.plan(ctx, planOptions{
		SessionID:             options.SessionID,
		Operation:             domain.OperationCopy,
		SourceNamespace:       options.SourceNamespace,
		TemporaryNamespace:    options.TemporaryNamespace,
		DestinationNamespace:  options.DestinationNamespace,
		SessionNamespace:      options.SessionNamespace,
		StagingNamespace:      options.StagingNamespace,
		ToolImage:             options.ToolImage,
		CapacityAwareness:     options.CapacityAwareness,
		SourcePVCs:            slices.Clone(options.SourcePVCs),
		DestinationPVCs:       slices.Clone(options.DestinationPVCs),
		DestinationCapacities: slices.Clone(options.DestinationCapacities),
		SourcePaths:           slices.Clone(options.SourcePaths),
		DestinationPaths:      slices.Clone(options.DestinationPaths),
		AllowVolumeShrink:     options.AllowVolumeShrink,
		SkipSourceUsageCheck:  options.SkipSourceUsageCheck,
		PodName:               options.PodName,
		SourceNode:            options.SourceNode,
		TargetNode:            options.TargetNode,
		DestinationClass:      options.DestinationClass,
		Strategies:            slices.Clone(options.Strategies),
		Online:                options.Online,
		VerifyChecksum:        options.VerifyChecksum,
		DeleteExtraneous:      options.DeleteExtraneous,
	})
}
