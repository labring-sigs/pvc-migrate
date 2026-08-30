package cli

import (
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/planner"
	"github.com/spf13/cobra"
)

// reserveFlags is the CLI contract for storage reservation. It deliberately
// excludes copy-only controls such as --online.
type reserveFlags struct {
	sessionID             string
	sourceNamespace       string
	temporaryNamespace    string
	destinationNamespace  string
	sourcePVCs            []string
	destinationPVCs       []string
	destinationCapacities []string
	sourcePaths           []string
	destinationPaths      []string
	allowVolumeShrink     bool
	skipSourceUsageCheck  bool
	targetNode            string
	destinationClass      string
	capacityAwareness     string
	strategies            []string
	verifyChecksum        bool
	deleteExtraneous      bool
	podName               string
}

func (f *reserveFlags) bind(command *cobra.Command) {
	flags := command.Flags()
	flags.StringVar(&f.sessionID, "session", "", "Migration session ID")
	flags.StringVarP(&f.sourceNamespace, "source-namespace", "n", "default", "Source PVC namespace")
	flags.StringVar(&f.temporaryNamespace, "temporary-namespace", "pvc-migrate-system", "Namespace for staged destination PVCs")
	flags.StringVar(&f.destinationNamespace, "destination-namespace", "", "Destination namespace; defaults to source namespace")
	flags.StringSliceVar(&f.sourcePVCs, "source-pvc", nil, "Source PVC name; repeat for multiple claims")
	flags.StringSliceVar(&f.destinationPVCs, "destination-pvc", nil, "Destination PVC name; for multiple PVCs use source-pvc-name=destination-pvc-name")
	flags.StringSliceVar(&f.destinationCapacities, "destination-capacity", nil, "Destination PVC storage capacity; one value applies to all PVCs, or use source-pvc-name=capacity for explicit mappings")
	flags.StringArrayVar(&f.sourcePaths, "source-path", nil, "Source directory inside a PVC; repeat and use source-pvc-name=relative-path for multiple PVCs")
	flags.StringArrayVar(&f.destinationPaths, "destination-path", nil, "Destination directory inside a PVC; repeat and use source-pvc-name=relative-path for multiple PVCs")
	flags.BoolVar(&f.allowVolumeShrink, "allow-volume-shrink", false, "Allow destination capacity below the source PV capacity; only use when copied data is known to fit")
	flags.BoolVar(&f.skipSourceUsageCheck, "skip-source-usage-check", false, "Skip the storage-backend CRD usage check for a smaller destination")
	flags.StringVar(&f.targetNode, "target-node", domain.AutoValue, "Target node for provisioning and copy tools; auto selects a compatible Ready node")
	flags.StringVar(&f.destinationClass, "destination-storage-class", "", "Destination StorageClass; defaults to each source class")
	flags.StringVar(&f.capacityAwareness, "capacity-awareness", string(domain.CapacityAwarenessAuto), "CSIStorageCapacity policy: auto, require, or off")
	flags.StringSliceVar(&f.strategies, "strategy", []string{domain.StrategyAuto}, "pv-migrate strategy order; auto selects a topology-compatible order")
	flags.BoolVar(&f.verifyChecksum, "verify-checksum", true, "Use rsync checksum comparison during final sync")
	flags.BoolVar(&f.deleteExtraneous, "delete-extraneous", true, "Delete destination files absent from the source")
	flags.StringVar(&f.podName, "pod", "", "Pod whose PVCs define the reservation set")
}

func (f *reserveFlags) planOptions(state *rootState) (planner.ReserveOptions, error) {
	id := f.sessionID
	if id == "" {
		generated, err := domain.NewSessionID(time.Now())
		if err != nil {
			return planner.ReserveOptions{}, err
		}
		id = generated
		f.sessionID = id
	}
	destinationNamespace := f.destinationNamespace
	if destinationNamespace == "" {
		destinationNamespace = f.sourceNamespace
	}
	return planner.ReserveOptions{
		SessionID:             id,
		SourceNamespace:       f.sourceNamespace,
		TemporaryNamespace:    f.temporaryNamespace,
		DestinationNamespace:  destinationNamespace,
		SessionNamespace:      state.global.sessionNamespace,
		StagingNamespace:      f.temporaryNamespace,
		ToolImage:             state.global.toolImage,
		SourcePVCs:            append([]string(nil), f.sourcePVCs...),
		DestinationPVCs:       append([]string(nil), f.destinationPVCs...),
		DestinationCapacities: append([]string(nil), f.destinationCapacities...),
		SourcePaths:           append([]string(nil), f.sourcePaths...),
		DestinationPaths:      append([]string(nil), f.destinationPaths...),
		AllowVolumeShrink:     f.allowVolumeShrink,
		SkipSourceUsageCheck:  f.skipSourceUsageCheck,
		PodName:               f.podName,
		TargetNode:            f.targetNode,
		DestinationClass:      f.destinationClass,
		CapacityAwareness:     domain.CapacityAwareness(f.capacityAwareness),
		Strategies:            append([]string(nil), f.strategies...),
		VerifyChecksum:        f.verifyChecksum,
		DeleteExtraneous:      f.deleteExtraneous,
	}, nil
}
