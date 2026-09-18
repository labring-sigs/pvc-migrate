package cli

import (
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// reserveFlags is the CLI contract for storage reservation. It deliberately
// excludes copy-only controls such as --online.
type reserveFlags struct {
	sessionID             string
	sourceNamespace       string
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
	unusedStoragePolicy   string
}

func (f *reserveFlags) bind(command *cobra.Command) {
	flags := command.Flags()
	flags.StringVar(&f.sessionID, "session", "", "Migration session ID")
	flags.StringVarP(&f.sourceNamespace, "source-namespace", "n", "default", "Source PVC namespace")
	flags.StringVar(
		&f.destinationNamespace,
		"destination-namespace",
		"",
		"Destination namespace; defaults to source namespace",
	)
	flags.StringSliceVar(
		&f.sourcePVCs,
		"source-pvc",
		nil,
		"Source PVC name; repeat for multiple claims",
	)
	flags.StringSliceVar(
		&f.destinationPVCs,
		"destination-pvc",
		nil,
		"Destination PVC name; for multiple PVCs use source-pvc-name=destination-pvc-name",
	)
	flags.StringSliceVar(
		&f.destinationCapacities,
		"destination-capacity",
		nil,
		"Destination PVC storage capacity; one value applies to all PVCs, or use source-pvc-name=capacity for explicit mappings",
	)
	flags.StringArrayVar(
		&f.sourcePaths,
		"source-path",
		nil,
		"Source directory inside a PVC; repeat and use source-pvc-name=relative-path for multiple PVCs",
	)
	flags.StringArrayVar(
		&f.destinationPaths,
		"destination-path",
		nil,
		"Destination directory inside a PVC; repeat and use source-pvc-name=relative-path for multiple PVCs",
	)
	flags.BoolVar(
		&f.allowVolumeShrink,
		"allow-volume-shrink",
		false,
		"Allow destination capacity below the source PV capacity; only use when copied data is known to fit",
	)
	flags.BoolVar(
		&f.skipSourceUsageCheck,
		"skip-source-usage-check",
		false,
		"Skip the storage-backend CRD usage check for a smaller destination",
	)
	flags.StringVar(
		&f.targetNode,
		"target-node",
		domain.AutoValue,
		"Target node for provisioning and copy tools; auto selects a compatible Ready node",
	)
	flags.StringVar(
		&f.destinationClass,
		"destination-storage-class",
		"",
		"Destination StorageClass; defaults to each source class",
	)
	flags.StringVar(
		&f.capacityAwareness,
		"capacity-awareness",
		string(domain.CapacityAwarenessAuto),
		"CSIStorageCapacity policy: auto, require, or off",
	)
	flags.StringSliceVar(
		&f.strategies,
		"strategy",
		[]string{domain.StrategyAuto},
		"pv-migrate strategy order; auto selects a topology-compatible order",
	)
	flags.BoolVar(
		&f.verifyChecksum,
		"verify-checksum",
		false,
		"Use rsync checksum comparison during final sync",
	)
	flags.BoolVar(
		&f.deleteExtraneous,
		"delete-extraneous",
		true,
		"Delete destination files absent from the source",
	)
	flags.StringVar(&f.podName, "pod", "", "Pod whose PVCs define the reservation set")
	flags.StringVar(&f.unusedStoragePolicy, "unused-storage-policy", string(v1alpha1.UnusedStorageKeep), "What happens to storage that is no longer in use at a terminal state: Keep or Delete. The copy the workload uses is always kept")
}

func (f *reserveFlags) workflow(
	state *rootState,
	runtime *commandRuntime,
	submit bool,
) (*v1alpha1.ClusterReservation, error) {
	id := f.sessionID
	if id == "" {
		generated, err := domain.NewSessionID(time.Now())
		if err != nil {
			return nil, err
		}

		id = generated
		f.sessionID = id
	}

	destinationNamespace := f.destinationNamespace
	if destinationNamespace == "" {
		destinationNamespace = f.sourceNamespace
	}

	sessionNamespace, _ := state.controllerPlanNamespaces(runtime, domain.SessionTypeReserve,
		f.sourceNamespace, destinationNamespace, destinationNamespace, false, submit)

	object := &v1alpha1.ClusterReservation{
		ObjectMeta: metav1.ObjectMeta{Name: id},
		Spec: v1alpha1.ClusterReservationSpec{
			SourceNamespace:      v1alpha1.NamespaceName(f.sourceNamespace),
			DestinationNamespace: v1alpha1.NamespaceName(destinationNamespace),
			SessionNamespace:     v1alpha1.NamespaceName(sessionNamespace),
			ReservationSpec: v1alpha1.ReservationSpec{
				TransferOptions: v1alpha1.TransferOptions{
					UnusedStoragePolicy: v1alpha1.UnusedStoragePolicy(
						f.unusedStoragePolicy,
					),
					DestinationStorageClass: f.destinationClass,
					CapacityAwareness:       f.capacityAwareness,
					TargetNode:              f.targetNode,
					Strategies:              append([]string(nil), f.strategies...),
					VerifyChecksum:          f.verifyChecksum,
					DeleteExtraneous:        f.deleteExtraneous,
					AllowVolumeShrink:       f.allowVolumeShrink,
					SkipSourceUsageCheck:    f.skipSourceUsageCheck,
				},
			},
		},
	}
	if f.podName != "" {
		object.Spec.Pod = &v1alpha1.LocalResourceReference{Name: f.podName}
	}

	for _, name := range f.sourcePVCs {
		object.Spec.Volumes = append(object.Spec.Volumes,
			v1alpha1.VolumeRequest{SourcePVC: v1alpha1.LocalResourceReference{Name: name}})
	}

	if err := applyVolumeMappings(
		&object.Spec.TransferOptions,
		&object.Spec.Volumes,
		object.Spec.Pod != nil,
		f.destinationCapacities,
		f.destinationPVCs,
		f.sourcePaths,
		f.destinationPaths,
	); err != nil {
		return nil, err
	}

	return object, nil
}
