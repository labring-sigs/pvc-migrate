package cli

import (
	"errors"
	"fmt"
	"strings"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// copyFlags is the CLI contract for a finite data copy. Its fields are kept
// independent from reserveFlags so adding a copy mode cannot silently expose
// a new reserve parameter.
type copyFlags struct {
	sessionID                   string
	sourceNamespace             string
	destinationNamespace        string
	sourcePVCs                  []string
	destinationPVCs             []string
	destinationCapacities       []string
	sourcePaths                 []string
	destinationPaths            []string
	allowVolumeShrink           bool
	skipSourceUsageCheck        bool
	sourceNode                  string
	targetNode                  string
	destinationClass            string
	capacityAwareness           string
	strategies                  []string
	online                      bool
	verifyChecksum              bool
	deleteExtraneous            bool
	podName                     string
	destinationPVCReclaimPolicy string
}

func (f *copyFlags) bind(command *cobra.Command) {
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
		&f.sourceNode,
		"source-node",
		"",
		"Source tool node; inferred from active consumers when possible",
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
		&f.online,
		"online",
		false,
		"Allow active PVC consumers for one finite warm-copy pass",
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
	flags.StringVar(&f.podName, "pod", "", "Pod whose PVCs define the copy set")
	flags.StringVar(
		&f.destinationPVCReclaimPolicy,
		"destination-pvc-reclaim-policy",
		string(domain.DestinationPVCReclaimRetain),
		"Policy for the destination PVC during cleanup: Retain or Delete",
	)
}

func (f *copyFlags) workflow(
	state *rootState,
	runtime *commandRuntime,
	submit bool,
) (*v1alpha1.ClusterCopy, error) {
	id := f.sessionID
	if id == "" {
		generated, err := domain.NewSessionID(time.Now())
		if err != nil {
			return nil, err
		}

		id = generated
		f.sessionID = id
	}

	destination := f.destinationNamespace
	if destination == "" {
		destination = f.sourceNamespace
	}

	sessionNamespace, _ := state.controllerPlanNamespaces(
		runtime,
		domain.SessionTypeCopy,
		f.sourceNamespace,
		destination,
		destination,
		false,
		submit,
	)

	object := &v1alpha1.ClusterCopy{
		ObjectMeta: metav1.ObjectMeta{Name: id},
		Spec: v1alpha1.ClusterCopySpec{
			SourceNamespace:      v1alpha1.NamespaceName(f.sourceNamespace),
			DestinationNamespace: v1alpha1.NamespaceName(destination),
			SessionNamespace:     v1alpha1.NamespaceName(sessionNamespace),
			CopySpec: v1alpha1.CopySpec{
				Online: f.online,
				TransferOptions: v1alpha1.TransferOptions{
					DestinationPVCReclaimPolicy: v1alpha1.PVReclaimPolicy(
						f.destinationPVCReclaimPolicy,
					),
					DestinationStorageClass: f.destinationClass,
					CapacityAwareness:       f.capacityAwareness,
					SourceNode:              f.sourceNode,
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
		object.Spec.Volumes = append(
			object.Spec.Volumes,
			v1alpha1.VolumeRequest{SourcePVC: v1alpha1.LocalResourceReference{Name: name}},
		)
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

func applyVolumeMappings(
	options *v1alpha1.TransferOptions,
	volumes *[]v1alpha1.VolumeRequest,
	selectsPod bool,
	capacities, destinations, sourcePaths, destinationPaths []string,
) error {
	for _, field := range []struct {
		name   string
		values []string
	}{{"capacity", capacities}, {"destinationPVC", destinations}, {"sourcePath", sourcePaths}, {"destinationPath", destinationPaths}} {
		seen := make(map[string]bool)
		for index, raw := range field.values {
			name, value, named := strings.Cut(raw, "=")
			if strings.TrimSpace(raw) == "" || (named && (name == "" || value == "")) {
				return fmt.Errorf("invalid %s mapping %q", field.name, raw)
			}

			if !named {
				value = raw
				if field.name != "destinationPVC" {
					if len(field.values) != 1 {
						return fmt.Errorf(
							"%s requires one broadcast value or named PVC mappings",
							field.name,
						)
					}

					switch field.name {
					case "capacity":
						options.DestinationCapacity = value
					case "sourcePath":
						options.SourcePath = value
					case "destinationPath":
						options.DestinationPath = value
					}

					continue
				}

				if index >= len(*volumes) {
					return errors.New("destination PVC requires a source PVC mapping")
				}

				name = (*volumes)[index].SourcePVC.Name
			}

			if seen[name] {
				return fmt.Errorf("duplicate %s mapping for PVC %q", field.name, name)
			}

			seen[name] = true

			found := -1
			for i := range *volumes {
				if (*volumes)[i].SourcePVC.Name == name {
					found = i
					break
				}
			}

			if found < 0 {
				if !selectsPod {
					return fmt.Errorf("unknown source PVC %q", name)
				}

				*volumes = append(
					*volumes,
					v1alpha1.VolumeRequest{SourcePVC: v1alpha1.LocalResourceReference{Name: name}},
				)
				found = len(*volumes) - 1
			}

			volume := &(*volumes)[found]
			switch field.name {
			case "capacity":
				volume.Capacity = value
			case "destinationPVC":
				volume.DestinationPVC = &v1alpha1.LocalResourceReference{Name: value}
			case "sourcePath", "destinationPath":
				if volume.TransferScope == nil {
					volume.TransferScope = &v1alpha1.TransferScope{
						SourcePath:      domain.VolumeRootPath,
						DestinationPath: domain.VolumeRootPath,
					}
				}

				if field.name == "sourcePath" {
					volume.TransferScope.SourcePath = value
				} else {
					volume.TransferScope.DestinationPath = value
				}
			}
		}
	}

	return nil
}
