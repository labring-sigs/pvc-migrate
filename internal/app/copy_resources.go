package app

import (
	"context"
	"fmt"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// copyResources provides resource operations shared by both Copy API scopes.
// Workflow identity, lifecycle and persistence remain owned by each executor.
type copyResources struct {
	client   kubernetes.Interface
	reserver volumeReserver
	transfer *volumeCopyRunner
	config   CopyExecutorConfig
}

func newCopyResources(
	client kubernetes.Interface,
	engine copyengine.Engine,
	config CopyExecutorConfig,
) copyResources {
	reserver := kube.NewReserver(client).
		WithTrustedToolImage(config.Transfer.TrustedToolImage).
		WithLogger(config.Transfer.Logger)
	if config.Transfer.StreamToolLogs {
		reserver.WithToolLogs(
			kube.ToolLogOptions{
				Writer:     config.Transfer.Writer,
				Logger:     config.Transfer.Logger,
				Structured: config.Transfer.StructuredLogs,
			},
		)
	}

	return copyResources{
		client:   client,
		reserver: reserver,
		transfer: newVolumeCopyRunner(client, engine, config.Transfer),
		config:   config,
	}
}

func (c *copyResources) validateVolume(
	ctx context.Context,
	request kube.ReservationRequest,
	sourceNamespace, destinationNamespace string,
	volume v1alpha1.VolumeSpec,
	checkpoint v1alpha1.ClusterVolumeReservationStatus,
) error {
	desired, err := reservationManifest(
		qualifiedResourceReference(volume.DestinationPVC, destinationNamespace),
		volume.Capacity,
		volume.StorageClass,
		volume.VolumeMode,
		volume.AccessModes,
	)
	if err != nil {
		return err
	}

	return c.reserver.ValidateVolumeReservation(
		ctx,
		request,
		qualifiedResourceReference(volume.SourcePVC, sourceNamespace),
		qualifiedResourceReference(volume.SourcePV, ""),
		volume.SourceCapacity,
		desired,
		checkpoint,
	)
}

func (c *copyResources) sharedSource(
	ctx context.Context,
	sourcePV v1alpha1.ObjectReference,
) (bool, bool, error) {
	if c.config.SharedVolumeManager == nil {
		return false, false, nil
	}

	pv, err := c.client.CoreV1().PersistentVolumes().Get(ctx, sourcePV.Name, metav1.GetOptions{})
	if err != nil {
		return false, false, err
	}

	if pv.UID != sourcePV.UID {
		return false, false, domain.NewError(
			domain.ErrorConflict,
			"copy",
			"source PV identity changed",
		)
	}

	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != kube.OpenEBSLVMCSIDriver {
		return false, false, nil
	}

	shared, err := c.config.SharedVolumeManager.Shared(
		ctx,
		sourcePV,
		v1alpha1.ObjectReference{},
		"",
	)

	return true, shared, err
}

func (c *copyResources) probe(
	ctx context.Context,
	id, image string,
	targets []kube.ToolProbeTarget,
) ([]kube.ToolImageProbeResult, error) {
	if c.config.ToolImageProber == nil || len(targets) == 0 {
		return nil, nil
	}

	timeout := c.config.ProbeTimeout
	if timeout <= 0 {
		timeout = c.transfer.config.HelmTimeout
	}

	return c.config.ToolImageProber.Probe(ctx, kube.ToolImageProbeOptions{
		OperationID: id, Image: c.transfer.toolImage(image), Targets: targets, Timeout: timeout,
		Writer: c.transfer.config.Writer, Logger: c.transfer.config.Logger,
	})
}

func (c *copyResources) validateConsumers(
	ctx context.Context,
	source v1alpha1.ObjectReference,
	accessModes []corev1.PersistentVolumeAccessMode,
	online bool,
	node string,
) error {
	pods, err := c.client.CoreV1().Pods(source.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	if pods == nil {
		return domain.NewError(
			domain.ErrorKubernetes,
			"copy",
			"source Pod inventory returned an empty object",
		)
	}

	_, err = validateCopyConsumersFromPods(online, source, accessModes, pods.Items, nil, node)

	return err
}

func (c *copyResources) validateStorage(
	ctx context.Context,
	id, sourceNamespace, destinationNamespace string,
	plan *v1alpha1.CopyPlan,
	sourceNode string,
	checkpoints map[string]v1alpha1.ClusterVolumeReservationStatus,
) (string, error) {
	node := sourceNode
	if node == "" {
		node = plan.SourceNode
	}

	pods, err := c.client.CoreV1().
		Pods(sourceNamespace).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}

	if pods == nil {
		return "", domain.NewError(
			domain.ErrorKubernetes,
			"copy",
			"source Pod inventory returned an empty object",
		)
	}

	for _, volume := range plan.Volumes {
		source := qualifiedResourceReference(volume.SourcePVC, sourceNamespace)

		inferred, err := validateCopyConsumersFromPods(
			plan.Online,
			source,
			volume.AccessModes,
			pods.Items,
			nil,
			node,
		)
		if err != nil {
			return "", err
		}

		if node == "" {
			node = inferred
		}

		if err := kube.VerifyVolumeShrinkUsage(
			ctx,
			c.config.VolumeUsageReader,
			c.config.Transfer.Logger,
			source,
			qualifiedResourceReference(volume.SourcePV, ""),
			volume.SourceCapacity,
			volume.Capacity,
			domain.SourceTransferPath(
				volume.TransferScope,
			),
			plan.SkipSourceUsageCheck,
		); err != nil {
			return "", err
		}

		checkpoint := v1alpha1.ClusterVolumeReservationStatus{SourcePVCName: source.Name}
		if saved, exists := checkpoints[source.Name]; exists {
			checkpoint = *saved.DeepCopy()
		}

		if err := c.validateVolume(
			ctx,
			kube.ReservationRequest{
				SessionID:  id,
				TargetNode: plan.TargetNode,
				ToolImage:  c.transfer.toolImage(plan.ToolImage),
			},
			sourceNamespace,
			destinationNamespace,
			volume,
			checkpoint,
		); err != nil {
			return "", err
		}

		active := slices.ContainsFunc(
			pods.Items,
			func(pod corev1.Pod) bool { return kube.ActivePodUsesPVC(&pod, source.Name) },
		)
		if active && c.config.SharedVolumeManager != nil {
			lvm, shared, err := c.sharedSource(ctx, qualifiedResourceReference(volume.SourcePV, ""))
			if err != nil {
				return "", err
			}

			if lvm && !shared {
				return "", domain.NewError(
					domain.ErrorPrecondition,
					"copy",
					fmt.Sprintf(
						"active OpenEBS LVM source PVC %s/%s requires shared mounting for online copy",
						source.Namespace,
						source.Name,
					),
				)
			}
		}
	}

	return node, nil
}

func (c *copyResources) probeTargets(
	ctx context.Context,
	plan *v1alpha1.CopyPlan,
	sourceNamespace, destinationNamespace, sourceNode string,
) ([]kube.ToolProbeTarget, error) {
	var targets []kube.ToolProbeTarget
	if plan.TargetNode != "" {
		components := []string{kube.ToolComponentRsync}
		if slices.Contains(plan.Strategies, domain.StrategyLocal) {
			components = append(components, kube.ToolComponentSSHD)
		}

		targets = append(
			targets,
			toolProbeTargetsForNamespaces(
				[]string{destinationNamespace},
				plan.TargetNode,
				components,
			)...,
		)
	}

	for _, volume := range plan.Volumes {
		_, shared, err := c.sharedSource(ctx, qualifiedResourceReference(volume.SourcePV, ""))
		if err != nil {
			return nil, err
		}

		source := kube.ToolProbeTarget{
			Namespace:        sourceNamespace,
			NodeName:         sourceNode,
			PVCName:          volume.SourcePVC.Name,
			WritablePVCMount: shared,
		}
		if sessionNeedsSourceSSHD(plan.Strategies) {
			source.Components = []string{kube.ToolComponentSSHD}
		}

		if path := domain.SourceTransferPath(volume.TransferScope); path != domain.VolumeRootPath {
			source.RequiredPath = path
		}

		targets = append(targets, source)
		if path := domain.DestinationTransferPath(
			volume.TransferScope,
		); path != domain.VolumeRootPath {
			targets = append(targets, kube.ToolProbeTarget{
				Namespace:        destinationNamespace,
				PVCName:          volume.DestinationPVC.Name,
				NodeName:         plan.TargetNode,
				WritablePVCMount: true,
				RequiredPath:     path,
				CreatePath:       true,
				Components:       []string{kube.ToolComponentRsync},
			})
		}
	}

	return targets, nil
}
