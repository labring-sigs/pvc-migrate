package crosscluster

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Options struct {
	SessionID               string
	SessionNamespace        string
	SourceNamespace         string
	DestinationNamespace    string
	SourcePVCs              []string
	DestinationPVCs         []string
	DestinationCapacities   []string
	SourcePaths             []string
	DestinationPaths        []string
	DestinationStorageClass string
	AllowVolumeShrink       bool
	SkipSourceUsageCheck    bool
	Online                  bool
	VerifyChecksum          bool
	DeleteExtraneous        bool
	TargetNode              string
	ToolImage               string
	Strategies              []string
}

type Service struct {
	source                                    *kube.Clients
	destination                               *kube.Clients
	copier                                    copyengine.Engine
	sourceKubeconfig, sourceContext           string
	destinationKubeconfig, destinationContext string
	now                                       func() time.Time
	interval                                  time.Duration
	helmTimeout                               time.Duration
	writer                                    io.Writer
	logger                                    *slog.Logger
	store                                     kube.SessionLocker
}

func NewService(source, destination *kube.Clients, copier copyengine.Engine) *Service {
	service := &Service{
		source:      source,
		destination: destination,
		copier:      copier,
		now:         time.Now,
		interval:    time.Second,
		helmTimeout: 10 * time.Minute,
		writer:      io.Discard,
		logger:      slog.Default(),
	}
	if source != nil {
		service.store = kube.NewConfigMapSessionStore(source.Kubernetes)
	}

	return service
}

func (s *Service) WithRuntime(
	writer io.Writer,
	logger *slog.Logger,
	helmTimeout time.Duration,
) *Service {
	if writer != nil {
		s.writer = writer
	}

	if logger != nil {
		s.logger = logger
	}

	if helmTimeout > 0 {
		s.helmTimeout = helmTimeout
	}

	return s
}

func (s *Service) WithConnections(
	sourceKubeconfig, sourceContext, destinationKubeconfig, destinationContext string,
) *Service {
	s.sourceKubeconfig, s.sourceContext = sourceKubeconfig, sourceContext
	s.destinationKubeconfig, s.destinationContext = destinationKubeconfig, destinationContext
	return s
}

func (s *Service) Plan(ctx context.Context, options Options) (*Plan, error) {
	if err := s.validateClients(); err != nil {
		return nil, err
	}

	if options.SessionID == "" || options.SessionNamespace == "" ||
		options.SourceNamespace == "" || options.DestinationNamespace == "" {
		return nil, errors.New(
			"session ID, session namespace, and both PVC namespaces are required",
		)
	}

	sourceID, err := kube.Identity(ctx, s.source)
	if err != nil {
		return nil, err
	}

	destID, err := kube.Identity(ctx, s.destination)
	if err != nil {
		return nil, err
	}

	plan := newCrossClusterPlan(options, sourceID, destID)

	destinationClass := s.planCrossClusterEnvironment(ctx, plan, options)
	if destinationClass == nil {
		return plan, nil
	}

	inputs, ok := resolveCrossClusterInputs(plan, options)
	if !ok {
		return plan, nil
	}

	s.planCrossClusterVolumes(ctx, plan, options, destinationClass, inputs)

	return plan, nil
}

type crossClusterPlanInputs struct {
	destinations []string
	capacities   []string
	sourcePaths  []string
	destPaths    []string
}

func newCrossClusterPlan(options Options, sourceID, destinationID kube.ClusterIdentity) *Plan {
	return &Plan{
		APIVersion:           APIVersion,
		Kind:                 Kind,
		SessionID:            options.SessionID,
		SourceCluster:        sourceID,
		DestinationCluster:   destinationID,
		SourceNamespace:      options.SourceNamespace,
		DestinationNamespace: options.DestinationNamespace,
		Strategies:           normalizeStrategies(options.Strategies),
		Ready:                true,
	}
}

func (s *Service) planCrossClusterEnvironment(
	ctx context.Context,
	plan *Plan,
	options Options,
) *storagev1.StorageClass {
	if err := ValidateSessionID(options.SessionID); err != nil {
		plan.AddCheck("session-id", false, err.Error())
	}

	if plan.SourceCluster.ID == plan.DestinationCluster.ID {
		plan.AddCheck(
			"clusters",
			false,
			"source and destination resolve to the same Kubernetes API endpoint; use single-cluster copy",
		)
	}

	if _, err := kube.NormalizeToolImage(options.ToolImage); err != nil {
		plan.AddCheck("tool-image", false, err.Error())
	}

	_, err := s.destination.Kubernetes.CoreV1().Namespaces().Get(
		ctx,
		options.DestinationNamespace,
		metav1.GetOptions{},
	)
	switch {
	case err != nil && !apierrors.IsNotFound(err):
		plan.AddCheck(
			"destination-namespace",
			false,
			fmt.Sprintf("read destination namespace %s: %v", options.DestinationNamespace, err),
		)
	case apierrors.IsNotFound(err):
		plan.AddCheck(
			"destination-namespace",
			true,
			fmt.Sprintf(
				"destination namespace %s will be created by reserve",
				options.DestinationNamespace,
			),
		)
	default:
		plan.AddCheck(
			"destination-namespace",
			true,
			fmt.Sprintf("destination namespace %s exists", options.DestinationNamespace),
		)
	}

	var destinationClass *storagev1.StorageClass
	if options.DestinationStorageClass == "" {
		plan.AddCheck(
			"destination-storage-class",
			false,
			"--destination-storage-class is required for cross-cluster copy",
		)
	} else if storageClass, getErr := s.destination.Kubernetes.StorageV1().StorageClasses().Get(
		ctx,
		options.DestinationStorageClass,
		metav1.GetOptions{},
	); getErr != nil {
		plan.AddCheck(
			"destination-storage-class",
			false,
			fmt.Sprintf(
				"read destination StorageClass %s: %v",
				options.DestinationStorageClass,
				getErr,
			),
		)
	} else {
		destinationClass = storageClass

		plan.AddCheck(
			"destination-storage-class",
			true,
			fmt.Sprintf(
				"destination StorageClass %s is available",
				options.DestinationStorageClass,
			),
		)
	}

	if destinationClass != nil {
		node, selectErr := s.selectTargetNode(ctx, options.TargetNode, destinationClass)
		if selectErr != nil {
			plan.AddCheck("target-node", false, selectErr.Error())
		} else {
			plan.TargetNode = node.Name
			plan.AddCheck(
				"target-node",
				true,
				fmt.Sprintf(
					"destination node %s is Ready, schedulable, and topology-compatible",
					node.Name,
				),
			)
		}
	}

	if err := validateStrategies(plan.Strategies); err != nil {
		plan.AddCheck("strategy", false, err.Error())
	} else {
		plan.AddCheck(
			"strategy",
			true,
			"cross-cluster transfer uses "+strings.Join(plan.Strategies, ", "),
		)
	}

	return destinationClass
}

func resolveCrossClusterInputs(plan *Plan, options Options) (crossClusterPlanInputs, bool) {
	if len(options.SourcePVCs) == 0 {
		plan.AddCheck("source-pvc", false, "at least one --source-pvc is required")
		return crossClusterPlanInputs{}, false
	}

	seen := make(map[string]struct{}, len(options.SourcePVCs))
	for _, source := range options.SourcePVCs {
		if source == "" {
			plan.AddCheck("source-pvc", false, "source PVC names must not be empty")
			return crossClusterPlanInputs{}, false
		}

		if _, exists := seen[source]; exists {
			plan.AddCheck(
				"source-pvc",
				false,
				fmt.Sprintf(
					"source PVC %s was specified more than once; use one explicit mapping per PVC",
					source,
				),
			)

			return crossClusterPlanInputs{}, false
		}

		seen[source] = struct{}{}
	}

	if len(options.SourcePVCs) > 1 && len(options.DestinationPVCs) == 1 &&
		!strings.Contains(options.DestinationPVCs[0], "=") {
		plan.AddCheck(
			"destination-pvc",
			false,
			"multiple source PVCs require explicit source=destination mappings",
		)

		return crossClusterPlanInputs{}, false
	}

	inputs := crossClusterPlanInputs{}

	var err error
	if inputs.destinations, err = resolveNames(
		options.DestinationPVCs,
		options.SourcePVCs,
	); err != nil {
		plan.AddCheck("destination-pvc", false, err.Error())
		return crossClusterPlanInputs{}, false
	}

	if inputs.capacities, err = resolveValues(
		options.DestinationCapacities,
		options.SourcePVCs,
	); err != nil {
		plan.AddCheck("destination-capacity", false, err.Error())
		return crossClusterPlanInputs{}, false
	}

	if inputs.sourcePaths, err = resolvePaths(options.SourcePaths, options.SourcePVCs); err != nil {
		plan.AddCheck("source-path", false, err.Error())
		return crossClusterPlanInputs{}, false
	}

	if inputs.destPaths, err = resolvePaths(
		options.DestinationPaths,
		options.SourcePVCs,
	); err != nil {
		plan.AddCheck("destination-path", false, err.Error())
		return crossClusterPlanInputs{}, false
	}

	return inputs, true
}

func (s *Service) planCrossClusterVolumes(
	ctx context.Context,
	plan *Plan,
	options Options,
	destinationClass *storagev1.StorageClass,
	inputs crossClusterPlanInputs,
) {
	for index, name := range options.SourcePVCs {
		if volume, ok := s.planCrossClusterVolume(
			ctx,
			plan,
			options,
			destinationClass,
			inputs,
			index,
			name,
		); ok {
			plan.Volumes = append(plan.Volumes, volume)
		}
	}

	if len(plan.Volumes) == len(options.SourcePVCs) {
		plan.AddCheck(
			"source-pvcs",
			true,
			fmt.Sprintf("validated %d source PVC(s)", len(plan.Volumes)),
		)
	}
}

func (s *Service) planCrossClusterVolume(
	ctx context.Context,
	plan *Plan,
	options Options,
	destinationClass *storagev1.StorageClass,
	inputs crossClusterPlanInputs,
	index int,
	name string,
) (VolumePlan, bool) {
	sourcePath, err := domain.NormalizeTransferPath(inputs.sourcePaths[index])
	if err != nil {
		plan.AddCheck(
			"source-path",
			false,
			fmt.Sprintf("source path for %s is invalid: %v", name, err),
		)

		return VolumePlan{}, false
	}

	destinationPath, err := domain.NormalizeTransferPath(inputs.destPaths[index])
	if err != nil {
		plan.AddCheck(
			"destination-path",
			false,
			fmt.Sprintf("destination path for %s is invalid: %v", name, err),
		)

		return VolumePlan{}, false
	}

	existing, err := s.destination.Kubernetes.CoreV1().
		PersistentVolumeClaims(options.DestinationNamespace).
		Get(
			ctx,
			inputs.destinations[index],
			metav1.GetOptions{},
		)
	if err == nil {
		plan.AddCheck(
			"destination-pvc",
			false,
			fmt.Sprintf(
				"destination PVC %s/%s already exists with UID %s",
				existing.Namespace,
				existing.Name,
				existing.UID,
			),
		)

		return VolumePlan{}, false
	}

	if !apierrors.IsNotFound(err) {
		plan.AddCheck(
			"destination-pvc",
			false,
			fmt.Sprintf(
				"read destination PVC %s/%s: %v",
				options.DestinationNamespace,
				inputs.destinations[index],
				err,
			),
		)

		return VolumePlan{}, false
	}

	pvc, err := s.source.Kubernetes.CoreV1().
		PersistentVolumeClaims(options.SourceNamespace).
		Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		plan.AddCheck(
			"source-pvc",
			false,
			fmt.Sprintf("read source PVC %s/%s: %v", options.SourceNamespace, name, err),
		)

		return VolumePlan{}, false
	}

	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		plan.AddCheck(
			"source-pvc",
			false,
			fmt.Sprintf("source PVC %s/%s must be Bound", pvc.Namespace, pvc.Name),
		)

		return VolumePlan{}, false
	}

	if !kube.HasWritableAccessMode(pvc.Spec.AccessModes) {
		plan.AddCheck(
			"access-mode",
			false,
			fmt.Sprintf(
				"source PVC %s/%s has no writable access mode for the destination copy",
				pvc.Namespace,
				pvc.Name,
			),
		)

		return VolumePlan{}, false
	}

	pv, err := s.source.Kubernetes.CoreV1().
		PersistentVolumes().
		Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		plan.AddCheck(
			"source-pv",
			false,
			fmt.Sprintf("read source PV %s: %v", pvc.Spec.VolumeName, err),
		)

		return VolumePlan{}, false
	}

	if pvc.UID == "" || pv.UID == "" || pv.Spec.ClaimRef == nil ||
		pv.Spec.ClaimRef.UID == "" ||
		pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		plan.AddCheck(
			"source-binding",
			false,
			fmt.Sprintf(
				"source PV %s claimRef does not match PVC %s/%s UID %s",
				pv.Name,
				pvc.Namespace,
				pvc.Name,
				pvc.UID,
			),
		)

		return VolumePlan{}, false
	}

	capacity := pv.Spec.Capacity[corev1.ResourceStorage]
	if capacity.Sign() <= 0 {
		plan.AddCheck(
			"capacity",
			false,
			fmt.Sprintf("source PV %s has no positive storage capacity", pv.Name),
		)

		return VolumePlan{}, false
	}

	destinationCapacity, ok := resolveCrossClusterCapacity(
		plan,
		options,
		name,
		capacity,
		inputs.capacities[index],
	)
	if !ok {
		return VolumePlan{}, false
	}

	s.checkCrossClusterConsumers(ctx, plan, options, pvc)

	if pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		plan.AddCheck(
			"volume-mode",
			false,
			fmt.Sprintf("source PVC %s/%s is not a filesystem volume", pvc.Namespace, pvc.Name),
		)

		return VolumePlan{}, false
	}

	return VolumePlan{
		SourceNamespace:      pvc.Namespace,
		SourcePVC:            pvc.Name,
		SourcePVCUID:         pvc.UID,
		SourcePVUID:          pv.UID,
		DestinationNamespace: options.DestinationNamespace,
		DestinationPVC:       inputs.destinations[index],
		SourceCapacity:       capacity.String(),
		Capacity:             destinationCapacity.String(),
		StorageClass:         options.DestinationStorageClass,
		StorageClassUID:      destinationClass.UID,
		SourcePath:           sourcePath,
		DestinationPath:      destinationPath,
	}, true
}

func resolveCrossClusterCapacity(
	plan *Plan,
	options Options,
	name string,
	sourceCapacity resource.Quantity,
	requested string,
) (resource.Quantity, bool) {
	destinationCapacity := sourceCapacity
	if requested == "" {
		return destinationCapacity, true
	}

	parsed, err := resource.ParseQuantity(requested)
	if err != nil || parsed.Sign() <= 0 {
		plan.AddCheck(
			"destination-capacity",
			false,
			fmt.Sprintf("destination capacity for %s is invalid", name),
		)

		return destinationCapacity, false
	}

	if parsed.Cmp(sourceCapacity) < 0 {
		switch {
		case !options.AllowVolumeShrink:
			plan.AddCheck(
				"destination-capacity",
				false,
				fmt.Sprintf(
					"destination capacity for %s is smaller than source; pass --allow-volume-shrink",
					name,
				),
			)
		case !options.SkipSourceUsageCheck:
			plan.AddCheck(
				"source-usage",
				false,
				fmt.Sprintf(
					"cross-cluster shrink for %s has no trusted storage-backend usage reader; independently verify the selected data fits, then pass --skip-source-usage-check",
					name,
				),
			)
		default:
			plan.AddCheck(
				"source-usage",
				true,
				fmt.Sprintf("source usage check for %s was explicitly skipped", name),
			)
		}
	}

	return parsed, true
}

func (s *Service) checkCrossClusterConsumers(
	ctx context.Context,
	plan *Plan,
	options Options,
	pvc *corev1.PersistentVolumeClaim,
) {
	consumers, err := activeConsumers(ctx, s.source.Kubernetes, pvc.Namespace, pvc.Name)
	switch {
	case err != nil:
		plan.AddCheck("source-consumers", false, err.Error())
	case !options.Online && len(consumers) > 0:
		plan.AddCheck(
			"source-consumers",
			false,
			fmt.Sprintf(
				"source PVC %s/%s has active consumers: %s",
				pvc.Namespace,
				pvc.Name,
				strings.Join(consumers, ", "),
			),
		)
	case options.Online && len(consumers) > 0 && !hasSharedAccessMode(pvc.Spec.AccessModes):
		plan.AddCheck(
			"source-consumers",
			false,
			fmt.Sprintf(
				"online cross-cluster copy requires RWX/ROX while PVC %s/%s has active consumers; a second source tool Pod cannot safely mount this access mode",
				pvc.Namespace,
				pvc.Name,
			),
		)
	case options.Online:
		plan.AddCheck(
			"source-consumers",
			true,
			fmt.Sprintf(
				"online copy source PVC %s/%s has a compatible shared access mode",
				pvc.Namespace,
				pvc.Name,
			),
		)
	}
}

func (s *Service) CreateSession(
	ctx context.Context,
	options Options,
	plan *Plan,
) (*Session, error) {
	if plan == nil || !plan.Ready {
		return nil, errors.New("cross-cluster plan contains failed checks")
	}

	sourceID, err := kube.Identity(ctx, s.source)
	if err != nil {
		return nil, err
	}

	destID, err := kube.Identity(ctx, s.destination)
	if err != nil {
		return nil, err
	}

	if sourceID.ID != plan.SourceCluster.ID || destID.ID != plan.DestinationCluster.ID {
		return nil, errors.New(
			"cluster identity changed after planning; generate a new cross-cluster plan",
		)
	}

	volumes := make([]VolumeSpec, 0, len(plan.Volumes))
	for _, p := range plan.Volumes {
		pvc, err := s.source.Kubernetes.CoreV1().
			PersistentVolumeClaims(p.SourceNamespace).
			Get(ctx, p.SourcePVC, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		pv, err := s.source.Kubernetes.CoreV1().
			PersistentVolumes().
			Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		if pvc.UID != p.SourcePVCUID || pv.UID != p.SourcePVUID {
			return nil, fmt.Errorf(
				"source PVC/PV identity changed after planning for %s/%s; generate a new cross-cluster plan",
				p.SourceNamespace,
				p.SourcePVC,
			)
		}

		expectedSourceCapacity, parseErr := resource.ParseQuantity(p.SourceCapacity)
		if parseErr != nil {
			return nil, fmt.Errorf(
				"planned source capacity for %s is invalid: %w",
				p.SourcePVC,
				parseErr,
			)
		}

		if current := pv.Spec.Capacity[corev1.ResourceStorage]; current.Cmp(
			expectedSourceCapacity,
		) != 0 {
			return nil, fmt.Errorf(
				"source PV capacity changed after planning for %s; generate a new cross-cluster plan",
				p.SourcePVC,
			)
		}

		mode := corev1.PersistentVolumeFilesystem
		if pvc.Spec.VolumeMode != nil {
			mode = *pvc.Spec.VolumeMode
		}

		storageClass := options.DestinationStorageClass

		destinationClass, err := s.destination.Kubernetes.StorageV1().
			StorageClasses().
			Get(ctx, storageClass, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		if destinationClass.UID != p.StorageClassUID {
			return nil, errors.New(
				"destination StorageClass changed after planning; generate a new cross-cluster plan",
			)
		}

		volumes = append(volumes, VolumeSpec{
			Source: SourceVolumeSpec{
				PVC: ClusterResourceRef{
					ClusterID:  sourceID.ID,
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Namespace:  pvc.Namespace,
					Name:       pvc.Name,
					UID:        pvc.UID,
				},
				PV: ClusterResourceRef{
					ClusterID:  sourceID.ID,
					APIVersion: "v1",
					Kind:       "PersistentVolume",
					Name:       pv.Name,
					UID:        pv.UID,
				},
				Capacity: p.SourceCapacity,
			},
			Destination: DestinationVolumeSpec{
				PVC: ClusterResourceRef{
					ClusterID:  destID.ID,
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Namespace:  p.DestinationNamespace,
					Name:       p.DestinationPVC,
				},
				Capacity: p.Capacity,
				StorageClass: ClusterResourceRef{
					ClusterID:  destID.ID,
					APIVersion: "storage.k8s.io/v1",
					Kind:       "StorageClass",
					Name:       destinationClass.Name,
					UID:        destinationClass.UID,
				},
				AccessModes: append(
					[]corev1.PersistentVolumeAccessMode(nil),
					pvc.Spec.AccessModes...,
				),
				VolumeMode: mode,
			},
			Transfer: TransferSpec{SourcePath: p.SourcePath, DestinationPath: p.DestinationPath},
		})
	}

	session := NewSession(
		options.SessionID,
		Spec{
			SessionNamespace:     options.SessionNamespace,
			SourceCluster:        sourceID,
			DestinationCluster:   destID,
			SourceNamespace:      options.SourceNamespace,
			DestinationNamespace: options.DestinationNamespace,
			ToolImage:            options.ToolImage,
			Strategies:           normalizeStrategies(options.Strategies),
			Online:               options.Online,
			VerifyChecksum:       options.VerifyChecksum,
			DeleteExtraneous:     options.DeleteExtraneous,
			AllowVolumeShrink:    options.AllowVolumeShrink,
			SkipSourceUsageCheck: options.SkipSourceUsageCheck,
			TargetNode:           plan.TargetNode,
			Volumes:              volumes,
		},
		s.now(),
	)
	if _, err := kube.NormalizeToolImage(session.Spec.ToolImage); err != nil {
		return nil, err
	}

	if err := kube.EnsureNamespace(
		ctx,
		s.source.Kubernetes,
		session.Spec.SessionNamespace,
		session.ID,
		false,
	); err != nil {
		return nil, err
	}

	if err := s.save(ctx, session, true); err != nil {
		return nil, err
	}

	return session, nil
}

func (s *Service) Reserve(ctx context.Context, session *Session) error {
	if s.store != nil {
		return s.withLock(
			ctx,
			session,
			func(locked context.Context) error { return s.reserve(locked, session) },
		)
	}

	return s.reserve(ctx, session)
}

func (s *Service) reserve(ctx context.Context, session *Session) error {
	if err := s.validateSession(ctx, session); err != nil {
		return err
	}

	if session.Status.Phase == PhaseCleaned || session.Status.Phase == PhaseCleaning {
		return errors.New("cross-cluster session is already being cleaned or has been cleaned")
	}

	if session.Status.Phase == PhaseReserved || session.Status.Phase == PhaseTransferring ||
		session.Status.Phase == PhaseCompleted {
		return nil
	}

	session.Status.Phase = PhaseReserving
	session.Status.Message = "creating destination PVCs"
	s.touch(session)

	if err := kube.EnsureNamespace(
		ctx,
		s.destination.Kubernetes,
		session.Spec.DestinationNamespace,
		session.ID,
		false,
	); err != nil {
		return err
	}

	for i := range session.Spec.Volumes {
		if session.Status.Volumes[i].Reservation.PV.UID != "" {
			continue
		}

		if err := s.reserveVolume(ctx, session, i); err != nil {
			session.Status.Phase = PhaseFailed
			session.Status.Message = err.Error()
			s.touch(session)
			_ = s.save(ctx, session, false)

			return err
		}

		session.Status.Volumes[i].Reservation.PV = session.Spec.Volumes[i].Destination.PV

		session.Status.Volumes[i].Reservation.PVC = session.Spec.Volumes[i].Destination.PVC
		if err := s.save(ctx, session, false); err != nil {
			return err
		}
	}

	session.Status.Phase = PhaseReserved
	session.Status.Message = "destination PVCs are bound"
	s.touch(session)

	return s.save(ctx, session, false)
}

func (s *Service) Copy(ctx context.Context, session *Session, retries int, noCompress bool) error {
	if err := s.validateSession(ctx, session); err != nil {
		return err
	}

	if s.store != nil {
		return s.withLock(
			ctx,
			session,
			func(locked context.Context) error { return s.copy(locked, session, retries, noCompress) },
		)
	}

	return s.copy(ctx, session, retries, noCompress)
}

func (s *Service) copy(ctx context.Context, session *Session, retries int, noCompress bool) error {
	if err := s.validateSession(ctx, session); err != nil {
		return err
	}

	if session.Status.Phase == PhaseCleaned || session.Status.Phase == PhaseCleaning {
		return errors.New("cross-cluster session is already being cleaned or has been cleaned")
	}

	if err := s.reserve(ctx, session); err != nil {
		return err
	}

	if s.copier == nil {
		return errors.New("copy engine is unavailable")
	}

	session.Status.Phase = PhaseTransferring
	session.Status.Message = "copying PVC data"
	s.touch(session)

	if err := s.save(ctx, session, false); err != nil {
		return err
	}

	if retries < 1 {
		retries = 1
	}

	for i := range session.Spec.Volumes {
		status := &session.Status.Volumes[i]
		if status.Transfer.CompletedAt != nil {
			if err := s.validateDestinationVolume(ctx, session, i); err != nil {
				status.Transfer.LastError = err.Error()
				session.Status.Phase = PhaseFailed
				session.Status.Message = err.Error()
				s.touch(session)

				if saveErr := s.save(ctx, session, false); saveErr != nil {
					return errors.Join(err, saveErr)
				}

				return err
			}

			continue
		}

		volume := &session.Spec.Volumes[i]
		if err := s.validateTransferVolume(ctx, session, i); err != nil {
			status.Transfer.LastError = err.Error()
			session.Status.Phase = PhaseFailed
			session.Status.Message = err.Error()
			s.touch(session)

			if saveErr := s.save(ctx, session, false); saveErr != nil {
				return errors.Join(err, saveErr)
			}

			return err
		}

		var last error

		previousAttempts := status.Transfer.Attempts
		for retry := 1; retry <= retries; retry++ {
			attempt := previousAttempts + retry
			status.Transfer.Attempts = attempt
			req := copyengine.Request{
				SessionID:                 session.ID + "-" + volume.Source.PVC.Name,
				ToolImage:                 session.Spec.ToolImage,
				Source:                    objectRef(volume.Source.PVC),
				Destination:               objectRef(volume.Destination.PVC),
				SourcePath:                volume.Transfer.SourcePath,
				DestinationPath:           volume.Transfer.DestinationPath,
				Mode:                      copyengine.ModeFinal,
				Attempt:                   attempt,
				KubeconfigPath:            s.sourceKubeconfig,
				Context:                   s.sourceContext,
				DestinationKubeconfigPath: s.destinationKubeconfig,
				DestinationContext:        s.destinationContext,
				Strategies:                session.Spec.Strategies,
				DeleteExtraneousFiles:     session.Spec.DeleteExtraneous,
				VerifyChecksum:            session.Spec.VerifyChecksum,
				IgnoreSizes: capacitySmaller(
					volume.Destination.Capacity,
					volume.Source.Capacity,
				),
				NoCompress:  noCompress,
				HelmTimeout: s.helmTimeout,
				Writer:      s.writer,
				Logger:      s.logger,
			}

			last = s.copier.Copy(ctx, req, nil)
			if last == nil {
				break
			}
		}

		if last != nil {
			status.Transfer.LastError = last.Error()
			session.Status.Phase = PhaseFailed
			session.Status.Message = last.Error()
			s.touch(session)
			_ = s.save(ctx, session, false)

			return last
		}

		now := metav1.NewTime(s.now().UTC())
		status.Transfer.CompletedAt = &now
		status.Transfer.LastError = ""

		if err := s.save(ctx, session, false); err != nil {
			return err
		}
	}

	now := metav1.NewTime(s.now().UTC())
	session.Status.CompletedAt = &now
	session.Status.Phase = PhaseCompleted
	session.Status.Message = "cross-cluster copy completed"
	s.touch(session)

	return s.save(ctx, session, false)
}

func (s *Service) Cleanup(
	ctx context.Context,
	session *Session,
	deleteDestination, deleteSession bool,
) error {
	if s.store != nil {
		return s.withLock(ctx, session, func(locked context.Context) error {
			return s.cleanup(locked, session, deleteDestination, deleteSession)
		})
	}

	return s.cleanup(ctx, session, deleteDestination, deleteSession)
}

func (s *Service) cleanup(
	ctx context.Context,
	session *Session,
	deleteDestination, deleteSession bool,
) error {
	if err := s.validateSession(ctx, session); err != nil {
		return err
	}

	if deleteSession && !deleteDestination {
		for i, volume := range session.Spec.Volumes {
			status := session.Status.Volumes[i].Reservation
			if volume.Destination.PVC.UID != "" || volume.Destination.PV.UID != "" ||
				status.PVC.UID != "" || status.PV.UID != "" {
				return errors.New(
					"deleting a cross-cluster session with destination resources requires --delete-destination",
				)
			}

			if err := s.rejectUnrecordedDestinationResources(ctx, session, i); err != nil {
				return err
			}
		}
	}

	session.Status.Phase = PhaseCleaning
	session.Status.Message = "cleaning cross-cluster resources"
	s.touch(session)

	if err := s.save(ctx, session, false); err != nil {
		return err
	}

	if deleteDestination {
		for i := range session.Spec.Volumes {
			if err := s.cleanupDestinationVolume(ctx, session, i); err != nil {
				return err
			}

			session.Status.Message = fmt.Sprintf(
				"cleaned destination PVC %s/%s",
				session.Spec.Volumes[i].Destination.PVC.Namespace,
				session.Spec.Volumes[i].Destination.PVC.Name,
			)
			s.touch(session)

			if err := s.save(ctx, session, false); err != nil {
				return err
			}
		}
	}

	session.Status.Phase = PhaseCleaned

	session.Status.Message = "cross-cluster cleanup completed"
	if !deleteDestination {
		session.Status.Message = "cross-cluster session cleaned; destination resources were retained"
	}

	s.touch(session)

	if deleteSession {
		if err := s.delete(ctx, session); err != nil {
			return err
		}

		if cleaner, ok := s.store.(kube.SessionLeaseCleaner); ok {
			return cleaner.DeleteSessionLease(ctx, session.Spec.SessionNamespace, session.ID)
		}

		return nil
	}

	return s.save(ctx, session, false)
}

func (s *Service) rejectUnrecordedDestinationResources(
	ctx context.Context,
	session *Session,
	index int,
) error {
	volume := &session.Spec.Volumes[index]
	pvcs := s.destination.Kubernetes.CoreV1().
		PersistentVolumeClaims(volume.Destination.PVC.Namespace)

	_, err := pvcs.Get(ctx, volume.Destination.PVC.Name, metav1.GetOptions{})
	if err == nil {
		return fmt.Errorf(
			"destination PVC %s/%s exists but its identity is not recorded; use --delete-destination after inspecting it",
			volume.Destination.PVC.Namespace,
			volume.Destination.PVC.Name,
		)
	}

	if !apierrors.IsNotFound(err) {
		return fmt.Errorf(
			"inspect destination PVC %s/%s before deleting session: %w",
			volume.Destination.PVC.Namespace,
			volume.Destination.PVC.Name,
			err,
		)
	}

	pvs, err := s.destination.Kubernetes.CoreV1().
		PersistentVolumes().
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf(
			"inspect destination PVs before deleting session: %w",
			err,
		)
	}

	for _, pv := range pvs.Items {
		claimRef := pv.Spec.ClaimRef
		if claimRef != nil &&
			claimRef.Namespace == volume.Destination.PVC.Namespace &&
			claimRef.Name == volume.Destination.PVC.Name {
			return fmt.Errorf(
				"destination PV %s claims PVC %s/%s but its identity is not recorded; use --delete-destination after inspecting it",
				pv.Name,
				volume.Destination.PVC.Namespace,
				volume.Destination.PVC.Name,
			)
		}
	}

	return nil
}

func (s *Service) cleanupDestinationVolume(ctx context.Context, session *Session, index int) error {
	volume := &session.Spec.Volumes[index]

	client := s.destination.Kubernetes
	if err := s.deleteReservationConsumer(ctx, session, index); err != nil {
		return err
	}

	pvc, err := client.CoreV1().
		PersistentVolumeClaims(volume.Destination.PVC.Namespace).
		Get(ctx, volume.Destination.PVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return s.cleanupDestinationPV(ctx, volume, "")
	}

	if err != nil {
		return fmt.Errorf(
			"read destination PVC %s/%s: %w",
			volume.Destination.PVC.Namespace,
			volume.Destination.PVC.Name,
			err,
		)
	}

	if volume.Destination.PVC.UID != "" && pvc.UID != volume.Destination.PVC.UID {
		return fmt.Errorf("destination PVC %s/%s UID changed", pvc.Namespace, pvc.Name)
	}

	if pvc.Labels[SessionKey] != session.ID || pvc.Labels[ManagedByLabel] != ManagedBy {
		return fmt.Errorf(
			"destination PVC %s/%s ownership changed; refusing to delete it",
			pvc.Namespace,
			pvc.Name,
		)
	}

	volume.Destination.PVC.UID = pvc.UID

	pvName := pvc.Spec.VolumeName
	if volume.Destination.PV.Name != "" && pvName != "" && volume.Destination.PV.Name != pvName {
		return fmt.Errorf(
			"destination PVC %s/%s is bound to unexpected PV %s",
			pvc.Namespace,
			pvc.Name,
			pvName,
		)
	}

	if err := ensureNoActiveConsumers(ctx, client, pvc.Namespace, pvc.Name); err != nil {
		return err
	}

	uid := pvc.UID
	if err := client.CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Delete(ctx, pvc.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil &&
		!apierrors.IsNotFound(err) {
		return fmt.Errorf("delete destination PVC %s/%s: %w", pvc.Namespace, pvc.Name, err)
	}

	return s.cleanupDestinationPV(ctx, volume, pvName)
}

func (s *Service) cleanupDestinationPV(
	ctx context.Context,
	volume *VolumeSpec,
	pvName string,
) error {
	if pvName == "" {
		pvName = volume.Destination.PV.Name
	}

	if pvName == "" {
		return nil
	}

	client := s.destination.Kubernetes.CoreV1().PersistentVolumes()

	return kube.WaitFor(
		ctx,
		s.interval,
		"destination PV "+pvName+" release",
		func(waitCtx context.Context) (bool, error) {
			pv, err := client.Get(waitCtx, pvName, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			if err != nil {
				return false, err
			}

			if volume.Destination.PV.UID != "" && pv.UID != volume.Destination.PV.UID {
				return false, fmt.Errorf("destination PV %s UID changed; refusing cleanup", pvName)
			}

			if pv.Spec.ClaimRef != nil &&
				(pv.Spec.ClaimRef.Namespace != volume.Destination.PVC.Namespace || pv.Spec.ClaimRef.Name != volume.Destination.PVC.Name || volume.Destination.PVC.UID == "" || pv.Spec.ClaimRef.UID != volume.Destination.PVC.UID) {
				return false, fmt.Errorf(
					"destination PV %s claim reference changed; refusing cleanup",
					pvName,
				)
			}

			if pv.Status.Phase == corev1.VolumeFailed {
				return false, fmt.Errorf(
					"destination PV %s is Failed; inspect the provisioner before deleting it",
					pvName,
				)
			}

			if pv.Status.Phase != corev1.VolumeReleased &&
				pv.Status.Phase != corev1.VolumeAvailable {
				return false, nil
			}

			uid := pv.UID

			preconditions := &metav1.Preconditions{UID: &uid}
			if pv.ResourceVersion != "" {
				resourceVersion := pv.ResourceVersion
				preconditions.ResourceVersion = &resourceVersion
			}

			if err := client.Delete(
				waitCtx,
				pv.Name,
				metav1.DeleteOptions{Preconditions: preconditions},
			); err != nil &&
				!apierrors.IsNotFound(err) {
				return false, err
			}

			return true, nil
		},
	)
}

func (s *Service) deleteReservationConsumer(
	ctx context.Context,
	session *Session,
	index int,
) error {
	ref := session.Status.Volumes[index].Reservation.ConsumerPod
	if ref.Name == "" {
		return nil
	}

	pods := s.destination.Kubernetes.CoreV1().Pods(ref.Namespace)

	pod, err := pods.Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return err
	}

	if (ref.UID != "" && pod.UID != ref.UID) || pod.Labels[SessionKey] != session.ID ||
		pod.Labels[ManagedByLabel] != ManagedBy {
		return fmt.Errorf("reservation Pod %s/%s ownership or UID changed", pod.Namespace, pod.Name)
	}

	uid := pod.UID
	if err := pods.Delete(
		ctx,
		pod.Name,
		metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}},
	); err != nil &&
		!apierrors.IsNotFound(err) {
		return err
	}

	return kube.WaitFor(
		ctx,
		s.interval,
		"reservation Pod "+pod.Namespace+"/"+pod.Name+" deletion",
		func(waitCtx context.Context) (bool, error) {
			current, err := pods.Get(waitCtx, pod.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			if err != nil {
				return false, err
			}

			if current.UID != pod.UID {
				return false, fmt.Errorf(
					"reservation Pod %s/%s UID changed during deletion",
					pod.Namespace,
					pod.Name,
				)
			}

			return false, nil
		},
	)
}

func ensureNoActiveConsumers(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, pvcName string,
) error {
	consumers, err := activeConsumers(ctx, client, namespace, pvcName)
	if err != nil {
		return fmt.Errorf("check destination PVC %s/%s consumers: %w", namespace, pvcName, err)
	}

	if len(consumers) > 0 {
		return fmt.Errorf(
			"destination PVC %s/%s has active consumers: %s; stop them before cleanup",
			namespace,
			pvcName,
			strings.Join(consumers, ", "),
		)
	}

	return nil
}

func (s *Service) reserveVolume(ctx context.Context, session *Session, index int) error {
	v := &session.Spec.Volumes[index]

	capacity, err := resource.ParseQuantity(v.Destination.Capacity)
	if err != nil {
		return err
	}

	clients := s.destination.Kubernetes

	pvc, err := clients.CoreV1().
		PersistentVolumeClaims(v.Destination.PVC.Namespace).
		Get(ctx, v.Destination.PVC.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		storageClass := v.Destination.StorageClass.Name
		pvc = &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:        v.Destination.PVC.Name,
				Namespace:   v.Destination.PVC.Namespace,
				Labels:      map[string]string{ManagedByLabel: ManagedBy, SessionKey: session.ID},
				Annotations: map[string]string{SessionKey: session.ID},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: append(
					[]corev1.PersistentVolumeAccessMode(nil),
					v.Destination.AccessModes...,
				),
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: capacity},
				},
				StorageClassName: &storageClass,
				VolumeMode:       &v.Destination.VolumeMode,
			},
		}
		pvc, err = clients.CoreV1().
			PersistentVolumeClaims(v.Destination.PVC.Namespace).
			Create(ctx, pvc, metav1.CreateOptions{})
	}

	if err != nil {
		return fmt.Errorf(
			"create destination PVC %s/%s: %w",
			v.Destination.PVC.Namespace,
			v.Destination.PVC.Name,
			err,
		)
	}

	if pvc.Labels[ManagedByLabel] != ManagedBy || pvc.Labels[SessionKey] != session.ID {
		return fmt.Errorf("destination PVC %s/%s is not owned by session", pvc.Namespace, pvc.Name)
	}

	if v.Destination.PVC.UID != "" && pvc.UID != v.Destination.PVC.UID {
		return fmt.Errorf("destination PVC %s/%s UID changed", pvc.Namespace, pvc.Name)
	}

	if err := validateDestinationPVCSpec(pvc, v); err != nil {
		return err
	}

	v.Destination.PVC.UID = pvc.UID

	storageClass, err := clients.StorageV1().
		StorageClasses().
		Get(ctx, v.Destination.StorageClass.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if storageClass.UID != v.Destination.StorageClass.UID {
		return fmt.Errorf("destination StorageClass %s UID changed", storageClass.Name)
	}

	if storageClass.VolumeBindingMode != nil &&
		*storageClass.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer &&
		v.Destination.PV.UID == "" {
		if err := s.createReservationConsumer(ctx, session, v); err != nil {
			return err
		}

		if err := s.save(ctx, session, false); err != nil {
			return fmt.Errorf("persist reservation Pod ownership: %w", err)
		}
	}

	err = kube.WaitFor(
		ctx,
		s.interval,
		"destination PVC "+pvc.Namespace+"/"+pvc.Name+" binding",
		func(waitCtx context.Context) (bool, error) {
			current, e := clients.CoreV1().
				PersistentVolumeClaims(pvc.Namespace).
				Get(waitCtx, pvc.Name, metav1.GetOptions{})
			if e != nil {
				return false, e
			}

			if current.UID != pvc.UID {
				return false, errors.New("destination PVC UID changed")
			}

			if current.Status.Phase != corev1.ClaimBound {
				return false, nil
			}

			if current.Spec.VolumeName == "" {
				return false, nil
			}

			v.Destination.PVC.UID = current.UID
			v.Destination.PVC.ResourceVersion = current.ResourceVersion

			pv, pvErr := clients.CoreV1().
				PersistentVolumes().
				Get(waitCtx, current.Spec.VolumeName, metav1.GetOptions{})
			if pvErr != nil {
				return false, pvErr
			}

			v.Destination.PV = ClusterResourceRef{
				ClusterID:  session.Spec.DestinationCluster.ID,
				APIVersion: "v1",
				Kind:       "PersistentVolume",
				Name:       pv.Name,
				UID:        pv.UID,
			}

			status := &session.Status.Volumes[volumeIndex(session, v.Source.PVC.Name)]
			if ref := status.Reservation.ConsumerPod; ref.UID != "" {
				if deleteErr := s.deleteReservationConsumer(
					waitCtx,
					session,
					volumeIndex(session, v.Source.PVC.Name),
				); deleteErr != nil {
					return false, deleteErr
				}

				now := metav1.NewTime(s.now().UTC())
				status.Reservation.CompletedAt = &now
			}

			return true, nil
		},
	)

	return err
}

func validateDestinationPVCSpec(pvc *corev1.PersistentVolumeClaim, volume *VolumeSpec) error {
	if pvc == nil || volume == nil {
		return errors.New("destination PVC specification is unavailable")
	}

	if pvc.Spec.StorageClassName == nil ||
		*pvc.Spec.StorageClassName != volume.Destination.StorageClass.Name {
		return fmt.Errorf(
			"destination PVC %s/%s StorageClass does not match the plan",
			pvc.Namespace,
			pvc.Name,
		)
	}

	if pvc.Spec.VolumeMode == nil || *pvc.Spec.VolumeMode != volume.Destination.VolumeMode {
		return fmt.Errorf(
			"destination PVC %s/%s volume mode does not match the plan",
			pvc.Namespace,
			pvc.Name,
		)
	}

	requested, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if !ok {
		return fmt.Errorf("destination PVC %s/%s has no storage request", pvc.Namespace, pvc.Name)
	}

	expected, err := resource.ParseQuantity(volume.Destination.Capacity)
	if err != nil || requested.Cmp(expected) != 0 {
		return fmt.Errorf(
			"destination PVC %s/%s storage request does not match the plan",
			pvc.Namespace,
			pvc.Name,
		)
	}

	if !sameAccessModes(pvc.Spec.AccessModes, volume.Destination.AccessModes) {
		return fmt.Errorf(
			"destination PVC %s/%s access modes do not match the plan",
			pvc.Namespace,
			pvc.Name,
		)
	}

	return nil
}

func sameAccessModes(left, right []corev1.PersistentVolumeAccessMode) bool {
	if len(left) != len(right) {
		return false
	}

	set := make(map[corev1.PersistentVolumeAccessMode]struct{}, len(left))
	for _, mode := range left {
		set[mode] = struct{}{}
	}

	for _, mode := range right {
		if _, ok := set[mode]; !ok {
			return false
		}
	}

	return true
}

func (s *Service) createReservationConsumer(
	ctx context.Context,
	session *Session,
	volume *VolumeSpec,
) error {
	name := reservationConsumerName(session.ID, volume.Source.PVC.Name)

	node := session.Spec.TargetNode
	if node == "" || node == domain.AutoValue {
		return fmt.Errorf(
			"WFFC destination PVC %s/%s requires a resolved target node",
			volume.Destination.PVC.Namespace,
			volume.Destination.PVC.Name,
		)
	}

	target, err := s.destination.Kubernetes.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read destination node %s: %w", node, err)
	}

	hostname := target.Labels[corev1.LabelHostname]
	if hostname == "" {
		return fmt.Errorf("destination node %s lacks %s", node, corev1.LabelHostname)
	}

	runAs := int64(0)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: volume.Destination.PVC.Namespace,
			Labels:    map[string]string{ManagedByLabel: ManagedBy, SessionKey: session.ID},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			NodeSelector:  map[string]string{corev1.LabelHostname: hostname},
			Tolerations:   nodeTolerations(target),
			Containers: []corev1.Container{
				{
					Name:            "reserve",
					Image:           session.Spec.ToolImage,
					Command:         []string{"sh", "-c", "test -d /data && sleep 3600"},
					SecurityContext: &corev1.SecurityContext{RunAsUser: &runAs, RunAsGroup: &runAs},
					VolumeMounts:    []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: volume.Destination.PVC.Name,
						},
					},
				},
			},
		},
	}
	client := s.destination.Kubernetes.CoreV1().Pods(pod.Namespace)

	existing, err := client.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		existing, err = client.Create(ctx, pod, metav1.CreateOptions{})
	}

	if err != nil {
		return fmt.Errorf("create reservation Pod %s/%s: %w", pod.Namespace, name, err)
	}

	if existing.Labels[ManagedByLabel] != ManagedBy || existing.Labels[SessionKey] != session.ID {
		return fmt.Errorf(
			"reservation Pod %s/%s is not owned by session",
			existing.Namespace,
			existing.Name,
		)
	}

	volumeStatus := &session.Status.Volumes[volumeIndex(session, volume.Source.PVC.Name)]
	volumeStatus.Reservation.ConsumerPod = ClusterResourceRef{
		ClusterID:  session.Spec.DestinationCluster.ID,
		APIVersion: "v1",
		Kind:       "Pod",
		Namespace:  existing.Namespace,
		Name:       existing.Name,
		UID:        existing.UID,
	}

	return kube.WaitFor(
		ctx,
		s.interval,
		"reservation Pod "+existing.Namespace+"/"+existing.Name+" scheduling",
		func(waitCtx context.Context) (bool, error) {
			current, e := client.Get(waitCtx, name, metav1.GetOptions{})
			if e != nil {
				return false, e
			}

			if current.UID != existing.UID {
				return false, errors.New("reservation Pod UID changed")
			}

			if current.Status.Phase == corev1.PodFailed {
				return false, fmt.Errorf(
					"reservation Pod %s/%s failed",
					current.Namespace,
					current.Name,
				)
			}

			for _, condition := range current.Status.Conditions {
				if condition.Type == corev1.PodScheduled &&
					condition.Status == corev1.ConditionTrue {
					return true, nil
				}
			}

			return current.Status.Phase == corev1.PodRunning, nil
		},
	)
}

func reservationConsumerName(sessionID, pvc string) string {
	name := sessionID + "-reserve-" + pvc
	if len(name) <= 63 {
		return name
	}

	digest := sha256.Sum256([]byte(name))
	suffix := fmt.Sprintf("-%x", digest[:4])
	prefix := strings.TrimRight(name[:63-len(suffix)], "-")

	return prefix + suffix
}

func volumeIndex(session *Session, name string) int {
	for i, v := range session.Spec.Volumes {
		if v.Source.PVC.Name == name {
			return i
		}
	}

	return 0
}

func nodeTolerations(node *corev1.Node) []corev1.Toleration {
	var out []corev1.Toleration
	for _, taint := range node.Spec.Taints {
		if taint.Effect == corev1.TaintEffectNoSchedule ||
			taint.Effect == corev1.TaintEffectNoExecute {
			out = append(
				out,
				corev1.Toleration{
					Key:      taint.Key,
					Operator: corev1.TolerationOpEqual,
					Value:    taint.Value,
					Effect:   taint.Effect,
				},
			)
		}
	}

	return out
}

func (s *Service) selectTargetNode(
	ctx context.Context,
	requested string,
	sc *storagev1.StorageClass,
) (*corev1.Node, error) {
	nodes, err := s.destination.Kubernetes.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list destination nodes: %w", err)
	}

	for i := range nodes.Items {
		node := &nodes.Items[i]
		if requested != "" && requested != domain.AutoValue && requested != node.Name {
			continue
		}

		if node.Spec.Unschedulable || !nodeReady(node) ||
			(sc.AllowedTopologies != nil && !matchesAllowedTopologies(sc, node)) {
			continue
		}

		return node, nil
	}

	if requested != "" && requested != domain.AutoValue {
		return nil, fmt.Errorf(
			"destination node %s is not Ready, schedulable, or allowed by StorageClass topology",
			requested,
		)
	}

	return nil, fmt.Errorf(
		"no Ready, schedulable destination node is compatible with StorageClass %s",
		sc.Name,
	)
}

func nodeReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}

func matchesAllowedTopologies(sc *storagev1.StorageClass, node *corev1.Node) bool {
	if len(sc.AllowedTopologies) == 0 {
		return true
	}

	for _, term := range sc.AllowedTopologies {
		matched := true
		for _, expr := range term.MatchLabelExpressions {
			value, ok := node.Labels[expr.Key]
			if !ok {
				matched = false
				break
			}

			found := slices.Contains(expr.Values, value)

			if !found {
				matched = false
				break
			}
		}

		if matched {
			return true
		}
	}

	return false
}

func hasSharedAccessMode(modes []corev1.PersistentVolumeAccessMode) bool {
	for _, mode := range modes {
		if mode == corev1.ReadWriteMany || mode == corev1.ReadOnlyMany {
			return true
		}
	}

	return false
}

func (s *Service) validateClients() error {
	if s == nil || s.source == nil || s.destination == nil || s.source.Kubernetes == nil ||
		s.destination.Kubernetes == nil {
		return errors.New("source and destination Kubernetes clients are required")
	}

	return nil
}

func (s *Service) validateSession(ctx context.Context, session *Session) error {
	if err := session.Validate(); err != nil {
		return err
	}

	src, err := kube.Identity(ctx, s.source)
	if err != nil {
		return err
	}

	dst, err := kube.Identity(ctx, s.destination)
	if err != nil {
		return err
	}

	if src.ID != session.Spec.SourceCluster.ID || dst.ID != session.Spec.DestinationCluster.ID {
		return errors.New("connected cluster identity does not match session")
	}

	return nil
}

func (s *Service) validateTransferVolume(ctx context.Context, session *Session, index int) error {
	if index < 0 || index >= len(session.Spec.Volumes) {
		return fmt.Errorf("cross-cluster volume index %d is invalid", index)
	}

	volume := &session.Spec.Volumes[index]

	sourcePVC, err := s.source.Kubernetes.CoreV1().
		PersistentVolumeClaims(volume.Source.PVC.Namespace).
		Get(ctx, volume.Source.PVC.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf(
			"read source PVC %s/%s before copy: %w",
			volume.Source.PVC.Namespace,
			volume.Source.PVC.Name,
			err,
		)
	}

	if sourcePVC.UID != volume.Source.PVC.UID ||
		sourcePVC.Spec.VolumeName != volume.Source.PV.Name ||
		sourcePVC.Status.Phase != corev1.ClaimBound {
		return fmt.Errorf(
			"source PVC %s/%s identity or binding changed; generate a new session",
			sourcePVC.Namespace,
			sourcePVC.Name,
		)
	}

	sourcePV, err := s.source.Kubernetes.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.Source.PV.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read source PV %s before copy: %w", volume.Source.PV.Name, err)
	}

	if sourcePV.UID != volume.Source.PV.UID || sourcePV.Spec.ClaimRef == nil ||
		sourcePV.Spec.ClaimRef.Namespace != sourcePVC.Namespace ||
		sourcePV.Spec.ClaimRef.Name != sourcePVC.Name ||
		sourcePV.Spec.ClaimRef.UID != sourcePVC.UID {
		return fmt.Errorf(
			"source PV %s identity or claim reference changed; generate a new session",
			sourcePV.Name,
		)
	}

	sourceCapacity, parseErr := resource.ParseQuantity(volume.Source.Capacity)

	actualSourceCapacity, hasSourceCapacity := sourcePV.Spec.Capacity[corev1.ResourceStorage]
	if parseErr != nil || !hasSourceCapacity || actualSourceCapacity.Cmp(sourceCapacity) != 0 {
		return fmt.Errorf("source PV %s capacity changed; generate a new session", sourcePV.Name)
	}

	consumers, err := activeConsumers(ctx, s.source.Kubernetes, sourcePVC.Namespace, sourcePVC.Name)
	if err != nil {
		return fmt.Errorf(
			"check source PVC %s/%s consumers: %w",
			sourcePVC.Namespace,
			sourcePVC.Name,
			err,
		)
	}

	if !session.Spec.Online && len(consumers) > 0 {
		return fmt.Errorf(
			"source PVC %s/%s has active consumers: %s",
			sourcePVC.Namespace,
			sourcePVC.Name,
			strings.Join(consumers, ", "),
		)
	}

	if session.Spec.Online && len(consumers) > 0 &&
		!hasSharedAccessMode(sourcePVC.Spec.AccessModes) {
		return fmt.Errorf(
			"online cross-cluster copy requires RWX/ROX while PVC %s/%s has active consumers",
			sourcePVC.Namespace,
			sourcePVC.Name,
		)
	}

	return s.validateDestinationVolume(ctx, session, index)
}

func (s *Service) validateDestinationVolume(
	ctx context.Context,
	session *Session,
	index int,
) error {
	if index < 0 || index >= len(session.Spec.Volumes) {
		return fmt.Errorf("cross-cluster volume index %d is invalid", index)
	}

	volume := &session.Spec.Volumes[index]

	destinationPVC, err := s.destination.Kubernetes.CoreV1().
		PersistentVolumeClaims(volume.Destination.PVC.Namespace).
		Get(ctx, volume.Destination.PVC.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf(
			"read destination PVC %s/%s before copy: %w",
			volume.Destination.PVC.Namespace,
			volume.Destination.PVC.Name,
			err,
		)
	}

	if destinationPVC.UID != volume.Destination.PVC.UID ||
		destinationPVC.Labels[ManagedByLabel] != ManagedBy ||
		destinationPVC.Labels[SessionKey] != session.ID {
		return fmt.Errorf(
			"destination PVC %s/%s identity or ownership changed; refusing to copy",
			destinationPVC.Namespace,
			destinationPVC.Name,
		)
	}

	if err := validateDestinationPVCSpec(destinationPVC, volume); err != nil {
		return err
	}

	if destinationPVC.Spec.VolumeName == "" ||
		destinationPVC.Spec.VolumeName != volume.Destination.PV.Name {
		return fmt.Errorf(
			"destination PVC %s/%s binding changed; refusing to copy",
			destinationPVC.Namespace,
			destinationPVC.Name,
		)
	}

	destinationPV, err := s.destination.Kubernetes.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.Destination.PV.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read destination PV %s before copy: %w", volume.Destination.PV.Name, err)
	}

	if destinationPV.UID != volume.Destination.PV.UID || destinationPV.Spec.ClaimRef == nil ||
		destinationPV.Spec.ClaimRef.Namespace != destinationPVC.Namespace ||
		destinationPV.Spec.ClaimRef.Name != destinationPVC.Name ||
		destinationPV.Spec.ClaimRef.UID != destinationPVC.UID {
		return fmt.Errorf(
			"destination PV %s identity or claim reference changed; refusing to copy",
			destinationPV.Name,
		)
	}

	destinationCapacity, parseErr := resource.ParseQuantity(volume.Destination.Capacity)

	actualDestinationCapacity, hasDestinationCapacity := destinationPV.Spec.Capacity[corev1.ResourceStorage]
	if parseErr != nil || !hasDestinationCapacity ||
		actualDestinationCapacity.Cmp(destinationCapacity) < 0 {
		return fmt.Errorf(
			"destination PV %s capacity is smaller than the session request; refusing to copy",
			destinationPV.Name,
		)
	}

	return nil
}

func (s *Service) touch(
	session *Session,
) {
	session.Status.UpdatedAt = metav1.NewTime(s.now().UTC())
}

const sessionPrefix = "pvc-migrate-cross-cluster-"

func sessionName(id string) string { return sessionPrefix + id }
func (s *Service) save(ctx context.Context, session *Session, create bool) error {
	if err := session.Validate(); err != nil {
		return err
	}

	raw, err := json.Marshal(session)
	if err != nil {
		return err
	}

	cmClient := s.source.Kubernetes.CoreV1().ConfigMaps(session.Spec.SessionNamespace)
	if create {
		created, createErr := cmClient.Create(
			ctx,
			&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      sessionName(session.ID),
					Namespace: session.Spec.SessionNamespace,
					Labels:    map[string]string{ManagedByLabel: ManagedBy, SessionKey: session.ID},
				},
				Data: map[string]string{"session.json": string(raw)},
			},
			metav1.CreateOptions{},
		)
		if createErr == nil {
			session.ResourceVersion = created.ResourceVersion
		}

		err = createErr

		return err
	}

	cm, err := cmClient.Get(ctx, sessionName(session.ID), metav1.GetOptions{})
	if err != nil {
		return err
	}

	if session.ResourceVersion != "" && cm.ResourceVersion != session.ResourceVersion {
		return errors.New("cross-cluster session changed while operation was running")
	}

	if cm.Labels[ManagedByLabel] != ManagedBy || cm.Labels[SessionKey] != session.ID {
		return errors.New("cross-cluster session ConfigMap ownership changed")
	}

	cm.Data = map[string]string{"session.json": string(raw)}

	updated, err := cmClient.Update(ctx, cm, metav1.UpdateOptions{})
	if err == nil {
		session.ResourceVersion = updated.ResourceVersion
	}

	return err
}

func (s *Service) Get(ctx context.Context, namespace, id string) (*Session, error) {
	if s == nil || s.source == nil {
		return nil, errors.New("source client is required")
	}

	if err := ValidateSessionID(id); err != nil {
		return nil, err
	}

	cm, err := s.source.Kubernetes.CoreV1().
		ConfigMaps(namespace).
		Get(ctx, sessionName(id), metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	var session Session
	if err := json.Unmarshal([]byte(cm.Data["session.json"]), &session); err != nil {
		return nil, err
	}

	if cm.Name != sessionName(id) || cm.Namespace != namespace ||
		cm.Labels[ManagedByLabel] != ManagedBy ||
		cm.Labels[SessionKey] != id ||
		session.ID != id ||
		session.Spec.SessionNamespace != namespace {
		return nil, fmt.Errorf(
			"cross-cluster session ConfigMap ownership does not match session %q",
			id,
		)
	}

	session.ResourceVersion = cm.ResourceVersion

	return &session, session.Validate()
}

func (s *Service) delete(ctx context.Context, session *Session) error {
	cm, err := s.source.Kubernetes.CoreV1().
		ConfigMaps(session.Spec.SessionNamespace).
		Get(ctx, sessionName(session.ID), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return err
	}

	if cm.UID == "" || cm.Name != sessionName(session.ID) ||
		cm.Namespace != session.Spec.SessionNamespace ||
		cm.Labels[ManagedByLabel] != ManagedBy ||
		cm.Labels[SessionKey] != session.ID {
		return errors.New(
			"cross-cluster session ConfigMap ownership changed; refusing to delete it",
		)
	}

	var persisted Session
	if err := json.Unmarshal(
		[]byte(cm.Data["session.json"]),
		&persisted,
	); err != nil || persisted.ID != session.ID || persisted.Kind != Kind ||
		persisted.APIVersion != APIVersion {
		return errors.New(
			"cross-cluster session ConfigMap contents do not match session; refusing to delete it",
		)
	}

	if session.ResourceVersion != "" && cm.ResourceVersion != session.ResourceVersion {
		return errors.New("cross-cluster session changed while deleting")
	}

	return s.source.Kubernetes.CoreV1().
		ConfigMaps(cm.Namespace).
		Delete(ctx, cm.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &cm.UID}})
}

func (s *Service) withLock(
	ctx context.Context,
	session *Session,
	fn func(context.Context) error,
) error {
	if session == nil {
		return errors.New("cross-cluster session is required")
	}

	lock, err := s.store.AcquireSessionLock(ctx, session.Spec.SessionNamespace, session.ID)
	if err != nil {
		return err
	}

	operationCtx, cancelOperation := lock.Bind(ctx)
	operationErr := fn(operationCtx)

	cancelOperation()

	releaseErr := lock.Release(context.Background())

	return errors.Join(operationErr, lock.Err(), releaseErr)
}

func objectRef(v ClusterResourceRef) domain.ObjectReference {
	return domain.ObjectReference{
		APIVersion:      v.APIVersion,
		Kind:            v.Kind,
		Namespace:       v.Namespace,
		Name:            v.Name,
		UID:             v.UID,
		ResourceVersion: v.ResourceVersion,
	}
}

func capacitySmaller(destination, source string) bool {
	d, err1 := resource.ParseQuantity(destination)
	s, err2 := resource.ParseQuantity(source)
	return err1 == nil && err2 == nil && d.Cmp(s) < 0
}

func normalizeStrategies(in []string) []string {
	if len(in) == 0 {
		return []string{"local"}
	}
	return append([]string(nil), in...)
}

func validateStrategies(in []string) error {
	for _, v := range in {
		switch v {
		case "local", "loadbalancer", "nodeport":
		default:
			return fmt.Errorf(
				"strategy %q is not supported for cross-cluster copy; use local, loadbalancer, or nodeport",
				v,
			)
		}
	}

	return nil
}

func activeConsumers(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name string,
) ([]string, error) {
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var out []string
	for i := range pods.Items {
		if pods.Items[i].Status.Phase == corev1.PodSucceeded ||
			pods.Items[i].Status.Phase == corev1.PodFailed {
			continue
		}

		if kube.ActivePodUsesPVC(&pods.Items[i], name) {
			out = append(out, pods.Items[i].Name)
		}
	}

	sort.Strings(out)

	return out, nil
}

func resolveNames(values, source []string) ([]string, error) {
	if len(values) == 0 {
		out := make([]string, len(source))
		for i := range source {
			out[i] = source[i] + "-copy"
		}

		return out, nil
	}

	if len(values) == 1 && !strings.Contains(values[0], "=") && len(source) == 1 {
		return []string{values[0]}, nil
	}

	if len(source) > 1 {
		for _, value := range values {
			if !strings.Contains(value, "=") {
				return nil, errors.New(
					"multiple source PVCs require explicit destination mappings such as source=destination",
				)
			}
		}
	}

	out := make([]string, len(source))

	seen := map[string]bool{}
	for _, v := range values {
		key, val, ok := strings.Cut(v, "=")
		if !ok || key == "" || val == "" {
			return nil, fmt.Errorf("destination PVC mapping %q must use source=name", v)
		}

		matched := false
		for i, s := range source {
			if s == key {
				matched = true

				if seen[key] {
					return nil, fmt.Errorf("duplicate destination PVC mapping for %s", key)
				}

				out[i] = val
				seen[key] = true
			}
		}

		if !matched {
			return nil, fmt.Errorf("destination PVC mapping references unknown source PVC %s", key)
		}
	}

	for i := range out {
		if out[i] == "" {
			return nil, fmt.Errorf("destination PVC mapping is missing source %s", source[i])
		}
	}

	seenDestinations := make(map[string]struct{}, len(out))
	for _, destination := range out {
		if _, exists := seenDestinations[destination]; exists {
			return nil, fmt.Errorf(
				"destination PVC %s is mapped from more than one source PVC",
				destination,
			)
		}

		seenDestinations[destination] = struct{}{}
	}

	return out, nil
}

func resolveValues(values, source []string) ([]string, error) {
	if len(values) == 0 {
		return make([]string, len(source)), nil
	}

	if len(values) == 1 && !strings.Contains(values[0], "=") && len(source) == 1 {
		return []string{values[0]}, nil
	}

	if len(source) > 1 {
		for _, value := range values {
			if !strings.Contains(value, "=") {
				return nil, errors.New(
					"multiple source PVCs require explicit capacity mappings such as source=capacity",
				)
			}
		}
	}

	out := make([]string, len(source))

	seen := map[string]bool{}
	for _, v := range values {
		key, val, ok := strings.Cut(v, "=")
		if !ok || key == "" || val == "" {
			return nil, fmt.Errorf("capacity mapping %q must use source=capacity", v)
		}

		found := false
		for i, s := range source {
			if s == key {
				if seen[key] {
					return nil, fmt.Errorf("duplicate capacity mapping for %s", key)
				}

				out[i] = val
				seen[key] = true
				found = true
			}
		}

		if !found {
			return nil, fmt.Errorf("capacity mapping references unknown source PVC %s", key)
		}
	}

	return out, nil
}

func resolvePaths(values, source []string) ([]string, error) {
	if len(values) == 0 {
		out := make([]string, len(source))
		for i := range out {
			out[i] = "."
		}

		return out, nil
	}

	if len(values) == 1 && !strings.Contains(values[0], "=") && len(source) == 1 {
		return []string{values[0]}, nil
	}

	if len(source) > 1 {
		for _, value := range values {
			if !strings.Contains(value, "=") {
				return nil, errors.New(
					"multiple source PVCs require explicit path mappings such as source=path",
				)
			}
		}
	}

	out := make([]string, len(source))

	seen := map[string]bool{}
	for _, v := range values {
		key, val, ok := strings.Cut(v, "=")
		if !ok || key == "" || val == "" {
			return nil, fmt.Errorf("path mapping %q must use source=path", v)
		}

		matched := false
		for i, s := range source {
			if s == key {
				matched = true

				if seen[key] {
					return nil, fmt.Errorf("duplicate path mapping for %s", key)
				}

				out[i] = val
				seen[key] = true
			}
		}

		if !matched {
			return nil, fmt.Errorf("path mapping references unknown source PVC %s", key)
		}
	}

	for i := range out {
		if out[i] == "" {
			return nil, fmt.Errorf("path mapping is missing source %s", source[i])
		}
	}

	return out, nil
}
