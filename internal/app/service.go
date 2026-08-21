package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const openEBSLVMSharedMountCleanupTimeout = 10 * time.Second

type Config struct {
	KubeconfigPath                string
	Context                       string
	Retries                       int
	RetryBackoff                  time.Duration
	HelmTimeout                   time.Duration
	NoCompress                    bool
	StreamToolLogs                bool
	StructuredLogs                bool
	Writer                        io.Writer
	Logger                        *slog.Logger
	ToolImageProber               kube.ToolImageProber
	VolumeUsageReader             kube.VolumeUsageReader
	OpenEBSLVMSharedVolumeManager kube.OpenEBSLVMSharedVolumeManager
}

type Service struct {
	client      kubernetes.Interface
	store       kube.SessionStore
	reserver    volumeReserver
	copier      copyengine.Engine
	controllers workloadController
	switcher    volumeSwitcher
	config      Config
	now         func() time.Time
	sleep       func(context.Context, time.Duration) error
}

type volumeReserver interface {
	ReserveVolume(
		ctx context.Context,
		session *domain.Session,
		volume *domain.VolumeSpec,
		status *domain.VolumeStatus,
		recreate bool,
	) error
}

type workloadController interface {
	Pause(ctx context.Context, session *domain.Session) error
	Resume(ctx context.Context, session *domain.Session) error
	VerifyPaused(ctx context.Context, session *domain.Session) error
}

type volumeSwitcher interface {
	VerifyVolumeOffline(ctx context.Context, volume *domain.VolumeSpec) error
	ActivateVolume(
		ctx context.Context,
		session *domain.Session,
		volume *domain.VolumeSpec,
		status *domain.VolumeStatus,
		progress kube.ProgressFunc,
	) error
	RollbackVolume(
		ctx context.Context,
		session *domain.Session,
		volume *domain.VolumeSpec,
		status *domain.VolumeStatus,
		progress kube.ProgressFunc,
	) error
	RenamePVC(
		ctx context.Context,
		session *domain.Session,
		volume *domain.VolumeSpec,
		progress kube.ProgressFunc,
	) (*corev1.PersistentVolumeClaim, error)
}

type batchVolumeSwitcher interface {
	VerifyVolumesOffline(ctx context.Context, volumes []*domain.VolumeSpec) error
}

type sessionBatchVolumeSwitcher interface {
	VerifyVolumesOfflineForSession(
		ctx context.Context,
		sessionID string,
		volumes []*domain.VolumeSpec,
	) error
}

func NewService(
	client kubernetes.Interface,
	store kube.SessionStore,
	reserver volumeReserver,
	copier copyengine.Engine,
	controllers workloadController,
	switcher volumeSwitcher,
	config Config,
) *Service {
	if config.Retries <= 0 {
		config.Retries = 3
	}

	if config.RetryBackoff <= 0 {
		config.RetryBackoff = 2 * time.Second
	}

	if config.HelmTimeout <= 0 {
		config.HelmTimeout = 10 * time.Minute
	}

	if config.Writer == nil {
		config.Writer = io.Discard
	}

	if config.Logger == nil {
		config.Logger = slog.New(slog.DiscardHandler)
	}

	return &Service{
		client:      client,
		store:       store,
		reserver:    reserver,
		copier:      copier,
		controllers: controllers,
		switcher:    switcher,
		config:      config,
		now:         time.Now,
		sleep:       sleepContext,
	}
}

type sessionLockContextKey struct{}

type heldSessionLock struct {
	lock      kube.SessionLock
	namespace string
	id        string
}

// withSessionIDLock serializes every mutating session operation when the
// configured store supports Kubernetes Lease fencing. The context marker
// makes nested stage calls re-entrant while preserving the same lease.
func (s *Service) withSessionIDLock(
	ctx context.Context,
	namespace, id string,
	fn func(context.Context) error,
) error {
	if namespace == "" || id == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"session lock",
			"session namespace and ID are required",
		)
	}

	if held, ok := ctx.Value(sessionLockContextKey{}).(heldSessionLock); ok &&
		held.namespace == namespace &&
		held.id == id {
		if err := held.lock.Err(); err != nil {
			return err
		}
		return fn(ctx)
	}

	locker, supported := s.store.(kube.SessionLocker)
	if !supported {
		return fn(ctx)
	}

	s.logInfo("acquiring session lock", "session", id, "namespace", namespace)

	lock, err := locker.AcquireSessionLock(ctx, namespace, id)
	if err != nil {
		return err
	}

	operationCtx, cancelOperation := lock.Bind(ctx)
	defer cancelOperation()

	lockedCtx := context.WithValue(
		operationCtx,
		sessionLockContextKey{},
		heldSessionLock{lock: lock, namespace: namespace, id: id},
	)

	operationErr := fn(lockedCtx)
	if leaseErr := lock.Err(); leaseErr != nil {
		operationErr = errors.Join(operationErr, leaseErr)
	}

	releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelRelease()

	releaseErr := lock.Release(releaseCtx)
	if releaseErr != nil {
		s.logWarn(
			"session lock release failed",
			"session",
			id,
			"namespace",
			namespace,
			"error",
			releaseErr,
		)
	} else {
		s.logInfo("session lock released", "session", id, "namespace", namespace)
	}

	return errors.Join(operationErr, releaseErr)
}

func (s *Service) withSessionLock(
	ctx context.Context,
	session *domain.Session,
	fn func(context.Context) error,
) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "session lock", "session is nil")
	}
	return s.withSessionIDLock(ctx, session.Spec.SessionNamespace, session.ID, fn)
}

func (s *Service) CreateSession(
	ctx context.Context,
	plan *domain.MigrationPlan,
	dryRun bool,
) (*domain.Session, error) {
	if plan == nil {
		return nil, domain.NewError(domain.ErrorValidation, "create session", "plan is nil")
	}

	if !plan.Ready {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"create session",
			"migration plan contains failed checks",
		)
	}

	session := domain.NewSession(plan.SessionID, plan.SessionSpec, s.now())
	if dryRun {
		if err := s.ensureSessionNamespaces(ctx, plan, true); err != nil {
			return nil, err
		}

		if err := s.ValidateReservation(ctx, session); err != nil {
			return nil, err
		}

		return session, nil
	}

	s.logInfo(
		"creating migration session",
		"session",
		plan.SessionID,
		"sessionNamespace",
		plan.SessionSpec.SessionNamespace,
		"temporaryNamespace",
		plan.SessionSpec.TemporaryNamespace,
	)
	// The Lease used to serialize creation lives in the session namespace.
	// Ensure that namespace exists before attempting to acquire the Lease.
	if err := kube.EnsureNamespace(
		ctx,
		s.client,
		plan.SessionSpec.SessionNamespace,
		plan.SessionID,
		false,
	); err != nil {
		return nil, err
	}

	createErr := s.withSessionIDLock(
		ctx,
		plan.SessionSpec.SessionNamespace,
		plan.SessionID,
		func(lockedCtx context.Context) error {
			if err := s.ensureSessionNamespaces(lockedCtx, plan, false); err != nil {
				return err
			}
			return s.store.Create(lockedCtx, session)
		},
	)
	if createErr != nil {
		return nil, createErr
	}

	return session, nil
}

func (s *Service) probeToolImage(
	ctx context.Context,
	session *domain.Session,
	targets []kube.ToolProbeTarget,
) ([]kube.ToolImageProbeResult, error) {
	if session == nil || s.config.ToolImageProber == nil {
		return nil, nil
	}

	if len(targets) == 0 {
		return nil, nil
	}

	s.logInfo(
		"tool image validation started",
		"session",
		session.ID,
		"image",
		session.Spec.WorkflowOptions().ToolImage,
		"targets",
		len(targets),
	)

	return s.config.ToolImageProber.Probe(ctx, kube.ToolImageProbeOptions{
		OperationID: session.ID,
		Image:       session.Spec.WorkflowOptions().ToolImage,
		Targets:     targets,
		Timeout:     s.config.HelmTimeout,
		Writer:      s.config.Writer,
		Logger:      s.config.Logger,
	})
}

func reservationToolProbeTargets(session *domain.Session) []kube.ToolProbeTarget {
	if session == nil {
		return nil
	}

	options := session.Spec.WorkflowOptions()
	if options.TargetNode == "" {
		return nil
	}

	return toolProbeTargetsForNamespaces(
		destinationVolumeNamespaces(session),
		options.TargetNode,
		nil,
	)
}

func copyToolProbeTargets(session *domain.Session, mountSourcePVC bool) []kube.ToolProbeTarget {
	if session == nil {
		return nil
	}

	options := session.Spec.WorkflowOptions()
	switch session.Spec.Operation() {
	case domain.OperationCopy, domain.OperationMigrate, domain.OperationMigratePod:
		targets := destinationTransferPathProbeTargets(session, options.TargetNode)
		if options.TargetNode == "" {
			return targets
		}

		targetNamespaces := destinationVolumeNamespaces(session)
		targets = append(
			targets,
			toolProbeTargetsForNamespaces(
				targetNamespaces,
				options.TargetNode,
				[]string{kube.ToolComponentRsync},
			)...,
		)
		needsSSHD := sessionNeedsSourceSSHD(session)

		if slices.Contains(options.Strategies, domain.StrategyLocal) {
			targets = append(
				targets,
				toolProbeTargetsForNamespaces(
					targetNamespaces,
					options.TargetNode,
					[]string{kube.ToolComponentSSHD},
				)...,
			)
		}

		if (!needsSSHD && !mountSourcePVC) || options.SourceNode == "" {
			return targets
		}

		targets = append(
			targets,
			sourceToolProbeTargets(session, options.SourceNode, mountSourcePVC)...,
		)

		return targets
	}

	return nil
}

func sessionNeedsSourceSSHD(session *domain.Session) bool {
	if session == nil {
		return false
	}

	operation := session.Spec.Operation()
	if operation != domain.OperationCopy && operation != domain.OperationMigrate &&
		operation != domain.OperationMigratePod {
		return false
	}

	strategies := session.Spec.WorkflowOptions().Strategies
	if len(strategies) == 0 {
		return true
	}

	for _, strategy := range strategies {
		if strategy != domain.StrategyMount {
			return true
		}
	}

	return false
}

func (s *Service) resolveCopyToolProbeTargets(
	ctx context.Context,
	session *domain.Session,
	mountSourcePVC bool,
) ([]kube.ToolProbeTarget, error) {
	targets := copyToolProbeTargets(session, mountSourcePVC)
	if !sessionNeedsSourceSSHD(session) && !mountSourcePVC {
		return targets, nil
	}

	options := session.Spec.WorkflowOptions()

	sourceTargets, err := s.resolveSourceToolProbeTargets(ctx, session, mountSourcePVC)
	if err != nil {
		return nil, err
	}

	if options.SourceNode == "" {
		targets = append(targets, sourceTargets...)
	}

	if mountSourcePVC {
		if err := s.markSharedOpenEBSLVMProbeMounts(ctx, session, targets); err != nil {
			return nil, err
		}
	}

	return targets, nil
}

func (s *Service) markSharedOpenEBSLVMProbeMounts(
	ctx context.Context,
	session *domain.Session,
	targets []kube.ToolProbeTarget,
) error {
	if session == nil {
		return nil
	}

	probedPVCs := make(map[string]struct{}, len(targets))
	for index := range targets {
		target := &targets[index]
		if target.PVCName == "" || target.SkipPVCMount {
			continue
		}

		probedPVCs[target.Namespace+"/"+target.PVCName] = struct{}{}
	}

	sharedPVCs := make(map[string]bool, len(probedPVCs))
	for volumeIndex := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[volumeIndex]

		key := volume.SourcePVC.Namespace + "/" + volume.SourcePVC.Name
		if _, probed := probedPVCs[key]; !probed {
			continue
		}

		isLVM, shared, err := s.sharedOpenEBSLVMSource(ctx, session, volume)
		if err != nil {
			return err
		}

		if isLVM && !shared {
			active, err := s.sourcePVCIsActive(ctx, volume)
			if err != nil {
				return err
			}

			if active && !session.Spec.WorkflowOptions().OpenEBSLVMEnableShared {
				return activeUnsharedOpenEBSLVMError(session, volume)
			}
		}

		sharedPVCs[key] = shared
	}

	for index := range targets {
		target := &targets[index]
		if target.PVCName != "" && !target.SkipPVCMount &&
			sharedPVCs[target.Namespace+"/"+target.PVCName] {
			target.WritablePVCMount = true
		}
	}

	return nil
}

func activeUnsharedOpenEBSLVMError(session *domain.Session, volume *domain.VolumeSpec) error {
	recovery := "stop all active PVC consumers and retry"
	if session != nil {
		switch session.Spec.Operation() {
		case domain.OperationCopy:
			recovery = "abort this pre-cutover session, clean its retained resources, and rerun the copy without --online"
		case domain.OperationMigrate, domain.OperationMigratePod:
			if session.Spec.WorkflowOptions().OpenEBSLVMEnableShared {
				recovery = "retry the session so temporary shared-mount preparation can use the current active consumer state"
			} else {
				recovery = "abort this pre-cutover session, clean its retained resources, and rerun with --precopy-passes 0 or --openebs-lvm-enable-shared"
			}
		}
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		"warm-copy mount preflight",
		fmt.Sprintf(
			"source PVC %s/%s is active and its OpenEBS LVMVolume does not currently have spec.shared=yes; %s",
			volume.SourcePVC.Namespace,
			volume.SourcePVC.Name,
			recovery,
		),
	)
}

func (s *Service) sharedOpenEBSLVMSource(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
) (bool, bool, error) {
	manager := s.config.OpenEBSLVMSharedVolumeManager
	if manager == nil {
		return false, false, nil
	}

	isLVM, err := s.openEBSLVMSource(ctx, volume)
	if err != nil || !isLVM {
		return false, false, err
	}

	sharedSessionID := ""

	expectedLVMVolume := domain.ObjectReference{}
	if state, found := pendingOpenEBSLVMSharedMount(session, volume); found {
		sharedSessionID = session.ID
		expectedLVMVolume = state.LVMVolume
	}

	shared, err := manager.Shared(ctx, volume.SourcePV, expectedLVMVolume, sharedSessionID)

	return true, shared, err
}

func pendingOpenEBSLVMSharedMount(
	session *domain.Session,
	volume *domain.VolumeSpec,
) (domain.OpenEBSLVMSharedMount, bool) {
	if session == nil || volume == nil {
		return domain.OpenEBSLVMSharedMount{}, false
	}

	for _, state := range session.Status.OpenEBSLVMSharedMounts {
		if state.SourcePV.Name == volume.SourcePV.Name &&
			state.SourcePV.UID == volume.SourcePV.UID {
			return state, true
		}
	}

	return domain.OpenEBSLVMSharedMount{}, false
}

func (s *Service) openEBSLVMSource(ctx context.Context, volume *domain.VolumeSpec) (bool, error) {
	if s == nil || s.client == nil || volume == nil {
		return false, nil
	}

	if volume.SourcePVC.Namespace == "" || volume.SourcePVC.Name == "" ||
		volume.SourcePV.Name == "" {
		return false, domain.NewError(
			domain.ErrorValidation,
			"OpenEBS LVM shared mount",
			"source PVC and PV identities are required",
		)
	}

	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			"OpenEBS LVM shared mount",
			fmt.Sprintf("read source PVC %s/%s", volume.SourcePVC.Namespace, volume.SourcePVC.Name),
			err,
		)
	}

	if pvc.UID != volume.SourcePVC.UID || pvc.Status.Phase != corev1.ClaimBound ||
		pvc.Spec.VolumeName != volume.SourcePV.Name {
		return false, domain.NewError(
			domain.ErrorConflict,
			"OpenEBS LVM shared mount",
			fmt.Sprintf(
				"source PVC %s/%s identity or binding changed",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	pv, err := s.client.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			"OpenEBS LVM shared mount",
			"read source PV "+volume.SourcePV.Name,
			err,
		)
	}

	if pv.UID != volume.SourcePV.UID || pv.Spec.ClaimRef == nil ||
		pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		return false, domain.NewError(
			domain.ErrorConflict,
			"OpenEBS LVM shared mount",
			fmt.Sprintf("source PV %s identity or claimRef changed", volume.SourcePV.Name),
		)
	}

	return pv.Spec.CSI != nil && pv.Spec.CSI.Driver == kube.OpenEBSLVMCSIDriver, nil
}

func (s *Service) enableOpenEBSLVMSharedMounts(
	ctx context.Context,
	session *domain.Session,
) (resultErr error) {
	if session == nil || !session.Spec.WorkflowOptions().OpenEBSLVMEnableShared {
		return nil
	}

	manager := s.config.OpenEBSLVMSharedVolumeManager
	if manager == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"OpenEBS LVM shared mount",
			"OpenEBS LVMVolume manager is required when --openebs-lvm-enable-shared is set",
		)
	}
	defer func() {
		if resultErr == nil {
			return
		}

		if err := s.restoreOpenEBSLVMSharedMountsAfterFailure(ctx, session); err != nil {
			resultErr = errors.Join(resultErr, err)
		}
	}()

	type preparedSharedMount struct {
		state     domain.OpenEBSLVMSharedMount
		reference string
	}

	prepared := make([]preparedSharedMount, 0, len(session.Spec.Volumes))
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		isLVM, err := s.openEBSLVMSource(ctx, volume)
		if err != nil {
			return err
		}

		if !isLVM {
			continue
		}

		active, err := s.sourcePVCIsActive(ctx, volume)
		if err != nil {
			return err
		}

		if !active {
			continue
		}

		result, err := manager.PrepareShared(ctx, volume.SourcePV)
		if err != nil {
			return err
		}

		if !result.NeedsChange {
			continue
		}

		state := domain.OpenEBSLVMSharedMount{
			SourcePV:          volume.SourcePV,
			LVMVolume:         result.LVMVolume,
			PreviousShared:    result.PreviousShared,
			PreviousSharedSet: result.PreviousSharedSet,
		}
		prepared = append(prepared, preparedSharedMount{state: state, reference: result.Reference})
	}

	for _, item := range prepared {
		session.Status.OpenEBSLVMSharedMounts = append(
			session.Status.OpenEBSLVMSharedMounts,
			item.state,
		)
		if err := s.persist(ctx, session); err != nil {
			return err
		}

		if err := manager.EnableShared(ctx, session.ID, item.state); err != nil {
			return err
		}

		s.logInfo(
			"OpenEBS LVM shared mount configured",
			"sourcePV",
			item.state.SourcePV.Name,
			"resource",
			item.reference,
			"previousShared",
			item.state.PreviousShared,
		)
	}

	return nil
}

// A temporary shared-volume patch must be reverted even after the operation
// deadline or cancellation fires. Preserve context values such as the session
// lock while giving cleanup its own bounded lifetime.
func (s *Service) restoreOpenEBSLVMSharedMountsAfterFailure(
	ctx context.Context,
	session *domain.Session,
) error {
	if err := sessionFenceError(ctx); err != nil {
		return err
	}

	restoreCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		openEBSLVMSharedMountCleanupTimeout,
	)
	defer cancel()

	return s.restoreOpenEBSLVMSharedMounts(restoreCtx, session)
}

func sessionFenceError(ctx context.Context) error {
	if held, ok := ctx.Value(sessionLockContextKey{}).(heldSessionLock); ok {
		return held.lock.Err()
	}
	return nil
}

func (s *Service) validateOpenEBSLVMSharedMountRestore(
	ctx context.Context,
	session *domain.Session,
) error {
	if session == nil || len(session.Status.OpenEBSLVMSharedMounts) == 0 {
		return nil
	}

	manager := s.config.OpenEBSLVMSharedVolumeManager
	if manager == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"restore OpenEBS LVM shared mount",
			"OpenEBS LVMVolume manager is required to restore session-managed shared mounts",
		)
	}

	for _, state := range session.Status.OpenEBSLVMSharedMounts {
		if err := manager.ValidateRestoreShared(ctx, session.ID, state); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) restoreOpenEBSLVMSharedMounts(
	ctx context.Context,
	session *domain.Session,
) error {
	if session == nil {
		return nil
	}

	if err := sessionFenceError(ctx); err != nil {
		return err
	}

	if err := kube.CleanupSessionToolProbePods(
		ctx,
		s.client,
		session.ID,
		sessionToolProbeNamespaces(session),
	); err != nil {
		return err
	}

	if len(session.Status.OpenEBSLVMSharedMounts) == 0 {
		return nil
	}

	manager := s.config.OpenEBSLVMSharedVolumeManager
	if manager == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"restore OpenEBS LVM shared mount",
			"OpenEBS LVMVolume manager is required to restore session-managed shared mounts",
		)
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	remaining := make([]domain.OpenEBSLVMSharedMount, 0, len(session.Status.OpenEBSLVMSharedMounts))
	for index, state := range session.Status.OpenEBSLVMSharedMounts {
		if err := sessionFenceError(ctx); err != nil {
			return err
		}

		if err := manager.RestoreShared(ctx, session.ID, state); err != nil {
			remaining = append(remaining, session.Status.OpenEBSLVMSharedMounts[index:]...)

			session.Status.OpenEBSLVMSharedMounts = remaining
			if persistErr := s.persist(ctx, session); persistErr != nil {
				return errors.Join(err, persistErr)
			}

			return err
		}

		s.logInfo(
			"OpenEBS LVM shared mount restored",
			"session",
			session.ID,
			"sourcePV",
			state.SourcePV.Name,
			"previousShared",
			state.PreviousShared,
			"previousSharedSet",
			state.PreviousSharedSet,
		)
	}

	if err := sessionFenceError(ctx); err != nil {
		return err
	}

	session.Status.OpenEBSLVMSharedMounts = nil

	return s.persist(ctx, session)
}

func sessionToolProbeNamespaces(session *domain.Session) []string {
	if session == nil {
		return nil
	}

	namespaces := []string{
		session.Spec.SourceNamespace,
		session.Spec.TemporaryNamespace,
		session.Spec.DestinationNamespace,
	}
	for _, volume := range session.Spec.Volumes {
		namespaces = append(namespaces, volume.SourcePVC.Namespace, volume.DestinationPVC.Namespace)
	}

	result := make([]string, 0, len(namespaces))
	for _, namespace := range namespaces {
		if namespace != "" && !slices.Contains(result, namespace) {
			result = append(result, namespace)
		}
	}

	sort.Strings(result)

	return result
}

func (s *Service) sourcePVCIsActive(ctx context.Context, volume *domain.VolumeSpec) (bool, error) {
	if s == nil || s.client == nil || volume == nil {
		return false, nil
	}

	pods, err := s.client.CoreV1().Pods(volume.SourcePVC.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, domain.WrapError(
			domain.ErrorKubernetes,
			"OpenEBS LVM shared mount",
			"list Pods in namespace "+volume.SourcePVC.Namespace,
			err,
		)
	}

	for index := range pods.Items {
		if kube.ActivePodUsesPVC(&pods.Items[index], volume.SourcePVC.Name) {
			return true, nil
		}
	}

	return false, nil
}

type sourcePodListResult struct {
	pods []corev1.Pod
	err  error
}

func (s *Service) resolveSourceToolProbeTargets(
	ctx context.Context,
	session *domain.Session,
	mountSourcePVC bool,
) ([]kube.ToolProbeTarget, error) {
	if session == nil {
		return nil, domain.NewError(domain.ErrorValidation, "tool image probe", "session is nil")
	}

	options := session.Spec.WorkflowOptions()
	namespaces := sourceVolumeNamespaces(session)

	podLists := make([]sourcePodListResult, len(namespaces))
	parallel.For(len(namespaces), func(index int) {
		pods, err := s.client.CoreV1().Pods(namespaces[index]).List(ctx, metav1.ListOptions{})
		if err == nil && pods == nil {
			err = fmt.Errorf("list PVC consumers in %s returned an empty object", namespaces[index])
		}

		if pods != nil {
			podLists[index].pods = pods.Items
		}

		podLists[index].err = err
	})

	resolvedNodes, needsTopology, activeCopyNodes, err := resolveConsumerNodes(
		session,
		namespaces,
		podLists,
		options.SourceNode,
	)
	if err != nil {
		return nil, err
	}

	if len(activeCopyNodes) > 1 {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"tool image probe",
			"online copy consumers span multiple source nodes",
		)
	}

	if options.SourceNode != "" {
		return nil, nil
	}

	needsAnyTopology := slices.Contains(needsTopology, true)

	var nodes []corev1.Node
	if needsAnyTopology {
		nodeList, err := s.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, domain.WrapError(
				domain.ErrorKubernetes,
				"tool image probe",
				"list nodes for source PV topology",
				err,
			)
		}

		if nodeList == nil {
			return nil, domain.NewError(
				domain.ErrorKubernetes,
				"tool image probe",
				"list nodes for source PV topology returned an empty object",
			)
		}

		nodes = nodeList.Items

		type pvResult struct {
			pv  *corev1.PersistentVolume
			err error
		}

		pvs := make([]pvResult, len(session.Spec.Volumes))
		parallel.For(len(session.Spec.Volumes), func(index int) {
			if !needsTopology[index] || session.Spec.Volumes[index].SourcePV.Name == "" {
				return
			}

			pvs[index].pv, pvs[index].err = s.client.CoreV1().
				PersistentVolumes().
				Get(ctx, session.Spec.Volumes[index].SourcePV.Name, metav1.GetOptions{})
		})

		for index := range pvs {
			if pvs[index].err != nil {
				return nil, domain.WrapError(
					domain.ErrorKubernetes,
					"tool image probe",
					"read source PV "+session.Spec.Volumes[index].SourcePV.Name,
					pvs[index].err,
				)
			}

			if pvs[index].pv != nil {
				resolvedNodes[index] = kube.PVUniqueNodeName(pvs[index].pv, nodes)
			}
		}
	}

	targets := make([]kube.ToolProbeTarget, 0, len(session.Spec.Volumes))

	var sourceComponents []string
	if sessionNeedsSourceSSHD(session) {
		sourceComponents = []string{kube.ToolComponentSSHD}
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		target := kube.ToolProbeTarget{
			Namespace:    volume.SourcePVC.Namespace,
			NodeName:     resolvedNodes[index],
			PVCName:      volume.SourcePVC.Name,
			SkipPVCMount: resolvedNodes[index] != "" && !mountSourcePVC,
			Components:   slices.Clone(sourceComponents),
		}
		if mountSourcePVC &&
			domain.SourceTransferPath(volume.TransferScope) != domain.VolumeRootPath {
			target.RequiredPath = domain.SourceTransferPath(volume.TransferScope)
		}

		targets = append(targets, target)
	}

	return targets, nil
}

func resolveConsumerNodes(
	session *domain.Session,
	namespaces []string,
	podLists []sourcePodListResult,
	sourceNode string,
) ([]string, []bool, map[string]struct{}, error) {
	resolved := make([]string, len(session.Spec.Volumes))
	needsTopology := make([]bool, len(session.Spec.Volumes))

	activeCopyNodes := map[string]struct{}{}
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		namespaceIndex := slices.Index(namespaces, volume.SourcePVC.Namespace)
		if namespaceIndex < 0 {
			return nil, nil, nil, domain.NewError(
				domain.ErrorInternal,
				"tool image probe",
				fmt.Sprintf("source namespace %s was not inventoried", volume.SourcePVC.Namespace),
			)
		}

		result := podLists[namespaceIndex]
		if result.err != nil {
			return nil, nil, nil, domain.WrapError(
				domain.ErrorKubernetes,
				"tool image probe",
				"list PVC consumers in "+volume.SourcePVC.Namespace,
				result.err,
			)
		}

		node, err := resolveVolumeConsumerNode(session, volume, result.pods)
		if err != nil {
			return nil, nil, nil, err
		}

		resolved[index] = node

		needsTopology[index] = node == ""
		if node != "" && session.Spec.Operation() == domain.OperationCopy {
			activeCopyNodes[node] = struct{}{}
		}

		if sourceNode != "" && node != "" && node != sourceNode {
			return nil, nil, nil, domain.NewError(
				domain.ErrorConflict,
				"tool image probe",
				fmt.Sprintf(
					"PVC %s/%s consumer runs on %s, session source node is %s",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					node,
					sourceNode,
				),
			)
		}
	}

	return resolved, needsTopology, activeCopyNodes, nil
}

func resolveVolumeConsumerNode(
	session *domain.Session,
	volume *domain.VolumeSpec,
	pods []corev1.Pod,
) (string, error) {
	activeCount, scheduledCount := 0, 0

	nodes := map[string]struct{}{}
	for index := range pods {
		pod := &pods[index]
		if !kube.ActivePodUsesPVC(pod, volume.SourcePVC.Name) {
			continue
		}

		activeCount++

		if pod.Spec.NodeName != "" {
			scheduledCount++
			nodes[pod.Spec.NodeName] = struct{}{}
		}
	}

	if err := validateCopyConsumers(session, volume, activeCount, scheduledCount); err != nil {
		return "", err
	}

	if len(nodes) > 1 {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"tool image probe",
			fmt.Sprintf(
				"PVC %s/%s active consumers span multiple nodes",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	for node := range nodes {
		return node, nil
	}

	return "", nil
}

func validateCopyConsumers(
	session *domain.Session,
	volume *domain.VolumeSpec,
	active, scheduled int,
) error {
	if session.Spec.Operation() != domain.OperationCopy || active == 0 {
		return nil
	}

	if !session.Spec.Online() {
		return domain.NewError(
			domain.ErrorPrecondition,
			"tool image probe",
			fmt.Sprintf(
				"offline copy requires PVC %s/%s to have zero active Pod consumers",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if kube.HasAccessMode(volume.AccessModes, corev1.ReadWriteOncePod) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"tool image probe",
			fmt.Sprintf(
				"active RWOP PVC %s/%s cannot be warm-copied",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if kube.HasAccessMode(volume.AccessModes, corev1.ReadWriteOnce) && scheduled != active {
		return domain.NewError(
			domain.ErrorPrecondition,
			"tool image probe",
			fmt.Sprintf(
				"every active consumer of RWO PVC %s/%s must be scheduled before online copy",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	return nil
}

func destinationVolumeNamespaces(session *domain.Session) []string {
	return volumeNamespaces(
		session,
		func(volume domain.VolumeSpec) string { return volume.DestinationPVC.Namespace },
		session.Spec.TemporaryNamespace,
	)
}

func sourceVolumeNamespaces(session *domain.Session) []string {
	return volumeNamespaces(
		session,
		func(volume domain.VolumeSpec) string { return volume.SourcePVC.Namespace },
		session.Spec.SourceNamespace,
	)
}

func volumeNamespaces(
	session *domain.Session,
	namespaceFor func(domain.VolumeSpec) string,
	fallback string,
) []string {
	namespaces := make([]string, 0, len(session.Spec.Volumes))
	for _, volume := range session.Spec.Volumes {
		namespace := namespaceFor(volume)
		if namespace != "" && !slices.Contains(namespaces, namespace) {
			namespaces = append(namespaces, namespace)
		}
	}

	if len(namespaces) == 0 && fallback != "" {
		namespaces = append(namespaces, fallback)
	}

	return namespaces
}

func toolProbeTargetsForNamespaces(
	namespaces []string,
	nodeName string,
	components []string,
) []kube.ToolProbeTarget {
	targets := make([]kube.ToolProbeTarget, 0, len(namespaces))
	for _, namespace := range namespaces {
		targets = append(
			targets,
			kube.ToolProbeTarget{
				Namespace:  namespace,
				NodeName:   nodeName,
				Components: slices.Clone(components),
			},
		)
	}

	return targets
}

func sourceToolProbeTargets(
	session *domain.Session,
	nodeName string,
	mountSourcePVC bool,
) []kube.ToolProbeTarget {
	if session == nil {
		return nil
	}

	targets := make([]kube.ToolProbeTarget, 0, len(session.Spec.Volumes))

	var sourceComponents []string
	if sessionNeedsSourceSSHD(session) {
		sourceComponents = []string{kube.ToolComponentSSHD}
	}

	for _, volume := range session.Spec.Volumes {
		target := kube.ToolProbeTarget{
			Namespace:    volume.SourcePVC.Namespace,
			NodeName:     nodeName,
			PVCName:      volume.SourcePVC.Name,
			SkipPVCMount: !mountSourcePVC,
			Components:   slices.Clone(sourceComponents),
		}
		if mountSourcePVC &&
			domain.SourceTransferPath(volume.TransferScope) != domain.VolumeRootPath {
			target.RequiredPath = domain.SourceTransferPath(volume.TransferScope)
		}

		targets = append(targets, target)
	}

	return targets
}

func destinationTransferPathProbeTargets(
	session *domain.Session,
	nodeName string,
) []kube.ToolProbeTarget {
	if session == nil {
		return nil
	}

	targets := make([]kube.ToolProbeTarget, 0, len(session.Spec.Volumes))
	for _, volume := range session.Spec.Volumes {
		path := domain.DestinationTransferPath(volume.TransferScope)
		if path == domain.VolumeRootPath {
			continue
		}

		targets = append(targets, kube.ToolProbeTarget{
			Namespace:        volume.DestinationPVC.Namespace,
			NodeName:         nodeName,
			PVCName:          volume.DestinationPVC.Name,
			WritablePVCMount: true,
			RequiredPath:     path,
			CreatePath:       true,
			Components:       []string{kube.ToolComponentRsync},
		})
	}

	return targets
}

func (s *Service) ensureSessionNamespaces(
	ctx context.Context,
	plan *domain.MigrationPlan,
	dryRun bool,
) error {
	s.logInfo("ensuring migration namespaces", "session", plan.SessionID, "dryRun", dryRun)

	if err := kube.EnsureNamespace(
		ctx,
		s.client,
		plan.SessionSpec.SessionNamespace,
		plan.SessionID,
		dryRun,
	); err != nil {
		return err
	}

	if plan.SessionSpec.TemporaryNamespace != plan.SessionSpec.SessionNamespace {
		if err := kube.EnsureNamespace(
			ctx,
			s.client,
			plan.SessionSpec.TemporaryNamespace,
			plan.SessionID,
			dryRun,
		); err != nil {
			return err
		}
	}

	ensured := map[string]struct{}{
		plan.SessionSpec.SessionNamespace:   {},
		plan.SessionSpec.TemporaryNamespace: {},
	}
	for _, volume := range plan.SessionSpec.Volumes {
		if _, ok := ensured[volume.DestinationPVC.Namespace]; ok {
			continue
		}

		if err := kube.EnsureNamespace(
			ctx,
			s.client,
			volume.DestinationPVC.Namespace,
			plan.SessionID,
			dryRun,
		); err != nil {
			return err
		}

		ensured[volume.DestinationPVC.Namespace] = struct{}{}
	}

	return nil
}

func (s *Service) ValidateReservation(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "reservation dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	if err := s.verifyShrinkUsage(ctx, session); err != nil {
		return err
	}

	s.logInfo(
		"reservation preflight started",
		"session",
		session.ID,
		"volumes",
		len(session.Spec.Volumes),
	)

	for index := range session.Spec.Volumes {
		volume := session.Spec.Volumes[index]

		status := session.Status.Volumes[index]
		if err := s.validateReservedVolume(ctx, session, &volume, &status); err != nil {
			return err
		}
	}

	return nil
}

// ValidateWarmCopy performs every read-only check needed before reservation
// or warm-copy mutation. It deliberately leaves source-node inference and
// temporary OpenEBS shared-mount preparation unpersisted.
func (s *Service) ValidateWarmCopy(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "warm copy dry-run", "session is nil")
	}

	if err := validateRetryableSessionFailure(session); err != nil {
		return err
	}

	valid := session.Status.Phase == domain.PhasePlanned ||
		session.Status.Phase == domain.PhaseReserving ||
		session.Status.Phase == domain.PhaseReserved ||
		session.Status.Phase == domain.PhaseWarmCopied ||
		session.Status.Phase == domain.PhaseWarmCopying ||
		(session.Status.Phase == domain.PhaseFailed && (session.Status.ResumeFrom == domain.PhaseReserving || session.Status.ResumeFrom == domain.PhaseWarmCopying))
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"warm copy dry-run",
			fmt.Sprintf("session phase %s cannot warm-copy", session.Status.Phase),
		)
	}

	if err := s.ValidateReservation(ctx, session); err != nil {
		return err
	}

	if err := s.validateCopyConsumersBatch(ctx, session, false); err != nil {
		return err
	}

	if err := s.validateWarmCopyOpenEBSLVM(ctx, session); err != nil {
		return err
	}

	_, err := s.resolveSourceToolProbeTargets(ctx, session, true)

	return err
}

func (s *Service) verifyShrinkUsage(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "source usage check", "session is nil")
	}

	options := session.Spec.WorkflowOptions()
	for _, volume := range session.Spec.Volumes {
		source, sourceErr := resource.ParseQuantity(volume.SourceCapacity)

		destination, destinationErr := resource.ParseQuantity(volume.Capacity)
		if sourceErr != nil || destinationErr != nil || destination.Cmp(source) >= 0 {
			continue
		}

		if options.SkipSourceUsageCheck {
			s.logWarn(
				"source usage check skipped by explicit approval",
				"session",
				session.ID,
				"pvc",
				volume.SourcePVC.Name,
				"destinationCapacity",
				destination.String(),
			)

			continue
		}

		if s.config.VolumeUsageReader == nil {
			return domain.NewError(
				domain.ErrorPrecondition,
				"source usage check",
				fmt.Sprintf(
					"PVC %s/%s has no trusted storage-backend CRD usage reader; pass --skip-source-usage-check only after independently verifying that its data fits destination capacity %s",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					destination.String(),
				),
			)
		}

		usage, err := s.config.VolumeUsageReader.Read(
			ctx,
			kube.VolumeUsageReadOptions{SourcePVC: volume.SourcePVC, SourcePV: volume.SourcePV},
		)
		if err != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"source usage check",
				fmt.Sprintf(
					"PVC %s/%s usage could not be read from its storage backend CRD; pass --skip-source-usage-check only after independently verifying that its data fits",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
				),
				err,
			)
		}

		if usage.UsedBytes < 0 {
			return domain.NewError(
				domain.ErrorPrecondition,
				"source usage check",
				fmt.Sprintf(
					"PVC %s/%s storage backend returned invalid used bytes %d",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					usage.UsedBytes,
				),
			)
		}

		usageSource := strings.TrimSpace(usage.Source)
		if usageSource == "" {
			usageSource = "the storage backend CRD"
		}

		if usage.UsedBytes > destination.Value() {
			if sourcePath := domain.SourceTransferPath(
				volume.TransferScope,
			); sourcePath != domain.VolumeRootPath {
				return domain.NewError(
					domain.ErrorConflict,
					"source usage check",
					fmt.Sprintf(
						"PVC %s/%s whole-volume usage is %d bytes according to %s, above destination capacity %s; this cannot prove that selected source directory %q fits; abort this session and create a new one with a larger destination, or use --skip-source-usage-check only after independently measuring the selected data",
						volume.SourcePVC.Namespace,
						volume.SourcePVC.Name,
						usage.UsedBytes,
						usageSource,
						destination.String(),
						sourcePath,
					),
				)
			}

			return domain.NewError(
				domain.ErrorConflict,
				"source usage check",
				fmt.Sprintf(
					"PVC %s/%s uses %d bytes according to %s, above destination capacity %s; increase --destination-capacity or abort this shrink",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					usage.UsedBytes,
					usageSource,
					destination.String(),
				),
			)
		}
	}

	return nil
}

func (s *Service) validateWarmCopyOpenEBSLVM(ctx context.Context, session *domain.Session) error {
	manager := s.config.OpenEBSLVMSharedVolumeManager
	if manager == nil {
		return nil
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		isLVM, err := s.openEBSLVMSource(ctx, volume)
		if err != nil {
			return err
		}

		if !isLVM {
			continue
		}

		active, err := s.sourcePVCIsActive(ctx, volume)
		if err != nil {
			return err
		}

		if !active {
			continue
		}

		if state, pending := pendingOpenEBSLVMSharedMount(session, volume); pending {
			previousShared := strings.TrimSpace(state.PreviousShared)
			if state.PreviousSharedSet && previousShared != "" &&
				!strings.EqualFold(previousShared, "no") &&
				!strings.EqualFold(previousShared, "yes") {
				return domain.NewError(
					domain.ErrorPrecondition,
					"OpenEBS LVM shared mount",
					fmt.Sprintf(
						"LVMVolume %s/%s has unsupported recorded spec.shared value %q",
						state.LVMVolume.Namespace,
						state.LVMVolume.Name,
						state.PreviousShared,
					),
				)
			}

			needsChange := !state.PreviousSharedSet || previousShared == "" ||
				strings.EqualFold(previousShared, "no")
			if needsChange && !session.Spec.WorkflowOptions().OpenEBSLVMEnableShared {
				return activeUnsharedOpenEBSLVMError(session, volume)
			}

			continue
		}

		prepared, err := manager.PrepareShared(ctx, volume.SourcePV)
		if err != nil {
			return err
		}

		if prepared.NeedsChange && !session.Spec.WorkflowOptions().OpenEBSLVMEnableShared {
			return activeUnsharedOpenEBSLVMError(session, volume)
		}
	}

	return nil
}

func (s *Service) validateReservedVolume(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
) error {
	checkVolume := *volume
	checkStatus := *status
	return s.reserver.ReserveVolume(ctx, session, &checkVolume, &checkStatus, true)
}

// ValidateResume performs the read-only checks for the next resumable stage.
// Copy and controller mutations remain behind --dry-run=false.
func (s *Service) ValidateResume(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "resume dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if err := validateRetryableSessionFailure(session); err != nil {
		return err
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	switch session.Spec.Operation() {
	case domain.OperationReserve,
		domain.OperationCopy,
		domain.OperationRename,
		domain.OperationMove:
		return s.validateSingleOperationResume(ctx, session, phase)
	case domain.OperationBackup:
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume session",
			"backup sessions require the backup resume workflow",
		)
	}

	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving, domain.PhaseReserved:
		if session.Status.WarmPassesCompleted < session.Spec.PrecopyPasses() {
			return s.ValidateWarmCopy(ctx, session)
		}
		return s.ValidateReservation(ctx, session)
	case domain.PhaseWarmCopying:
		return s.ValidateWarmCopy(ctx, session)
	case domain.PhaseWarmCopied:
		if session.Status.WarmPassesCompleted < session.Spec.PrecopyPasses() {
			return s.ValidateWarmCopy(ctx, session)
		}
		return s.ValidateReservation(ctx, session)
	case domain.PhasePausing:
		return s.ValidateReservation(ctx, session)
	case domain.PhasePaused, domain.PhaseFinalSyncing:
		return s.ValidateFinalSync(ctx, session)
	case domain.PhaseFinalSynced, domain.PhaseActivating:
		return s.ValidateActivation(ctx, session)
	case domain.PhaseActivated:
		if err := s.controllers.VerifyPaused(ctx, session); err != nil {
			return err
		}
		return s.validateWorkloadResume(ctx, session)
	case domain.PhaseResuming:
		return s.validateWorkloadResume(ctx, session)
	case domain.PhaseRenaming, domain.PhaseMoving:
		return s.validateOfflineVolumes(ctx, session)
	case domain.PhaseRollingBack:
		return s.ValidateRollback(ctx, session)
	case domain.PhaseAborting:
		return s.ValidateAbort(ctx, session)
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return nil
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume dry-run",
			fmt.Sprintf("phase %s cannot be resumed", phase),
		)
	}
}

func (s *Service) validateSingleOperationResume(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
) error {
	if err := validateSingleResumePhase(session.Spec.Operation(), phase); err != nil {
		return err
	}

	switch phase {
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return nil
	case domain.PhaseRollingBack:
		return s.ValidateRollback(ctx, session)
	case domain.PhaseAborting:
		return s.ValidateAbort(ctx, session)
	}

	switch session.Spec.Operation() {
	case domain.OperationReserve:
		switch phase {
		case domain.PhasePlanned, domain.PhaseReserving:
			return s.ValidateReservation(ctx, session)
		case domain.PhaseReserved:
			return nil
		default:
			return domain.NewError(
				domain.ErrorPrecondition,
				"resume dry-run",
				fmt.Sprintf("phase %s cannot be resumed for %s", phase, session.Spec.Operation()),
			)
		}
	case domain.OperationCopy:
		switch phase {
		case domain.PhasePlanned, domain.PhaseReserving:
			return s.ValidateWarmCopy(ctx, session)
		case domain.PhaseReserved, domain.PhaseWarmCopying:
			return s.ValidateWarmCopy(ctx, session)
		case domain.PhaseWarmCopied:
			return nil
		default:
			return domain.NewError(
				domain.ErrorPrecondition,
				"resume dry-run",
				fmt.Sprintf("phase %s cannot be resumed for %s", phase, session.Spec.Operation()),
			)
		}
	case domain.OperationRename, domain.OperationMove:
		expected := domain.PhaseRenaming
		if session.Spec.Operation() == domain.OperationMove {
			expected = domain.PhaseMoving
		}

		switch phase {
		case domain.PhasePlanned, expected:
			return s.validateRebindOfflineVolumes(ctx, session)
		case domain.PhaseCompleted:
			return nil
		default:
			return domain.NewError(
				domain.ErrorPrecondition,
				"resume dry-run",
				fmt.Sprintf("phase %s cannot be resumed for %s", phase, session.Spec.Operation()),
			)
		}
	default:
		return nil
	}
}

func validateSingleResumePhase(operation domain.Operation, phase domain.Phase) error {
	allowed := false
	switch operation {
	case domain.OperationReserve:
		allowed = phase == domain.PhasePlanned || phase == domain.PhaseReserving ||
			phase == domain.PhaseReserved
	case domain.OperationCopy:
		allowed = phase == domain.PhasePlanned || phase == domain.PhaseReserving ||
			phase == domain.PhaseReserved ||
			phase == domain.PhaseWarmCopying ||
			phase == domain.PhaseWarmCopied
	case domain.OperationRename:
		allowed = phase == domain.PhasePlanned || phase == domain.PhaseRenaming
	case domain.OperationMove:
		allowed = phase == domain.PhasePlanned || phase == domain.PhaseMoving
	}

	switch phase {
	case domain.PhaseCompleted,
		domain.PhaseAborted,
		domain.PhaseRolledBack,
		domain.PhaseRollingBack,
		domain.PhaseAborting:
		allowed = true
	}

	if !allowed {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume session",
			fmt.Sprintf("phase %s cannot be resumed for operation %s", phase, operation),
		)
	}

	return nil
}

// ValidateAbort checks the phase and any paused workload that abort would
// resume. It performs no controller or resource mutation.
func (s *Service) ValidateAbort(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "abort dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed || phase == domain.PhaseAborting {
		phase = session.Status.ResumeFrom
	}

	if phase == domain.PhaseRollingBack {
		return domain.NewError(
			domain.ErrorPrecondition,
			"abort dry-run",
			"rollback recovery must continue through session resume or rollback",
		)
	}

	if phase == domain.PhaseActivated || phase == domain.PhaseCompleted ||
		phase == domain.PhaseResuming ||
		session.Status.ResumeFrom == domain.PhaseActivating ||
		session.Status.ResumeFrom == domain.PhaseResuming {
		return domain.NewError(
			domain.ErrorPrecondition,
			"abort dry-run",
			"activated sessions require rollback",
		)
	}

	if abortRequiresWorkloadResume(session) {
		if err := s.verifySourceStorage(ctx, session); err != nil {
			return err
		}
	}

	if phase == domain.PhasePaused || phase == domain.PhaseFinalSyncing ||
		phase == domain.PhaseFinalSynced {
		return s.controllers.VerifyPaused(ctx, session)
	}

	return nil
}

// ValidateRollback verifies the identities and offline boundary needed by a
// rollback. A completed migration is expected to be running, so its active
// PVC bindings are checked instead of requiring a paused workload.
func (s *Service) ValidateRollback(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "rollback dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if session.Spec.Type == domain.SessionTypeBackup {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rollback dry-run",
			"backup sessions do not change PVC identity and cannot be rolled back",
		)
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseRolledBack {
		return nil
	}

	rollbackOrigin := session.Status.ResumeFrom
	if rollbackOrigin == domain.PhaseRollingBack {
		rollbackOrigin = phaseBefore(session, domain.PhaseRollingBack)
	}

	recoveringRollback := phase == domain.PhaseRollingBack ||
		(phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseRollingBack)
	wasRunning := phase == domain.PhaseCompleted ||
		((phase == domain.PhaseFailed || phase == domain.PhaseRollingBack) && (rollbackOrigin == domain.PhaseResuming || rollbackOrigin == domain.PhaseCompleted))
	failedDuringCutover := phase == domain.PhaseFailed &&
		(session.Status.ResumeFrom == domain.PhaseActivating || session.Status.ResumeFrom == domain.PhaseResuming || session.Status.ResumeFrom == domain.PhaseRollingBack)

	valid := wasRunning || phase == domain.PhaseActivated || phase == domain.PhaseActivating ||
		phase == domain.PhaseFinalSynced ||
		phase == domain.PhaseRollingBack ||
		failedDuringCutover
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rollback dry-run",
			fmt.Sprintf("session phase %s cannot roll back", phase),
		)
	}

	if session.Spec.Operation().RebindsPVC() {
		return s.validateRebindRollbackVolumes(ctx, session)
	}

	if err := s.validateRollbackStorage(
		ctx,
		session,
		phase,
		rollbackOrigin,
		recoveringRollback,
		wasRunning,
	); err != nil {
		return err
	}

	if recoveringRollback {
		if wasRunning {
			return s.validateRollbackConsumers(ctx, session)
		}
		return nil
	}

	if wasRunning {
		return s.validateRollbackConsumers(ctx, session)
	}

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}

	return nil
}

// validateRollbackConsumers mirrors the Pod reference guard in RollbackVolume
// while allowing consumers that the recorded workload adapter will pause.
func (s *Service) validateRollbackConsumers(ctx context.Context, session *domain.Session) error {
	allowed := make(map[string]struct{})

	workload := session.Spec.Workload()
	if workload.Adapter != domain.WorkloadNone {
		for _, ref := range append([]domain.ObjectReference{workload.Pod}, workload.AffectedPods...) {
			if ref.Namespace != "" && ref.Name != "" {
				allowed[ref.Namespace+"/"+ref.Name] = struct{}{}
			}
		}
	}

	namespaces := make([]string, 0)

	seenNamespaces := make(map[string]struct{})
	for index := range session.Spec.Volumes {
		namespace := session.Spec.Volumes[index].SourcePVC.Namespace
		if _, exists := seenNamespaces[namespace]; exists {
			continue
		}

		seenNamespaces[namespace] = struct{}{}
		namespaces = append(namespaces, namespace)
	}

	sort.Strings(namespaces)

	type podList struct {
		items []corev1.Pod
		err   error
	}

	results := make([]podList, len(namespaces))
	parallel.For(len(namespaces), func(index int) {
		pods, err := s.client.CoreV1().Pods(namespaces[index]).List(ctx, metav1.ListOptions{})
		if err == nil && pods == nil {
			err = fmt.Errorf("list Pods in %s returned an empty object", namespaces[index])
		}

		if pods != nil {
			results[index].items = pods.Items
		}

		results[index].err = err
	})

	for volumeIndex := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[volumeIndex]

		result := results[sort.SearchStrings(namespaces, volume.SourcePVC.Namespace)]
		if result.err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"rollback dry-run",
				"list Pods in "+volume.SourcePVC.Namespace,
				result.err,
			)
		}

		for podIndex := range result.items {
			pod := &result.items[podIndex]
			if !kube.PodPreventsSafePVCDeletion(pod, volume.SourcePVC.Name) {
				continue
			}

			if _, controlled := allowed[pod.Namespace+"/"+pod.Name]; controlled {
				continue
			}

			return domain.NewError(
				domain.ErrorPrecondition,
				"rollback dry-run",
				fmt.Sprintf(
					"PVC %s/%s is referenced by Pod %s, which is outside the recorded workload pause scope",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					pod.Name,
				),
			)
		}
	}

	return nil
}

// validateRebindRollbackVolumes checks the PVC identity currently serving the
// workload. Rename and move sessions replace the original PVC name in place,
// so the recorded source PVC is intentionally absent after cutover.
func (s *Service) validateRebindRollbackVolumes(
	ctx context.Context,
	session *domain.Session,
) error {
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		status := &session.Status.Volumes[index]

		active := status.Activation.ActivePVC
		if active.Namespace == "" {
			active.Namespace = volume.DestinationPVC.Namespace
		}

		if active.Name == "" || active.UID == "" {
			return domain.NewError(
				domain.ErrorPrecondition,
				"rollback dry-run",
				fmt.Sprintf("PVC %s has no recorded active identity", volume.SourcePVC.Name),
			)
		}

		if err := s.validateRebindTransition(
			ctx,
			session,
			volume,
			active,
			volume.SourcePVC,
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) validateOfflineVolumes(ctx context.Context, session *domain.Session) error {
	volumes := make([]*domain.VolumeSpec, len(session.Spec.Volumes))
	for index := range session.Spec.Volumes {
		volumes[index] = &session.Spec.Volumes[index]
	}

	return s.verifyVolumesOffline(ctx, session, volumes)
}

func (s *Service) verifyVolumesOffline(
	ctx context.Context,
	session *domain.Session,
	volumes []*domain.VolumeSpec,
) error {
	if switcher, ok := s.switcher.(sessionBatchVolumeSwitcher); ok {
		return switcher.VerifyVolumesOfflineForSession(ctx, session.ID, volumes)
	}

	if switcher, ok := s.switcher.(batchVolumeSwitcher); ok {
		return switcher.VerifyVolumesOffline(ctx, volumes)
	}

	for _, volume := range volumes {
		if err := s.switcher.VerifyVolumeOffline(ctx, volume); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) validateRebindOfflineVolumes(ctx context.Context, session *domain.Session) error {
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		if err := s.validateRebindTransition(
			ctx,
			session,
			volume,
			volume.SourcePVC,
			volume.DestinationPVC,
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) validateRebindTransition(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	from, to domain.ObjectReference,
) error {
	if from.Namespace == "" || from.Name == "" || to.Namespace == "" || to.Name == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rebind PVC dry-run",
			"source and destination PVC identities are required",
		)
	}

	fromPVC, fromErr := s.client.CoreV1().
		PersistentVolumeClaims(from.Namespace).
		Get(ctx, from.Name, metav1.GetOptions{})
	sameEndpoint := from.Namespace == to.Namespace && from.Name == to.Name

	toPVC, toErr := fromPVC, fromErr
	if !sameEndpoint {
		toPVC, toErr = s.client.CoreV1().
			PersistentVolumeClaims(to.Namespace).
			Get(ctx, to.Name, metav1.GetOptions{})
	}

	if fromErr != nil && !apierrors.IsNotFound(fromErr) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"rebind PVC dry-run",
			fmt.Sprintf("read PVC %s/%s", from.Namespace, from.Name),
			fromErr,
		)
	}

	if toErr != nil && !apierrors.IsNotFound(toErr) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"rebind PVC dry-run",
			fmt.Sprintf("read PVC %s/%s", to.Namespace, to.Name),
			toErr,
		)
	}

	fromExists := fromErr == nil

	toExists := toErr == nil
	if !sameEndpoint && fromExists && toExists {
		return domain.NewError(
			domain.ErrorConflict,
			"rebind PVC dry-run",
			fmt.Sprintf(
				"both PVC endpoints %s/%s and %s/%s exist",
				from.Namespace,
				from.Name,
				to.Namespace,
				to.Name,
			),
		)
	}

	if !fromExists && !toExists {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rebind PVC dry-run",
			fmt.Sprintf(
				"neither PVC endpoint %s/%s nor %s/%s exists",
				from.Namespace,
				from.Name,
				to.Namespace,
				to.Name,
			),
		)
	}

	current := fromPVC

	expected := from
	if !fromExists {
		current = toPVC
		expected = to
	}

	if current == nil || current.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			"rebind PVC dry-run",
			"read PVC endpoint returned an empty object",
		)
	}

	if !fromExists && current.Annotations[kube.SessionKey] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"rebind PVC dry-run",
			fmt.Sprintf(
				"destination PVC %s/%s is not owned by session %s",
				current.Namespace,
				current.Name,
				session.ID,
			),
		)
	}

	if expected.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rebind PVC dry-run",
			fmt.Sprintf("PVC %s/%s has no recorded UID", expected.Namespace, expected.Name),
		)
	}

	if current.UID != expected.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"rebind PVC dry-run",
			fmt.Sprintf("PVC %s/%s UID changed", current.Namespace, current.Name),
		)
	}

	currentRef := domain.ObjectReference{
		APIVersion: domain.CoreAPIVersion,
		Kind:       domain.KindPersistentVolumeClaim,
		Namespace:  current.Namespace,
		Name:       current.Name,
		UID:        current.UID,
	}
	check := *volume
	check.SourcePVC = currentRef
	check.SourcePV = volume.SourcePV
	check.DestinationPVC = currentRef
	check.DestinationPV = volume.SourcePV

	return s.switcher.VerifyVolumeOffline(ctx, &check)
}

// ValidateCleanup checks ownership, reclaim-policy, and deletion prerequisites
// through read-only API calls. It mirrors Cleanup's destructive guards.
func (s *Service) ValidateCleanup(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "cleanup dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if !cleanupPhaseAllowed(session) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			fmt.Sprintf("session phase %s is still active", session.Status.Phase),
		)
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	if session.Spec.Type == domain.SessionTypeBackup &&
		(options.Finalize || options.DeleteSession) {
		if err := kube.ValidateBackupCredentialsSecretCleanup(
			ctx,
			s.client,
			backupCredentialsCleanupReference(session),
			session.ID,
		); err != nil {
			return err
		}
	}

	if err := s.validateReservationPods(ctx, session); err != nil {
		return err
	}

	if options.Finalize {
		if err := s.validateStandalonePodOwnershipRelease(ctx, session); err != nil {
			return err
		}
	}

	s.logInfo(
		"cleanup preflight started",
		"session",
		session.ID,
		"volumes",
		len(session.Spec.Volumes),
		"deleteTemporary",
		options.DeleteTemporary,
		"deleteRollback",
		options.DeleteRollback,
		"finalize",
		options.Finalize,
		"deleteSession",
		options.DeleteSession,
	)

	if options.DeleteSession && !options.Finalize {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			"deleting the session requires --finalize",
		)
	}

	for index := range session.Spec.Volumes {
		if err := s.validateCleanupVolume(ctx, session, options, index); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) validateCleanupVolume(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
	index int,
) error {
	volumeValue := session.Spec.Volumes[index]

	volume := &volumeValue
	if !session.Spec.Operation().RebindsPVC() &&
		(options.DeleteTemporary || options.DeleteRollback || options.DeleteSession) {
		recoverySession := *session
		recoverySession.Spec.Volumes = slices.Clone(session.Spec.Volumes)

		recoverySession.Spec.Volumes[index] = volumeValue
		if _, err := s.discoverDestinationRefs(ctx, &recoverySession, index); err != nil {
			return err
		}

		volume = &recoverySession.Spec.Volumes[index]
	}

	active, rollback, policy := cleanupPVRefs(session, volume)
	if options.DeleteSession && rollback.Name != "" && !options.DeleteRollback &&
		!preservesCopyOutput(session, options) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			"deleting the session requires --delete-rollback-pv while a rollback PV is recorded",
		)
	}

	if err := s.validateCleanupPVC(ctx, session, options, volume); err != nil {
		return err
	}

	if err := s.validateCleanupRollbackPV(
		ctx,
		session,
		options,
		index,
		volume,
		rollback,
	); err != nil {
		return err
	}

	if uncheckpointedSource(session, index) {
		return s.validateUncheckpointedSource(ctx, session.ID, volume)
	}

	if !options.Finalize || active.Name == "" {
		return nil
	}

	return s.validateCleanupActivePV(ctx, session, options, index, volume, active, policy)
}

func (s *Service) validateCleanupPVC(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
	volume *domain.VolumeSpec,
) error {
	if !options.DeleteTemporary || volume.DestinationPVC.UID == "" {
		return nil
	}

	pvc, err := s.inspectPVCUnusedForSession(ctx, volume.DestinationPVC, session)
	if err != nil || pvc == nil {
		return err
	}

	if pvc.UID != volume.DestinationPVC.UID || pvc.Labels[kube.SessionKey] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PVC %s/%s identity or session ownership changed", pvc.Namespace, pvc.Name),
		)
	}

	return nil
}

func (s *Service) validateCleanupRollbackPV(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
	index int,
	volume *domain.VolumeSpec,
	rollback domain.ObjectReference,
) error {
	if !options.DeleteRollback || rollback.Name == "" {
		return nil
	}

	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, rollback.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup dry-run",
			"read rollback PV "+rollback.Name,
			err,
		)
	}

	var uncheckpointedClaim *domain.ObjectReference
	if uncheckpointedDestination(session, index) {
		uncheckpointedClaim = &volume.DestinationPVC
	}

	if !cleanupPVIdentityMatches(
		pv,
		rollback,
		session.ID,
		cleanupRollbackRole(session),
		uncheckpointedClaim,
	) {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PV %s identity, ownership, or role changed", pv.Name),
		)
	}

	if pv.Status.Phase == corev1.VolumeReleased || pv.Status.Phase == corev1.VolumeAvailable {
		return nil
	}

	deletionWillReleaseClaim := options.DeleteTemporary && pv.Status.Phase == corev1.VolumeBound &&
		pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Namespace == volume.DestinationPVC.Namespace &&
		pv.Spec.ClaimRef.Name == volume.DestinationPVC.Name && pv.Spec.ClaimRef.UID != "" &&
		pv.Spec.ClaimRef.UID == volume.DestinationPVC.UID
	if deletionWillReleaseClaim {
		return nil
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		"cleanup dry-run",
		fmt.Sprintf("PV %s phase %s must be Released or Available", pv.Name, pv.Status.Phase),
	)
}

func (s *Service) validateCleanupActivePV(
	ctx context.Context,
	session *domain.Session,
	options CleanupOptions,
	index int,
	volume *domain.VolumeSpec,
	active domain.ObjectReference,
	policy corev1.PersistentVolumeReclaimPolicy,
) error {
	maySkipMissing := session.Status.Phase == domain.PhaseAborted &&
		session.Status.Volumes[index].Activation.ActivePVC.Name == ""
	if policy == "" && !maySkipMissing {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			fmt.Sprintf("PV %s has no recorded reclaim policy", active.Name),
		)
	}

	pv, err := s.client.CoreV1().PersistentVolumes().Get(ctx, active.Name, metav1.GetOptions{})
	if maySkipMissing && apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup dry-run",
			"read active PV "+active.Name,
			err,
		)
	}

	if policy == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			fmt.Sprintf("PV %s has no recorded reclaim policy", active.Name),
		)
	}

	if err := validateFinalizablePV(pv, active, session.ID, policy); err != nil {
		return err
	}

	if !preservesCopyOutput(session, options) || volume.DestinationPV.Name == "" {
		return nil
	}

	destinationPV, err := s.client.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.DestinationPV.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"cleanup dry-run",
			"read copy destination PV "+volume.DestinationPV.Name,
			err,
		)
	}

	return validateFinalizablePV(
		destinationPV,
		volume.DestinationPV,
		session.ID,
		volume.DestinationPolicy,
	)
}

func validateFinalizablePV(
	pv *corev1.PersistentVolume,
	ref domain.ObjectReference,
	sessionID string,
	policy corev1.PersistentVolumeReclaimPolicy,
) error {
	if policy == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"cleanup dry-run",
			fmt.Sprintf("PV %s has no recorded reclaim policy", ref.Name),
		)
	}

	if pv.UID != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
		)
	}

	role := pv.Labels[kube.ResourceRoleLabel]
	if pv.Labels[kube.SessionKey] == "" && role == "" &&
		pv.Spec.PersistentVolumeReclaimPolicy == policy &&
		pv.Annotations[kube.OriginalPolicyAnnotation] == "" {
		return nil
	}

	if pv.Labels[kube.SessionKey] != sessionID ||
		(role != kube.ResourceRoleActive && role != kube.ResourceRoleSource && role != kube.ResourceRoleRename && role != kube.ResourceRoleDestination) {
		return domain.NewError(
			domain.ErrorConflict,
			"cleanup dry-run",
			fmt.Sprintf("PV %s identity, ownership, or role changed", ref.Name),
		)
	}

	return nil
}

// ValidateFinalSync performs the read-only checks required immediately before
// a final synchronization. It leaves the persisted session and workload intact.
func (s *Service) ValidateFinalSync(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "final sync dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if err := validateRetryableSessionFailure(session); err != nil {
		return err
	}

	if !session.Spec.Orchestrated() {
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync dry-run",
			"final sync requires an orchestrated migration session",
		)
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	switch phase {
	case domain.PhaseReserved, domain.PhaseWarmCopied, domain.PhasePausing:
		return s.ValidateReservation(ctx, session)
	case domain.PhasePaused, domain.PhaseFinalSyncing, domain.PhaseFinalSynced:
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync dry-run",
			fmt.Sprintf("session phase %s cannot final-sync", session.Status.Phase),
		)
	}

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}

	if err := s.verifyShrinkUsage(ctx, session); err != nil {
		return err
	}

	return s.validateOfflineVolumes(ctx, session)
}

// ValidateActivation performs activation preconditions through read-only API
// calls. The mutating PV/PVC switch remains behind --dry-run=false.
func (s *Service) ValidateActivation(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(domain.ErrorValidation, "activation dry-run", "session is nil")
	}

	if err := session.Validate(); err != nil {
		return err
	}

	if !session.Spec.Orchestrated() {
		return domain.NewError(
			domain.ErrorPrecondition,
			"activation dry-run",
			"activation requires an orchestrated migration session",
		)
	}

	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseActivated || phase == domain.PhaseResuming ||
		phase == domain.PhaseCompleted ||
		(phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseResuming) {
		return nil
	}

	valid := phase == domain.PhaseFinalSynced || phase == domain.PhaseActivating ||
		(phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseActivating)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"activation dry-run",
			fmt.Sprintf("session phase %s cannot activate", session.Status.Phase),
		)
	}

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		status := &session.Status.Volumes[index]
		if status.Sync.FinalCompletedAt == nil {
			return domain.NewError(
				domain.ErrorPrecondition,
				"activation dry-run",
				fmt.Sprintf("PVC %s has no completed final sync", volume.SourcePVC.Name),
			)
		}
	}

	if err := s.validateActivationPVCPolicies(ctx, session); err != nil {
		return err
	}

	if phase == domain.PhaseActivating || phase == domain.PhaseFailed {
		return s.validateActivationStorage(ctx, session)
	}

	return s.validateOfflineVolumes(ctx, session)
}

func (s *Service) Reserve(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.reserve(lockedCtx, session) },
	)
}

func (s *Service) reserve(ctx context.Context, session *domain.Session) error {
	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	if session.Status.Phase == domain.PhaseReserved ||
		phaseAfter(session.Status.Phase, domain.PhaseReserved) {
		return nil
	}

	if err := s.ValidateReservation(ctx, session); err != nil {
		return err
	}

	if _, err := s.probeToolImage(ctx, session, reservationToolProbeTargets(session)); err != nil {
		return err
	}

	if err := s.begin(
		ctx,
		session,
		domain.PhaseReserving,
		"reserving destination storage",
	); err != nil {
		return err
	}

	if err := kube.EnsureNamespace(
		ctx,
		s.client,
		session.Spec.TemporaryNamespace,
		session.ID,
		false,
	); err != nil {
		return s.failContext(ctx, session, err)
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		status := &session.Status.Volumes[index]
		if status.Reserved && volume.DestinationPV.UID != "" {
			continue
		}

		s.logInfo(
			"destination storage reservation started",
			"session",
			session.ID,
			"pvc",
			volume.SourcePVC.Name,
			"destination",
			volume.DestinationPVC.Namespace+"/"+volume.DestinationPVC.Name,
			"node",
			session.Spec.WorkflowOptions().TargetNode,
		)

		if err := s.reserver.ReserveVolume(ctx, session, volume, status, false); err != nil {
			return s.failContext(ctx, session, err)
		}

		if err := s.store.Update(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	return s.finish(
		ctx,
		session,
		domain.PhaseReserved,
		"destination storage is provisioned and retained",
	)
}

func (s *Service) WarmCopy(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.warmCopy(lockedCtx, session) },
	)
}

func (s *Service) warmCopy(ctx context.Context, session *domain.Session) (resultErr error) {
	valid := session.Status.Phase == domain.PhaseReserved ||
		session.Status.Phase == domain.PhaseWarmCopied ||
		session.Status.Phase == domain.PhaseWarmCopying ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseWarmCopying)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"warm copy",
			fmt.Sprintf("session phase %s cannot warm-copy", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	s.logInfo(
		"warm copy preflight started",
		"session",
		session.ID,
		"volumes",
		len(session.Spec.Volumes),
	)

	if err := s.ValidateWarmCopy(ctx, session); err != nil {
		return err
	}
	// Checkpoint inferred online-copy placement only after the full read-only
	// preflight has passed for every volume.
	if err := s.validateCopyConsumersBatch(ctx, session, true); err != nil {
		return err
	}

	if err := s.enableOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	restoreSharedMounts := true
	defer func() {
		if !restoreSharedMounts {
			return
		}

		if err := s.restoreOpenEBSLVMSharedMountsAfterFailure(ctx, session); err != nil {
			resultErr = errors.Join(resultErr, s.failContext(ctx, session, err))
		}
	}()

	targets, err := s.resolveCopyToolProbeTargets(ctx, session, true)
	if err != nil {
		return err
	}

	probeResults, err := s.probeToolImage(ctx, session, targets)
	if err != nil {
		return warmCopyProbeError(session.Spec.Operation(), targets, err)
	}

	if session.Status.Phase == domain.PhaseWarmCopied {
		for i := range session.Status.Volumes {
			session.Status.Volumes[i].Sync.WarmCompletedAt = nil
			session.Status.Volumes[i].Sync.LastError = ""
		}
	}

	if err := s.begin(ctx, session, domain.PhaseWarmCopying, "running warm copy"); err != nil {
		return err
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		status := &session.Status.Volumes[index]
		if status.Sync.WarmCompletedAt != nil {
			continue
		}

		if err := s.validateCopyConsumers(ctx, session, volume); err != nil {
			return s.failContext(ctx, session, err)
		}

		if err := s.copyWithRetry(
			ctx,
			session,
			volume,
			status,
			copyengine.ModeWarm,
			probeResults,
		); err != nil {
			return s.failContext(ctx, session, err)
		}

		now := metav1.NewTime(s.now().UTC())
		status.Sync.WarmCompletedAt = &now
		status.Sync.LastError = ""

		if err := s.store.Update(ctx, session); err != nil {
			return err
		}
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	restoreSharedMounts = false

	if session.Spec.Operation() == domain.OperationMigrate ||
		session.Spec.Operation() == domain.OperationMigratePod {
		session.CompleteWarmPass()
	}

	return s.finish(ctx, session, domain.PhaseWarmCopied, "warm copy completed for all volumes")
}

func warmCopyProbeError(
	operation domain.Operation,
	targets []kube.ToolProbeTarget,
	err error,
) error {
	if err == nil || !kube.IsConcurrentMountFailureMessage(err.Error()) {
		return err
	}

	pvcs := make([]string, 0, len(targets))
	for _, target := range targets {
		if target.PVCName == "" || target.SkipPVCMount {
			continue
		}

		ref := target.Namespace + "/" + target.PVCName
		if !slices.Contains(pvcs, ref) {
			pvcs = append(pvcs, ref)
		}
	}

	if len(pvcs) == 0 {
		return err
	}

	sort.Strings(pvcs)

	recovery := "disable warm copy after making sure the source PVC has no active Pod consumers"
	switch operation {
	case domain.OperationCopy:
		recovery = "rerun the copy without --online after the source PVC has no active Pod consumers"
	case domain.OperationMigrate, domain.OperationMigratePod:
		recovery = "rerun the migration with --precopy-passes 0"
	}

	return domain.WrapError(
		domain.ErrorPrecondition,
		"warm-copy mount probe",
		fmt.Sprintf(
			"second-Pod mount failed for source PVC(s) %s while the source workload is active: %v; abort this pre-cutover session, clean its retained resources, and %s",
			strings.Join(pvcs, ","),
			err,
			recovery,
		),
		err,
	)
}

func (s *Service) Pause(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.pause(lockedCtx, session) },
	)
}

func (s *Service) pause(ctx context.Context, session *domain.Session) error {
	if !session.Spec.Orchestrated() {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pause workload",
			"workload pause requires an orchestrated migration session",
		)
	}

	if session.Status.Phase == domain.PhasePaused ||
		session.Status.Phase == domain.PhaseFinalSyncing ||
		session.Status.Phase == domain.PhaseFinalSynced {
		if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
			return err
		}
		return s.controllers.VerifyPaused(ctx, session)
	}

	valid := session.Status.Phase == domain.PhaseReserved ||
		session.Status.Phase == domain.PhaseWarmCopied ||
		session.Status.Phase == domain.PhasePausing ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhasePausing)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pause workload",
			fmt.Sprintf("session phase %s cannot pause", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	if err := s.ValidateReservation(ctx, session); err != nil {
		return err
	}

	if err := s.begin(ctx, session, domain.PhasePausing, "pausing workload"); err != nil {
		return err
	}

	if err := s.controllers.Pause(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}
	// Controller adapters may record resource UIDs and original pause state
	// while applying the pause. Persist that recovery data before verification.
	if err := s.store.Update(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	return s.finish(ctx, session, domain.PhasePaused, "workload is safely paused")
}

func (s *Service) FinalSync(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.finalSync(lockedCtx, session) },
	)
}

func (s *Service) finalSync(ctx context.Context, session *domain.Session) error {
	if !session.Spec.Orchestrated() {
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync",
			"final sync requires an orchestrated migration session",
		)
	}

	valid := session.Status.Phase == domain.PhasePaused ||
		session.Status.Phase == domain.PhaseFinalSyncing ||
		session.Status.Phase == domain.PhaseFinalSynced ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseFinalSyncing)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync",
			fmt.Sprintf("session phase %s cannot final-sync", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	s.logInfo(
		"final sync preflight started",
		"session",
		session.ID,
		"volumes",
		len(session.Spec.Volumes),
	)

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return err
	}

	if err := s.verifyShrinkUsage(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	if err := s.validateOfflineVolumes(ctx, session); err != nil {
		return err
	}

	targets, err := s.resolveCopyToolProbeTargets(ctx, session, false)
	if err != nil {
		return err
	}

	probeResults, err := s.probeToolImage(ctx, session, targets)
	if err != nil {
		return err
	}

	return s.finalSyncWithProbeResults(ctx, session, probeResults)
}

// PauseAndFinalSync verifies the tool image while holding the same Session
// Lease used to pause the workload and launch the offline copy.
func (s *Service) PauseAndFinalSync(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.pauseAndFinalSync(lockedCtx, session)
	})
}

func (s *Service) pauseAndFinalSync(ctx context.Context, session *domain.Session) error {
	if session == nil || !session.Spec.Orchestrated() {
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync",
			"final sync requires an orchestrated migration session",
		)
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	switch phase {
	case domain.PhaseReserved, domain.PhaseWarmCopied, domain.PhasePausing,
		domain.PhasePaused, domain.PhaseFinalSyncing, domain.PhaseFinalSynced:
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"final sync",
			fmt.Sprintf("session phase %s cannot final-sync", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	s.logInfo(
		"pause and final sync preflight started",
		"session",
		session.ID,
		"volumes",
		len(session.Spec.Volumes),
		"alreadyPaused",
		phase == domain.PhasePaused || phase == domain.PhaseFinalSyncing ||
			phase == domain.PhaseFinalSynced,
	)

	alreadyPaused := phase == domain.PhasePaused || phase == domain.PhaseFinalSyncing ||
		phase == domain.PhaseFinalSynced
	if alreadyPaused {
		if err := s.controllers.VerifyPaused(ctx, session); err != nil {
			return err
		}

		if err := s.verifyShrinkUsage(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}

		if err := s.validateOfflineVolumes(ctx, session); err != nil {
			return err
		}
	} else if err := s.ValidateReservation(ctx, session); err != nil {
		return err
	}

	targets, err := s.resolveCopyToolProbeTargets(ctx, session, false)
	if err != nil {
		return err
	}

	probeResults, err := s.probeToolImage(ctx, session, targets)
	if err != nil {
		return err
	}

	if !alreadyPaused {
		if err := s.pause(ctx, session); err != nil {
			return err
		}

		if err := s.verifyShrinkUsage(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	return s.finalSyncWithProbeResults(ctx, session, probeResults)
}

func (s *Service) finalSyncWithProbeResults(
	ctx context.Context,
	session *domain.Session,
	probeResults []kube.ToolImageProbeResult,
) error {
	pathTargets, err := s.sourceTransferPathProbeTargets(ctx, session)
	if err != nil {
		return err
	}

	pathProbeResults, err := s.probeToolImage(ctx, session, pathTargets)
	if err != nil {
		return err
	}

	probeResults = append(probeResults, pathProbeResults...)

	if session.Status.Phase == domain.PhaseFinalSynced {
		for i := range session.Status.Volumes {
			session.Status.Volumes[i].Sync.FinalCompletedAt = nil
			session.Status.Volumes[i].Sync.ChecksumVerified = false
		}
	}

	if err := s.begin(
		ctx,
		session,
		domain.PhaseFinalSyncing,
		"running offline final sync",
	); err != nil {
		return err
	}

	if err := s.validateOfflineVolumes(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		status := &session.Status.Volumes[index]
		if status.Sync.FinalCompletedAt != nil {
			continue
		}

		if err := s.switcher.VerifyVolumeOffline(ctx, volume); err != nil {
			return s.failContext(ctx, session, err)
		}

		if err := s.copyWithRetry(
			ctx,
			session,
			volume,
			status,
			copyengine.ModeFinal,
			probeResults,
		); err != nil {
			return s.failContext(ctx, session, err)
		}

		now := metav1.NewTime(s.now().UTC())
		status.Sync.FinalCompletedAt = &now
		status.Sync.ChecksumVerified = session.Spec.WorkflowOptions().VerifyChecksum
		status.Sync.LastError = ""

		if err := s.store.Update(ctx, session); err != nil {
			return err
		}
	}

	return s.finish(
		ctx,
		session,
		domain.PhaseFinalSynced,
		"offline final sync completed for all volumes",
	)
}

func (s *Service) sourceTransferPathProbeTargets(
	ctx context.Context,
	session *domain.Session,
) ([]kube.ToolProbeTarget, error) {
	if session == nil {
		return nil, nil
	}

	hasPartialSource := false
	for _, volume := range session.Spec.Volumes {
		if domain.SourceTransferPath(volume.TransferScope) != domain.VolumeRootPath {
			hasPartialSource = true
			break
		}
	}

	if !hasPartialSource {
		return nil, nil
	}

	var (
		targets []kube.ToolProbeTarget
		err     error
	)
	if nodeName := session.Spec.WorkflowOptions().SourceNode; nodeName != "" {
		targets = sourceToolProbeTargets(session, nodeName, true)
	} else {
		targets, err = s.resolveSourceToolProbeTargets(ctx, session, true)
		if err != nil {
			return nil, err
		}
	}

	filtered := targets[:0]
	for _, target := range targets {
		if target.RequiredPath == "" {
			continue
		}

		target.Components = nil
		filtered = append(filtered, target)
	}

	return filtered, nil
}

func (s *Service) Activate(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.activate(lockedCtx, session) },
	)
}

func (s *Service) activate(ctx context.Context, session *domain.Session) error {
	if !session.Spec.Orchestrated() {
		return domain.NewError(
			domain.ErrorPrecondition,
			"activate",
			"activation requires an orchestrated migration session",
		)
	}

	if session.Status.Phase == domain.PhaseActivated ||
		session.Status.Phase == domain.PhaseResuming ||
		session.Status.Phase == domain.PhaseCompleted ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseResuming) {
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	}

	valid := session.Status.Phase == domain.PhaseFinalSynced ||
		session.Status.Phase == domain.PhaseActivating ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseActivating)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"activate",
			fmt.Sprintf("session phase %s cannot activate", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	if err := s.ValidateActivation(ctx, session); err != nil {
		return err
	}

	if err := s.begin(
		ctx,
		session,
		domain.PhaseActivating,
		"activating destination volumes",
	); err != nil {
		return err
	}

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		status := &session.Status.Volumes[index]
		if status.Activation.ActivatedAt != nil {
			if err := s.verifyActiveStorageVolume(ctx, session, index); err != nil {
				return s.failContext(ctx, session, err)
			}
			continue
		}

		s.logInfo(
			"volume activation started",
			"session",
			session.ID,
			"index",
			index,
			"source",
			volume.SourcePVC.Namespace+"/"+volume.SourcePVC.Name,
			"destination",
			volume.DestinationPVC.Namespace+"/"+volume.DestinationPVC.Name,
			"pv",
			volume.DestinationPV.Name,
		)

		if err := s.switcher.ActivateVolume(ctx, session, volume, status, func() error {
			return s.store.Update(ctx, session)
		}); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	return s.finish(ctx, session, domain.PhaseActivated, "all destination volumes are active")
}

func (s *Service) ResumeWorkload(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.resumeWorkload(lockedCtx, session) },
	)
}

func (s *Service) resumeWorkload(ctx context.Context, session *domain.Session) error {
	valid := session.Status.Phase == domain.PhaseActivated ||
		session.Status.Phase == domain.PhaseResuming ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseResuming)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume workload",
			fmt.Sprintf("session phase %s cannot resume", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return err
	}

	if err := s.validateWorkloadResume(ctx, session); err != nil {
		return err
	}

	if err := s.begin(ctx, session, domain.PhaseResuming, "resuming workload"); err != nil {
		return err
	}

	if err := s.controllers.Resume(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	if err := s.verifyActiveVolumes(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	return s.finish(
		ctx,
		session,
		domain.PhaseCompleted,
		"migration completed and workload is ready",
	)
}

func (s *Service) Migrate(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(ctx, session, func(lockedCtx context.Context) error {
		return s.migrate(lockedCtx, session)
	})
}

func (s *Service) migrateAfterWarmCopy(ctx context.Context, session *domain.Session) error {
	if err := s.PauseAndFinalSync(ctx, session); err != nil {
		return err
	}

	if err := s.Activate(ctx, session); err != nil {
		return err
	}

	return s.ResumeWorkload(ctx, session)
}

func (s *Service) migrate(ctx context.Context, session *domain.Session) error {
	warmPasses := session.Spec.PrecopyPasses()
	if warmPasses < 0 {
		return domain.NewError(
			domain.ErrorValidation,
			"migrate",
			"warm passes must be non-negative",
		)
	}

	if session.Spec.WorkflowOptionsPtr() == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"migrate",
			"session workflow options are missing",
		)
	}

	if session.Status.WarmPassesCompleted < warmPasses {
		if err := s.ValidateWarmCopy(ctx, session); err != nil {
			return err
		}
	}

	if err := s.Reserve(ctx, session); err != nil {
		return err
	}

	if err := s.runRemainingWarmCopies(ctx, session, warmPasses); err != nil {
		return err
	}

	return s.migrateAfterWarmCopy(ctx, session)
}

func (s *Service) runRemainingWarmCopies(
	ctx context.Context,
	session *domain.Session,
	warmPasses int,
) error {
	for session.Status.WarmPassesCompleted < warmPasses {
		if err := s.WarmCopy(ctx, session); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) ResumeSession(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.resumeSession(lockedCtx, session) },
	)
}

func (s *Service) resumeSession(ctx context.Context, session *domain.Session) error {
	if err := validateRetryableSessionFailure(session); err != nil {
		return err
	}

	phase := session.Status.Phase
	if phase == domain.PhaseFailed {
		phase = session.Status.ResumeFrom
	}

	if session.Spec.Operation() == domain.OperationMigrate ||
		session.Spec.Operation() == domain.OperationMigratePod {
		return s.resumeComposite(ctx, session, phase)
	}

	return s.resumeSingle(ctx, session, phase)
}

func (s *Service) resumeSingle(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
) error {
	if session.Spec.Operation() == domain.OperationBackup {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume session",
			"backup sessions require the backup resume workflow",
		)
	}

	if err := validateSingleResumePhase(session.Spec.Operation(), phase); err != nil {
		return err
	}

	if phase == domain.PhasePlanned {
		switch session.Spec.Operation() {
		case domain.OperationReserve:
			return s.Reserve(ctx, session)
		case domain.OperationCopy:
			if err := s.Reserve(ctx, session); err != nil {
				return err
			}
			return s.WarmCopy(ctx, session)
		case domain.OperationRename, domain.OperationMove:
			return s.Rename(ctx, session)
		default:
			return domain.NewError(
				domain.ErrorPrecondition,
				"resume session",
				fmt.Sprintf(
					"planned phase cannot be resumed for operation %s",
					session.Spec.Operation(),
				),
			)
		}
	}

	switch phase {
	case domain.PhaseReserving:
		if session.Spec.Operation() == domain.OperationCopy {
			if err := s.Reserve(ctx, session); err != nil {
				return err
			}
			return s.WarmCopy(ctx, session)
		}

		return s.Reserve(ctx, session)
	case domain.PhaseReserved:
		if session.Spec.Operation() == domain.OperationCopy {
			return s.WarmCopy(ctx, session)
		}
		return nil
	case domain.PhaseWarmCopying:
		return s.WarmCopy(ctx, session)
	case domain.PhasePausing:
		return s.Pause(ctx, session)
	case domain.PhaseFinalSyncing:
		return s.FinalSync(ctx, session)
	case domain.PhaseActivating:
		return s.Activate(ctx, session)
	case domain.PhaseActivated, domain.PhaseResuming:
		return s.ResumeWorkload(ctx, session)
	case domain.PhaseRollingBack:
		return s.Rollback(ctx, session)
	case domain.PhaseAborting:
		return s.Abort(ctx, session)
	case domain.PhaseRenaming, domain.PhaseMoving:
		if (session.Spec.Operation() == domain.OperationRename && phase == domain.PhaseRenaming) ||
			(session.Spec.Operation() == domain.OperationMove && phase == domain.PhaseMoving) {
			return s.Rename(ctx, session)
		}

		return domain.NewError(
			domain.ErrorPrecondition,
			"resume session",
			fmt.Sprintf(
				"phase %s does not belong to operation %s",
				phase,
				session.Spec.Operation(),
			),
		)
	case domain.PhaseWarmCopied, domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume session",
			fmt.Sprintf(
				"phase %s cannot be resumed for operation %s",
				phase,
				session.Spec.Operation(),
			),
		)
	}
}

func (s *Service) resumeComposite(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
) error {
	switch phase {
	case domain.PhasePlanned, domain.PhaseReserving:
		if err := s.Reserve(ctx, session); err != nil {
			return err
		}
		return s.Migrate(ctx, session)
	case domain.PhaseReserved:
		return s.Migrate(ctx, session)
	case domain.PhaseWarmCopying:
		if err := s.WarmCopy(ctx, session); err != nil {
			return err
		}

		if err := s.runRemainingWarmCopies(ctx, session, session.Spec.PrecopyPasses()); err != nil {
			return err
		}

		return s.migrateAfterWarmCopy(ctx, session)
	case domain.PhaseWarmCopied, domain.PhasePausing:
		if phase == domain.PhaseWarmCopied {
			if err := s.runRemainingWarmCopies(
				ctx,
				session,
				session.Spec.PrecopyPasses(),
			); err != nil {
				return err
			}
		}

		return s.migrateAfterWarmCopy(ctx, session)
	case domain.PhasePaused, domain.PhaseFinalSyncing:
		if err := s.FinalSync(ctx, session); err != nil {
			return err
		}

		if err := s.Activate(ctx, session); err != nil {
			return err
		}

		return s.ResumeWorkload(ctx, session)
	case domain.PhaseFinalSynced, domain.PhaseActivating:
		if err := s.Activate(ctx, session); err != nil {
			return err
		}
		return s.ResumeWorkload(ctx, session)
	case domain.PhaseActivated, domain.PhaseResuming:
		return s.ResumeWorkload(ctx, session)
	case domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack:
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	case domain.PhaseRollingBack:
		return s.Rollback(ctx, session)
	case domain.PhaseAborting:
		return s.Abort(ctx, session)
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume session",
			fmt.Sprintf("phase %s cannot be resumed", phase),
		)
	}
}

func (s *Service) Abort(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.abort(lockedCtx, session) },
	)
}

func (s *Service) abort(ctx context.Context, session *domain.Session) error {
	if session.Status.Phase == domain.PhaseAborted {
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	}

	if session.Status.Phase == domain.PhaseRollingBack ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseRollingBack) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"abort session",
			"rollback recovery must continue through session resume or rollback",
		)
	}

	if session.Status.Phase == domain.PhaseActivated ||
		session.Status.Phase == domain.PhaseCompleted ||
		session.Status.ResumeFrom == domain.PhaseActivating ||
		session.Status.ResumeFrom == domain.PhaseResuming {
		return domain.NewError(
			domain.ErrorPrecondition,
			"abort session",
			"activated sessions require rollback",
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	if err := s.ValidateAbort(ctx, session); err != nil {
		return err
	}

	resumeWorkload := abortRequiresWorkloadResume(session)
	if err := s.begin(ctx, session, domain.PhaseAborting, "aborting migration"); err != nil {
		return err
	}

	if resumeWorkload {
		if err := s.controllers.Resume(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	message := "migration aborted; reserved volumes are retained for cleanup"
	if session.Spec.Type == domain.SessionTypeBackup {
		message = "backup aborted; no recovery point was published"
	}

	return s.finish(ctx, session, domain.PhaseAborted, message)
}

func (s *Service) Rollback(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.rollback(lockedCtx, session) },
	)
}

func (s *Service) rollback(ctx context.Context, session *domain.Session) error {
	if session != nil && session.Spec.Type == domain.SessionTypeBackup {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rollback",
			"backup sessions do not change PVC identity and cannot be rolled back",
		)
	}

	return s.rollbackMigration(ctx, session)
}

func (s *Service) rollbackMigration(ctx context.Context, session *domain.Session) error {
	if session.Status.Phase == domain.PhaseRolledBack {
		return s.restoreOpenEBSLVMSharedMounts(ctx, session)
	}

	rollbackOrigin := session.Status.ResumeFrom
	if rollbackOrigin == domain.PhaseRollingBack {
		rollbackOrigin = phaseBefore(session, domain.PhaseRollingBack)
	}

	wasRunning := session.Status.Phase == domain.PhaseCompleted ||
		((session.Status.Phase == domain.PhaseFailed || session.Status.Phase == domain.PhaseRollingBack) && (rollbackOrigin == domain.PhaseResuming || rollbackOrigin == domain.PhaseCompleted))
	failedDuringCutover := session.Status.Phase == domain.PhaseFailed &&
		(session.Status.ResumeFrom == domain.PhaseActivating || session.Status.ResumeFrom == domain.PhaseResuming || session.Status.ResumeFrom == domain.PhaseRollingBack)

	valid := wasRunning || session.Status.Phase == domain.PhaseActivated ||
		session.Status.Phase == domain.PhaseActivating ||
		session.Status.Phase == domain.PhaseFinalSynced ||
		session.Status.Phase == domain.PhaseRollingBack ||
		failedDuringCutover
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rollback",
			fmt.Sprintf("session phase %s cannot roll back", session.Status.Phase),
		)
	}

	if err := s.restoreOpenEBSLVMSharedMounts(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	if err := s.ValidateRollback(ctx, session); err != nil {
		return err
	}

	if err := s.begin(
		ctx,
		session,
		domain.PhaseRollingBack,
		"rolling back to source volumes",
	); err != nil {
		return err
	}

	if session.Spec.Operation().RebindsPVC() {
		if len(session.Spec.Volumes) != 1 {
			return s.failContext(
				ctx,
				session,
				domain.NewError(
					domain.ErrorInternal,
					"rollback rename",
					"rename session must contain one volume",
				),
			)
		}

		volume := &session.Spec.Volumes[0]
		status := &session.Status.Volumes[0]
		reverse := *volume
		reverse.SourcePVC = volume.DestinationPVC
		reverse.SourcePVC.UID = status.Activation.ActivePVC.UID
		reverse.SourcePVC.ResourceVersion = status.Activation.ActivePVC.ResourceVersion
		reverse.DestinationPVC = volume.SourcePVC
		reverse.DestinationPVC.UID = ""
		reverse.DestinationPVC.ResourceVersion = ""

		pvc, err := s.switcher.RenamePVC(
			ctx,
			session,
			&reverse,
			func() error { return s.store.Update(ctx, session) },
		)
		if err != nil {
			return s.failContext(ctx, session, err)
		}

		now := metav1.NewTime(s.now().UTC())
		status.Activation.ActivePVC = domain.ObjectReference{
			APIVersion:      domain.CoreAPIVersion,
			Kind:            domain.KindPersistentVolumeClaim,
			Namespace:       pvc.Namespace,
			Name:            pvc.Name,
			UID:             pvc.UID,
			ResourceVersion: pvc.ResourceVersion,
		}
		status.Activation.RolledBackAt = &now

		return s.finish(ctx, session, domain.PhaseRolledBack, "PVC name restored")
	}

	if wasRunning {
		if err := s.controllers.Pause(ctx, session); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	if err := s.controllers.VerifyPaused(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	for index := len(session.Spec.Volumes) - 1; index >= 0; index-- {
		volume := &session.Spec.Volumes[index]
		status := &session.Status.Volumes[index]
		s.logInfo(
			"volume rollback started",
			"session",
			session.ID,
			"index",
			index,
			"source",
			volume.SourcePVC.Namespace+"/"+volume.SourcePVC.Name,
			"destination",
			volume.DestinationPVC.Namespace+"/"+volume.DestinationPVC.Name,
			"pv",
			volume.DestinationPV.Name,
		)

		if err := s.switcher.RollbackVolume(ctx, session, volume, status, func() error {
			return s.store.Update(ctx, session)
		}); err != nil {
			return s.failContext(ctx, session, err)
		}
	}

	if err := s.verifyRollbackStorage(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	if err := s.controllers.Resume(ctx, session); err != nil {
		return s.failContext(ctx, session, err)
	}

	return s.finish(
		ctx,
		session,
		domain.PhaseRolledBack,
		"source volumes restored and workload resumed",
	)
}

func (s *Service) Rename(ctx context.Context, session *domain.Session) error {
	return s.withSessionLock(
		ctx,
		session,
		func(lockedCtx context.Context) error { return s.rename(lockedCtx, session) },
	)
}

func (s *Service) rename(ctx context.Context, session *domain.Session) error {
	if !session.Spec.Operation().RebindsPVC() || len(session.Spec.Volumes) != 1 {
		return domain.NewError(
			domain.ErrorValidation,
			"rebind PVC",
			"session is not a single-volume PVC identity operation",
		)
	}

	if session.Status.Phase == domain.PhaseCompleted {
		return nil
	}

	phase := domain.PhaseRenaming

	message := "renaming PVC while retaining its PV"
	if session.Spec.Operation() == domain.OperationMove {
		phase = domain.PhaseMoving
		message = "moving PVC while retaining its PV"
	}

	valid := session.Status.Phase == domain.PhasePlanned || session.Status.Phase == phase ||
		(session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == phase)
	if !valid {
		return domain.NewError(
			domain.ErrorPrecondition,
			"rebind PVC",
			fmt.Sprintf("session phase %s cannot rebind PVC", session.Status.Phase),
		)
	}

	if err := s.validateRebindOfflineVolumes(ctx, session); err != nil {
		return err
	}

	if err := s.begin(ctx, session, phase, message); err != nil {
		return err
	}

	s.logInfo(
		"PVC identity change started",
		"session",
		session.ID,
		"operation",
		session.Spec.Operation(),
		"source",
		session.Spec.Volumes[0].SourcePVC.Namespace+"/"+session.Spec.Volumes[0].SourcePVC.Name,
		"destination",
		session.Spec.Volumes[0].DestinationPVC.Namespace+"/"+session.Spec.Volumes[0].DestinationPVC.Name,
	)
	volume := &session.Spec.Volumes[0]
	status := &session.Status.Volumes[0]

	pvc, err := s.switcher.RenamePVC(
		ctx,
		session,
		volume,
		func() error { return s.store.Update(ctx, session) },
	)
	if err != nil {
		return s.failContext(ctx, session, err)
	}

	now := metav1.NewTime(s.now().UTC())
	status.Activation.ActivePVC = domain.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolumeClaim,
		Namespace:       pvc.Namespace,
		Name:            pvc.Name,
		UID:             pvc.UID,
		ResourceVersion: pvc.ResourceVersion,
	}
	status.Activation.ActivatedAt = &now

	if session.Spec.Operation() == domain.OperationMove {
		return s.finish(ctx, session, domain.PhaseCompleted, "PVC move completed")
	}

	return s.finish(ctx, session, domain.PhaseCompleted, "PVC rename completed")
}

func (s *Service) validateCopyConsumers(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
) error {
	if session.Spec.Operation() != domain.OperationCopy {
		return nil
	}

	pods, err := s.client.CoreV1().Pods(volume.SourcePVC.Namespace).List(ctx, metav1.ListOptions{})
	if err == nil && pods == nil {
		err = fmt.Errorf(
			"list PVC consumers in %s returned an empty object",
			volume.SourcePVC.Namespace,
		)
	}

	var items []corev1.Pod
	if pods != nil {
		items = pods.Items
	}

	_, err = s.validateCopyConsumersFromPods(
		session,
		volume,
		items,
		err,
		session.Spec.WorkflowOptions().SourceNode,
	)

	return err
}

func (s *Service) validateCopyConsumersBatch(
	ctx context.Context,
	session *domain.Session,
	checkpointSourceNode bool,
) error {
	if session.Spec.Operation() != domain.OperationCopy {
		return nil
	}

	options := session.Spec.WorkflowOptionsPtr()
	if options == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"copy preflight",
			"copy session workflow options are missing",
		)
	}

	namespaces := make([]string, 0)

	seen := map[string]struct{}{}
	for index := range session.Spec.Volumes {
		namespace := session.Spec.Volumes[index].SourcePVC.Namespace
		if _, exists := seen[namespace]; exists {
			continue
		}

		seen[namespace] = struct{}{}
		namespaces = append(namespaces, namespace)
	}

	sort.Strings(namespaces)

	type result struct {
		pods []corev1.Pod
		err  error
	}

	results := make([]result, len(namespaces))
	parallel.For(len(namespaces), func(index int) {
		pods, err := s.client.CoreV1().Pods(namespaces[index]).List(ctx, metav1.ListOptions{})
		if err == nil && pods == nil {
			err = fmt.Errorf("list PVC consumers in %s returned an empty object", namespaces[index])
		}

		if pods != nil {
			results[index].pods = pods.Items
		}

		results[index].err = err
	})

	resolvedSourceNode := options.SourceNode
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		namespaceIndex := sort.SearchStrings(namespaces, volume.SourcePVC.Namespace)
		result := results[namespaceIndex]

		inferredSourceNode, err := s.validateCopyConsumersFromPods(
			session,
			volume,
			result.pods,
			result.err,
			resolvedSourceNode,
		)
		if err != nil {
			return err
		}

		if resolvedSourceNode == "" {
			resolvedSourceNode = inferredSourceNode
		}
	}

	if !checkpointSourceNode || options.SourceNode != "" || resolvedSourceNode == "" {
		return nil
	}

	options.SourceNode = resolvedSourceNode
	if s.store != nil {
		if err := s.store.Update(ctx, session); err != nil {
			options.SourceNode = ""

			return domain.WrapError(
				domain.ErrorKubernetes,
				"copy preflight",
				"persist inferred source node",
				err,
			)
		}
	}

	return nil
}

func (s *Service) validateCopyConsumersFromPods(
	session *domain.Session,
	volume *domain.VolumeSpec,
	pods []corev1.Pod,
	listErr error,
	sourceNode string,
) (string, error) {
	if listErr != nil {
		return "", domain.WrapError(
			domain.ErrorKubernetes,
			"copy preflight",
			"list PVC consumers in "+volume.SourcePVC.Namespace,
			listErr,
		)
	}

	active := make([]*corev1.Pod, 0)
	nodes := map[string]struct{}{}

	scheduledCount := 0
	for index := range pods {
		pod := &pods[index]
		if !kube.ActivePodUsesPVC(pod, volume.SourcePVC.Name) {
			continue
		}

		active = append(active, pod)
		if pod.Spec.NodeName != "" {
			scheduledCount++
			nodes[pod.Spec.NodeName] = struct{}{}
		}
	}

	if len(active) == 0 {
		return "", nil
	}

	options := session.Spec.WorkflowOptionsPtr()
	if options == nil {
		return "", domain.NewError(
			domain.ErrorValidation,
			"copy preflight",
			"copy session workflow options are missing",
		)
	}

	if !session.Spec.Online() {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"copy preflight",
			fmt.Sprintf(
				"offline copy requires PVC %s/%s to have zero active Pod consumers",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if kube.HasAccessMode(volume.AccessModes, corev1.ReadWriteOncePod) {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"copy preflight",
			fmt.Sprintf(
				"active RWOP PVC %s/%s cannot be warm-copied",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if kube.HasAccessMode(volume.AccessModes, corev1.ReadWriteOnce) &&
		scheduledCount != len(active) {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"copy preflight",
			fmt.Sprintf(
				"every active consumer of RWO PVC %s/%s must be scheduled before online copy",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if len(nodes) > 1 {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"copy preflight",
			fmt.Sprintf(
				"online copy consumers for PVC %s/%s moved across multiple nodes",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	inferredSourceNode := ""
	for node := range nodes {
		inferredSourceNode = node
		if sourceNode != "" && node != sourceNode {
			return "", domain.NewError(
				domain.ErrorConflict,
				"copy preflight",
				fmt.Sprintf(
					"PVC %s/%s consumer runs on %s, session source node is %s",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					node,
					sourceNode,
				),
			)
		}
	}

	return inferredSourceNode, nil
}

func (s *Service) copyWithRetry(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	mode copyengine.Mode,
	probeResults []kube.ToolImageProbeResult,
) error {
	values, err := s.helmSchedulingValues(
		ctx,
		session,
		probedSourceNode(session, volume, probeResults),
	)
	if err != nil {
		return err
	}

	pullSecretValues, err := kube.ToolImagePullSecretHelmValues(probeResults)
	if err != nil {
		return err
	}

	values = append(values, pullSecretValues...)

	var last error

	options := session.Spec.WorkflowOptions()
	for retryIndex := range s.config.Retries {
		if err := s.validateReservedVolume(ctx, session, volume, status); err != nil {
			return err
		}

		sourceMountReadWrite := false
		if mode == copyengine.ModeWarm {
			_, sourceMountReadWrite, err = s.sharedOpenEBSLVMSource(ctx, session, volume)
			if err != nil {
				return err
			}
		}

		status.Sync.Attempts++
		status.Sync.LastError = ""

		if err := s.store.Update(ctx, session); err != nil {
			return err
		}

		request := copyengine.Request{
			SessionID:             session.ID,
			ToolImage:             options.ToolImage,
			Source:                volume.SourcePVC,
			Destination:           volume.DestinationPVC,
			SourcePath:            domain.SourceTransferPath(volume.TransferScope),
			DestinationPath:       domain.DestinationTransferPath(volume.TransferScope),
			Mode:                  mode,
			Attempt:               status.Sync.Attempts,
			KubeconfigPath:        s.config.KubeconfigPath,
			Context:               s.config.Context,
			Strategies:            options.Strategies,
			DeleteExtraneousFiles: options.DeleteExtraneous,
			VerifyChecksum:        mode == copyengine.ModeFinal && options.VerifyChecksum,
			SourceMountReadWrite:  sourceMountReadWrite,
			IgnoreSizes:           volumeCapacityIsSmaller(volume),
			NoCompress:            s.config.NoCompress,
			HelmTimeout:           s.config.HelmTimeout,
			HelmStringValues:      values,
			Writer:                s.config.Writer,
			Logger:                s.config.Logger,
		}
		s.logInfo(
			"copy started",
			"session",
			session.ID,
			"pvc",
			volume.SourcePVC.Name,
			"mode",
			mode,
			"attempt",
			status.Sync.Attempts,
			"source",
			volume.SourcePVC.Namespace+"/"+volume.SourcePVC.Name,
			"sourcePath",
			request.SourcePath,
			"destination",
			volume.DestinationPVC.Namespace+"/"+volume.DestinationPVC.Name,
			"destinationPath",
			request.DestinationPath,
		)

		toolLogOptions := kube.ToolLogOptions{
			Namespaces:  []string{volume.SourcePVC.Namespace, volume.DestinationPVC.Namespace},
			OperationID: copyengine.OperationID(request),
		}
		if s.config.StreamToolLogs {
			toolLogOptions.Writer = s.config.Writer
			toolLogOptions.Logger = s.config.Logger
			toolLogOptions.Structured = s.config.StructuredLogs
		}

		toolLogs := kube.StartPVMigrateToolLogs(ctx, s.client, toolLogOptions)
		copyErr := s.copier.Copy(ctx, request, func(progress copyengine.Progress) {
			s.logInfo(
				"copy progress",
				"session",
				session.ID,
				"pvc",
				volume.SourcePVC.Name,
				"mode",
				progress.Mode,
				"attempt",
				progress.Attempt,
				"state",
				progress.State,
				"message",
				progress.Message,
			)
		})

		toolLogs.Stop()
		copyErr = mergeToolLogError(copyErr, toolLogs.ObservedError())

		s.logInfo(
			"waiting for copy tool Pods to release PVCs",
			"session",
			session.ID,
			"pvc",
			volume.SourcePVC.Name,
		)

		last = errors.Join(copyErr, s.waitForCopyToolRelease(ctx, volume))
		if last == nil {
			return nil
		}

		status.Sync.LastError = last.Error()
		if err := s.store.Update(ctx, session); err != nil {
			// Preserve the copy failure when the operation context was canceled;
			// failContext checkpoints the updated status with an independent context.
			if ctx.Err() != nil {
				return last
			}
			return err
		}

		if isDestinationNoSpaceError(last) {
			return domain.WrapError(
				domain.ErrorConflict,
				"copy capacity",
				fmt.Sprintf(
					"destination PVC %s/%s ran out of space; abort and clean up this session, then create a new session with a larger --destination-capacity",
					volume.DestinationPVC.Namespace,
					volume.DestinationPVC.Name,
				),
				last,
			)
		}

		if retryIndex+1 < s.config.Retries {
			delay := time.Duration(math.Pow(2, float64(retryIndex))) * s.config.RetryBackoff
			s.logInfo(
				"copy retry scheduled",
				"session",
				session.ID,
				"pvc",
				volume.SourcePVC.Name,
				"mode",
				mode,
				"attempt",
				status.Sync.Attempts,
				"nextAttempt",
				status.Sync.Attempts+1,
				"backoff",
				delay,
				"error",
				last,
			)

			if err := s.sleep(ctx, delay); err != nil {
				return domain.WrapError(
					domain.ErrorTimeout,
					"copy retry",
					"context ended during retry backoff",
					err,
				)
			}
		}
	}

	return last
}

func mergeToolLogError(copyErr, observedErr error) error {
	if copyErr == nil {
		return nil
	}
	return errors.Join(copyErr, observedErr)
}

func isDestinationNoSpaceError(err error) bool {
	if err == nil {
		return false
	}

	if copyengine.IsDestinationNoSpaceError(err) {
		return true
	}

	var visit func(error) bool

	visit = func(candidate error) bool {
		if candidate == nil {
			return false
		}

		if errors.Is(candidate, kube.ErrToolPodNoSpace) {
			return true
		}

		message := strings.ToLower(candidate.Error())
		if strings.Contains(message, "no space left on device") ||
			strings.Contains(message, "enospc") {
			return true
		}

		if joined, ok := candidate.(interface{ Unwrap() []error }); ok {
			return slices.ContainsFunc(joined.Unwrap(), visit)
		}

		return visit(errors.Unwrap(candidate))
	}

	return visit(err)
}

func volumeCapacityIsSmaller(volume *domain.VolumeSpec) bool {
	if volume == nil || volume.SourceCapacity == "" || volume.Capacity == "" {
		return false
	}

	source, sourceErr := resource.ParseQuantity(volume.SourceCapacity)
	destination, destinationErr := resource.ParseQuantity(volume.Capacity)

	return sourceErr == nil && destinationErr == nil && destination.Cmp(source) < 0
}

func (s *Service) waitForCopyToolRelease(ctx context.Context, volume *domain.VolumeSpec) error {
	claims := map[string]map[string]struct{}{}
	for _, ref := range []domain.ObjectReference{volume.SourcePVC, volume.DestinationPVC} {
		if claims[ref.Namespace] == nil {
			claims[ref.Namespace] = map[string]struct{}{}
		}

		claims[ref.Namespace][ref.Name] = struct{}{}
	}

	return kube.WaitFor(
		ctx,
		time.Second,
		fmt.Sprintf(
			"pv-migrate tools to release PVC %s/%s",
			volume.SourcePVC.Namespace,
			volume.SourcePVC.Name,
		),
		func(waitCtx context.Context) (bool, error) {
			for namespace, namespaceClaims := range claims {
				pods, err := s.client.CoreV1().Pods(namespace).List(waitCtx, metav1.ListOptions{})
				if err != nil {
					return false, domain.WrapError(
						domain.ErrorKubernetes,
						"copy cleanup",
						"list Pods in "+namespace,
						err,
					)
				}

				for i := range pods.Items {
					if isPVMigrateToolForClaims(&pods.Items[i], namespaceClaims) {
						return false, nil
					}
				}
			}

			return true, nil
		},
	)
}

func isPVMigrateToolForClaims(pod *corev1.Pod, claims map[string]struct{}) bool {
	if _, tool := pvmigrateToolInstance(pod); !tool {
		return false
	}

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		if _, exists := claims[volume.PersistentVolumeClaim.ClaimName]; exists {
			return true
		}
	}

	return false
}

func pvmigrateToolInstance(pod *corev1.Pod) (string, bool) {
	if pod == nil {
		return "", false
	}

	instance := pod.Labels[kube.AppInstanceLabel]
	if !strings.HasPrefix(instance, "pv-migrate-") {
		return "", false
	}

	switch pod.Labels[kube.AppComponentLabel] {
	case kube.ToolComponentSSHD, kube.ToolComponentRsync, kube.ToolComponentRclone:
		return instance, true
	default:
		return "", false
	}
}

func (s *Service) helmSchedulingValues(
	ctx context.Context,
	session *domain.Session,
	sourceNode string,
) ([]string, error) {
	values := kube.ZeroResourceHelmValues()

	options := session.Spec.WorkflowOptions()
	if sourceNode == "" {
		sourceNode = options.SourceNode
	}

	type schedulingTarget struct {
		component string
		nodes     []string
		pinNode   bool
	}

	targets := []schedulingTarget{
		{component: "rsync", nodes: []string{options.TargetNode}, pinNode: true},
	}
	if slices.Contains(options.Strategies, domain.StrategyLocal) {
		// The local strategy deploys an SSHD on both sides. PVC topology places
		// each Pod on its volume's node, while the combined tolerations allow
		// both source and destination nodes to accept their respective Pod.
		targets = append(
			targets,
			schedulingTarget{component: "sshd", nodes: []string{sourceNode, options.TargetNode}},
		)
	} else {
		targets = append(
			targets,
			schedulingTarget{component: "sshd", nodes: []string{sourceNode}, pinNode: true},
		)
	}

	nodeNames := make([]string, 0, 2)

	nodeIndexes := map[string]int{}
	for _, target := range targets {
		for _, nodeName := range target.nodes {
			if nodeName == "" {
				continue
			}

			if _, exists := nodeIndexes[nodeName]; exists {
				continue
			}

			nodeIndexes[nodeName] = len(nodeNames)
			nodeNames = append(nodeNames, nodeName)
		}
	}

	type nodeResult struct {
		node *corev1.Node
		err  error
	}

	nodes := make([]nodeResult, len(nodeNames))
	parallel.For(len(nodeNames), func(index int) {
		nodes[index].node, nodes[index].err = s.client.CoreV1().
			Nodes().
			Get(ctx, nodeNames[index], metav1.GetOptions{})
	})

	for _, target := range targets {
		seenNodes := map[string]struct{}{}

		componentNodes := make([]*corev1.Node, 0, len(target.nodes))
		for _, nodeName := range target.nodes {
			if nodeName == "" {
				continue
			}

			if _, seen := seenNodes[nodeName]; seen {
				continue
			}

			seenNodes[nodeName] = struct{}{}

			result := nodes[nodeIndexes[nodeName]]
			if result.err != nil {
				return nil, domain.WrapError(
					domain.ErrorKubernetes,
					"copy scheduling",
					"read node "+nodeName,
					result.err,
				)
			}

			node := result.node
			if node == nil || node.Name == "" {
				return nil, domain.NewError(
					domain.ErrorKubernetes,
					"copy scheduling",
					fmt.Sprintf("read node %s returned an empty object", nodeName),
				)
			}

			componentNodes = append(componentNodes, node)
			if target.pinNode {
				hostname := node.Labels[corev1.LabelHostname]
				if hostname == "" {
					return nil, domain.NewError(
						domain.ErrorPrecondition,
						"copy scheduling",
						fmt.Sprintf("node %s lacks %s", nodeName, corev1.LabelHostname),
					)
				}

				values = append(
					values,
					fmt.Sprintf(
						"%s.nodeSelector.kubernetes\\.io/hostname=%s",
						target.component,
						hostname,
					),
				)
			}
		}

		values = append(
			values,
			kube.ToolComponentTolerationHelmValues(target.component, componentNodes...)...,
		)
	}

	return values, nil
}

func probedSourceNode(
	session *domain.Session,
	volume *domain.VolumeSpec,
	results []kube.ToolImageProbeResult,
) string {
	if session == nil || volume == nil {
		return ""
	}

	if sourceNode := session.Spec.WorkflowOptions().SourceNode; sourceNode != "" {
		return sourceNode
	}

	for _, result := range results {
		if result.Target.Namespace == volume.SourcePVC.Namespace &&
			result.Target.PVCName == volume.SourcePVC.Name &&
			slices.Contains(result.Target.Components, kube.ToolComponentSSHD) {
			return result.NodeName
		}
	}

	return ""
}

func (s *Service) verifySourceStorage(ctx context.Context, session *domain.Session) error {
	results := make([]error, len(session.Spec.Volumes))
	parallel.For(len(session.Spec.Volumes), func(index int) {
		volume := &session.Spec.Volumes[index]
		if volume.SourcePVC.Namespace == "" || volume.SourcePVC.Name == "" ||
			volume.SourcePVC.UID == "" {
			results[index] = domain.NewError(
				domain.ErrorPrecondition,
				"verify source storage",
				fmt.Sprintf("source PVC reference for volume %d is incomplete", index),
			)

			return
		}

		if volume.SourcePV.Name == "" || volume.SourcePV.UID == "" {
			results[index] = domain.NewError(
				domain.ErrorPrecondition,
				"verify source storage",
				fmt.Sprintf(
					"source PV reference for PVC %s/%s is incomplete",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
				),
			)

			return
		}

		pvc, err := s.client.CoreV1().
			PersistentVolumeClaims(volume.SourcePVC.Namespace).
			Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
		if err != nil {
			results[index] = domain.WrapError(
				domain.ErrorKubernetes,
				"verify source storage",
				fmt.Sprintf(
					"read source PVC %s/%s",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
				),
				err,
			)

			return
		}

		if pvc == nil || pvc.Name == "" {
			results[index] = domain.NewError(
				domain.ErrorKubernetes,
				"verify source storage",
				fmt.Sprintf(
					"read source PVC %s/%s returned an empty object",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
				),
			)

			return
		}

		if pvc.UID != volume.SourcePVC.UID || pvc.Status.Phase != corev1.ClaimBound ||
			pvc.Spec.VolumeName != volume.SourcePV.Name {
			results[index] = domain.NewError(
				domain.ErrorConflict,
				"verify source storage",
				fmt.Sprintf(
					"source PVC %s/%s identity or binding changed",
					pvc.Namespace,
					pvc.Name,
				),
			)

			return
		}

		pv, err := s.client.CoreV1().
			PersistentVolumes().
			Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
		if err != nil {
			results[index] = domain.WrapError(
				domain.ErrorKubernetes,
				"verify source storage",
				"read source PV "+volume.SourcePV.Name,
				err,
			)

			return
		}

		if pv == nil || pv.Name == "" {
			results[index] = domain.NewError(
				domain.ErrorKubernetes,
				"verify source storage",
				fmt.Sprintf("read source PV %s returned an empty object", volume.SourcePV.Name),
			)

			return
		}

		if pv.UID != volume.SourcePV.UID || pv.Spec.ClaimRef == nil ||
			pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
			pv.Spec.ClaimRef.Name != pvc.Name ||
			pv.Spec.ClaimRef.UID != pvc.UID {
			results[index] = domain.NewError(
				domain.ErrorConflict,
				"verify source storage",
				fmt.Sprintf("source PV %s identity or claimRef changed", pv.Name),
			)
		}
	})

	for _, err := range results {
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) verifyActiveStorage(ctx context.Context, session *domain.Session) error {
	results := make([]error, len(session.Spec.Volumes))
	parallel.For(len(session.Spec.Volumes), func(index int) {
		results[index] = s.verifyActiveStorageVolume(ctx, session, index)
	})

	for _, err := range results {
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) validateActivationStorage(ctx context.Context, session *domain.Session) error {
	offline := make([]*domain.VolumeSpec, 0, len(session.Spec.Volumes))
	for index := range session.Spec.Volumes {
		status := &session.Status.Volumes[index]
		if status.Activation.ActivatedAt != nil || status.Activation.ActivePVC.Name != "" {
			if err := s.verifyActiveStorageVolume(ctx, session, index); err != nil {
				return err
			}
			continue
		}

		offline = append(offline, &session.Spec.Volumes[index])
	}

	return s.verifyVolumesOffline(ctx, session, offline)
}

func (s *Service) validateActivationPVCPolicies(
	ctx context.Context,
	session *domain.Session,
) error {
	groups := make(map[string][]kube.PVCAdmissionChange)
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		status := &session.Status.Volumes[index]
		if status.Activation.ActivatedAt != nil || status.Activation.ActivePVC.Name != "" {
			continue
		}

		requested, err := resource.ParseQuantity(volume.Capacity)
		if err != nil || requested.Sign() <= 0 {
			return domain.NewError(
				domain.ErrorValidation,
				"activation preflight",
				fmt.Sprintf(
					"PVC %s/%s has invalid destination capacity %q",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					volume.Capacity,
				),
			)
		}

		existing := volume.SourcePVCSpec.Resources.Requests[corev1.ResourceStorage]

		sourceClass := ""
		if volume.SourcePVCSpec.StorageClassName != nil {
			sourceClass = *volume.SourcePVCSpec.StorageClassName
		}

		groups[volume.SourcePVC.Namespace] = append(
			groups[volume.SourcePVC.Namespace],
			kube.PVCAdmissionChange{
				Namespace:             volume.SourcePVC.Namespace,
				Name:                  volume.SourcePVC.Name,
				RequestedStorage:      requested,
				RequestedStorageClass: volume.StorageClass,
				Existing:              !status.Activation.SourcePVCDeleted,
				ExistingStorage:       existing,
				ExistingStorageClass:  sourceClass,
			},
		)
	}

	namespaces := make([]string, 0, len(groups))
	for namespace := range groups {
		namespaces = append(namespaces, namespace)
	}

	sort.Strings(namespaces)

	for _, namespace := range namespaces {
		report, err := kube.CheckPVCAdmissionPolicies(ctx, s.client, groups[namespace])
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"activation preflight",
				"check application PVC admission in "+namespace,
				err,
			)
		}

		if len(report.QuotaViolations) > 0 {
			return domain.NewError(
				domain.ErrorPrecondition,
				"activation preflight",
				"application PVC quota rejected the replacement: "+strings.Join(
					report.QuotaViolations,
					"; ",
				),
			)
		}

		if len(report.LimitRangeViolations) > 0 {
			return domain.NewError(
				domain.ErrorPrecondition,
				"activation preflight",
				"application PVC LimitRange rejected the replacement: "+strings.Join(
					report.LimitRangeViolations,
					"; ",
				),
			)
		}
	}

	return nil
}

func (s *Service) validateRollbackRecoveryStorage(
	ctx context.Context,
	session *domain.Session,
	rollbackOrigin domain.Phase,
) error {
	offline := make([]*domain.VolumeSpec, 0, len(session.Spec.Volumes))

	originWasActive := rollbackOrigin == domain.PhaseActivated ||
		rollbackOrigin == domain.PhaseResuming ||
		rollbackOrigin == domain.PhaseCompleted
	for index := range session.Spec.Volumes {
		status := &session.Status.Volumes[index]
		if status.Activation.RolledBackAt != nil {
			if err := s.verifyRollbackStorageVolume(ctx, session, index); err != nil {
				return err
			}
			continue
		}

		if originWasActive || status.Activation.ActivatedAt != nil ||
			status.Activation.ActivePVC.Name != "" {
			if err := s.verifyActiveStorageVolume(ctx, session, index); err != nil {
				return err
			}
			continue
		}

		offline = append(offline, &session.Spec.Volumes[index])
	}

	return s.verifyVolumesOffline(ctx, session, offline)
}

func (s *Service) validateRollbackStorage(
	ctx context.Context,
	session *domain.Session,
	phase, rollbackOrigin domain.Phase,
	recoveringRollback, wasRunning bool,
) error {
	if recoveringRollback {
		return s.validateRollbackRecoveryStorage(ctx, session, rollbackOrigin)
	}

	if wasRunning {
		return s.verifyActiveVolumes(ctx, session)
	}

	if phase == domain.PhaseActivated {
		return s.verifyActiveStorage(ctx, session)
	}

	if phase == domain.PhaseActivating ||
		(phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseActivating) {
		return s.validateActivationStorage(ctx, session)
	}

	return s.validateOfflineVolumes(ctx, session)
}

func (s *Service) verifyRollbackStorage(ctx context.Context, session *domain.Session) error {
	results := make([]error, len(session.Spec.Volumes))
	parallel.For(len(session.Spec.Volumes), func(index int) {
		results[index] = s.verifyRollbackStorageVolume(ctx, session, index)
	})

	for _, err := range results {
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) verifyRollbackStorageVolume(
	ctx context.Context,
	session *domain.Session,
	index int,
) error {
	volume := &session.Spec.Volumes[index]

	active := session.Status.Volumes[index].Activation.ActivePVC
	if active.Namespace == "" || active.Name == "" || active.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify rollback",
			fmt.Sprintf(
				"PVC %s/%s has no recorded restored identity",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if active.Namespace != volume.SourcePVC.Namespace || active.Name != volume.SourcePVC.Name {
		return domain.NewError(
			domain.ErrorConflict,
			"verify rollback",
			fmt.Sprintf(
				"recorded restored PVC %s/%s does not match source PVC %s/%s",
				active.Namespace,
				active.Name,
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if volume.SourcePV.Name == "" || volume.SourcePV.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify rollback",
			fmt.Sprintf(
				"PVC %s/%s has no recorded source PV identity",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(active.Namespace).
		Get(ctx, active.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify rollback",
			fmt.Sprintf("read restored PVC %s/%s", active.Namespace, active.Name),
			err,
		)
	}

	if pvc == nil || pvc.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			"verify rollback",
			fmt.Sprintf(
				"read restored PVC %s/%s returned an empty object",
				active.Namespace,
				active.Name,
			),
		)
	}

	if pvc.UID != active.UID || pvc.Status.Phase != corev1.ClaimBound ||
		pvc.Spec.VolumeName != volume.SourcePV.Name {
		return domain.NewError(
			domain.ErrorConflict,
			"verify rollback",
			fmt.Sprintf("restored PVC %s/%s identity or binding changed", pvc.Namespace, pvc.Name),
		)
	}

	if pvc.UID != volume.SourcePVC.UID && pvc.Annotations[kube.SessionKey] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify rollback",
			fmt.Sprintf(
				"restored PVC %s/%s is not the original or session-owned PVC",
				pvc.Namespace,
				pvc.Name,
			),
		)
	}

	pv, err := s.client.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify rollback",
			"read restored PV "+volume.SourcePV.Name,
			err,
		)
	}

	if pv == nil || pv.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			"verify rollback",
			fmt.Sprintf("read restored PV %s returned an empty object", volume.SourcePV.Name),
		)
	}

	if pv.UID != volume.SourcePV.UID || pv.Spec.ClaimRef == nil ||
		pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify rollback",
			fmt.Sprintf("restored PV %s identity or claimRef changed", pv.Name),
		)
	}

	return nil
}

func (s *Service) verifyActiveStorageVolume(
	ctx context.Context,
	session *domain.Session,
	index int,
) error {
	volume := &session.Spec.Volumes[index]

	active := session.Status.Volumes[index].Activation.ActivePVC
	if active.Namespace == "" || active.Name == "" || active.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify migration",
			fmt.Sprintf(
				"PVC %s/%s has no recorded active identity",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if active.Namespace != volume.SourcePVC.Namespace || active.Name != volume.SourcePVC.Name {
		return domain.NewError(
			domain.ErrorConflict,
			"verify migration",
			fmt.Sprintf(
				"recorded active PVC %s/%s does not match application PVC %s/%s",
				active.Namespace,
				active.Name,
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if volume.DestinationPV.Name == "" || volume.DestinationPV.UID == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify migration",
			fmt.Sprintf(
				"PVC %s/%s has no recorded destination PV identity",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	pvc, err := s.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify migration",
			fmt.Sprintf("read PVC %s/%s", volume.SourcePVC.Namespace, volume.SourcePVC.Name),
			err,
		)
	}

	if pvc == nil || pvc.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			"verify migration",
			fmt.Sprintf(
				"read PVC %s/%s returned an empty object",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName != volume.DestinationPV.Name ||
		pvc.Annotations[kube.SessionKey] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify migration",
			fmt.Sprintf(
				"PVC %s/%s is not active on destination PV %s",
				pvc.Namespace,
				pvc.Name,
				volume.DestinationPV.Name,
			),
		)
	}

	if pvc.UID != active.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify migration",
			fmt.Sprintf("active PVC %s/%s UID changed", pvc.Namespace, pvc.Name),
		)
	}

	pv, err := s.client.CoreV1().
		PersistentVolumes().
		Get(ctx, volume.DestinationPV.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify migration",
			"read active PV "+volume.DestinationPV.Name,
			err,
		)
	}

	if pv == nil || pv.Name == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			"verify migration",
			fmt.Sprintf("read active PV %s returned an empty object", volume.DestinationPV.Name),
		)
	}

	if pv.UID != volume.DestinationPV.UID || pv.Spec.ClaimRef == nil ||
		pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.ClaimRef.UID != pvc.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify migration",
			fmt.Sprintf("active PV %s identity or claimRef changed", pv.Name),
		)
	}

	return nil
}

func (s *Service) validateWorkloadResume(ctx context.Context, session *domain.Session) error {
	if err := s.validateOpenEBSLVMSharedMountRestore(ctx, session); err != nil {
		return err
	}

	if err := s.verifyActiveStorage(ctx, session); err != nil {
		return err
	}

	options := session.Spec.WorkflowOptions()
	// TargetNode is the exact placement contract only for the standalone
	// adapter. Controller-managed workloads keep their own scheduling policy;
	// the tool node can become unavailable while the workload still has a
	// valid placement elsewhere.
	if session.Spec.Workload().Adapter != domain.WorkloadStandalone || options.TargetNode == "" {
		return nil
	}

	node, err := s.client.CoreV1().Nodes().Get(ctx, options.TargetNode, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "resume dry-run", "read target node", err)
	}

	if !nodeReadyAndSchedulable(node) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume dry-run",
			fmt.Sprintf("target node %s must be Ready and schedulable", node.Name),
		)
	}

	return nil
}

func (s *Service) verifyActiveVolumes(ctx context.Context, session *domain.Session) error {
	if err := s.verifyActiveStorage(ctx, session); err != nil {
		return err
	}

	options := session.Spec.WorkflowOptions()
	// TargetNode pins reservation and copy tools for every workload. The
	// standalone adapter also pins the recreated Pod; controller-managed
	// workloads retain their own scheduler policy and may validly land on a
	// different node when the destination volume is topology-independent.
	if session.Spec.Workload().Adapter == domain.WorkloadStandalone && options.TargetNode != "" {
		ref := session.Spec.Workload().Pod

		pod, err := s.client.CoreV1().Pods(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"verify migration",
				"read resumed Pod",
				err,
			)
		}

		if ref.UID == "" || pod.UID != ref.UID || pod.Annotations[kube.SessionKey] != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"verify migration",
				fmt.Sprintf(
					"Pod %s/%s identity or session ownership changed",
					pod.Namespace,
					pod.Name,
				),
			)
		}

		if pod.Spec.NodeName != options.TargetNode {
			return domain.NewError(
				domain.ErrorPrecondition,
				"verify migration",
				fmt.Sprintf(
					"Pod %s/%s runs on %s, expected %s",
					pod.Namespace,
					pod.Name,
					pod.Spec.NodeName,
					options.TargetNode,
				),
			)
		}
	}

	return nil
}

func nodeReadyAndSchedulable(node *corev1.Node) bool {
	if node == nil || node.Spec.Unschedulable {
		return false
	}

	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}

func (s *Service) begin(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
	message string,
) error {
	if session.Status.Phase == phase {
		s.logInfo(
			"migration stage resumed",
			"session",
			session.ID,
			"phase",
			phase,
			"message",
			message,
		)

		return nil
	}

	if session.Status.Phase == domain.PhaseFailed {
		if phase != domain.PhaseRollingBack && phase != domain.PhaseAborting &&
			session.Status.ResumeFrom != phase {
			return domain.NewError(
				domain.ErrorPrecondition,
				"resume phase",
				fmt.Sprintf(
					"failed session resumes from %s, requested %s",
					session.Status.ResumeFrom,
					phase,
				),
			)
		}
	}

	previousStatus := session.Status
	if err := session.Transition(phase, message, s.now()); err != nil {
		return err
	}

	if err := s.persist(ctx, session); err != nil {
		session.Status = previousStatus
		s.logError(
			"migration stage persistence failed",
			"session",
			session.ID,
			"phase",
			phase,
			"message",
			message,
			"error",
			err,
		)

		return err
	}

	s.logInfo("migration stage started", "session", session.ID, "phase", phase, "message", message)

	return nil
}

func (s *Service) finish(
	ctx context.Context,
	session *domain.Session,
	phase domain.Phase,
	message string,
) error {
	previousStatus := session.Status
	if err := session.Transition(phase, message, s.now()); err != nil {
		return err
	}

	if err := s.persist(ctx, session); err != nil {
		session.Status = previousStatus
		s.logError(
			"migration stage persistence failed",
			"session",
			session.ID,
			"phase",
			phase,
			"message",
			message,
			"error",
			err,
		)

		return err
	}

	s.logInfo(
		"migration stage completed",
		"session",
		session.ID,
		"phase",
		phase,
		"message",
		message,
	)

	return nil
}

func (s *Service) fail(ctx context.Context, session *domain.Session, cause error) error {
	return s.failContext(ctx, session, cause)
}

func (s *Service) failContext(ctx context.Context, session *domain.Session, cause error) error {
	if session.Status.Phase != domain.PhaseFailed {
		session.Status.FailureReason = failureReason(cause)
		if err := session.Transition(domain.PhaseFailed, cause.Error(), s.now()); err != nil {
			return errors.Join(cause, err)
		}
		// The operation context may already be canceled when a stage fails.
		// Keep the failure checkpoint alive long enough to persist ResumeFrom,
		// while checking the session fence before and after the write.
		if held, ok := ctx.Value(sessionLockContextKey{}).(heldSessionLock); ok {
			if err := held.lock.Err(); err != nil {
				return errors.Join(cause, err)
			}
		}

		checkpointCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := s.store.Update(checkpointCtx, session)

		cancel()

		if held, ok := ctx.Value(sessionLockContextKey{}).(heldSessionLock); ok {
			if lockErr := held.lock.Err(); lockErr != nil {
				err = errors.Join(err, lockErr)
			}
		}

		if err != nil {
			return errors.Join(cause, err)
		}
	}

	return cause
}

func failureReason(err error) domain.SessionFailureReason {
	if isDestinationNoSpaceError(err) {
		return domain.FailureDestinationCapacityExhausted
	}
	return ""
}

func validateRetryableSessionFailure(session *domain.Session) error {
	if session != nil && session.Status.Phase == domain.PhaseFailed &&
		session.Status.FailureReason == domain.FailureDestinationCapacityExhausted {
		return domain.NewError(
			domain.ErrorConflict,
			"resume session",
			"destination capacity was exhausted and cannot be changed in this session; abort and clean up this session, then create a new session with a larger --destination-capacity",
		)
	}

	return nil
}

func (s *Service) persist(ctx context.Context, session *domain.Session) error {
	persistCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return s.store.Update(persistCtx, session)
}

func (s *Service) logInfo(message string, args ...any) {
	if s != nil && s.config.Logger != nil {
		s.config.Logger.Info(message, args...)
	}
}

func (s *Service) logWarn(message string, args ...any) {
	if s != nil && s.config.Logger != nil {
		s.config.Logger.Warn(message, args...)
	}
}

func (s *Service) logError(message string, args ...any) {
	if s != nil && s.config.Logger != nil {
		s.config.Logger.Error(message, args...)
	}
}

func phaseAfter(current, reference domain.Phase) bool {
	order := []domain.Phase{
		domain.PhasePlanned,
		domain.PhaseReserving,
		domain.PhaseReserved,
		domain.PhaseWarmCopying,
		domain.PhaseWarmCopied,
		domain.PhasePausing,
		domain.PhasePaused,
		domain.PhaseFinalSyncing,
		domain.PhaseFinalSynced,
		domain.PhaseActivating,
		domain.PhaseActivated,
		domain.PhaseResuming,
		domain.PhaseCompleted,
	}
	index := func(value domain.Phase) int {
		for i, phase := range order {
			if value == phase {
				return i
			}
		}

		return -1
	}

	return index(current) > index(reference)
}

func phaseBefore(session *domain.Session, target domain.Phase) domain.Phase {
	for index := len(session.Status.History) - 1; index > 0; index-- {
		if session.Status.History[index].Phase == target {
			return session.Status.History[index-1].Phase
		}
	}

	return ""
}

func abortRequiresWorkloadResume(session *domain.Session) bool {
	previous := session.Status.Phase
	if previous == domain.PhaseFailed || previous == domain.PhaseAborting {
		previous = session.Status.ResumeFrom
	}

	if previous == domain.PhaseAborting {
		previous = phaseBefore(session, domain.PhaseAborting)
	}

	switch previous {
	case domain.PhasePausing, domain.PhasePaused, domain.PhaseFinalSyncing, domain.PhaseFinalSynced:
		return true
	}

	if session.Status.Phase == domain.PhaseAborting ||
		session.Status.ResumeFrom == domain.PhaseAborting {
		return abortOriginWasPaused(session)
	}

	return false
}

func abortOriginWasPaused(session *domain.Session) bool {
	for _, v := range slices.Backward(session.Status.History) {
		switch v.Phase {
		case domain.PhaseFailed, domain.PhaseAborting:
			continue
		case domain.PhasePausing,
			domain.PhasePaused,
			domain.PhaseFinalSyncing,
			domain.PhaseFinalSynced:
			return true
		default:
			return false
		}
	}

	return false
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
