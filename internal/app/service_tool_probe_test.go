package app

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

type recordingToolImageProber struct {
	calls   []kube.ToolImageProbeOptions
	results []kube.ToolImageProbeResult
	err     error
	onProbe func(context.Context)
}

type recordingOpenEBSLVMSharedVolumeManager struct {
	shared              bool
	sharedPVs           []string
	sharedLVMVolumes    []domain.ObjectReference
	sharedSessionIDs    []string
	prepareErr          error
	prepareErrs         map[string]error
	preparePVs          []string
	ensurePVCs          []string
	ensurePVCUIDs       []types.UID
	ensurePVs           []string
	ensureErr           error
	enablePVs           []string
	onEnable            func()
	restoreErr          error
	validateRestoreErr  error
	validateRestoreErrs map[string]error
	validateRestorePVs  []string
	restorePVs          []string
	restoreContextErrs  []error
}

type toolProbeContextStore struct {
	memoryStore
	updateContextErrs []error
}

func (s *toolProbeContextStore) Update(ctx context.Context, session *domain.Session) error {
	err := ctx.Err()

	s.updateContextErrs = append(s.updateContextErrs, err)
	if err != nil {
		return err
	}

	return s.memoryStore.Update(ctx, session)
}

func (m *recordingOpenEBSLVMSharedVolumeManager) Shared(
	_ context.Context,
	sourcePV, lvmVolume domain.ObjectReference,
	sessionID string,
) (bool, error) {
	m.sharedPVs = append(m.sharedPVs, sourcePV.Name)
	m.sharedLVMVolumes = append(m.sharedLVMVolumes, lvmVolume)
	m.sharedSessionIDs = append(m.sharedSessionIDs, sessionID)
	return m.shared, nil
}

func (m *recordingOpenEBSLVMSharedVolumeManager) PrepareShared(
	_ context.Context,
	sourcePV domain.ObjectReference,
) (kube.OpenEBSLVMSharedResult, error) {
	m.preparePVs = append(m.preparePVs, sourcePV.Name)
	if m.prepareErr != nil {
		return kube.OpenEBSLVMSharedResult{}, m.prepareErr
	}

	if err := m.prepareErrs[sourcePV.Name]; err != nil {
		return kube.OpenEBSLVMSharedResult{}, err
	}

	return kube.OpenEBSLVMSharedResult{
		Reference: "LVMVolume openebs/" + sourcePV.Name,
		LVMVolume: domain.ObjectReference{
			APIVersion: "local.openebs.io/v1alpha1",
			Kind:       "LVMVolume",
			Namespace:  "openebs",
			Name:       sourcePV.Name,
			UID:        types.UID("lvm-" + sourcePV.Name),
		},
		PreviousShared:    "no",
		PreviousSharedSet: true,
		NeedsChange:       true,
	}, nil
}

func (m *recordingOpenEBSLVMSharedVolumeManager) EnsureShared(
	_ context.Context,
	pvc domain.ObjectReference,
	pv domain.ObjectReference,
) (kube.OpenEBSLVMSharedResult, error) {
	m.ensurePVCs = append(m.ensurePVCs, pvc.Namespace+"/"+pvc.Name)
	m.ensurePVCUIDs = append(m.ensurePVCUIDs, pvc.UID)

	m.ensurePVs = append(m.ensurePVs, pv.Name)
	if m.ensureErr != nil {
		return kube.OpenEBSLVMSharedResult{}, m.ensureErr
	}

	return kube.OpenEBSLVMSharedResult{
		Reference:   "LVMVolume openebs/" + pv.Name,
		NeedsChange: true,
	}, nil
}

func (m *recordingOpenEBSLVMSharedVolumeManager) EnableShared(
	_ context.Context,
	_ string,
	state domain.OpenEBSLVMSharedMount,
) error {
	m.enablePVs = append(m.enablePVs, state.SourcePV.Name)

	m.shared = true
	if m.onEnable != nil {
		m.onEnable()
	}

	return nil
}

func (m *recordingOpenEBSLVMSharedVolumeManager) ValidateRestoreShared(
	_ context.Context,
	_ string,
	state domain.OpenEBSLVMSharedMount,
) error {
	m.validateRestorePVs = append(m.validateRestorePVs, state.SourcePV.Name)
	if err := m.validateRestoreErrs[state.SourcePV.Name]; err != nil {
		return err
	}

	return m.validateRestoreErr
}

func (m *recordingOpenEBSLVMSharedVolumeManager) RestoreShared(
	ctx context.Context,
	_ string,
	state domain.OpenEBSLVMSharedMount,
) error {
	m.restorePVs = append(m.restorePVs, state.SourcePV.Name)

	m.restoreContextErrs = append(m.restoreContextErrs, ctx.Err())
	if err := ctx.Err(); err != nil {
		return err
	}

	if m.restoreErr != nil {
		return m.restoreErr
	}

	m.shared = false

	return nil
}

func openEBSLVMSourcePV(volume domain.VolumeSpec) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volume.SourcePV.Name, UID: volume.SourcePV.UID},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       kube.OpenEBSLVMCSIDriver,
					VolumeHandle: volume.SourcePV.Name,
				},
			},
			ClaimRef: &corev1.ObjectReference{
				Namespace: volume.SourcePVC.Namespace,
				Name:      volume.SourcePVC.Name,
				UID:       volume.SourcePVC.UID,
			},
		},
	}
}

func openEBSLVMSourcePVC(volume domain.VolumeSpec) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: volume.SourcePVC.Namespace,
			Name:      volume.SourcePVC.Name,
			UID:       volume.SourcePVC.UID,
		},
		Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: volume.SourcePV.Name},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
}

func TestConcurrentDestinationMountUsesActualVolumeAndConsumerCount(t *testing.T) {
	for _, test := range []struct {
		name           string
		consumerCount  int
		accessModes    []corev1.PersistentVolumeAccessMode
		driver         string
		flag           bool
		operation      domain.Operation
		withoutManager bool
		actualUID      types.UID
		wantCategory   domain.ErrorCategory
		wantEnsure     bool
	}{
		{
			name:          "authorized OpenEBS RWO target",
			consumerCount: 2,
			accessModes:   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			driver:        kube.OpenEBSLVMCSIDriver,
			flag:          true,
			actualUID:     "destination-pv-uid",
			wantEnsure:    true,
		},
		{
			name:           "OpenEBS target requires manager",
			consumerCount:  2,
			accessModes:    []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			driver:         kube.OpenEBSLVMCSIDriver,
			flag:           true,
			withoutManager: true,
			actualUID:      "destination-pv-uid",
			wantCategory:   domain.ErrorInternal,
		},
		{
			name:          "OpenEBS target requires authorization",
			consumerCount: 2,
			accessModes:   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			driver:        kube.OpenEBSLVMCSIDriver,
			actualUID:     "destination-pv-uid",
			wantCategory:  domain.ErrorPrecondition,
		},
		{
			name:          "destination PV identity changed",
			consumerCount: 2,
			accessModes:   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			driver:        kube.OpenEBSLVMCSIDriver,
			flag:          true,
			actualUID:     "replacement-pv-uid",
			wantCategory:  domain.ErrorConflict,
		},
		{
			name:          "non OpenEBS target",
			consumerCount: 2,
			accessModes:   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			driver:        "example.csi.io",
			actualUID:     "destination-pv-uid",
		},
		{
			name:          "copy workflow has no application destination",
			consumerCount: 2,
			accessModes:   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			operation:     domain.OperationCopy,
		},
		{
			name:          "single RWO consumer",
			consumerCount: 1,
			accessModes:   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		},
		{
			name:          "multiple RWX consumers",
			consumerCount: 2,
			accessModes:   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := &recordingOpenEBSLVMSharedVolumeManager{}

			objects := []runtime.Object{}
			if test.driver != "" {
				objects = append(objects, &corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{Name: "pv-destination", UID: test.actualUID},
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{Driver: test.driver},
						},
					},
				})
			}

			config := Config{OpenEBSLVMSharedVolumeManager: manager}
			if test.withoutManager {
				config.OpenEBSLVMSharedVolumeManager = nil
			}

			service := &Service{client: fake.NewClientset(objects...), config: config}

			session := appTestSession()
			if test.operation != "" {
				setSessionOperation(session, test.operation)
			}

			if session.Spec.MigratePod != nil {
				session.Spec.MigratePod.Workload.Adapter = domain.WorkloadStatefulSet
				session.Spec.MigratePod.OpenEBSLVMEnableShared = test.flag
			}

			volume := &session.Spec.Volumes[0]
			volume.AccessModes = test.accessModes
			volume.ConcurrentConsumers = test.consumerCount
			volume.DestinationPV = domain.ObjectReference{
				Name: "pv-destination",
				UID:  "destination-pv-uid",
			}

			err := service.ensureConcurrentDestinationMount(context.Background(), session, 0)
			if test.wantCategory == "" {
				if err != nil {
					t.Fatalf("error=%v", err)
				}
			} else if got := domain.CategoryOf(err); got != test.wantCategory {
				t.Fatalf("category=%s error=%v, want %s", got, err, test.wantCategory)
			}

			if got := len(manager.ensurePVs) == 1; got != test.wantEnsure {
				t.Fatalf("ensure calls=%v, want call=%t", manager.ensurePVs, test.wantEnsure)
			}
		})
	}
}

func TestReserveRetryRevalidatesConcurrentDestinationMount(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)
	volume := &session.Spec.Volumes[0]
	volume.ConcurrentConsumers = 2
	volume.DestinationPVC.UID = "existing-destination-pvc-uid"
	volume.DestinationPV = domain.ObjectReference{
		Name: "existing-destination-pv",
		UID:  "existing-destination-pv-uid",
	}
	session.Status.Volumes[0].Reserved = true
	session.Spec.MigratePod.OpenEBSLVMEnableShared = true
	manager := &recordingOpenEBSLVMSharedVolumeManager{}
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager

	if _, err := fixture.client.CoreV1().PersistentVolumes().Create(
		context.Background(),
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: volume.DestinationPV.Name,
				UID:  volume.DestinationPV.UID,
			},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{Driver: kube.OpenEBSLVMCSIDriver},
				},
			},
		},
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	fixture.reserver.failures["logs"] = errors.New("reservation failed")
	if err := fixture.service.Reserve(context.Background(), session); err == nil {
		t.Fatal("Reserve() unexpectedly succeeded")
	}

	delete(fixture.reserver.failures, "logs")

	if err := fixture.service.Reserve(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(
		manager.ensurePVs,
		[]string{"existing-destination-pv", "existing-destination-pv"},
	) {
		t.Fatalf("destination shared validations=%v", manager.ensurePVs)
	}

	if !slices.Equal(
		manager.ensurePVCs,
		[]string{
			volume.DestinationPVC.Namespace + "/" + volume.DestinationPVC.Name,
			volume.DestinationPVC.Namespace + "/" + volume.DestinationPVC.Name,
		},
	) {
		t.Fatalf("destination shared PVC validations=%v", manager.ensurePVCs)
	}
}

func TestResumeRevalidatesConcurrentDestinationMountBeforeController(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseActivated
	session.Spec.Volumes[0].ConcurrentConsumers = 2
	session.Spec.MigratePod.OpenEBSLVMEnableShared = true
	_ = session.Spec.SetWorkload(domain.WorkloadSpec{
		Adapter: domain.WorkloadStatefulSet,
		Pod: domain.ObjectReference{
			Namespace: "app",
			Name:      "database-0",
			UID:       "database-0-uid",
		},
		Controller: domain.ObjectReference{
			Namespace: "app",
			Name:      "database",
			UID:       "database-uid",
		},
	})
	createActiveDestinationStorage(t, fixture, session)

	pv, err := fixture.client.CoreV1().PersistentVolumes().Get(
		context.Background(),
		session.Spec.Volumes[0].DestinationPV.Name,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	pv.Spec.CSI = &corev1.CSIPersistentVolumeSource{Driver: kube.OpenEBSLVMCSIDriver}
	if _, err := fixture.client.CoreV1().PersistentVolumes().Update(
		context.Background(),
		pv,
		metav1.UpdateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	manager := &recordingOpenEBSLVMSharedVolumeManager{}
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager
	controllerCalledAfterEnsure := false
	fixture.controller.resumeHook = func(context.Context, *domain.Session) error {
		controllerCalledAfterEnsure = len(manager.ensurePVs) == 1
		return nil
	}

	if err := fixture.service.ResumePodWorkload(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if !controllerCalledAfterEnsure {
		t.Fatalf(
			"controller resumed before destination shared validation: calls=%v",
			manager.ensurePVs,
		)
	}

	activePVC := session.Status.Volumes[0].Activation.ActivePVC
	if !slices.Equal(
		manager.ensurePVCs,
		[]string{activePVC.Namespace + "/" + activePVC.Name},
	) || !slices.Equal(manager.ensurePVCUIDs, []types.UID{activePVC.UID}) {
		t.Fatalf(
			"destination shared PVC validations=%v UIDs=%v, want active PVC %s/%s UID %s",
			manager.ensurePVCs,
			manager.ensurePVCUIDs,
			activePVC.Namespace,
			activePVC.Name,
			activePVC.UID,
		)
	}
}

func createOpenEBSLVMSourceObjects(
	t *testing.T,
	client kubernetes.Interface,
	session *domain.Session,
) {
	t.Helper()

	for _, volume := range session.Spec.Volumes {
		if _, err := client.CoreV1().
			PersistentVolumeClaims(volume.SourcePVC.Namespace).
			Create(context.Background(), openEBSLVMSourcePVC(volume), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}

		if _, err := client.CoreV1().
			PersistentVolumes().
			Create(context.Background(), openEBSLVMSourcePV(volume), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}

func (p *recordingToolImageProber) Probe(
	ctx context.Context,
	options kube.ToolImageProbeOptions,
) ([]kube.ToolImageProbeResult, error) {
	p.calls = append(p.calls, options)
	if p.onProbe != nil {
		p.onProbe(ctx)
	}

	if p.err != nil {
		return nil, p.err
	}

	if p.results != nil {
		return slices.Clone(p.results), nil
	}

	results := make([]kube.ToolImageProbeResult, len(options.Targets))
	for index, target := range options.Targets {
		nodeName := target.NodeName
		if nodeName == "" {
			nodeName = "scheduler-node"
		}

		results[index] = kube.ToolImageProbeResult{Target: target, NodeName: nodeName}
	}

	return results, nil
}

func TestSessionToolProbeTargetsFollowSelectedStrategies(t *testing.T) {
	for _, test := range []struct {
		name       string
		operation  domain.Operation
		strategies []string
		want       map[string][]string
	}{
		{
			name: "reserve probes the target shell", operation: domain.OperationReserve,
			strategies: []string{domain.StrategyMount}, want: map[string][]string{"system/target-node": nil},
		},
		{
			name: "mount needs only destination rsync", operation: domain.OperationCopy,
			strategies: []string{domain.StrategyMount}, want: map[string][]string{"system/target-node": {kube.ToolComponentRsync}},
		},
		{
			name: "clusterip adds source sshd", operation: domain.OperationMigrate,
			strategies: []string{domain.StrategyClusterIP}, want: map[string][]string{
				"app/source-node":    {kube.ToolComponentSSHD},
				"system/target-node": {kube.ToolComponentRsync},
			},
		},
		{
			name: "nodeport adds source sshd", operation: domain.OperationMigrate,
			strategies: []string{domain.StrategyNodePort}, want: map[string][]string{
				"app/source-node":    {kube.ToolComponentSSHD},
				"system/target-node": {kube.ToolComponentRsync},
			},
		},
		{
			name: "loadbalancer adds source sshd", operation: domain.OperationMigrate,
			strategies: []string{domain.StrategyLoadBalancer}, want: map[string][]string{
				"app/source-node":    {kube.ToolComponentSSHD},
				"system/target-node": {kube.ToolComponentRsync},
			},
		},
		{
			name: "mount fallback probes topology dependencies", operation: domain.OperationMigrate,
			strategies: []string{domain.StrategyMount, domain.StrategyNodePort}, want: map[string][]string{
				"app/source-node":    {kube.ToolComponentSSHD},
				"system/target-node": {kube.ToolComponentRsync},
			},
		},
		{
			name: "local adds sshd on both nodes", operation: domain.OperationMigratePod,
			strategies: []string{domain.StrategyLocal}, want: map[string][]string{
				"app/source-node":    {kube.ToolComponentSSHD},
				"system/target-node": {kube.ToolComponentRsync, kube.ToolComponentSSHD},
			},
		},
		{
			name: "rename does not use a tool image", operation: domain.OperationRename,
			strategies: []string{domain.StrategyMount}, want: map[string][]string{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := appTestSession()
			options := session.Spec.WorkflowOptions()
			options.Strategies = slices.Clone(test.strategies)
			common := session.Spec.SessionCommon

			workload := session.Spec.Workload()
			switch test.operation {
			case domain.OperationMigrate:
				session.Spec = domain.NewOfflineMigrationSessionSpec(common, options)
			case domain.OperationMigratePod:
				session.Spec = domain.NewPodMigrationSessionSpec(
					common,
					workload,
					options,
					0,
					false,
				)
			default:
				session.Spec = domain.NewSessionSpec(
					test.operation,
					common,

					false,
					options,
				)
			}

			var targets []kube.ToolProbeTarget
			if test.operation == domain.OperationReserve {
				targets = reservationToolProbeTargets(session)
			} else {
				targets = copyToolProbeTargets(session, false)
			}

			got := canonicalToolProbeTargets(targets)
			if len(got) != len(test.want) {
				t.Fatalf(
					"targets=%v want=%v type=%s operation=%s options=%#v",
					got,
					test.want,
					session.Spec.Type,
					session.Spec.Operation(),
					session.Spec.WorkflowOptions(),
				)
			}

			for key, want := range test.want {
				if !slices.Equal(got[key], want) {
					t.Fatalf("target %s components=%v want=%v", key, got[key], want)
				}
			}
		})
	}
}

func TestSessionToolProbeTargetsUseActualVolumeNamespaces(t *testing.T) {
	session := appTestSession()
	session.Spec.Volumes = append(session.Spec.Volumes, session.Spec.Volumes[0])
	session.Spec.Volumes[0].SourcePVC.Namespace = "source-a"
	session.Spec.Volumes[0].DestinationPVC.Namespace = "destination-a"
	session.Spec.Volumes[1].SourcePVC.Namespace = "source-b"
	session.Spec.Volumes[1].DestinationPVC.Namespace = "destination-b"
	session.Spec.WorkflowOptionsPtr().Strategies = []string{domain.StrategyClusterIP}

	got := canonicalToolProbeTargets(copyToolProbeTargets(session, false))
	for _, key := range []string{"source-a/source-node", "source-b/source-node"} {
		if !slices.Equal(got[key], []string{kube.ToolComponentSSHD}) {
			t.Fatalf("target %s components=%v", key, got[key])
		}
	}

	for _, key := range []string{"destination-a/target-node", "destination-b/target-node"} {
		if !slices.Equal(got[key], []string{kube.ToolComponentRsync}) {
			t.Fatalf("target %s components=%v", key, got[key])
		}
	}
}

func TestPartialTransferProbeTargetsValidateSourceAndCreateDestination(t *testing.T) {
	session := appTestSession()
	session.Spec.Volumes[0].TransferScope = &domain.TransferScope{
		SourcePath:      "mysql/current",
		DestinationPath: "restored/mysql",
	}
	targets := copyToolProbeTargets(session, true)

	var source, destination *kube.ToolProbeTarget
	for index := range targets {
		target := &targets[index]
		if target.PVCName == session.Spec.Volumes[0].SourcePVC.Name && target.RequiredPath != "" {
			source = target
		}

		if target.PVCName == session.Spec.Volumes[0].DestinationPVC.Name &&
			target.RequiredPath != "" {
			destination = target
		}
	}

	if source == nil || source.RequiredPath != "mysql/current" || source.SkipPVCMount ||
		source.CreatePath {
		t.Fatalf("source target=%#v all=%#v", source, targets)
	}

	if destination == nil || destination.RequiredPath != "restored/mysql" ||
		!destination.CreatePath ||
		!destination.WritablePVCMount {
		t.Fatalf("destination target=%#v all=%#v", destination, targets)
	}
}

func TestPartialTransferPreparesDestinationWithoutPreselectedTargetNode(t *testing.T) {
	session := appTestSession()
	session.Spec.WorkflowOptionsPtr().TargetNode = ""
	session.Spec.Volumes[0].TransferScope = &domain.TransferScope{
		SourcePath:      ".",
		DestinationPath: "restored/mysql",
	}

	targets := copyToolProbeTargets(session, false)
	if len(targets) != 1 {
		t.Fatalf("targets=%#v", targets)
	}

	target := targets[0]
	if target.NodeName != "" || target.PVCName != session.Spec.Volumes[0].DestinationPVC.Name ||
		target.RequiredPath != "restored/mysql" ||
		!target.CreatePath ||
		!target.WritablePVCMount ||
		!slices.Equal(target.Components, []string{kube.ToolComponentRsync}) {
		t.Fatalf("target=%#v", target)
	}
}

func TestResolveSessionToolProbeTargetsUsesActiveConsumerNode(t *testing.T) {
	session := copyToolProbeSession(true)
	client := fake.NewClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "consumer"},
		Spec: corev1.PodSpec{NodeName: "node-a", Volumes: []corev1.Volume{
			{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data",
					},
				},
			},
		}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	})
	service := &Service{client: client}

	targets, err := service.resolveCopyToolProbeTargets(context.Background(), session, false)
	if err != nil {
		t.Fatal(err)
	}

	got := canonicalToolProbeTargets(targets)
	if !slices.Equal(got["app/node-a"], []string{kube.ToolComponentSSHD}) {
		t.Fatalf("source target=%v all=%v", got["app/node-a"], got)
	}

	if session.Spec.WorkflowOptions().SourceNode != "" {
		t.Fatalf(
			"probe resolution mutated sourceNode=%q",
			session.Spec.WorkflowOptions().SourceNode,
		)
	}
}

func TestResolveSessionToolProbeTargetsChecksLocalDestinationSSHDWithoutSourceNode(t *testing.T) {
	session := copyToolProbeSession(true)
	session.Spec.WorkflowOptionsPtr().Strategies = []string{domain.StrategyLocal}
	client := fake.NewClientset(probeConsumerPod("consumer", "data", "node-a"))
	service := &Service{client: client}

	targets, err := service.resolveCopyToolProbeTargets(context.Background(), session, false)
	if err != nil {
		t.Fatal(err)
	}

	got := canonicalToolProbeTargets(targets)
	if !slices.Equal(got["app/node-a"], []string{kube.ToolComponentSSHD}) {
		t.Fatalf("source target=%v all=%v", got["app/node-a"], got)
	}

	if !slices.Equal(
		got["system/target-node"],
		[]string{kube.ToolComponentRsync, kube.ToolComponentSSHD},
	) {
		t.Fatalf("destination target=%v all=%v", got["system/target-node"], got)
	}
}

func TestResolveSessionToolProbeTargetsUsesUniquePVTopology(t *testing.T) {
	session := copyToolProbeSession(true)
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-source"},
		Spec: corev1.PersistentVolumeSpec{
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      corev1.LabelHostname,
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{"storage-host"},
							},
						},
					},
				}},
			},
		},
	}
	client := fake.NewClientset(
		pv,
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "node-object",
				Labels: map[string]string{corev1.LabelHostname: "storage-host"},
			},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "other-node",
				Labels: map[string]string{corev1.LabelHostname: "other-host"},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "completed-consumer"},
			Spec: corev1.PodSpec{NodeName: "stale-node", Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "data",
						},
					},
				},
			}},
			Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
		},
	)
	service := &Service{client: client}

	targets, err := service.resolveCopyToolProbeTargets(context.Background(), session, false)
	if err != nil {
		t.Fatal(err)
	}

	got := canonicalToolProbeTargets(targets)
	if !slices.Equal(got["app/node-object"], []string{kube.ToolComponentSSHD}) {
		t.Fatalf("source target=%v all=%v", got["app/node-object"], got)
	}
}

func TestResolveSessionToolProbeTargetsUsesPVCConstrainedScheduling(t *testing.T) {
	session := copyToolProbeSession(true)
	client := fake.NewClientset(
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-source"}},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "node-a",
				Labels: map[string]string{corev1.LabelHostname: "node-a"},
			},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "node-b",
				Labels: map[string]string{corev1.LabelHostname: "node-b"},
			},
		},
	)
	service := &Service{client: client}

	targets, err := service.resolveCopyToolProbeTargets(context.Background(), session, false)
	if err != nil {
		t.Fatal(err)
	}

	var source *kube.ToolProbeTarget
	for index := range targets {
		if targets[index].Namespace == "app" {
			source = &targets[index]
		}
	}

	if source == nil || source.NodeName != "" || source.PVCName != "data" ||
		!slices.Equal(source.Components, []string{kube.ToolComponentSSHD}) {
		t.Fatalf("source target=%#v all=%#v", source, targets)
	}
}

func TestResolveSessionToolProbeTargetsCorrelatesExplicitSourceNodeWithoutMount(t *testing.T) {
	session := copyToolProbeSession(true)
	session.Spec.WorkflowOptionsPtr().SourceNode = "node-a"
	client := fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node-a", Labels: map[string]string{corev1.LabelHostname: "node-a"},
	}})
	service := &Service{client: client}

	targets, err := service.resolveCopyToolProbeTargets(context.Background(), session, false)
	if err != nil {
		t.Fatal(err)
	}

	var source *kube.ToolProbeTarget
	for index := range targets {
		if targets[index].Namespace == session.Spec.Volumes[0].SourcePVC.Namespace {
			source = &targets[index]
			break
		}
	}

	if source == nil || source.NodeName != "node-a" ||
		source.PVCName != session.Spec.Volumes[0].SourcePVC.Name ||
		!source.SkipPVCMount {
		t.Fatalf("source target=%#v all=%#v", source, targets)
	}

	prober := &recordingToolImageProber{
		results: []kube.ToolImageProbeResult{{Target: *source, NodeName: "node-a"}},
	}
	service.config.ToolImageProber = prober

	results, err := service.probeToolImage(context.Background(), session, targets)
	if err != nil {
		t.Fatal(err)
	}

	if got := probedSourceNode(session, &session.Spec.Volumes[0], results); got != "node-a" {
		t.Fatalf("probed source node=%q", got)
	}
}

func TestResolveSessionToolProbeTargetsMountsSourcePVCForWarmCopy(t *testing.T) {
	session := copyToolProbeSession(true)
	session.Spec.WorkflowOptionsPtr().SourceNode = "node-a"
	service := &Service{client: fake.NewClientset()}

	targets, err := service.resolveCopyToolProbeTargets(context.Background(), session, true)
	if err != nil {
		t.Fatal(err)
	}

	for _, target := range targets {
		if target.Namespace != session.Spec.Volumes[0].SourcePVC.Namespace {
			continue
		}

		if target.PVCName != session.Spec.Volumes[0].SourcePVC.Name || target.SkipPVCMount {
			t.Fatalf("warm-copy source target=%#v", target)
		}

		return
	}

	t.Fatal("warm-copy source target was not created")
}

func TestResolveSessionToolProbeTargetsUsesWritableMountForSharedOpenEBSLVM(t *testing.T) {
	session := copyToolProbeSession(true)
	session.Spec.WorkflowOptionsPtr().SourceNode = "node-a"
	storageClass := *session.Spec.Volumes[0].SourcePVCSpec.StorageClassName
	client := fake.NewClientset(
		&storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: storageClass},
			Provisioner: "local.csi.openebs.io",
			Parameters:  map[string]string{"shared": "yes"},
		},
		probeConsumerPod("consumer", session.Spec.Volumes[0].SourcePVC.Name, "node-a"),
		openEBSLVMSourcePVC(session.Spec.Volumes[0]),
		openEBSLVMSourcePV(session.Spec.Volumes[0]),
	)
	service := &Service{
		client: client,
		config: Config{
			OpenEBSLVMSharedVolumeManager: &recordingOpenEBSLVMSharedVolumeManager{shared: true},
		},
	}

	targets, err := service.resolveCopyToolProbeTargets(context.Background(), session, true)
	if err != nil {
		t.Fatal(err)
	}

	for _, target := range targets {
		if target.Namespace == session.Spec.Volumes[0].SourcePVC.Namespace &&
			target.PVCName == session.Spec.Volumes[0].SourcePVC.Name {
			if target.SkipPVCMount || !target.WritablePVCMount {
				t.Fatalf("shared LVM source target=%#v", target)
			}
			return
		}
	}

	t.Fatal("shared LVM source target was not created")
}

func TestResolveSessionToolProbeTargetsRejectsActiveUnsharedOpenEBSLVM(t *testing.T) {
	session := copyToolProbeSession(true)
	session.Spec.WorkflowOptionsPtr().SourceNode = "node-a"
	storageClass := *session.Spec.Volumes[0].SourcePVCSpec.StorageClassName
	client := fake.NewClientset(
		&storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: storageClass},
			Provisioner: kube.OpenEBSLVMCSIDriver,
			Parameters:  map[string]string{"shared": "yes"},
		},
		probeConsumerPod("consumer", session.Spec.Volumes[0].SourcePVC.Name, "node-a"),
		openEBSLVMSourcePVC(session.Spec.Volumes[0]),
		openEBSLVMSourcePV(session.Spec.Volumes[0]),
	)
	service := &Service{
		client: client,
		config: Config{OpenEBSLVMSharedVolumeManager: &recordingOpenEBSLVMSharedVolumeManager{}},
	}

	_, err := service.resolveCopyToolProbeTargets(context.Background(), session, true)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "does not currently have spec.shared=yes") ||
		!strings.Contains(err.Error(), "without --online") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestResolveSessionToolProbeTargetsAllowsInactiveUnsharedOpenEBSLVM(t *testing.T) {
	session := copyToolProbeSession(true)
	session.Spec.WorkflowOptionsPtr().SourceNode = "node-a"
	client := fake.NewClientset(
		openEBSLVMSourcePVC(session.Spec.Volumes[0]),
		openEBSLVMSourcePV(session.Spec.Volumes[0]),
	)
	service := &Service{
		client: client,
		config: Config{OpenEBSLVMSharedVolumeManager: &recordingOpenEBSLVMSharedVolumeManager{}},
	}

	targets, err := service.resolveCopyToolProbeTargets(context.Background(), session, true)
	if err != nil {
		t.Fatal(err)
	}

	for _, target := range targets {
		if target.Namespace == session.Spec.Volumes[0].SourcePVC.Namespace &&
			target.PVCName == session.Spec.Volumes[0].SourcePVC.Name &&
			target.WritablePVCMount {
			t.Fatalf("inactive unshared LVMVolume produced writable probe mount: %#v", target)
		}
	}
}

func TestResolveSessionToolProbeTargetsGuidesUnsharedMigrationRecovery(t *testing.T) {
	session := appTestSession()
	client := fake.NewClientset(
		probeConsumerPod("consumer", session.Spec.Volumes[0].SourcePVC.Name, "source-node"),
		openEBSLVMSourcePVC(session.Spec.Volumes[0]),
		openEBSLVMSourcePV(session.Spec.Volumes[0]),
	)
	service := &Service{
		client: client,
		config: Config{OpenEBSLVMSharedVolumeManager: &recordingOpenEBSLVMSharedVolumeManager{}},
	}

	_, err := service.resolveCopyToolProbeTargets(context.Background(), session, true)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "--precopy-passes 0") ||
		!strings.Contains(err.Error(), "--openebs-lvm-enable-shared") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestMarkSharedOpenEBSLVMProbeMountsReadsEachSourcePVCOnce(t *testing.T) {
	session := appTestSession()
	additional := session.Spec.Volumes[0]
	additional.SourcePVC.Name = "data-2"
	additional.SourcePV.Name = "pv-source-2"
	additional.SourcePV.UID = "source-pv-2-uid"
	session.Spec.Volumes = append(session.Spec.Volumes, additional)
	storageClass := *session.Spec.Volumes[0].SourcePVCSpec.StorageClassName
	client := fake.NewClientset(
		&storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: storageClass},
			Provisioner: "local.csi.openebs.io",
		},
		openEBSLVMSourcePVC(session.Spec.Volumes[0]),
		openEBSLVMSourcePVC(session.Spec.Volumes[1]),
		openEBSLVMSourcePV(session.Spec.Volumes[0]),
		openEBSLVMSourcePV(session.Spec.Volumes[1]),
	)
	manager := &recordingOpenEBSLVMSharedVolumeManager{shared: true}
	service := &Service{client: client, config: Config{OpenEBSLVMSharedVolumeManager: manager}}

	targets := []kube.ToolProbeTarget{
		{Namespace: "app", PVCName: "data"},
		{Namespace: "app", PVCName: "data-2"},
		{Namespace: "app", PVCName: "data"},
	}
	if err := service.markSharedOpenEBSLVMProbeMounts(
		context.Background(),
		session,
		targets,
	); err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(manager.sharedPVs, []string{"pv-source", "pv-source-2"}) {
		t.Fatalf("shared PV reads=%v", manager.sharedPVs)
	}

	for _, target := range targets {
		if !target.WritablePVCMount {
			t.Fatalf("target=%#v", target)
		}
	}
}

func TestResolveSessionToolProbeTargetsMountsSourcePVCForMountStrategy(t *testing.T) {
	session := copyToolProbeSession(true)
	options := session.Spec.WorkflowOptionsPtr()
	options.SourceNode = "node-a"
	options.Strategies = []string{domain.StrategyMount}
	service := &Service{client: fake.NewClientset()}

	targets, err := service.resolveCopyToolProbeTargets(context.Background(), session, true)
	if err != nil {
		t.Fatal(err)
	}

	for _, target := range targets {
		if target.PVCName == session.Spec.Volumes[0].SourcePVC.Name {
			if target.SkipPVCMount || len(target.Components) != 0 {
				t.Fatalf("mount-strategy source target=%#v", target)
			}
			return
		}
	}

	t.Fatal("mount strategy did not create a source PVC probe target")
}

func TestWarmCopyProbeErrorClassifiesConcurrentMountFailure(t *testing.T) {
	targets := []kube.ToolProbeTarget{{Namespace: "app", PVCName: "data"}}

	cause := domain.NewError(
		domain.ErrorTimeout,
		"tool image probe",
		"MountVolume.SetUp failed: device already mounted",
	)
	for _, test := range []struct {
		operation domain.Operation
		want      string
		absent    string
	}{
		{operation: domain.OperationMigratePod, want: "--precopy-passes 0", absent: "without --online"},
		{operation: domain.OperationCopy, want: "without --online", absent: "--precopy-passes"},
	} {
		err := warmCopyProbeError(test.operation, targets, cause)
		if domain.CategoryOf(err) != domain.ErrorPrecondition ||
			!strings.Contains(err.Error(), "warm-copy mount probe") ||
			!strings.Contains(err.Error(), test.want) ||
			strings.Contains(err.Error(), test.absent) {
			t.Fatalf("operation=%s error=%v", test.operation, err)
		}

		if !errors.Is(err, cause) {
			t.Fatalf("wrapped error does not preserve cause: %v", err)
		}
	}
}

func TestWarmCopyProbeErrorPreservesNonMountFailure(t *testing.T) {
	targets := []kube.ToolProbeTarget{{Namespace: "app", PVCName: "data"}}

	cause := domain.NewError(domain.ErrorPrecondition, "tool image probe", "ImagePullBackOff")
	if err := warmCopyProbeError(
		domain.OperationMigrate,
		targets,
		cause,
	); !errors.Is(err, cause) ||
		strings.Contains(err.Error(), "warm-copy mount probe") {
		t.Fatalf("error=%v want original=%v", err, cause)
	}
}

func TestWarmCopyProbeErrorDoesNotMisclassifyGenericMountFailure(t *testing.T) {
	targets := []kube.ToolProbeTarget{{Namespace: "app", PVCName: "data"}}

	cause := domain.NewError(
		domain.ErrorPrecondition,
		"tool image probe",
		"FailedMount: filesystem needs repair",
	)
	if err := warmCopyProbeError(
		domain.OperationMigrate,
		targets,
		cause,
	); !errors.Is(err, cause) ||
		strings.Contains(err.Error(), "warm-copy mount probe") {
		t.Fatalf("error=%v want original=%v", err, cause)
	}
}

func TestResolveSessionToolProbeTargetsRejectsCopyConsumerConflicts(t *testing.T) {
	for _, test := range []struct {
		name       string
		online     bool
		sourceNode string
		accessMode corev1.PersistentVolumeAccessMode
		podNodes   []string
		want       domain.ErrorCategory
	}{
		{name: "offline active consumer", podNodes: []string{"node-a"}, want: domain.ErrorPrecondition},
		{name: "active RWOP", online: true, accessMode: corev1.ReadWriteOncePod, podNodes: []string{"node-a"}, want: domain.ErrorPrecondition},
		{name: "unscheduled RWO consumer", online: true, podNodes: []string{""}, want: domain.ErrorPrecondition},
		{name: "one PVC across nodes", online: true, podNodes: []string{"node-a", "node-b"}, want: domain.ErrorPrecondition},
		{name: "explicit source mismatch", online: true, sourceNode: "node-b", podNodes: []string{"node-a"}, want: domain.ErrorConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := copyToolProbeSession(test.online)

			session.Spec.WorkflowOptionsPtr().SourceNode = test.sourceNode
			if test.accessMode != "" {
				session.Spec.Volumes[0].AccessModes = []corev1.PersistentVolumeAccessMode{
					test.accessMode,
				}
			}

			objects := make([]runtime.Object, 0, len(test.podNodes))
			for index, nodeName := range test.podNodes {
				objects = append(objects, &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "app",
						Name:      fmt.Sprintf("consumer-%d", index),
					},
					Spec: corev1.PodSpec{NodeName: nodeName, Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "data",
								},
							},
						},
					}},
					Status: corev1.PodStatus{Phase: corev1.PodRunning},
				})
			}

			service := &Service{client: fake.NewClientset(objects...)}

			_, err := service.resolveCopyToolProbeTargets(context.Background(), session, false)
			if domain.CategoryOf(err) != test.want {
				t.Fatalf("category=%s want=%s error=%v", domain.CategoryOf(err), test.want, err)
			}
		})
	}
}

func TestResolveSessionToolProbeTargetsRejectsOnlineVolumesOnDifferentNodes(t *testing.T) {
	session := copyToolProbeSession(true)
	addSecondVolume(session)

	pods := []runtime.Object{
		probeConsumerPod("consumer-a", "data", "node-a"),
		probeConsumerPod("consumer-b", "logs", "node-b"),
	}
	service := &Service{client: fake.NewClientset(pods...)}

	_, err := service.resolveCopyToolProbeTargets(context.Background(), session, false)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "multiple source nodes") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestResolveSessionToolProbeTargetsRejectsOrchestratedConsumerMove(t *testing.T) {
	session := appTestSession()
	setSessionOperation(session, domain.OperationMigratePod)
	session.Spec.WorkflowOptionsPtr().SourceNode = "source-node"
	session.Spec.WorkflowOptionsPtr().Strategies = []string{domain.StrategyClusterIP}
	service := &Service{
		client: fake.NewClientset(probeConsumerPod("consumer", "data", "moved-node")),
	}

	_, err := service.resolveCopyToolProbeTargets(context.Background(), session, false)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "consumer runs on moved-node") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func copyToolProbeSession(online bool) *domain.Session {
	session := appTestSession()
	options := session.Spec.WorkflowOptions()
	options.SourceNode = ""
	options.Strategies = []string{domain.StrategyNodePort}
	session.Spec = domain.NewSessionSpec(
		domain.OperationCopy,
		session.Spec.SessionCommon,

		online,
		options,
	)

	return session
}

func probeConsumerPod(name, claim, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: name, UID: types.UID(name + "-uid")},
		Spec: corev1.PodSpec{NodeName: node, Volumes: []corev1.Volume{
			{
				Name: claim,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: claim,
					},
				},
			},
		}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func TestCreateSessionPersistsBeforeStageProbe(t *testing.T) {
	client := fake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}})
	assignLeaseUIDs(client)
	store := kube.NewConfigMapSessionStore(client)
	probeErr := domain.NewError(domain.ErrorPrecondition, "tool image probe", "image pull failed")
	prober := &recordingToolImageProber{err: probeErr}
	reserver := &scriptedReserver{failures: map[string]error{}}
	service := NewService(client, store, reserver, nil, nil, nil, Config{
		HelmTimeout:     47 * time.Second,
		ToolImageProber: prober,
	})
	session := appTestSession()
	plan := &domain.MigrationPlan{SessionID: session.ID, SessionSpec: session.Spec, Ready: true}

	created, err := service.CreateSession(context.Background(), plan, false)
	if err != nil {
		t.Fatal(err)
	}

	if len(prober.calls) != 0 {
		t.Fatalf("session creation probe calls=%d", len(prober.calls))
	}

	if err := service.Reserve(context.Background(), created); !errors.Is(err, probeErr) {
		t.Fatalf("Reserve() error=%v", err)
	}

	if created.Status.Phase != domain.PhasePlanned {
		t.Fatalf("phase=%s", created.Status.Phase)
	}

	if len(prober.calls) != 1 || prober.calls[0].Timeout != 47*time.Second {
		t.Fatalf("probe calls=%#v", prober.calls)
	}

	if _, err := store.Get(
		context.Background(),
		created.Spec.SessionNamespace,
		created.ID,
	); err != nil {
		t.Fatalf("persisted session unavailable after probe failure: %v", err)
	}
}

func TestStageProbeRunsInsideSessionLease(t *testing.T) {
	client := fake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}})
	assignLeaseUIDs(client)
	store := kube.NewConfigMapSessionStore(client)
	probeErr := domain.NewError(
		domain.ErrorPrecondition,
		"tool image probe",
		"stop after lock assertion",
	)
	prober := &recordingToolImageProber{err: probeErr, onProbe: func(ctx context.Context) {
		if _, ok := ctx.Value(sessionLockContextKey{}).(heldSessionLock); !ok {
			t.Fatal("probe did not inherit the held session Lease")
		}
	}}
	reserver := &scriptedReserver{failures: map[string]error{}}
	service := NewService(client, store, reserver, nil, nil, nil, Config{ToolImageProber: prober})
	session := appTestSession()
	plan := &domain.MigrationPlan{SessionID: session.ID, SessionSpec: session.Spec, Ready: true}

	created, err := service.CreateSession(context.Background(), plan, false)
	if err != nil {
		t.Fatal(err)
	}

	if err := service.Reserve(context.Background(), created); !errors.Is(err, probeErr) {
		t.Fatalf("Reserve() error=%v", err)
	}
}

func TestCompletedCopyStagesProbeBeforeResettingCheckpoints(t *testing.T) {
	probeErr := domain.NewError(domain.ErrorPrecondition, "tool image probe", "image pull failed")

	t.Run("warm copy", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		setSessionOperation(session, domain.OperationCopy)
		session.Status.Phase = domain.PhaseWarmCopied
		completedAt := metav1.Now()
		session.Status.Volumes[0].Sync.WarmCompletedAt = &completedAt
		prober := &recordingToolImageProber{err: probeErr}
		fixture.service.config.ToolImageProber = prober

		if err := fixture.service.WarmCopy(
			context.Background(),
			session,
		); !errors.Is(
			err,
			probeErr,
		) {
			t.Fatalf("WarmCopy() error=%v", err)
		}

		if len(prober.calls) != 1 || session.Status.Volumes[0].Sync.WarmCompletedAt == nil ||
			session.Status.Phase != domain.PhaseWarmCopied {
			t.Fatalf(
				"calls=%d phase=%s sync=%+v",
				len(prober.calls),
				session.Status.Phase,
				session.Status.Volumes[0].Sync,
			)
		}
	})

	t.Run("final sync", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		session.Status.Phase = domain.PhaseFinalSynced
		completedAt := metav1.Now()
		session.Status.Volumes[0].Sync.FinalCompletedAt = &completedAt
		prober := &recordingToolImageProber{err: probeErr}
		fixture.service.config.ToolImageProber = prober

		if err := fixture.service.PodFinalSync(
			context.Background(),
			session,
		); !errors.Is(
			err,
			probeErr,
		) {
			t.Fatalf("FinalSync() error=%v", err)
		}

		if len(prober.calls) != 1 || session.Status.Volumes[0].Sync.FinalCompletedAt == nil ||
			session.Status.Phase != domain.PhaseFinalSynced {
			t.Fatalf(
				"calls=%d phase=%s sync=%+v",
				len(prober.calls),
				session.Status.Phase,
				session.Status.Volumes[0].Sync,
			)
		}
	})
}

func TestPodPauseAndFinalSyncProbesBeforeWorkloadPause(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseWarmCopied
	probeErr := domain.NewError(domain.ErrorPrecondition, "tool image probe", "image pull failed")
	prober := &recordingToolImageProber{err: probeErr, onProbe: func(context.Context) {
		if fixture.controller.pauses != 0 {
			t.Fatalf("workload paused before probe: pauses=%d", fixture.controller.pauses)
		}
	}}
	fixture.service.config.ToolImageProber = prober

	if err := fixture.service.PodPauseAndFinalSync(
		context.Background(),
		session,
	); !errors.Is(
		err,
		probeErr,
	) {
		t.Fatalf("PodPauseAndFinalSync() error=%v", err)
	}

	if fixture.controller.pauses != 0 || session.Status.Phase != domain.PhaseWarmCopied {
		t.Fatalf("pauses=%d phase=%s", fixture.controller.pauses, session.Status.Phase)
	}
}

func TestPartialPodPauseAndFinalSyncCreatesDestinationBeforePauseAndValidatesSourceAfterPause(
	t *testing.T,
) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseWarmCopied
	session.Spec.Volumes[0].TransferScope = &domain.TransferScope{
		SourcePath:      "mysql/current",
		DestinationPath: "restored/mysql",
	}
	prober := &recordingToolImageProber{}
	prober.onProbe = func(context.Context) {
		if len(prober.calls) == 1 && fixture.controller.pauses != 0 {
			t.Fatalf(
				"destination preparation ran after pause: pauses=%d",
				fixture.controller.pauses,
			)
		}

		if len(prober.calls) == 2 && fixture.controller.pauses != 1 {
			t.Fatalf(
				"source path validation ran before pause: pauses=%d",
				fixture.controller.pauses,
			)
		}
	}
	fixture.service.config.ToolImageProber = prober

	if err := fixture.service.PodPauseAndFinalSync(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if len(prober.calls) != 2 {
		t.Fatalf("probe calls=%d", len(prober.calls))
	}

	first, second := prober.calls[0].Targets, prober.calls[1].Targets

	foundDestination := false
	for _, target := range first {
		if target.PVCName == session.Spec.Volumes[0].DestinationPVC.Name &&
			target.RequiredPath == "restored/mysql" &&
			target.CreatePath {
			foundDestination = true
		}
	}

	if !foundDestination || len(second) != 1 ||
		second[0].PVCName != session.Spec.Volumes[0].SourcePVC.Name ||
		second[0].RequiredPath != "mysql/current" ||
		second[0].CreatePath {
		t.Fatalf("first targets=%#v second targets=%#v", first, second)
	}
}

func TestPodPauseAndFinalSyncRestoresPendingOpenEBSLVMSharedBeforePausing(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	session.Status.OpenEBSLVMSharedMounts = []domain.OpenEBSLVMSharedMount{
		{
			SourcePV: session.Spec.Volumes[0].SourcePV,
			LVMVolume: domain.ObjectReference{
				Namespace: "openebs",
				Name:      "pv-source",
				UID:       "lvm-pv-source",
			},
			PreviousShared:    "no",
			PreviousSharedSet: true,
		},
	}
	manager := &recordingOpenEBSLVMSharedVolumeManager{shared: true}
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager

	if err := fixture.service.PodPauseAndFinalSync(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != domain.PhaseFinalSynced || fixture.controller.pauses != 1 {
		t.Fatalf("phase=%s pauses=%d", session.Status.Phase, fixture.controller.pauses)
	}

	if !slices.Equal(manager.restorePVs, []string{"pv-source"}) || manager.shared ||
		len(session.Status.OpenEBSLVMSharedMounts) != 0 {
		t.Fatalf(
			"restore=%v shared=%t pending=%#v",
			manager.restorePVs,
			manager.shared,
			session.Status.OpenEBSLVMSharedMounts,
		)
	}
}

func TestValidatePodFinalSyncRejectsUnsafePendingOpenEBSLVMRestore(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	session.Status.OpenEBSLVMSharedMounts = []domain.OpenEBSLVMSharedMount{
		{
			SourcePV: session.Spec.Volumes[0].SourcePV,
			LVMVolume: domain.ObjectReference{
				Namespace: "openebs",
				Name:      "pv-source",
				UID:       "lvm-pv-source",
			},
			PreviousShared:    "no",
			PreviousSharedSet: true,
		},
	}
	validationErr := domain.NewError(
		domain.ErrorConflict,
		"restore OpenEBS LVM shared mount",
		"LVMVolume changed",
	)
	manager := &recordingOpenEBSLVMSharedVolumeManager{
		shared:             true,
		validateRestoreErr: validationErr,
	}
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager

	err := fixture.service.ValidatePodFinalSync(context.Background(), session)
	if !errors.Is(err, validationErr) {
		t.Fatalf("ValidatePodFinalSync() error=%v", err)
	}

	if fixture.controller.pauses != 0 || len(fixture.reserver.calls) != 0 {
		t.Fatalf(
			"controller pauses=%d reservation calls=%v",
			fixture.controller.pauses,
			fixture.reserver.calls,
		)
	}
}

func TestNoOpAndInvalidStagesSkipProbe(t *testing.T) {
	fixture := newRecoveryFixture(t)
	prober := &recordingToolImageProber{}
	fixture.service.config.ToolImageProber = prober

	reserved := appTestSession()
	setSessionOperation(reserved, domain.OperationReserve)

	reserved.Status.Phase = domain.PhaseReserved
	if err := fixture.service.Reserve(context.Background(), reserved); err != nil {
		t.Fatal(err)
	}

	completed := appTestSession()

	completed.Status.Phase = domain.PhaseCompleted
	if err := fixture.service.resumeWorkflowForTest(context.Background(), completed); err != nil {
		t.Fatal(err)
	}

	invalid := appTestSession()
	setSessionOperation(invalid, domain.OperationCopy)

	invalid.Status.Phase = domain.PhaseCompleted
	if err := fixture.service.WarmCopy(
		context.Background(),
		invalid,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if len(prober.calls) != 0 {
		t.Fatalf("probe calls=%d", len(prober.calls))
	}
}

func TestWarmCopyReusesSchedulerSelectedProbeNodeAndPullSecrets(t *testing.T) {
	fixture := newRecoveryFixture(t)
	if _, err := fixture.client.CoreV1().
		Nodes().
		Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name:   "scheduler-node",
			Labels: map[string]string{corev1.LabelHostname: "scheduler-host"},
		}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	session := appTestSession()
	setSessionOperation(session, domain.OperationCopy)
	session.Status.Phase = domain.PhaseReserved
	session.Spec.WorkflowOptionsPtr().SourceNode = ""
	session.Spec.WorkflowOptionsPtr().Strategies = []string{domain.StrategyClusterIP}
	createSourceStorage(t, fixture, session)
	prober := &recordingToolImageProber{results: []kube.ToolImageProbeResult{
		{
			Target: kube.ToolProbeTarget{
				Namespace:  session.Spec.Volumes[0].DestinationPVC.Namespace,
				NodeName:   "target-node",
				Components: []string{kube.ToolComponentRsync},
			},
			NodeName:         "target-node",
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: "destination-pull"}},
		},
		{
			Target: kube.ToolProbeTarget{
				Namespace:  session.Spec.Volumes[0].SourcePVC.Namespace,
				PVCName:    session.Spec.Volumes[0].SourcePVC.Name,
				Components: []string{kube.ToolComponentSSHD},
			},
			NodeName:         "scheduler-node",
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: "source-pull"}},
		},
	}}
	fixture.service.config.ToolImageProber = prober

	if err := fixture.service.WarmCopy(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if len(fixture.copier.requests) != 1 {
		t.Fatalf("copy requests=%d", len(fixture.copier.requests))
	}

	values := fixture.copier.requests[0].HelmStringValues
	for _, expected := range []string{
		"sshd.nodeSelector.kubernetes\\.io/hostname=scheduler-host",
		"sshd.imagePullSecrets[0].name=source-pull",
		"rsync.imagePullSecrets[0].name=destination-pull",
	} {
		if !slices.Contains(values, expected) {
			t.Fatalf("missing %q in %v", expected, values)
		}
	}
}

func TestWarmCopyUsesWritableSourceMountForSharedOpenEBSLVM(t *testing.T) {
	fixture := newRecoveryFixture(t)

	storageClass := *appTestSession().Spec.Volumes[0].SourcePVCSpec.StorageClassName
	if _, err := fixture.client.StorageV1().
		StorageClasses().
		Create(context.Background(), &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: storageClass,
			},
			Provisioner: "local.csi.openebs.io",
			Parameters:  map[string]string{"shared": "yes"},
		}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	session := appTestSession()
	createOpenEBSLVMSourceObjects(t, fixture.client, session)

	manager := &recordingOpenEBSLVMSharedVolumeManager{shared: true}
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager

	setSessionOperation(session, domain.OperationCopy)

	session.Status.Phase = domain.PhaseReserved
	if err := fixture.service.WarmCopy(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if len(fixture.copier.requests) != 1 || !fixture.copier.requests[0].SourceMountReadWrite {
		t.Fatalf("copy requests=%#v", fixture.copier.requests)
	}

	if !slices.Equal(manager.sharedSessionIDs, []string{"", ""}) {
		t.Fatalf("shared session reads=%v", manager.sharedSessionIDs)
	}
}

func TestWarmCopyRechecksOpenEBSLVMSourceIdentityBeforeRetry(t *testing.T) {
	fixture := newRecoveryFixture(t)
	fixture.service.config.Retries = 2
	fixture.service.config.RetryBackoff = time.Millisecond
	fixture.copier.failures["warm/data"] = 1
	session := appTestSession()
	createOpenEBSLVMSourceObjects(t, fixture.client, session)

	manager := &recordingOpenEBSLVMSharedVolumeManager{shared: true}
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager
	fixture.copier.copyHook = func() {
		if len(fixture.copier.requests) != 1 {
			return
		}

		if err := fixture.client.CoreV1().
			PersistentVolumeClaims("app").
			Delete(context.Background(), "data", metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}

		replacement := openEBSLVMSourcePVC(session.Spec.Volumes[0])

		replacement.UID = "replacement-pvc-uid"
		if _, err := fixture.client.CoreV1().
			PersistentVolumeClaims("app").
			Create(context.Background(), replacement, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	session.Status.Phase = domain.PhaseReserved

	err := fixture.service.WarmCopy(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "identity or binding changed") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if len(fixture.copier.requests) != 1 {
		t.Fatalf("copy attempts=%d want=1", len(fixture.copier.requests))
	}

	if !slices.Equal(manager.sharedPVs, []string{"pv-source", "pv-source"}) {
		t.Fatalf("LVMVolume reads=%v", manager.sharedPVs)
	}
}

func TestWarmCopyEnablesOpenEBSLVMSharedBeforeProbe(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	session.Spec.MigratePod.OpenEBSLVMEnableShared = true

	storageClass := *session.Spec.Volumes[0].SourcePVCSpec.StorageClassName
	if _, err := fixture.client.StorageV1().
		StorageClasses().
		Create(context.Background(), &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{Name: storageClass}, Provisioner: "local.csi.openebs.io",
		}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := fixture.client.CoreV1().
		Pods("app").
		Create(context.Background(), probeConsumerPod("database-0", "data", "source-node"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	manager := &recordingOpenEBSLVMSharedVolumeManager{}

	createOpenEBSLVMSourceObjects(t, fixture.client, session)

	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager
	if err := fixture.service.WarmCopy(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(manager.preparePVs, []string{"pv-source", "pv-source"}) ||
		!slices.Equal(manager.enablePVs, []string{"pv-source"}) {
		t.Fatalf("prepared volumes=%v enabled volumes=%v", manager.preparePVs, manager.enablePVs)
	}

	if !slices.Equal(manager.restorePVs, []string{"pv-source"}) || manager.shared {
		t.Fatalf("shared mount restore=%v shared=%t", manager.restorePVs, manager.shared)
	}

	if len(session.Status.OpenEBSLVMSharedMounts) != 0 {
		t.Fatalf("pending shared mounts=%#v", session.Status.OpenEBSLVMSharedMounts)
	}

	if len(fixture.copier.requests) != 1 || !fixture.copier.requests[0].SourceMountReadWrite {
		t.Fatalf("copy requests=%#v", fixture.copier.requests)
	}

	for _, sessionID := range manager.sharedSessionIDs {
		if sessionID != session.ID {
			t.Fatalf("shared session reads=%v", manager.sharedSessionIDs)
		}
	}

	for _, lvmVolume := range manager.sharedLVMVolumes {
		if lvmVolume.Name != "pv-source" || lvmVolume.UID == "" {
			t.Fatalf("shared LVMVolume reads=%#v", manager.sharedLVMVolumes)
		}
	}
}

func TestWarmCopyRejectsActiveUnsharedOpenEBSLVMBeforeProbe(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	createOpenEBSLVMSourceObjects(t, fixture.client, session)

	if _, err := fixture.client.CoreV1().
		Pods("app").
		Create(context.Background(), probeConsumerPod("database-0", "data", "source-node"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	prober := &recordingToolImageProber{}
	fixture.service.config.ToolImageProber = prober
	fixture.service.config.OpenEBSLVMSharedVolumeManager = &recordingOpenEBSLVMSharedVolumeManager{}

	err := fixture.service.WarmCopy(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "--precopy-passes 0") ||
		!strings.Contains(err.Error(), "--openebs-lvm-enable-shared") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if len(prober.calls) != 0 || len(fixture.copier.requests) != 0 ||
		session.Status.Phase != domain.PhaseReserved {
		t.Fatalf(
			"known invalid mount reached mutation: probes=%d copies=%d phase=%s",
			len(prober.calls),
			len(fixture.copier.requests),
			session.Status.Phase,
		)
	}
}

func TestValidateWarmCopyRejectsActiveUnsharedOpenEBSLVM(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	createOpenEBSLVMSourceObjects(t, fixture.client, session)

	if _, err := fixture.client.CoreV1().
		Pods("app").
		Create(context.Background(), probeConsumerPod("database-0", "data", "source-node"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	fixture.service.config.OpenEBSLVMSharedVolumeManager = &recordingOpenEBSLVMSharedVolumeManager{}

	err := fixture.service.ValidateWarmCopy(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "--precopy-passes 0") ||
		!strings.Contains(err.Error(), "--openebs-lvm-enable-shared") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if fixture.store.updates != 0 || len(fixture.reserver.calls) != 0 {
		t.Fatalf(
			"warm-copy dry-run mutated state: updates=%d reserveCalls=%v",
			fixture.store.updates,
			fixture.reserver.calls,
		)
	}
}

func TestValidateWarmCopyAcceptsRestorableSessionManagedOpenEBSLVM(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseWarmCopying
	session.Spec.MigratePod.OpenEBSLVMEnableShared = true
	createOpenEBSLVMSourceObjects(t, fixture.client, session)

	if _, err := fixture.client.CoreV1().
		Pods("app").
		Create(context.Background(), probeConsumerPod("database-0", "data", "source-node"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	session.Status.OpenEBSLVMSharedMounts = []domain.OpenEBSLVMSharedMount{
		{
			SourcePV: session.Spec.Volumes[0].SourcePV,
			LVMVolume: domain.ObjectReference{
				Namespace: "openebs",
				Name:      "pv-source",
				UID:       "lvm-pv-source",
			},
			PreviousShared:    "no",
			PreviousSharedSet: true,
		},
	}
	prepareErr := domain.NewError(
		domain.ErrorConflict,
		"OpenEBS LVM shared mount",
		"session already owns the temporary shared mount",
	)
	manager := &recordingOpenEBSLVMSharedVolumeManager{shared: true, prepareErr: prepareErr}
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager

	if err := fixture.service.ValidateWarmCopy(context.Background(), session); err != nil {
		t.Fatalf("ValidateWarmCopy() error=%v", err)
	}

	if !slices.Equal(manager.validateRestorePVs, []string{"pv-source"}) ||
		len(manager.preparePVs) != 0 {
		t.Fatalf("restore validation=%v prepare=%v", manager.validateRestorePVs, manager.preparePVs)
	}

	if fixture.store.updates != 0 || len(session.Status.OpenEBSLVMSharedMounts) != 1 {
		t.Fatalf(
			"warm-copy dry-run mutated state: updates=%d pending=%#v",
			fixture.store.updates,
			session.Status.OpenEBSLVMSharedMounts,
		)
	}
}

func TestWarmCopyPreparesEveryOpenEBSLVMVolumeBeforeEnable(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)
	session.Status.Phase = domain.PhaseReserved
	session.Spec.MigratePod.OpenEBSLVMEnableShared = true
	createOpenEBSLVMSourceObjects(t, fixture.client, session)

	for _, volume := range session.Spec.Volumes {
		if _, err := fixture.client.CoreV1().
			Pods(volume.SourcePVC.Namespace).
			Create(context.Background(), probeConsumerPod("consumer-"+volume.SourcePVC.Name, volume.SourcePVC.Name, "source-node"), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	prepareErr := domain.NewError(
		domain.ErrorConflict,
		"OpenEBS LVM shared mount",
		"second LVMVolume changed",
	)
	manager := &recordingOpenEBSLVMSharedVolumeManager{
		prepareErrs: map[string]error{"pv-source-logs": prepareErr},
	}
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager

	err := fixture.service.WarmCopy(context.Background(), session)
	if !errors.Is(err, prepareErr) {
		t.Fatalf("WarmCopy() error=%v", err)
	}

	if want := []string{"pv-source", "pv-source-logs"}; !slices.Equal(manager.preparePVs, want) {
		t.Fatalf("prepared=%v want=%v", manager.preparePVs, want)
	}

	if len(manager.enablePVs) != 0 || len(manager.restorePVs) != 0 ||
		len(session.Status.OpenEBSLVMSharedMounts) != 0 ||
		session.Status.Phase != domain.PhaseReserved {
		t.Fatalf(
			"shared state mutated before preparation completed: enabled=%v restored=%v pending=%#v phase=%s",
			manager.enablePVs,
			manager.restorePVs,
			session.Status.OpenEBSLVMSharedMounts,
			session.Status.Phase,
		)
	}
}

func TestOpenEBSLVMRestoreValidatesEveryVolumeBeforeMutation(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)

	for _, volume := range session.Spec.Volumes {
		session.Status.OpenEBSLVMSharedMounts = append(
			session.Status.OpenEBSLVMSharedMounts,
			domain.OpenEBSLVMSharedMount{
				SourcePV: volume.SourcePV,
				LVMVolume: domain.ObjectReference{
					Namespace: "openebs",
					Name:      volume.SourcePV.Name,
					UID:       types.UID("lvm-" + volume.SourcePV.Name),
				},
				PreviousShared:    "no",
				PreviousSharedSet: true,
			},
		)
	}

	validationErr := domain.NewError(
		domain.ErrorConflict,
		"restore OpenEBS LVM shared mount",
		"second LVMVolume ownership changed",
	)
	manager := &recordingOpenEBSLVMSharedVolumeManager{
		validateRestoreErrs: map[string]error{"pv-source-logs": validationErr},
	}
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager

	err := fixture.service.restoreOpenEBSLVMSharedMounts(context.Background(), session)
	if !errors.Is(err, validationErr) {
		t.Fatalf("restoreOpenEBSLVMSharedMounts() error=%v", err)
	}

	if want := []string{
		"pv-source",
		"pv-source-logs",
	}; !slices.Equal(
		manager.validateRestorePVs,
		want,
	) {
		t.Fatalf("validated=%v want=%v", manager.validateRestorePVs, want)
	}

	if len(manager.restorePVs) != 0 || len(session.Status.OpenEBSLVMSharedMounts) != 2 ||
		fixture.store.updates != 0 {
		t.Fatalf(
			"restore mutated before validation completed: restored=%v pending=%#v updates=%d",
			manager.restorePVs,
			session.Status.OpenEBSLVMSharedMounts,
			fixture.store.updates,
		)
	}
}

func TestWarmCopyRestoresOpenEBSLVMSharedAfterProbeFailure(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	session.Spec.MigratePod.OpenEBSLVMEnableShared = true

	storageClass := *session.Spec.Volumes[0].SourcePVCSpec.StorageClassName
	if _, err := fixture.client.StorageV1().
		StorageClasses().
		Create(context.Background(), &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{Name: storageClass}, Provisioner: "local.csi.openebs.io",
		}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := fixture.client.CoreV1().
		Pods("app").
		Create(context.Background(), probeConsumerPod("database-0", "data", "source-node"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	probeErr := errors.New("tool image probe failed")
	fixture.service.config.ToolImageProber = &recordingToolImageProber{err: probeErr}
	manager := &recordingOpenEBSLVMSharedVolumeManager{}

	createOpenEBSLVMSourceObjects(t, fixture.client, session)
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager

	err := fixture.service.WarmCopy(context.Background(), session)
	if !errors.Is(err, probeErr) {
		t.Fatalf("WarmCopy() error=%v", err)
	}

	if !slices.Equal(manager.restorePVs, []string{"pv-source"}) || manager.shared {
		t.Fatalf("shared mount restore=%v shared=%t", manager.restorePVs, manager.shared)
	}

	if len(session.Status.OpenEBSLVMSharedMounts) != 0 {
		t.Fatalf("pending shared mounts=%#v", session.Status.OpenEBSLVMSharedMounts)
	}
}

func TestWarmCopyRestoresOpenEBSLVMSharedAfterProbeCancellation(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	session.Spec.MigratePod.OpenEBSLVMEnableShared = true

	storageClass := *session.Spec.Volumes[0].SourcePVCSpec.StorageClassName
	if _, err := fixture.client.StorageV1().
		StorageClasses().
		Create(context.Background(), &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{Name: storageClass}, Provisioner: "local.csi.openebs.io",
		}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := fixture.client.CoreV1().
		Pods("app").
		Create(context.Background(), probeConsumerPod("database-0", "data", "source-node"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fixture.service.config.ToolImageProber = &recordingToolImageProber{
		err: context.Canceled,
		onProbe: func(context.Context) {
			cancel()
		},
	}
	manager := &recordingOpenEBSLVMSharedVolumeManager{}

	createOpenEBSLVMSourceObjects(t, fixture.client, session)
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager

	err := fixture.service.WarmCopy(ctx, session)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WarmCopy() error=%v", err)
	}

	if !slices.Equal(manager.restorePVs, []string{"pv-source"}) || manager.shared {
		t.Fatalf("shared mount restore=%v shared=%t", manager.restorePVs, manager.shared)
	}

	if !slices.Equal(manager.restoreContextErrs, []error{nil}) {
		t.Fatalf("restore context errors=%v", manager.restoreContextErrs)
	}

	if len(session.Status.OpenEBSLVMSharedMounts) != 0 {
		t.Fatalf("pending shared mounts=%#v", session.Status.OpenEBSLVMSharedMounts)
	}
}

func TestWarmCopyRestoresOpenEBSLVMSharedWhenContextIsCanceledAfterEnable(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	session.Spec.MigratePod.OpenEBSLVMEnableShared = true

	storageClass := *session.Spec.Volumes[0].SourcePVCSpec.StorageClassName
	if _, err := fixture.client.StorageV1().
		StorageClasses().
		Create(context.Background(), &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{Name: storageClass}, Provisioner: "local.csi.openebs.io",
		}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := fixture.client.CoreV1().
		Pods("app").
		Create(context.Background(), probeConsumerPod("database-0", "data", "source-node"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	createOpenEBSLVMSourceObjects(t, fixture.client, session)

	store := &toolProbeContextStore{}
	fixture.service.store = store
	manager := &recordingOpenEBSLVMSharedVolumeManager{onEnable: func() {
		if store.updates != 1 || len(session.Status.OpenEBSLVMSharedMounts) != 1 {
			t.Errorf(
				"shared state was not checkpointed before enable: updates=%d state=%#v",
				store.updates,
				session.Status.OpenEBSLVMSharedMounts,
			)
		}

		cancel()
	}}
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager

	err := fixture.service.WarmCopy(ctx, session)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WarmCopy() error=%v", err)
	}

	if !slices.Equal(manager.restorePVs, []string{"pv-source"}) || manager.shared {
		t.Fatalf("shared mount restore=%v shared=%t", manager.restorePVs, manager.shared)
	}

	if !slices.Equal(manager.restoreContextErrs, []error{nil}) {
		t.Fatalf("restore context errors=%v", manager.restoreContextErrs)
	}

	if len(store.updateContextErrs) != 3 || store.updateContextErrs[0] != nil ||
		!errors.Is(store.updateContextErrs[1], context.Canceled) ||
		store.updateContextErrs[2] != nil {
		t.Fatalf("checkpoint context errors=%v", store.updateContextErrs)
	}

	if len(session.Status.OpenEBSLVMSharedMounts) != 0 {
		t.Fatalf("pending shared mounts=%#v", session.Status.OpenEBSLVMSharedMounts)
	}
}

func TestOpenEBSLVMFailureRestoreStopsAfterSessionFenceLoss(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.OpenEBSLVMSharedMounts = []domain.OpenEBSLVMSharedMount{
		{
			SourcePV: session.Spec.Volumes[0].SourcePV,
			LVMVolume: domain.ObjectReference{
				Namespace: "openebs",
				Name:      "pv-source",
				UID:       "lvm-pv-source",
			},
			PreviousShared:    "no",
			PreviousSharedSet: true,
		},
	}
	manager := &recordingOpenEBSLVMSharedVolumeManager{shared: true}
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager
	fenceErr := domain.NewError(domain.ErrorConflict, "session lock", "lease ownership was fenced")
	ctx := context.WithValue(context.Background(), sessionLockContextKey{}, heldSessionLock{
		lock: &fakeSessionLock{
			err: fenceErr,
		},
		namespace: session.Spec.SessionNamespace,
		id:        session.ID,
	})

	err := fixture.service.restoreOpenEBSLVMSharedMountsAfterFailure(ctx, session)
	if !errors.Is(err, fenceErr) {
		t.Fatalf("restore error=%v", err)
	}

	if len(manager.validateRestorePVs) != 0 || len(manager.restorePVs) != 0 || !manager.shared ||
		len(session.Status.OpenEBSLVMSharedMounts) != 1 {
		t.Fatalf(
			"fenced restore mutated state: validate=%v restore=%v shared=%t pending=%#v",
			manager.validateRestorePVs,
			manager.restorePVs,
			manager.shared,
			session.Status.OpenEBSLVMSharedMounts,
		)
	}
}

func TestAbortRestoresPendingOpenEBSLVMSharedMount(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	session.Status.OpenEBSLVMSharedMounts = []domain.OpenEBSLVMSharedMount{
		{
			SourcePV: session.Spec.Volumes[0].SourcePV,
			LVMVolume: domain.ObjectReference{
				Namespace: "openebs",
				Name:      "pv-source",
				UID:       "lvm-pv-source",
			},
			PreviousShared:    "no",
			PreviousSharedSet: true,
		},
	}
	manager := &recordingOpenEBSLVMSharedVolumeManager{shared: true}
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager

	if err := fixture.service.abortWorkflowForTest(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != domain.PhaseAborted ||
		!slices.Equal(manager.restorePVs, []string{"pv-source"}) ||
		manager.shared {
		t.Fatalf(
			"phase=%s restore=%v shared=%t",
			session.Status.Phase,
			manager.restorePVs,
			manager.shared,
		)
	}

	if len(session.Status.OpenEBSLVMSharedMounts) != 0 {
		t.Fatalf("pending shared mounts=%#v", session.Status.OpenEBSLVMSharedMounts)
	}
}

func TestAbortDryRunRejectsUnsafeOpenEBSLVMSharedRestore(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	session.Status.OpenEBSLVMSharedMounts = []domain.OpenEBSLVMSharedMount{
		{
			SourcePV: session.Spec.Volumes[0].SourcePV,
			LVMVolume: domain.ObjectReference{
				Namespace: "openebs",
				Name:      "pv-source",
				UID:       "lvm-pv-source",
			},
			PreviousShared:    "no",
			PreviousSharedSet: true,
		},
	}
	validationErr := domain.NewError(
		domain.ErrorConflict,
		"restore OpenEBS LVM shared mount",
		"LVMVolume changed",
	)
	manager := &recordingOpenEBSLVMSharedVolumeManager{
		shared:             true,
		validateRestoreErr: validationErr,
	}
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager

	err := fixture.service.validateAbortWorkflowForTest(context.Background(), session)
	if !errors.Is(err, validationErr) {
		t.Fatalf("ValidateAbort() error=%v", err)
	}

	if !slices.Equal(manager.validateRestorePVs, []string{"pv-source"}) ||
		len(manager.restorePVs) != 0 {
		t.Fatalf("validate=%v restore=%v", manager.validateRestorePVs, manager.restorePVs)
	}
}

func canonicalToolProbeTargets(targets []kube.ToolProbeTarget) map[string][]string {
	result := make(map[string][]string)
	for _, target := range targets {
		key := target.Namespace + "/" + target.NodeName
		if _, exists := result[key]; !exists {
			result[key] = nil
		}

		for _, component := range target.Components {
			if !slices.Contains(result[key], component) {
				result[key] = append(result[key], component)
			}
		}
	}

	for key := range result {
		slices.Sort(result[key])
	}

	return result
}
