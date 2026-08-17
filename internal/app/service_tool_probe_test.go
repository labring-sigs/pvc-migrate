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
	"k8s.io/client-go/kubernetes/fake"
)

type recordingToolImageProber struct {
	calls   []kube.ToolImageProbeOptions
	results []kube.ToolImageProbeResult
	err     error
	onProbe func(context.Context)
}

type recordingOpenEBSLVMSharedVolumeManager struct {
	shared             bool
	sharedPVs          []string
	ensureErr          error
	ensurePVs          []string
	onEnsure           func()
	restoreErr         error
	restorePVs         []string
	restoreContextErrs []error
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

func (m *recordingOpenEBSLVMSharedVolumeManager) Shared(_ context.Context, sourcePV string) (bool, error) {
	m.sharedPVs = append(m.sharedPVs, sourcePV)
	return m.shared, nil
}

func (m *recordingOpenEBSLVMSharedVolumeManager) EnsureShared(_ context.Context, sourcePV string) (kube.OpenEBSLVMSharedResult, error) {
	m.ensurePVs = append(m.ensurePVs, sourcePV)
	if m.ensureErr != nil {
		return kube.OpenEBSLVMSharedResult{}, m.ensureErr
	}
	m.shared = true
	if m.onEnsure != nil {
		m.onEnsure()
	}
	return kube.OpenEBSLVMSharedResult{Reference: "LVMVolume openebs/" + sourcePV, PreviousShared: "no", PreviousSharedSet: true, Changed: true}, nil
}

func (m *recordingOpenEBSLVMSharedVolumeManager) RestoreShared(ctx context.Context, sourcePV, _ string, _ bool) error {
	m.restorePVs = append(m.restorePVs, sourcePV)
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

func (p *recordingToolImageProber) Probe(ctx context.Context, options kube.ToolImageProbeOptions) ([]kube.ToolImageProbeResult, error) {
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
			session.Spec = domain.NewSessionSpec(test.operation, common, workload, false, options)
			var targets []kube.ToolProbeTarget
			if test.operation == domain.OperationReserve {
				targets = reservationToolProbeTargets(session)
			} else {
				targets = copyToolProbeTargets(session, false)
			}
			got := canonicalToolProbeTargets(targets)
			if len(got) != len(test.want) {
				t.Fatalf("targets=%v want=%v type=%s operation=%s options=%#v", got, test.want, session.Spec.Type, session.Spec.Operation(), session.Spec.WorkflowOptions())
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

func TestResolveSessionToolProbeTargetsUsesActiveConsumerNode(t *testing.T) {
	session := copyToolProbeSession(true)
	client := fake.NewClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "consumer"},
		Spec: corev1.PodSpec{NodeName: "node-a", Volumes: []corev1.Volume{{
			Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}},
		}}},
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
		t.Fatalf("probe resolution mutated sourceNode=%q", session.Spec.WorkflowOptions().SourceNode)
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
	if !slices.Equal(got["system/target-node"], []string{kube.ToolComponentRsync, kube.ToolComponentSSHD}) {
		t.Fatalf("destination target=%v all=%v", got["system/target-node"], got)
	}
}

func TestResolveSessionToolProbeTargetsUsesUniquePVTopology(t *testing.T) {
	session := copyToolProbeSession(true)
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-source"},
		Spec: corev1.PersistentVolumeSpec{NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpIn, Values: []string{"storage-host"}}},
		}}}}},
	}
	client := fake.NewClientset(
		pv,
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-object", Labels: map[string]string{corev1.LabelHostname: "storage-host"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "other-node", Labels: map[string]string{corev1.LabelHostname: "other-host"}}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "completed-consumer"},
			Spec: corev1.PodSpec{NodeName: "stale-node", Volumes: []corev1.Volume{{
				Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}},
			}}},
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
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: "node-a"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{corev1.LabelHostname: "node-b"}}},
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
	if source == nil || source.NodeName != "" || source.PVCName != "data" || !slices.Equal(source.Components, []string{kube.ToolComponentSSHD}) {
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
	if source == nil || source.NodeName != "node-a" || source.PVCName != session.Spec.Volumes[0].SourcePVC.Name || !source.SkipPVCMount {
		t.Fatalf("source target=%#v all=%#v", source, targets)
	}
	prober := &recordingToolImageProber{results: []kube.ToolImageProbeResult{{Target: *source, NodeName: "node-a"}}}
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
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: storageClass}, Provisioner: "local.csi.openebs.io", Parameters: map[string]string{"shared": "yes"}},
		probeConsumerPod("consumer", session.Spec.Volumes[0].SourcePVC.Name, "node-a"),
	)
	service := &Service{client: client}

	targets, err := service.resolveCopyToolProbeTargets(context.Background(), session, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range targets {
		if target.Namespace == session.Spec.Volumes[0].SourcePVC.Namespace && target.PVCName == session.Spec.Volumes[0].SourcePVC.Name {
			if target.SkipPVCMount || !target.WritablePVCMount {
				t.Fatalf("shared LVM source target=%#v", target)
			}
			return
		}
	}
	t.Fatal("shared LVM source target was not created")
}

func TestMarkSharedOpenEBSLVMProbeMountsReadsEachSourcePVCOnce(t *testing.T) {
	session := appTestSession()
	additional := session.Spec.Volumes[0]
	additional.SourcePVC.Name = "data-2"
	additional.SourcePV.Name = "pv-source-2"
	session.Spec.Volumes = append(session.Spec.Volumes, additional)
	storageClass := *session.Spec.Volumes[0].SourcePVCSpec.StorageClassName
	client := fake.NewClientset(&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: storageClass}, Provisioner: "local.csi.openebs.io"})
	manager := &recordingOpenEBSLVMSharedVolumeManager{shared: true}
	service := &Service{client: client, config: Config{OpenEBSLVMSharedVolumeManager: manager}}
	targets := []kube.ToolProbeTarget{
		{Namespace: "app", PVCName: "data"},
		{Namespace: "app", PVCName: "data-2"},
		{Namespace: "app", PVCName: "data"},
	}
	if err := service.markSharedOpenEBSLVMProbeMounts(context.Background(), session, targets); err != nil {
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
	cause := domain.NewError(domain.ErrorTimeout, "tool image probe", "MountVolume.SetUp failed: device already mounted")
	for _, test := range []struct {
		operation domain.Operation
		want      string
		absent    string
	}{
		{operation: domain.OperationMigratePod, want: "--precopy-passes 0", absent: "without --online"},
		{operation: domain.OperationCopy, want: "without --online", absent: "--precopy-passes"},
	} {
		err := warmCopyProbeError(test.operation, targets, cause)
		if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "warm-copy mount probe") || !strings.Contains(err.Error(), test.want) || strings.Contains(err.Error(), test.absent) {
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
	if err := warmCopyProbeError(domain.OperationMigrate, targets, cause); !errors.Is(err, cause) || strings.Contains(err.Error(), "warm-copy mount probe") {
		t.Fatalf("error=%v want original=%v", err, cause)
	}
}

func TestWarmCopyProbeErrorDoesNotMisclassifyGenericMountFailure(t *testing.T) {
	targets := []kube.ToolProbeTarget{{Namespace: "app", PVCName: "data"}}
	cause := domain.NewError(domain.ErrorPrecondition, "tool image probe", "FailedMount: filesystem needs repair")
	if err := warmCopyProbeError(domain.OperationMigrate, targets, cause); !errors.Is(err, cause) || strings.Contains(err.Error(), "warm-copy mount probe") {
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
				session.Spec.Volumes[0].AccessModes = []corev1.PersistentVolumeAccessMode{test.accessMode}
			}
			objects := make([]runtime.Object, 0, len(test.podNodes))
			for index, nodeName := range test.podNodes {
				objects = append(objects, &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: fmt.Sprintf("consumer-%d", index)},
					Spec: corev1.PodSpec{NodeName: nodeName, Volumes: []corev1.Volume{{
						Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}},
					}}},
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
	if domain.CategoryOf(err) != domain.ErrorPrecondition || !strings.Contains(err.Error(), "multiple source nodes") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestResolveSessionToolProbeTargetsRejectsOrchestratedConsumerMove(t *testing.T) {
	session := appTestSession()
	setSessionOperation(session, domain.OperationMigratePod)
	session.Spec.WorkflowOptionsPtr().SourceNode = "source-node"
	session.Spec.WorkflowOptionsPtr().Strategies = []string{domain.StrategyClusterIP}
	service := &Service{client: fake.NewClientset(probeConsumerPod("consumer", "data", "moved-node"))}

	_, err := service.resolveCopyToolProbeTargets(context.Background(), session, false)
	if domain.CategoryOf(err) != domain.ErrorConflict || !strings.Contains(err.Error(), "consumer runs on moved-node") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func copyToolProbeSession(online bool) *domain.Session {
	session := appTestSession()
	options := session.Spec.WorkflowOptions()
	options.SourceNode = ""
	options.Strategies = []string{domain.StrategyNodePort}
	session.Spec = domain.NewSessionSpec(domain.OperationCopy, session.Spec.SessionCommon, session.Spec.Workload(), online, options)
	return session
}

func probeConsumerPod(name, claim, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: name, UID: types.UID(name + "-uid")},
		Spec: corev1.PodSpec{NodeName: node, Volumes: []corev1.Volume{{
			Name: claim, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func TestCreateSessionPersistsBeforeStageProbe(t *testing.T) {
	client := fake.NewClientset()
	store := kube.NewConfigMapSessionStore(client)
	probeErr := domain.NewError(domain.ErrorPrecondition, "tool image probe", "image pull failed")
	prober := &recordingToolImageProber{err: probeErr}
	service := NewService(client, store, nil, nil, nil, nil, Config{
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
	if _, err := store.Get(context.Background(), created.Spec.SessionNamespace, created.ID); err != nil {
		t.Fatalf("persisted session unavailable after probe failure: %v", err)
	}
}

func TestStageProbeRunsInsideSessionLease(t *testing.T) {
	client := fake.NewClientset()
	store := kube.NewConfigMapSessionStore(client)
	probeErr := domain.NewError(domain.ErrorPrecondition, "tool image probe", "stop after lock assertion")
	prober := &recordingToolImageProber{err: probeErr, onProbe: func(ctx context.Context) {
		if _, ok := ctx.Value(sessionLockContextKey{}).(heldSessionLock); !ok {
			t.Fatal("probe did not inherit the held session Lease")
		}
	}}
	service := NewService(client, store, nil, nil, nil, nil, Config{ToolImageProber: prober})
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

		if err := fixture.service.WarmCopy(context.Background(), session); !errors.Is(err, probeErr) {
			t.Fatalf("WarmCopy() error=%v", err)
		}
		if len(prober.calls) != 1 || session.Status.Volumes[0].Sync.WarmCompletedAt == nil || session.Status.Phase != domain.PhaseWarmCopied {
			t.Fatalf("calls=%d phase=%s sync=%+v", len(prober.calls), session.Status.Phase, session.Status.Volumes[0].Sync)
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

		if err := fixture.service.FinalSync(context.Background(), session); !errors.Is(err, probeErr) {
			t.Fatalf("FinalSync() error=%v", err)
		}
		if len(prober.calls) != 1 || session.Status.Volumes[0].Sync.FinalCompletedAt == nil || session.Status.Phase != domain.PhaseFinalSynced {
			t.Fatalf("calls=%d phase=%s sync=%+v", len(prober.calls), session.Status.Phase, session.Status.Volumes[0].Sync)
		}
	})
}

func TestPauseAndFinalSyncProbesBeforeWorkloadPause(t *testing.T) {
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

	if err := fixture.service.PauseAndFinalSync(context.Background(), session); !errors.Is(err, probeErr) {
		t.Fatalf("PauseAndFinalSync() error=%v", err)
	}
	if fixture.controller.pauses != 0 || session.Status.Phase != domain.PhaseWarmCopied {
		t.Fatalf("pauses=%d phase=%s", fixture.controller.pauses, session.Status.Phase)
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
	if err := fixture.service.ResumeSession(context.Background(), completed); err != nil {
		t.Fatal(err)
	}

	invalid := appTestSession()
	setSessionOperation(invalid, domain.OperationCopy)
	invalid.Status.Phase = domain.PhaseCompleted
	if err := fixture.service.WarmCopy(context.Background(), invalid); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	if len(prober.calls) != 0 {
		t.Fatalf("probe calls=%d", len(prober.calls))
	}
}

func TestWarmCopyReusesSchedulerSelectedProbeNodeAndPullSecrets(t *testing.T) {
	fixture := newRecoveryFixture(t)
	if _, err := fixture.client.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "scheduler-node", Labels: map[string]string{corev1.LabelHostname: "scheduler-host"},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	session := appTestSession()
	setSessionOperation(session, domain.OperationCopy)
	session.Status.Phase = domain.PhaseReserved
	session.Spec.WorkflowOptionsPtr().SourceNode = ""
	session.Spec.WorkflowOptionsPtr().Strategies = []string{domain.StrategyClusterIP}
	session.Spec.Volumes[0].SourcePV.Name = ""
	prober := &recordingToolImageProber{results: []kube.ToolImageProbeResult{
		{
			Target: kube.ToolProbeTarget{
				Namespace: session.Spec.Volumes[0].DestinationPVC.Namespace, NodeName: "target-node",
				Components: []string{kube.ToolComponentRsync},
			},
			NodeName: "target-node", ImagePullSecrets: []corev1.LocalObjectReference{{Name: "destination-pull"}},
		},
		{
			Target: kube.ToolProbeTarget{
				Namespace: session.Spec.Volumes[0].SourcePVC.Namespace, PVCName: session.Spec.Volumes[0].SourcePVC.Name,
				Components: []string{kube.ToolComponentSSHD},
			},
			NodeName: "scheduler-node", ImagePullSecrets: []corev1.LocalObjectReference{{Name: "source-pull"}},
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
	if _, err := fixture.client.StorageV1().StorageClasses().Create(context.Background(), &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: storageClass}, Provisioner: "local.csi.openebs.io", Parameters: map[string]string{"shared": "yes"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	session := appTestSession()
	setSessionOperation(session, domain.OperationCopy)
	session.Status.Phase = domain.PhaseReserved
	if err := fixture.service.WarmCopy(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if len(fixture.copier.requests) != 1 || !fixture.copier.requests[0].SourceMountReadWrite {
		t.Fatalf("copy requests=%#v", fixture.copier.requests)
	}
}

func TestWarmCopyEnablesOpenEBSLVMSharedBeforeProbe(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	session.Spec.WorkflowOptionsPtr().OpenEBSLVMEnableShared = true
	storageClass := *session.Spec.Volumes[0].SourcePVCSpec.StorageClassName
	if _, err := fixture.client.StorageV1().StorageClasses().Create(context.Background(), &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: storageClass}, Provisioner: "local.csi.openebs.io",
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.client.CoreV1().Pods("app").Create(context.Background(), probeConsumerPod("database-0", "data", "source-node"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	manager := &recordingOpenEBSLVMSharedVolumeManager{}
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager
	if err := fixture.service.WarmCopy(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(manager.ensurePVs, []string{"pv-source"}) {
		t.Fatalf("shared volumes=%v", manager.ensurePVs)
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
}

func TestWarmCopyRestoresOpenEBSLVMSharedAfterProbeFailure(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	session.Spec.WorkflowOptionsPtr().OpenEBSLVMEnableShared = true
	storageClass := *session.Spec.Volumes[0].SourcePVCSpec.StorageClassName
	if _, err := fixture.client.StorageV1().StorageClasses().Create(context.Background(), &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: storageClass}, Provisioner: "local.csi.openebs.io",
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.client.CoreV1().Pods("app").Create(context.Background(), probeConsumerPod("database-0", "data", "source-node"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	probeErr := errors.New("tool image probe failed")
	fixture.service.config.ToolImageProber = &recordingToolImageProber{err: probeErr}
	manager := &recordingOpenEBSLVMSharedVolumeManager{}
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
	session.Spec.WorkflowOptionsPtr().OpenEBSLVMEnableShared = true
	storageClass := *session.Spec.Volumes[0].SourcePVCSpec.StorageClassName
	if _, err := fixture.client.StorageV1().StorageClasses().Create(context.Background(), &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: storageClass}, Provisioner: "local.csi.openebs.io",
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.client.CoreV1().Pods("app").Create(context.Background(), probeConsumerPod("database-0", "data", "source-node"), metav1.CreateOptions{}); err != nil {
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

func TestWarmCopyRestoresOpenEBSLVMSharedWhenCheckpointIsCanceled(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	session.Spec.WorkflowOptionsPtr().OpenEBSLVMEnableShared = true
	storageClass := *session.Spec.Volumes[0].SourcePVCSpec.StorageClassName
	if _, err := fixture.client.StorageV1().StorageClasses().Create(context.Background(), &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: storageClass}, Provisioner: "local.csi.openebs.io",
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.client.CoreV1().Pods("app").Create(context.Background(), probeConsumerPod("database-0", "data", "source-node"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := &recordingOpenEBSLVMSharedVolumeManager{onEnsure: cancel}
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager
	store := &toolProbeContextStore{}
	fixture.service.store = store

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
	if len(store.updateContextErrs) != 2 || !errors.Is(store.updateContextErrs[0], context.Canceled) || store.updateContextErrs[1] != nil {
		t.Fatalf("checkpoint context errors=%v", store.updateContextErrs)
	}
	if len(session.Status.OpenEBSLVMSharedMounts) != 0 {
		t.Fatalf("pending shared mounts=%#v", session.Status.OpenEBSLVMSharedMounts)
	}
}

func TestAbortRestoresPendingOpenEBSLVMSharedMount(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	session.Status.OpenEBSLVMSharedMounts = []domain.OpenEBSLVMSharedMount{{
		SourcePV: "pv-source", PreviousShared: "no", PreviousSharedSet: true,
	}}
	manager := &recordingOpenEBSLVMSharedVolumeManager{shared: true}
	fixture.service.config.OpenEBSLVMSharedVolumeManager = manager

	if err := fixture.service.Abort(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if session.Status.Phase != domain.PhaseAborted || !slices.Equal(manager.restorePVs, []string{"pv-source"}) || manager.shared {
		t.Fatalf("phase=%s restore=%v shared=%t", session.Status.Phase, manager.restorePVs, manager.shared)
	}
	if len(session.Status.OpenEBSLVMSharedMounts) != 0 {
		t.Fatalf("pending shared mounts=%#v", session.Status.OpenEBSLVMSharedMounts)
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
