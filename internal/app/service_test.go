package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

type memoryStore struct {
	updates int
	deletes int
}

type contextAwareStore struct {
	memoryStore
	updateContextErr error
}

type failingUpdateStore struct{ memoryStore }

func (f *failingUpdateStore) Update(context.Context, *domain.Session) error {
	return errors.New("injected session update failure")
}

func (m *contextAwareStore) Update(ctx context.Context, session *domain.Session) error {
	m.updateContextErr = ctx.Err()
	if m.updateContextErr != nil {
		return m.updateContextErr
	}
	return m.memoryStore.Update(ctx, session)
}

func (m *memoryStore) Create(_ context.Context, session *domain.Session) error {
	session.ResourceVersion = "1"
	return nil
}

func (m *memoryStore) Get(context.Context, string, string) (*domain.Session, error) {
	return nil, errors.New("unused")
}

func (m *memoryStore) Update(_ context.Context, session *domain.Session) error {
	m.updates++
	session.ResourceVersion = fmt.Sprint(m.updates + 1)
	return nil
}

func (m *memoryStore) List(context.Context, string) ([]*domain.Session, error) { return nil, nil }
func (m *memoryStore) Delete(context.Context, *domain.Session) error {
	m.deletes++
	return nil
}

func TestSessionLockSupportsMultiLevelReentry(t *testing.T) {
	client := fake.NewClientset()
	service := &Service{store: kube.NewConfigMapSessionStore(client)}
	calls := 0
	err := service.withSessionIDLock(context.Background(), "system", "session", func(first context.Context) error {
		calls++
		return service.withSessionIDLock(first, "system", "session", func(second context.Context) error {
			calls++
			return service.withSessionIDLock(second, "system", "session", func(context.Context) error {
				calls++
				return nil
			})
		})
	})
	if err != nil {
		t.Fatalf("nested session lock: %v", err)
	}
	if calls != 3 {
		t.Fatalf("nested calls=%d, want 3", calls)
	}
}

type fakeReserver struct{}

func (f *fakeReserver) ReserveVolume(_ context.Context, _ *domain.Session, volume *domain.VolumeSpec, status *domain.VolumeStatus, _ bool) error {
	volume.DestinationPVC.UID = types.UID("dest-pvc-uid")
	volume.DestinationPV = domain.ObjectReference{Name: "pv-destination", UID: types.UID("dest-pv-uid")}
	volume.DestinationPolicy = corev1.PersistentVolumeReclaimDelete
	status.Reserved = true
	return nil
}

type fakeController struct {
	paused        int
	resumed       int
	pauseMutation bool
}

func (f *fakeController) Pause(_ context.Context, session *domain.Session) error {
	f.paused++
	if f.pauseMutation {
		session.Spec.WorkloadPtr().Pod.ResourceVersion = "pause-resource-version"
	}
	return nil
}

func (f *fakeController) Resume(context.Context, *domain.Session) error {
	f.resumed++
	return nil
}

func (f *fakeController) VerifyPaused(context.Context, *domain.Session) error { return nil }

type fakeCopier struct {
	modes     []copyengine.Mode
	failFinal int
}

func (f *fakeCopier) Copy(_ context.Context, request copyengine.Request, _ copyengine.ProgressFunc) error {
	f.modes = append(f.modes, request.Mode)
	if request.Mode == copyengine.ModeFinal && f.failFinal > 0 {
		f.failFinal--
		return domain.NewError(domain.ErrorCopy, "copy", "injected final-sync failure")
	}
	return nil
}

type fakeSwitcher struct {
	client kubernetes.Interface
}

func (f *fakeSwitcher) VerifyVolumeOffline(context.Context, *domain.VolumeSpec) error { return nil }

func (f *fakeSwitcher) ActivateVolume(ctx context.Context, session *domain.Session, volume *domain.VolumeSpec, status *domain.VolumeStatus, progress kube.ProgressFunc) error {
	pvc, err := f.client.CoreV1().PersistentVolumeClaims(volume.SourcePVC.Namespace).Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	pvc.Spec.VolumeName = volume.DestinationPV.Name
	pvc.Status.Phase = corev1.ClaimBound
	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}
	pvc.Annotations[kube.SessionKey] = session.ID
	pvc, err = f.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	now := metav1.Now()
	status.Activation.ActivatedAt = &now
	status.Activation.ActivePVC = domain.ObjectReference{Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID}
	if progress != nil {
		return progress()
	}
	return nil
}

func (f *fakeSwitcher) RollbackVolume(ctx context.Context, session *domain.Session, volume *domain.VolumeSpec, status *domain.VolumeStatus, progress kube.ProgressFunc) error {
	pvc, err := f.client.CoreV1().PersistentVolumeClaims(volume.SourcePVC.Namespace).Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	pvc.Spec.VolumeName = volume.SourcePV.Name
	pvc.Status.Phase = corev1.ClaimBound
	pvc.Annotations[kube.SessionKey] = session.ID
	if _, err := f.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{}); err != nil {
		return err
	}
	now := metav1.Now()
	status.Activation.RolledBackAt = &now
	if progress != nil {
		return progress()
	}
	return nil
}

func (f *fakeSwitcher) RenamePVC(context.Context, *domain.Session, *domain.VolumeSpec, kube.ProgressFunc) (*corev1.PersistentVolumeClaim, error) {
	return nil, errors.New("unused")
}

func appTestSession() *domain.Session {
	storageClass := "fast"
	mode := corev1.PersistentVolumeFilesystem
	session := domain.NewSession("session-123", domain.NewSessionSpec(domain.OperationMigrate, domain.SessionCommon{
		SourceNamespace:      "app",
		TemporaryNamespace:   "system",
		DestinationNamespace: "app",
		SessionNamespace:     "system",
		Volumes: []domain.VolumeSpec{{
			SourcePVC:      domain.ObjectReference{Namespace: "app", Name: "data", UID: types.UID("source-pvc-uid")},
			SourcePV:       domain.ObjectReference{Name: "pv-source", UID: types.UID("source-pv-uid")},
			SourcePVCSpec:  corev1.PersistentVolumeClaimSpec{StorageClassName: &storageClass, VolumeMode: &mode},
			DestinationPVC: domain.ObjectReference{Namespace: "system", Name: "data-migrated"},
			Capacity:       "1Gi",
			StorageClass:   "fast",
			AccessModes:    []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeMode:     mode,
		}},
	}, domain.WorkloadSpec{Adapter: domain.WorkloadNone}, false, domain.SessionWorkflowOptions{
		SourceNode: "source-node", TargetNode: "target-node", Strategies: []string{"mount"}, DeleteExtraneous: true,
	}), time.Unix(100, 0))
	session.ResourceVersion = "1"
	return session
}

func appTestService(t *testing.T, copier *fakeCopier) (*Service, *domain.Session, *fakeController, *memoryStore) {
	t.Helper()
	client := fake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "source-node", Labels: map[string]string{corev1.LabelHostname: "source-node"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "target-node", Labels: map[string]string{corev1.LabelHostname: "target-node"}}},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: types.UID("source-pvc-uid")},
			Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv-source"},
			Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-destination", UID: types.UID("dest-pv-uid")},
			Spec:       corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Namespace: "app", Name: "data", UID: types.UID("source-pvc-uid")}},
		},
	)
	store := &memoryStore{}
	controllers := &fakeController{}
	switcher := &fakeSwitcher{client: client}
	service := NewService(client, store, &fakeReserver{}, copier, controllers, switcher, Config{
		Retries:      1,
		RetryBackoff: time.Millisecond,
	})
	service.sleep = func(context.Context, time.Duration) error { return nil }
	return service, appTestSession(), controllers, store
}

func TestMigrateRunsAllStagesAndPersistsProgress(t *testing.T) {
	copier := &fakeCopier{}
	service, session, controllers, store := appTestService(t, copier)
	if err := service.Migrate(context.Background(), session, 1); err != nil {
		t.Fatal(err)
	}
	if session.Status.Phase != domain.PhaseCompleted {
		t.Fatalf("phase=%s want=%s", session.Status.Phase, domain.PhaseCompleted)
	}
	if fmt.Sprint(copier.modes) != "[warm final]" {
		t.Fatalf("copy modes=%v", copier.modes)
	}
	if controllers.paused != 1 || controllers.resumed != 1 {
		t.Fatalf("controller calls pause=%d resume=%d", controllers.paused, controllers.resumed)
	}
	if store.updates < 10 {
		t.Fatalf("session updates=%d, expected progress persistence", store.updates)
	}
}

func TestMigrateLogsLongRunningStageBoundaries(t *testing.T) {
	var logs bytes.Buffer
	copier := &fakeCopier{}
	service, session, _, _ := appTestService(t, copier)
	service.config.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	if err := service.Migrate(context.Background(), session, 1); err != nil {
		t.Fatal(err)
	}
	for _, event := range []string{
		"migration stage started",
		"destination storage reservation started",
		"copy started",
		"waiting for copy tool Pods to release PVCs",
		"migration stage completed",
	} {
		if !strings.Contains(logs.String(), event) {
			t.Fatalf("logs missing %q: %s", event, logs.String())
		}
	}
}

func TestFinishLogsCompletionAfterPersistence(t *testing.T) {
	var logs bytes.Buffer
	service, session, _, _ := appTestService(t, &fakeCopier{})
	service.store = &failingUpdateStore{}
	service.config.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	if err := service.finish(context.Background(), session, domain.PhaseReserving, "reserving destination storage"); err == nil {
		t.Fatal("expected persistence failure")
	}
	output := logs.String()
	if !strings.Contains(output, "migration stage persistence failed") || strings.Contains(output, "migration stage completed") {
		t.Fatalf("logs=%q", output)
	}
}

func TestFailContextPersistsAfterParentCancellation(t *testing.T) {
	store := &contextAwareStore{}
	service, session, _, _ := appTestService(t, &fakeCopier{})
	service.store = store
	session.Status.Phase = domain.PhaseWarmCopying
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cause := domain.NewError(domain.ErrorCopy, "warm copy", "copy canceled")

	if err := service.failContext(ctx, session, cause); !errors.Is(err, cause) {
		t.Fatalf("failContext error=%v want=%v", err, cause)
	}
	if session.Status.Phase != domain.PhaseFailed || session.Status.ResumeFrom != domain.PhaseWarmCopying {
		t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
	}
	if store.updateContextErr != nil || store.updates != 1 {
		t.Fatalf("checkpoint context err=%v updates=%d", store.updateContextErr, store.updates)
	}
}

func TestCopyConsumerPreflightSupportsOfflineAndOnlineBoundaries(t *testing.T) {
	service, session, _, _ := appTestService(t, &fakeCopier{})
	session.Spec = domain.NewSessionSpec(domain.OperationCopy, session.Spec.SessionCommon, domain.WorkloadSpec{Adapter: domain.WorkloadNone}, false)
	_, err := service.client.CoreV1().Pods("app").Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "writer"},
		Spec: corev1.PodSpec{
			NodeName: "source-node",
			Volumes:  []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.validateCopyConsumers(context.Background(), session, &session.Spec.Volumes[0]); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("offline consumer category=%s error=%v", domain.CategoryOf(err), err)
	}
	session.Spec.Copy.Online = true
	session.Spec.WorkflowOptionsPtr().SourceNode = ""
	if err := service.validateCopyConsumers(context.Background(), session, &session.Spec.Volumes[0]); err != nil {
		t.Fatalf("online RWO consumer error=%v", err)
	}
	if session.Spec.WorkflowOptions().SourceNode != "source-node" {
		t.Fatalf("inferred source node=%q", session.Spec.WorkflowOptions().SourceNode)
	}
	pod, err := service.client.CoreV1().Pods("app").Get(context.Background(), "writer", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Spec.NodeName = ""
	if _, err := service.client.CoreV1().Pods("app").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	session.Spec.WorkflowOptionsPtr().SourceNode = ""
	if err := service.validateCopyConsumers(context.Background(), session, &session.Spec.Volumes[0]); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("unscheduled RWO category=%s error=%v", domain.CategoryOf(err), err)
	}
	session.Spec.Volumes[0].AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod}
	if err := service.validateCopyConsumers(context.Background(), session, &session.Spec.Volumes[0]); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("online RWOP category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestFinalSyncFailureKeepsWorkloadPausedAndResumes(t *testing.T) {
	copier := &fakeCopier{failFinal: 1}
	service, session, controllers, _ := appTestService(t, copier)
	if err := service.Migrate(context.Background(), session, 0); domain.CategoryOf(err) != domain.ErrorCopy {
		t.Fatalf("migration error=%v category=%s", err, domain.CategoryOf(err))
	}
	if session.Status.Phase != domain.PhaseFailed || session.Status.ResumeFrom != domain.PhaseFinalSyncing {
		t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
	}
	if controllers.paused != 1 || controllers.resumed != 0 {
		t.Fatalf("controller calls pause=%d resume=%d", controllers.paused, controllers.resumed)
	}
	if err := service.ResumeSession(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if session.Status.Phase != domain.PhaseCompleted || controllers.resumed != 1 {
		t.Fatalf("phase=%s resume calls=%d", session.Status.Phase, controllers.resumed)
	}
}

func TestPausePersistsControllerRecoveryState(t *testing.T) {
	service, session, controllers, store := appTestService(t, &fakeCopier{})
	controllers.pauseMutation = true
	session.Status.Phase = domain.PhaseReserved
	if err := service.Pause(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if session.Spec.Workload().Pod.ResourceVersion != "pause-resource-version" {
		t.Fatalf("pause mutation=%q", session.Spec.Workload().Pod.ResourceVersion)
	}
	if store.updates != 3 {
		t.Fatalf("session updates=%d want 3 (begin, recovery state, finish)", store.updates)
	}
}

func TestRollbackRestoresSourceBindingAndResumes(t *testing.T) {
	copier := &fakeCopier{}
	service, session, controllers, _ := appTestService(t, copier)
	if err := service.Migrate(context.Background(), session, 0); err != nil {
		t.Fatal(err)
	}
	if err := service.Rollback(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if session.Status.Phase != domain.PhaseRolledBack {
		t.Fatalf("phase=%s", session.Status.Phase)
	}
	if controllers.paused != 2 || controllers.resumed != 2 {
		t.Fatalf("controller calls pause=%d resume=%d", controllers.paused, controllers.resumed)
	}
}

func TestPVMigrateToolIdentificationIsScopedToClaims(t *testing.T) {
	tool := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			"app.kubernetes.io/instance":  "pv-migrate-pm-123-clusterip",
			"app.kubernetes.io/component": "sshd",
		}},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
			Name: "source",
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: "data",
			}},
		}}},
	}
	if !isPVMigrateToolForClaims(tool, map[string]struct{}{"data": {}}) {
		t.Fatal("expected tool Pod to match its PVC")
	}
	if isPVMigrateToolForClaims(tool, map[string]struct{}{"other": {}}) {
		t.Fatal("tool Pod matched another PVC")
	}
	tool.Labels["app.kubernetes.io/instance"] = "application"
	if isPVMigrateToolForClaims(tool, map[string]struct{}{"data": {}}) {
		t.Fatal("application Pod matched a pv-migrate tool")
	}
}
