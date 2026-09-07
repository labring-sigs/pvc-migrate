package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
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

type fakeSessionLocker struct {
	memoryStore
	lock kube.SessionLock
}

func (f *fakeSessionLocker) AcquireSessionLock(
	context.Context,
	string,
	string,
) (kube.SessionLock, error) {
	return f.lock, nil
}

type fakeSessionLock struct {
	err        error
	releaseErr error
}

func (f *fakeSessionLock) Bind(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(ctx)
}

func (f *fakeSessionLock) Err() error { return f.err }

func (f *fakeSessionLock) Release(context.Context) error { return f.releaseErr }

func (f *fakeSessionLock) Delete(context.Context) error { return nil }

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

func (*memoryStore) StorageBackend() string { return kube.SessionBackendConfigMap }

func (m *memoryStore) GetByType(
	ctx context.Context,
	namespace, id string,
	_ domain.SessionType,
) (*domain.Session, error) {
	return m.Get(ctx, namespace, id)
}

func (*memoryStore) AcquireSessionLock(
	context.Context,
	string,
	string,
) (kube.SessionLock, error) {
	return &fakeSessionLock{}, nil
}

func (*memoryStore) DeleteSessionLease(context.Context, string, string) error { return nil }

func (m *memoryStore) Update(_ context.Context, session *domain.Session) error {
	m.updates++
	session.ResourceVersion = strconv.Itoa(m.updates + 1)
	return nil
}

func (m *memoryStore) List(context.Context, string) ([]*domain.Session, error) { return nil, nil }
func (m *memoryStore) Delete(context.Context, *domain.Session) error {
	m.deletes++
	return nil
}

func TestSessionLockSupportsMultiLevelReentry(t *testing.T) {
	client := fake.NewClientset()
	assignLeaseUIDs(client)
	service := &Service{store: kube.NewConfigMapSessionStore(client)}
	calls := 0

	err := service.withSessionIDLock(
		context.Background(),
		"system",
		"session",
		func(first context.Context) error {
			calls++

			return service.withSessionIDLock(
				first,
				"system",
				"session",
				func(second context.Context) error {
					calls++

					return service.withSessionIDLock(
						second,
						"system",
						"session",
						func(context.Context) error {
							calls++
							return nil
						},
					)
				},
			)
		},
	)
	if err != nil {
		t.Fatalf("nested session lock: %v", err)
	}

	if calls != 3 {
		t.Fatalf("nested calls=%d, want 3", calls)
	}
}

func TestSessionLockReleaseFailureIsLoggedAsWarning(t *testing.T) {
	var logs bytes.Buffer

	releaseErr := errors.New("lease update denied")
	service := &Service{
		store:  &fakeSessionLocker{lock: &fakeSessionLock{releaseErr: releaseErr}},
		config: Config{Logger: slog.New(slog.NewTextHandler(&logs, nil))},
	}

	err := service.withSessionIDLock(
		context.Background(),
		"system",
		"session",
		func(context.Context) error { return nil },
	)
	if !errors.Is(err, releaseErr) {
		t.Fatalf("error=%v, want release error", err)
	}

	output := logs.String()
	if !strings.Contains(output, "session lock release failed") ||
		!strings.Contains(output, releaseErr.Error()) {
		t.Fatalf("warning log missing release failure: %q", output)
	}

	if strings.Contains(output, "session lock released") {
		t.Fatalf("success log emitted after release failure: %q", output)
	}
}

type fakeReserver struct{}

func (f *fakeReserver) ReserveVolume(
	_ context.Context,
	_ *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	_ bool,
) error {
	volume.DestinationPVC.UID = types.UID("dest-pvc-uid")
	volume.DestinationPV = domain.ObjectReference{
		Name: "pv-destination",
		UID:  types.UID("dest-pv-uid"),
	}
	volume.DestinationPolicy = corev1.PersistentVolumeReclaimDelete
	status.Reserved = true

	return nil
}

type fakeController struct {
	paused         int
	resumed        int
	pauseMutation  bool
	resumeMutation bool
	resumeErr      error
}

func (f *fakeController) Pause(_ context.Context, session *domain.Session) error {
	f.paused++
	if f.pauseMutation {
		session.Spec.WorkloadPtr().Pod.ResourceVersion = "pause-resource-version"
	}

	return nil
}

func (f *fakeController) Resume(_ context.Context, session *domain.Session) error {
	f.resumed++
	if f.resumeMutation {
		session.Spec.WorkloadPtr().Pod.ResourceVersion = "resume-resource-version"
	}

	return f.resumeErr
}

func (f *fakeController) ValidateResume(context.Context, *domain.Session) error { return nil }

func (f *fakeController) VerifyPaused(context.Context, *domain.Session) error { return nil }

func (f *fakeController) CurrentRollbackPods(
	context.Context,
	*domain.Session,
) ([]domain.ObjectReference, error) {
	return nil, nil
}

type fakeCopier struct {
	modes     []copyengine.Mode
	requests  []copyengine.Request
	failFinal int
	err       error
}

func (*fakeCopier) Cleanup(context.Context, copyengine.Request) error { return nil }

func (f *fakeCopier) Copy(
	_ context.Context,
	request copyengine.Request,
	_ copyengine.ProgressFunc,
) error {
	f.modes = append(f.modes, request.Mode)

	f.requests = append(f.requests, request)

	if request.Mode == copyengine.ModeFinal && f.failFinal > 0 {
		f.failFinal--
		return domain.NewError(domain.ErrorCopy, "copy", "injected final-sync failure")
	}

	return f.err
}

type fakeSwitcher struct {
	client kubernetes.Interface
}

func (f *fakeSwitcher) VerifyVolumeOffline(context.Context, *domain.VolumeSpec) error { return nil }

func (f *fakeSwitcher) VerifyVolumesOfflineForSession(
	ctx context.Context,
	_ string,
	volumes []*domain.VolumeSpec,
) error {
	for _, volume := range volumes {
		if err := f.VerifyVolumeOffline(ctx, volume); err != nil {
			return err
		}
	}

	return nil
}

func (f *fakeSwitcher) VerifyActivationRecovery(
	ctx context.Context,
	sessionID string,
	volumes []*domain.VolumeSpec,
) error {
	return f.VerifyVolumesOfflineForSession(ctx, sessionID, volumes)
}

func (f *fakeSwitcher) VerifyPVCRebindRecovery(
	ctx context.Context,
	sessionID string,
	from, to, pv domain.ObjectReference,
) error {
	return nil
}

func (f *fakeSwitcher) ActivateVolume(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	progress kube.ProgressFunc,
) error {
	pvc, err := f.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	pvc.Spec.VolumeName = volume.DestinationPV.Name

	pvc.Status.Phase = corev1.ClaimBound
	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}

	pvc.Annotations[kube.SessionKey] = session.ID

	pvc, err = f.client.CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Update(ctx, pvc, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	now := metav1.Now()
	status.Activation.ActivatedAt = &now
	status.Activation.ActivePVC = domain.ObjectReference{
		Namespace: pvc.Namespace,
		Name:      pvc.Name,
		UID:       pvc.UID,
	}

	if progress != nil {
		return progress()
	}

	return nil
}

func (f *fakeSwitcher) RollbackVolume(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	progress kube.ProgressFunc,
) error {
	pvc, err := f.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	pvc.Spec.VolumeName = volume.SourcePV.Name
	pvc.Status.Phase = corev1.ClaimBound

	pvc.Annotations[kube.SessionKey] = session.ID
	if _, err := f.client.CoreV1().
		PersistentVolumeClaims(pvc.Namespace).
		Update(ctx, pvc, metav1.UpdateOptions{}); err != nil {
		return err
	}

	now := metav1.Now()
	status.Activation.RolledBackAt = &now

	if progress != nil {
		return progress()
	}

	return nil
}

func (f *fakeSwitcher) RenamePVC(
	context.Context,
	*domain.Session,
	*domain.VolumeSpec,
	kube.ProgressFunc,
) (*corev1.PersistentVolumeClaim, error) {
	return nil, errors.New("unused")
}

func appTestSession() *domain.Session {
	storageClass := "fast"
	mode := corev1.PersistentVolumeFilesystem
	session := domain.NewSession(
		"session-123",
		domain.NewPodMigrationSessionSpec(domain.SessionCommon{
			SourceNamespace:      "app",
			TemporaryNamespace:   "system",
			DestinationNamespace: "app",
			SessionNamespace:     "system",
			Volumes: []domain.VolumeSpec{
				{
					SourcePVC: domain.ObjectReference{
						Namespace: "app",
						Name:      "data",
						UID:       types.UID("source-pvc-uid"),
					},
					SourcePV: domain.ObjectReference{
						Name: "pv-source",
						UID:  types.UID("source-pv-uid"),
					},
					SourceReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
					SourcePVCSpec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: &storageClass,
						VolumeMode:       &mode,
					},
					DestinationPVC: domain.ObjectReference{
						Namespace: "system",
						Name:      "data-migrated",
					},
					Capacity:     "1Gi",
					StorageClass: "fast",
					AccessModes:  []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					VolumeMode:   mode,
				},
			},
		}, domain.WorkloadSpec{
			Adapter: domain.WorkloadStatefulSet,
			Pod: domain.ObjectReference{
				Namespace: "app", Name: "database-0", UID: "database-0-uid",
			},
			Controller: domain.ObjectReference{
				Namespace: "app", Name: "database", UID: "database-controller-uid",
			},
		}, domain.SessionWorkflowOptions{
			SourceNode:       "source-node",
			TargetNode:       "target-node",
			Strategies:       []string{"mount"},
			DeleteExtraneous: true,
		}, 1, false),
		time.Unix(100, 0),
	)
	session.ResourceVersion = "1"

	return session
}

func TestPhaseBeforeUsesMostRecentRecoveryStage(t *testing.T) {
	tests := []struct {
		name    string
		target  domain.Phase
		history []domain.Phase
		want    domain.Phase
	}{
		{
			name:   "repeated rollback skips failed and earlier rollback",
			target: domain.PhaseRollingBack,
			history: []domain.Phase{
				domain.PhasePlanned,
				domain.PhaseCompleted,
				domain.PhaseRollingBack,
				domain.PhaseFailed,
				domain.PhasePaused,
				domain.PhaseRollingBack,
			},
			want: domain.PhasePaused,
		},
		{
			name:   "repeated abort skips failed and earlier abort",
			target: domain.PhaseAborting,
			history: []domain.Phase{
				domain.PhasePlanned,
				domain.PhasePausing,
				domain.PhaseAborting,
				domain.PhaseFailed,
				domain.PhasePaused,
				domain.PhaseAborting,
			},
			want: domain.PhasePaused,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := appTestSession()

			session.Status.History = make([]domain.HistoryEntry, 0, len(test.history))
			for _, phase := range test.history {
				session.Status.History = append(
					session.Status.History,
					domain.HistoryEntry{Phase: phase},
				)
			}

			if got := phaseBefore(session, test.target); got != test.want {
				t.Fatalf("phaseBefore(%s)=%s, want %s", test.target, got, test.want)
			}
		})
	}
}

func appTestService(
	t *testing.T,
	copier *fakeCopier,
) (*Service, *domain.Session, *fakeController, *memoryStore) {
	t.Helper()

	client := fake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "source-node",
				Labels: map[string]string{corev1.LabelHostname: "source-node"},
			},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "target-node",
				Labels: map[string]string{corev1.LabelHostname: "target-node"},
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "app",
				Name:      "data",
				UID:       types.UID("source-pvc-uid"),
			},
			Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-source"},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-destination", UID: types.UID("dest-pv-uid")},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{
					Namespace: "app",
					Name:      "data",
					UID:       types.UID("source-pvc-uid"),
				},
			},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-source", UID: types.UID("source-pv-uid")},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{
					Namespace: "app",
					Name:      "data",
					UID:       types.UID("source-pvc-uid"),
				},
			},
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

func markTestSessionReserved(session *domain.Session) {
	session.Spec.Volumes[0].DestinationPVC.UID = types.UID("dest-pvc-uid")
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
		Name: "pv-destination",
		UID:  types.UID("dest-pv-uid"),
	}
	session.Status.Volumes[0].Reserved = true
}

func TestMigrateRunsAllStagesAndPersistsProgress(t *testing.T) {
	copier := &fakeCopier{}

	service, session, controllers, store := appTestService(t, copier)
	if err := service.MigratePod(context.Background(), session); err != nil {
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

	for _, request := range copier.requests {
		identityValues := kube.TransferServiceAccountHelmValues()
		for _, expected := range identityValues.Values {
			if !slices.Contains(request.HelmValues, expected) {
				t.Fatalf(
					"copy request lacks typed transfer identity value %q: %v",
					expected,
					request.HelmValues,
				)
			}
		}

		for _, expected := range identityValues.StringValues {
			if !slices.Contains(request.HelmStringValues, expected) {
				t.Fatalf(
					"copy request lacks transfer identity value %q: %v",
					expected,
					request.HelmStringValues,
				)
			}
		}
	}

	account, err := service.client.CoreV1().ServiceAccounts("app").Get(
		context.Background(), kube.TransferServiceAccountName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	if account.AutomountServiceAccountToken == nil || *account.AutomountServiceAccountToken {
		t.Fatalf(
			"transfer account automountServiceAccountToken=%v, want false",
			account.AutomountServiceAccountToken,
		)
	}

	if store.updates < 10 {
		t.Fatalf("session updates=%d, expected progress persistence", store.updates)
	}
}

func TestOfflineMigrateDoesNotOrchestrateWorkload(t *testing.T) {
	copier := &fakeCopier{}
	service, session, controllers, _ := appTestService(t, copier)
	setSessionOperation(session, domain.OperationMigrate)

	if err := service.OfflineMigrate(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != domain.PhaseCompleted {
		t.Fatalf("phase=%s want=%s", session.Status.Phase, domain.PhaseCompleted)
	}

	if got := fmt.Sprint(copier.modes); got != "[final]" {
		t.Fatalf("copy modes=%s want=[final]", got)
	}

	if controllers.paused != 0 || controllers.resumed != 0 {
		t.Fatalf(
			"offline migration orchestrated workload: pauses=%d resumes=%d",
			controllers.paused,
			controllers.resumed,
		)
	}
}

func TestOfflineAbortValidationDoesNotRequireWorkloadController(t *testing.T) {
	service, session, _, _ := appTestService(t, &fakeCopier{})
	setSessionOperation(session, domain.OperationMigrate)
	session.Status.Phase = domain.PhaseFinalSynced
	service.controllers = nil

	if err := service.ValidateOfflineMigrationAbort(context.Background(), session); err != nil {
		t.Fatalf("offline abort validation returned %v", err)
	}
}

func TestOfflineRollbackValidationDoesNotRequireWorkloadController(t *testing.T) {
	service, session, _, _ := appTestService(t, &fakeCopier{})
	setSessionOperation(session, domain.OperationMigrate)
	session.Status.Phase = domain.PhaseFinalSynced
	markTestSessionReserved(session)

	service.controllers = nil

	if err := service.ValidateOfflineMigrationRollback(context.Background(), session); err != nil {
		t.Fatalf("offline rollback validation returned %v", err)
	}
}

func TestMigratePodRejectsOfflineSession(t *testing.T) {
	service, session, _, _ := appTestService(t, &fakeCopier{})
	setSessionOperation(session, domain.OperationMigrate)

	if err := service.MigratePod(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestResumeSessionUsesPersistedPrecopyPasses(t *testing.T) {
	for _, test := range []struct {
		name   string
		passes int
		modes  string
	}{
		{name: "offline", passes: 0, modes: "[final]"},
		{name: "multiple warm passes", passes: 2, modes: "[warm warm final]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			copier := &fakeCopier{}
			service, session, _, _ := appTestService(t, copier)
			session.Status.Phase = domain.PhaseReserved
			markTestSessionReserved(session)

			session.Spec.MigratePod.PrecopyPasses = test.passes
			if err := service.resumeWorkflowForTest(context.Background(), session); err != nil {
				t.Fatal(err)
			}

			if got := fmt.Sprint(copier.modes); got != test.modes {
				t.Fatalf("copy modes=%s want=%s", got, test.modes)
			}
		})
	}
}

func TestResumeSessionCompletesRemainingPrecopyPasses(t *testing.T) {
	for _, test := range []struct {
		name      string
		phase     domain.Phase
		passes    int
		completed int
		modes     string
	}{
		{name: "after completed warm pass", phase: domain.PhaseWarmCopied, passes: 2, completed: 1, modes: "[warm final]"},
		{name: "during next warm pass", phase: domain.PhaseWarmCopying, passes: 3, completed: 1, modes: "[warm warm final]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			copier := &fakeCopier{}
			service, session, _, _ := appTestService(t, copier)
			session.Status.Phase = test.phase
			markTestSessionReserved(session)
			session.Spec.MigratePod.PrecopyPasses = test.passes

			session.Status.WarmPassesCompleted = test.completed
			if err := service.resumeWorkflowForTest(context.Background(), session); err != nil {
				t.Fatal(err)
			}

			if got := fmt.Sprint(copier.modes); got != test.modes {
				t.Fatalf("copy modes=%s want=%s", got, test.modes)
			}

			if got := session.Status.WarmPassesCompleted; got != test.passes {
				t.Fatalf("completed warm passes=%d want=%d", got, test.passes)
			}
		})
	}
}

func TestMigrateLogsLongRunningStageBoundaries(t *testing.T) {
	var logs bytes.Buffer

	copier := &fakeCopier{}
	service, session, _, _ := appTestService(t, copier)

	service.config.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	if err := service.MigratePod(context.Background(), session); err != nil {
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

func TestStageTransitionRestoresStatusAfterPersistenceFailure(t *testing.T) {
	tests := []struct {
		name  string
		apply func(*Service, *domain.Session) error
	}{
		{name: "begin", apply: func(service *Service, session *domain.Session) error {
			return service.begin(
				context.Background(),
				session,
				domain.PhaseReserving,
				"reserving destination storage",
			)
		}},
		{name: "finish", apply: func(service *Service, session *domain.Session) error {
			return service.finish(
				context.Background(),
				session,
				domain.PhaseReserving,
				"reserving destination storage",
			)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer

			service, session, _, _ := appTestService(t, &fakeCopier{})
			service.store = &failingUpdateStore{}
			service.config.Logger = slog.New(
				slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}),
			)
			initialPhase := session.Status.Phase
			initialMessage := session.Status.Message
			initialHistoryLen := len(session.Status.History)

			if err := test.apply(service, session); err == nil {
				t.Fatal("expected persistence failure")
			}

			if session.Status.Phase != initialPhase || session.Status.Message != initialMessage ||
				len(session.Status.History) != initialHistoryLen {
				t.Fatalf("session status advanced after failed persistence: %#v", session.Status)
			}

			output := logs.String()
			if !strings.Contains(output, "level=ERROR") ||
				!strings.Contains(output, "migration stage persistence failed") ||
				strings.Contains(output, "migration stage started") ||
				strings.Contains(output, "migration stage completed") {
				t.Fatalf("logs=%q", output)
			}
		})
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

	if session.Status.Phase != domain.PhaseFailed ||
		session.Status.ResumeFrom != domain.PhaseWarmCopying {
		t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
	}

	if store.updateContextErr != nil || store.updates != 1 {
		t.Fatalf("checkpoint context err=%v updates=%d", store.updateContextErr, store.updates)
	}
}

func TestCopyConsumerPreflightSupportsOfflineAndOnlineBoundaries(t *testing.T) {
	service, session, _, _ := appTestService(t, &fakeCopier{})
	session.Spec = domain.NewSessionSpec(
		domain.OperationCopy,
		session.Spec.SessionCommon,

		false,
		domain.SessionWorkflowOptions{},
	)

	_, err := service.client.CoreV1().Pods("app").Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "writer"},
		Spec: corev1.PodSpec{
			NodeName: "source-node",
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "data",
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := service.validateCopyConsumers(
		context.Background(),
		session,
		&session.Spec.Volumes[0],
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("offline consumer category=%s error=%v", domain.CategoryOf(err), err)
	}

	session.Spec.Copy.Online = true

	session.Spec.WorkflowOptionsPtr().SourceNode = ""
	if err := service.validateCopyConsumers(
		context.Background(),
		session,
		&session.Spec.Volumes[0],
	); err != nil {
		t.Fatalf("online RWO consumer error=%v", err)
	}

	if session.Spec.WorkflowOptions().SourceNode != "" {
		t.Fatalf(
			"single-volume recheck mutated source node=%q",
			session.Spec.WorkflowOptions().SourceNode,
		)
	}

	if err := service.validateCopyConsumersBatch(context.Background(), session, true); err != nil {
		t.Fatalf("online RWO batch error=%v", err)
	}

	if session.Spec.WorkflowOptions().SourceNode != "source-node" {
		t.Fatalf("inferred source node=%q", session.Spec.WorkflowOptions().SourceNode)
	}

	pod, err := service.client.CoreV1().
		Pods("app").
		Get(context.Background(), "writer", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pod.Spec.NodeName = ""
	if _, err := service.client.CoreV1().
		Pods("app").
		Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	session.Spec.WorkflowOptionsPtr().SourceNode = ""
	if err := service.validateCopyConsumers(
		context.Background(),
		session,
		&session.Spec.Volumes[0],
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("unscheduled RWO category=%s error=%v", domain.CategoryOf(err), err)
	}

	session.Spec.Volumes[0].AccessModes = []corev1.PersistentVolumeAccessMode{
		corev1.ReadWriteOncePod,
	}
	if err := service.validateCopyConsumers(
		context.Background(),
		session,
		&session.Spec.Volumes[0],
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("online RWOP category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestFinalSyncFailureKeepsWorkloadPausedAndResumes(t *testing.T) {
	copier := &fakeCopier{failFinal: 1}
	service, session, controllers, _ := appTestService(t, copier)

	session.Spec.MigratePod.PrecopyPasses = 0
	if err := service.MigratePod(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorCopy {
		t.Fatalf("migration error=%v category=%s", err, domain.CategoryOf(err))
	}

	if session.Status.Phase != domain.PhaseFailed ||
		session.Status.ResumeFrom != domain.PhaseFinalSyncing {
		t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
	}

	if controllers.paused != 1 || controllers.resumed != 0 {
		t.Fatalf("controller calls pause=%d resume=%d", controllers.paused, controllers.resumed)
	}

	if err := service.resumeWorkflowForTest(context.Background(), session); err != nil {
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
	if err := service.PodPause(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Spec.Workload().Pod.ResourceVersion != "pause-resource-version" {
		t.Fatalf("pause mutation=%q", session.Spec.Workload().Pod.ResourceVersion)
	}

	if store.updates != 3 {
		t.Fatalf("session updates=%d want 3 (begin, recovery state, finish)", store.updates)
	}
}

func TestResumeFailurePersistsControllerRecoveryState(t *testing.T) {
	service, session, controllers, store := appTestService(t, &fakeCopier{})
	controllers.resumeMutation = true
	controllers.resumeErr = errors.New("replacement Pods did not become Ready")

	err := service.MigratePod(context.Background(), session)
	if !errors.Is(err, controllers.resumeErr) {
		t.Fatalf("Migrate() error=%v want=%v", err, controllers.resumeErr)
	}

	if session.Status.Phase != domain.PhaseFailed ||
		session.Status.ResumeFrom != domain.PhaseResuming {
		t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
	}

	if session.Spec.Workload().Pod.ResourceVersion != "resume-resource-version" {
		t.Fatalf("resume mutation=%q", session.Spec.Workload().Pod.ResourceVersion)
	}

	if store.updates == 0 {
		t.Fatal("resume failure did not persist the session")
	}
}

func TestRollbackRestoresSourceBindingAndResumes(t *testing.T) {
	copier := &fakeCopier{}
	service, session, controllers, _ := appTestService(t, copier)

	session.Spec.MigratePod.PrecopyPasses = 0
	if err := service.MigratePod(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if err := service.rollbackWorkflowForTest(context.Background(), session); err != nil {
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
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{
			{
				Name: "source",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data",
					},
				},
			},
		}},
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

func TestDeleteCopyToolPodsScopesOperationAndUsesUIDPreconditions(t *testing.T) {
	toolPod := func(name, instance string, uid types.UID, claim string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "app",
				Name:      name,
				UID:       uid,
				Labels: map[string]string{
					kube.AppInstanceLabel:  instance,
					kube.AppComponentLabel: kube.ToolComponentSSHD,
				},
			},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{
						Name: "volume",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: claim,
							},
						},
					},
				},
			},
		}
	}

	client := fake.NewClientset(
		toolPod("current", "pv-migrate-pm-current-clusterip", "current-uid", "data"),
		toolPod("foreign", "pv-migrate-pm-foreign-clusterip", "foreign-uid", "data"),
	)
	service := &Service{client: client}
	volume := &domain.VolumeSpec{
		SourcePVC:      domain.ObjectReference{Namespace: "app", Name: "data"},
		DestinationPVC: domain.ObjectReference{Namespace: "app", Name: "destination"},
	}

	if err := service.deleteCopyToolPods(context.Background(), volume, "pm-current"); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().Pods("app").Get(
		context.Background(),
		"current",
		metav1.GetOptions{},
	); !apierrors.IsNotFound(err) {
		t.Fatalf("current tool Pod error=%v, want NotFound", err)
	}

	if _, err := client.CoreV1().Pods("app").Get(
		context.Background(),
		"foreign",
		metav1.GetOptions{},
	); err != nil {
		t.Fatalf("foreign tool Pod was deleted: %v", err)
	}

	for _, action := range client.Actions() {
		if action.GetVerb() != "delete" || action.GetResource().Resource != "pods" {
			continue
		}

		options, ok := action.(interface {
			GetDeleteOptions() metav1.DeleteOptions
		})
		if !ok || options.GetDeleteOptions().Preconditions == nil ||
			options.GetDeleteOptions().Preconditions.UID == nil ||
			*options.GetDeleteOptions().Preconditions.UID != "current-uid" {
			t.Fatalf("delete action lacks current UID precondition: %#v", action)
		}
	}
}

func TestDeleteCopyToolPodsRunsAfterOperationCancellation(t *testing.T) {
	const (
		podName = "copy-tool"
		podUID  = types.UID("copy-tool-uid")
	)

	deleted := false
	postDeleteLists := 0
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")

		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/api/v1/namespaces/app/pods":
			pods := &corev1.PodList{}
			if deleted {
				postDeleteLists++
			} else {
				pods.Items = []corev1.Pod{{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "app",
						Name:      podName,
						UID:       podUID,
						Labels: map[string]string{
							kube.AppInstanceLabel:  "pv-migrate-pm-current-clusterip",
							kube.AppComponentLabel: kube.ToolComponentSSHD,
						},
					},
					Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "data",
							},
						},
					}}},
				}}
			}

			if err := json.NewEncoder(writer).Encode(pods); err != nil {
				t.Errorf("encode Pod list: %v", err)
			}
		case request.Method == http.MethodDelete &&
			request.URL.Path == "/api/v1/namespaces/app/pods/"+podName:
			var options metav1.DeleteOptions
			if err := json.NewDecoder(request.Body).Decode(&options); err != nil {
				t.Errorf("decode delete options: %v", err)
			}

			if options.Preconditions == nil || options.Preconditions.UID == nil ||
				*options.Preconditions.UID != podUID {
				t.Errorf("delete UID precondition=%#v", options.Preconditions)
			}

			deleted = true

			writer.WriteHeader(http.StatusOK)

			status := &metav1.Status{Status: metav1.StatusSuccess}
			if err := json.NewEncoder(writer).Encode(status); err != nil {
				t.Errorf("encode delete response: %v", err)
			}
		default:
			http.NotFound(writer, request)
		}
	})

	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := kubernetes.NewForConfig(&rest.Config{
		Host: server.URL,
		ContentConfig: rest.ContentConfig{
			ContentType: "application/json",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	service := &Service{client: client}
	volume := &domain.VolumeSpec{
		SourcePVC:      domain.ObjectReference{Namespace: "app", Name: "data"},
		DestinationPVC: domain.ObjectReference{Namespace: "app", Name: "destination"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := service.cleanupCopyToolPods(ctx, volume, "pm-current"); err != nil {
		t.Fatal(err)
	}

	if !deleted {
		t.Fatal("canceled operation did not issue copy tool Pod deletion")
	}

	if postDeleteLists == 0 {
		t.Fatal("cleanup did not wait for the deleted copy tool Pod to release the PVC")
	}
}

func TestWaitForCopyToolReleaseAllowsAsynchronousGarbageCollection(t *testing.T) {
	tool := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "copy-tool",
			Labels: map[string]string{
				kube.AppInstanceLabel:  "pv-migrate-pm-gc-clusterip",
				kube.AppComponentLabel: kube.ToolComponentSSHD,
			},
		},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{
			{
				Name: "volume",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data",
					},
				},
			},
		}},
	}
	client := fake.NewClientset(tool)
	service := &Service{client: client}
	volume := &domain.VolumeSpec{
		SourcePVC:      domain.ObjectReference{Namespace: "app", Name: "data"},
		DestinationPVC: domain.ObjectReference{Namespace: "app", Name: "destination"},
	}

	go func() {
		time.Sleep(50 * time.Millisecond)

		_ = client.CoreV1().Pods("app").Delete(
			context.Background(),
			tool.Name,
			metav1.DeleteOptions{},
		)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := service.waitForCopyToolRelease(ctx, volume); err != nil {
		t.Fatalf("waitForCopyToolRelease() error=%v", err)
	}
}

func TestDestinationNoSpaceErrorStopsRetries(t *testing.T) {
	if !isDestinationNoSpaceError(errors.New("rsync: write failed: No space left on device")) {
		t.Fatal("expected ENOSPC to be recognized")
	}

	if !isDestinationNoSpaceError(errors.New("exit status 23: ENOSPC")) {
		t.Fatal("expected ENOSPC token to be recognized")
	}

	if !isDestinationNoSpaceError(
		domain.WrapError(
			domain.ErrorCopy,
			"copy PVC",
			"pv-migrate operation failed",
			errors.New("rsync: No space left on device"),
		),
	) {
		t.Fatal("expected nested ENOSPC to be recognized")
	}

	if isDestinationNoSpaceError(errors.New("checksum mismatch")) {
		t.Fatal("unexpected capacity error")
	}
}

func TestToolLogErrorOnlyClassifiesFailedCopy(t *testing.T) {
	copyErr := errors.New("copy failed")

	if err := mergeToolLogError(nil, kube.ErrToolPodNoSpace); err != nil {
		t.Fatalf("successful copy was replaced by observed log error: %v", err)
	}

	err := mergeToolLogError(copyErr, kube.ErrToolPodNoSpace)
	if !errors.Is(err, copyErr) || !errors.Is(err, kube.ErrToolPodNoSpace) {
		t.Fatalf("merged error=%v", err)
	}
}

func TestCopyToolLogsRespectServiceMode(t *testing.T) {
	service := &Service{config: Config{StreamToolLogs: false}}
	volume := &domain.VolumeSpec{}

	if stream := service.startCopyToolLogs(context.Background(), volume, "copy"); stream != nil {
		t.Fatal("controller-style service unexpectedly started a tool log stream")
	}
}

func TestDestinationNoSpaceErrorRequiresNewSession(t *testing.T) {
	copier := &fakeCopier{err: errors.New("rsync: write failed: No space left on device")}
	service, session, _, _ := appTestService(t, copier)
	markTestSessionReserved(session)

	err := service.copyWithRetry(
		context.Background(),
		session,
		&session.Spec.Volumes[0],
		&session.Status.Volumes[0],
		copyengine.ModeWarm,
		nil,
	)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(
			err.Error(),
			"create a new session with a larger --destination-capacity",
		) {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if strings.Contains(err.Error(), "rerun this session") {
		t.Fatalf("error suggests reusing an immutable session: %v", err)
	}
}

func TestDestinationCapacityFailureCannotResume(t *testing.T) {
	service, session, _, _ := appTestService(t, &fakeCopier{})
	session.Status.Phase = domain.PhaseFailed
	session.Status.ResumeFrom = domain.PhaseWarmCopying
	session.Status.FailureReason = domain.FailureDestinationCapacityExhausted

	for _, validate := range []func(context.Context, *domain.Session) error{
		service.ValidateWarmCopy,
		service.validateResumeWorkflowForTest,
		service.resumeWorkflowForTest,
	} {
		err := validate(context.Background(), session)
		if domain.CategoryOf(err) != domain.ErrorConflict ||
			!strings.Contains(err.Error(), "larger --destination-capacity") {
			t.Fatalf("error=%v category=%s", err, domain.CategoryOf(err))
		}
	}
}

func TestFailContextRecordsDestinationCapacityFailure(t *testing.T) {
	service, session, _, _ := appTestService(t, &fakeCopier{})
	session.Status.Phase = domain.PhaseWarmCopying
	cause := domain.WrapError(
		domain.ErrorConflict,
		"copy capacity",
		"destination ran out of space",
		kube.ErrToolPodNoSpace,
	)

	err := service.failContext(context.Background(), session, cause)
	if !errors.Is(err, cause) {
		t.Fatalf("error=%v", err)
	}

	if session.Status.Phase != domain.PhaseFailed ||
		session.Status.FailureReason != domain.FailureDestinationCapacityExhausted {
		t.Fatalf("phase=%s failureReason=%s", session.Status.Phase, session.Status.FailureReason)
	}
}
