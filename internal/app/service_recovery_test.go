package app

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

type snapshotStore struct {
	creates       int
	updates       int
	deletes       int
	podUIDUpdates []types.UID
	updateErrAt   int
}

func (s *snapshotStore) Create(_ context.Context, session *domain.Session) error {
	s.creates++
	session.ResourceVersion = "1"
	return nil
}

func (s *snapshotStore) Get(context.Context, string, string) (*domain.Session, error) {
	return nil, errors.New("unused")
}

func (s *snapshotStore) Update(_ context.Context, session *domain.Session) error {
	s.updates++
	if s.updateErrAt == s.updates {
		return errors.New("injected session update failure")
	}
	s.podUIDUpdates = append(s.podUIDUpdates, session.Spec.Workload().Pod.UID)
	session.ResourceVersion = fmt.Sprint(s.updates + 1)
	return nil
}

func (s *snapshotStore) List(context.Context, string) ([]*domain.Session, error) { return nil, nil }

func (s *snapshotStore) Delete(context.Context, *domain.Session) error {
	s.deletes++
	return nil
}

type scriptedReserver struct {
	calls    []string
	failures map[string]error
}

func (r *scriptedReserver) ReserveVolume(_ context.Context, _ *domain.Session, volume *domain.VolumeSpec, status *domain.VolumeStatus, _ bool) error {
	r.calls = append(r.calls, volume.SourcePVC.Name)
	if err := r.failures[volume.SourcePVC.Name]; err != nil {
		return err
	}
	volume.DestinationPVC.UID = types.UID("dest-pvc-" + volume.SourcePVC.Name)
	volume.DestinationPV = domain.ObjectReference{Name: "dest-pv-" + volume.SourcePVC.Name, UID: types.UID("dest-pv-uid-" + volume.SourcePVC.Name)}
	volume.DestinationPolicy = corev1.PersistentVolumeReclaimDelete
	status.Reserved = true
	return nil
}

type scriptedController struct {
	pauses     int
	resumes    int
	verifies   int
	pauseErr   error
	resumeErr  error
	verifyErr  error
	resumeHook func(context.Context, *domain.Session) error
}

func (c *scriptedController) Pause(context.Context, *domain.Session) error {
	c.pauses++
	return c.pauseErr
}

func (c *scriptedController) Resume(ctx context.Context, session *domain.Session) error {
	c.resumes++
	if c.resumeHook != nil {
		return c.resumeHook(ctx, session)
	}
	return c.resumeErr
}

func (c *scriptedController) VerifyPaused(context.Context, *domain.Session) error {
	c.verifies++
	return c.verifyErr
}

type scriptedCopier struct {
	requests  []copyengine.Request
	failures  map[string]int
	failure   error
	copyError error
	copyHook  func()
}

type cleanupAwareCopier struct {
	client            kubernetes.Interface
	helperNamespace   string
	helperName        string
	calls             int
	secondSawHelper   bool
	firstAttemptEnded chan struct{}
}

func (c *cleanupAwareCopier) Copy(ctx context.Context, _ copyengine.Request, _ copyengine.ProgressFunc) error {
	c.calls++
	if c.calls == 1 {
		close(c.firstAttemptEnded)
		return domain.NewError(domain.ErrorCopy, "copy", "injected helper failure")
	}
	_, err := c.client.CoreV1().Pods(c.helperNamespace).Get(ctx, c.helperName, metav1.GetOptions{})
	c.secondSawHelper = err == nil
	return nil
}

func (c *scriptedCopier) Copy(_ context.Context, request copyengine.Request, _ copyengine.ProgressFunc) error {
	c.requests = append(c.requests, request)
	if c.copyHook != nil {
		c.copyHook()
	}
	key := string(request.Mode) + "/" + request.Source.Name
	if c.failures[key] > 0 {
		c.failures[key]--
		if c.failure != nil {
			return c.failure
		}
		return domain.NewError(domain.ErrorCopy, "copy", "injected copy failure")
	}
	return c.copyError
}

type scriptedSwitcher struct {
	offlineCalls  []string
	activateCalls []string
	rollbackCalls []string
	renameCalls   []domain.VolumeSpec
	offlineErr    error
	activateErr   error
	rollbackErr   map[string]int
	renameErr     error
}

func (s *scriptedSwitcher) VerifyVolumeOffline(_ context.Context, volume *domain.VolumeSpec) error {
	s.offlineCalls = append(s.offlineCalls, volume.SourcePVC.Name)
	return s.offlineErr
}

func (s *scriptedSwitcher) ActivateVolume(_ context.Context, _ *domain.Session, volume *domain.VolumeSpec, status *domain.VolumeStatus, progress kube.ProgressFunc) error {
	s.activateCalls = append(s.activateCalls, volume.SourcePVC.Name)
	if s.activateErr != nil {
		return s.activateErr
	}
	now := metav1.Now()
	status.Activation.ActivatedAt = &now
	status.Activation.ActivePVC = volume.SourcePVC
	if progress != nil {
		return progress()
	}
	return nil
}

func (s *scriptedSwitcher) RollbackVolume(_ context.Context, _ *domain.Session, volume *domain.VolumeSpec, status *domain.VolumeStatus, progress kube.ProgressFunc) error {
	s.rollbackCalls = append(s.rollbackCalls, volume.SourcePVC.Name)
	if s.rollbackErr[volume.SourcePVC.Name] > 0 {
		s.rollbackErr[volume.SourcePVC.Name]--
		return domain.NewError(domain.ErrorKubernetes, "rollback", "injected rollback failure")
	}
	now := metav1.Now()
	status.Activation.RolledBackAt = &now
	if progress != nil {
		return progress()
	}
	return nil
}

func (s *scriptedSwitcher) RenamePVC(_ context.Context, _ *domain.Session, volume *domain.VolumeSpec, progress kube.ProgressFunc) (*corev1.PersistentVolumeClaim, error) {
	s.renameCalls = append(s.renameCalls, *volume)
	if s.renameErr != nil {
		return nil, s.renameErr
	}
	if progress != nil {
		if err := progress(); err != nil {
			return nil, err
		}
	}
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace:       volume.DestinationPVC.Namespace,
		Name:            volume.DestinationPVC.Name,
		UID:             types.UID("renamed-pvc-uid"),
		ResourceVersion: "23",
	}}, nil
}

type recoveryFixture struct {
	service    *Service
	client     kubernetes.Interface
	store      *snapshotStore
	reserver   *scriptedReserver
	controller *scriptedController
	copier     *scriptedCopier
	switcher   *scriptedSwitcher
}

func setSessionOperation(session *domain.Session, operation domain.Operation) {
	common := session.Spec.SessionCommon
	workload := session.Spec.Workload()
	session.Spec = domain.NewSessionSpec(operation, common, workload, operation == domain.OperationCopy && session.Spec.Online(), session.Spec.WorkflowOptions())
}

func newRecoveryFixture(t *testing.T) *recoveryFixture {
	t.Helper()
	client := fake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "source-node", Labels: map[string]string{corev1.LabelHostname: "source-host"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "target-node", Labels: map[string]string{corev1.LabelHostname: "target-host"}}},
	)
	store := &snapshotStore{}
	reserver := &scriptedReserver{failures: map[string]error{}}
	controller := &scriptedController{}
	copier := &scriptedCopier{failures: map[string]int{}}
	switcher := &scriptedSwitcher{rollbackErr: map[string]int{}}
	service := NewService(client, store, reserver, copier, controller, switcher, Config{
		Retries:      1,
		RetryBackoff: time.Millisecond,
	})
	service.sleep = func(context.Context, time.Duration) error { return nil }
	return &recoveryFixture{
		service: service, client: client, store: store, reserver: reserver,
		controller: controller, copier: copier, switcher: switcher,
	}
}

func addSecondVolume(session *domain.Session) {
	second := session.Spec.Volumes[0]
	second.SourcePVC = domain.ObjectReference{Namespace: "app", Name: "logs", UID: types.UID("source-logs-uid")}
	second.SourcePV = domain.ObjectReference{Name: "pv-source-logs", UID: types.UID("source-pv-logs-uid")}
	second.DestinationPVC = domain.ObjectReference{Namespace: "system", Name: "logs-migrated"}
	second.DestinationPV = domain.ObjectReference{}
	session.Spec.Volumes = append(session.Spec.Volumes, second)
	session.Status.Volumes = append(session.Status.Volumes, domain.VolumeStatus{SourcePVCName: "logs"})
}

func transitionThrough(t *testing.T, session *domain.Session, phases ...domain.Phase) {
	t.Helper()
	for index, phase := range phases {
		if err := session.Transition(phase, "test transition", time.Unix(int64(200+index), 0)); err != nil {
			t.Fatalf("transition to %s: %v", phase, err)
		}
	}
}

func requestSources(requests []copyengine.Request) []string {
	names := make([]string, 0, len(requests))
	for _, request := range requests {
		names = append(names, request.Source.Name)
	}
	return names
}

func TestCreateSessionValidatesPlanAndCreatesDistinctNamespaces(t *testing.T) {
	fixture := newRecoveryFixture(t)
	if _, err := fixture.service.CreateSession(context.Background(), nil, false); domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("nil plan category=%s error=%v", domain.CategoryOf(err), err)
	}
	if _, err := fixture.service.CreateSession(context.Background(), &domain.MigrationPlan{}, false); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("unready plan category=%s error=%v", domain.CategoryOf(err), err)
	}

	session := appTestSession()
	session.Spec.TemporaryNamespace = "temporary"
	session.Spec.SessionNamespace = "sessions"
	session.Spec.Volumes[0].DestinationPVC.Namespace = "destination"
	plan := &domain.MigrationPlan{SessionID: session.ID, SessionSpec: session.Spec, Ready: true}
	created, err := fixture.service.CreateSession(context.Background(), plan, false)
	if err != nil {
		t.Fatal(err)
	}
	if created.Status.Phase != domain.PhasePlanned || fixture.store.creates != 1 {
		t.Fatalf("phase=%s creates=%d", created.Status.Phase, fixture.store.creates)
	}
	for _, namespace := range []string{"sessions", "temporary", "destination"} {
		if _, err := fixture.client.CoreV1().Namespaces().Get(context.Background(), namespace, metav1.GetOptions{}); err != nil {
			t.Fatalf("namespace %s: %v", namespace, err)
		}
	}
}

func TestCreateSessionCreatesSessionNamespaceBeforeLease(t *testing.T) {
	client := fake.NewClientset()
	store := kube.NewConfigMapSessionStore(client)
	reserver := &scriptedReserver{failures: map[string]error{}}
	controller := &scriptedController{}
	copier := &scriptedCopier{failures: map[string]int{}}
	switcher := &scriptedSwitcher{rollbackErr: map[string]int{}}
	service := NewService(client, store, reserver, copier, controller, switcher, Config{
		Retries:      1,
		RetryBackoff: time.Millisecond,
	})

	session := appTestSession()
	session.Spec.SessionNamespace = "sessions"
	session.Spec.TemporaryNamespace = "temporary"
	session.Spec.Volumes[0].DestinationPVC.Namespace = "destination"
	plan := &domain.MigrationPlan{SessionID: session.ID, SessionSpec: session.Spec, Ready: true}
	created, err := service.CreateSession(context.Background(), plan, false)
	if err != nil {
		t.Fatal(err)
	}
	if created.Status.Phase != domain.PhasePlanned {
		t.Fatalf("phase=%s want=%s", created.Status.Phase, domain.PhasePlanned)
	}
	for _, namespace := range []string{"sessions", "temporary", "destination"} {
		if _, err := client.CoreV1().Namespaces().Get(context.Background(), namespace, metav1.GetOptions{}); err != nil {
			t.Fatalf("namespace %s: %v", namespace, err)
		}
	}
	if _, err := client.CoreV1().ConfigMaps("sessions").Get(context.Background(), kube.SessionConfigMapName(session.ID), metav1.GetOptions{}); err != nil {
		t.Fatalf("session ConfigMap: %v", err)
	}
	if _, err := client.CoordinationV1().Leases("sessions").Get(context.Background(), kube.SessionLockName(session.ID), metav1.GetOptions{}); err != nil {
		t.Fatalf("session Lease: %v", err)
	}
}

func TestCreateSessionDryRunValidatesEveryReservationWithoutPersistingState(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)
	plan := &domain.MigrationPlan{SessionID: session.ID, SessionSpec: session.Spec, Ready: true}

	created, err := fixture.service.CreateSession(context.Background(), plan, true)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"data", "logs"}; !slices.Equal(fixture.reserver.calls, want) {
		t.Fatalf("reservation validations=%v want=%v", fixture.reserver.calls, want)
	}
	if fixture.store.creates != 0 || created.Status.Volumes[0].Reserved || created.Spec.Volumes[0].DestinationPV.Name != "" {
		t.Fatalf("dry-run persisted or mutated session: creates=%d session=%+v", fixture.store.creates, created)
	}
}

func TestReserveResumesMultiVolumePartialProgress(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)
	session.Status.Volumes[0].Reserved = true
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{Name: "existing-destination", UID: types.UID("existing-destination-uid")}
	fixture.reserver.failures["logs"] = domain.NewError(domain.ErrorKubernetes, "reserve", "injected reservation failure")

	err := fixture.service.Reserve(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	if session.Status.Phase != domain.PhaseFailed || session.Status.ResumeFrom != domain.PhaseReserving {
		t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
	}
	delete(fixture.reserver.failures, "logs")
	if err := fixture.service.Reserve(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if session.Status.Phase != domain.PhaseReserved || !session.Status.Volumes[1].Reserved {
		t.Fatalf("phase=%s second reserved=%v", session.Status.Phase, session.Status.Volumes[1].Reserved)
	}
	if want := []string{"logs", "logs"}; !slices.Equal(fixture.reserver.calls, want) {
		t.Fatalf("reservation calls=%v want=%v", fixture.reserver.calls, want)
	}
	if err := fixture.service.Reserve(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if len(fixture.reserver.calls) != 2 {
		t.Fatalf("completed reserve repeated work: calls=%v", fixture.reserver.calls)
	}
}

func TestWarmCopyResumesPartialVolumeAndSupportsAnotherPass(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)
	transitionThrough(t, session, domain.PhaseReserving, domain.PhaseReserved, domain.PhaseWarmCopying)
	completed := metav1.NewTime(time.Unix(300, 0))
	session.Status.Volumes[0].Sync.WarmCompletedAt = &completed

	if err := fixture.service.WarmCopy(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if want := []string{"logs"}; !slices.Equal(requestSources(fixture.copier.requests), want) {
		t.Fatalf("first recovery pass sources=%v want=%v", requestSources(fixture.copier.requests), want)
	}
	if err := fixture.service.WarmCopy(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if want := []string{"logs", "data", "logs"}; !slices.Equal(requestSources(fixture.copier.requests), want) {
		t.Fatalf("copy sources=%v want=%v", requestSources(fixture.copier.requests), want)
	}
	for index := range session.Status.Volumes {
		if session.Status.Volumes[index].Sync.WarmCompletedAt == nil {
			t.Fatalf("volume %d has no warm completion", index)
		}
	}
}

func TestCopyRetriesPersistAttemptsAndUseExponentialBackoff(t *testing.T) {
	fixture := newRecoveryFixture(t)
	fixture.service.config.Retries = 3
	fixture.service.config.RetryBackoff = 5 * time.Millisecond
	var delays []time.Duration
	fixture.service.sleep = func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		return nil
	}
	fixture.copier.failures["warm/data"] = 2
	session := appTestSession()
	transitionThrough(t, session, domain.PhaseReserving, domain.PhaseReserved)

	if err := fixture.service.WarmCopy(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if session.Status.Volumes[0].Sync.Attempts != 3 || session.Status.Volumes[0].Sync.LastError != "" {
		t.Fatalf("sync state=%+v", session.Status.Volumes[0].Sync)
	}
	if want := []time.Duration{5 * time.Millisecond, 10 * time.Millisecond}; !slices.Equal(delays, want) {
		t.Fatalf("retry delays=%v want=%v", delays, want)
	}
	for index, request := range fixture.copier.requests {
		if request.Attempt != index+1 || request.Mode != copyengine.ModeWarm {
			t.Fatalf("request %d attempt=%d mode=%s", index, request.Attempt, request.Mode)
		}
	}
}

func TestCopyFailureWaitsForHelperReleaseBeforeRetry(t *testing.T) {
	fixture := newRecoveryFixture(t)
	helper := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "pv-migrate-helper",
			Labels: map[string]string{
				"app.kubernetes.io/instance":  "pv-migrate-pm-test-clusterip",
				"app.kubernetes.io/component": "sshd",
			},
		},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
			Name: "source",
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: "data",
			}},
		}}},
	}
	if _, err := fixture.client.CoreV1().Pods(helper.Namespace).Create(context.Background(), helper, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	copier := &cleanupAwareCopier{
		client: fixture.client, helperNamespace: helper.Namespace, helperName: helper.Name,
		firstAttemptEnded: make(chan struct{}),
	}
	fixture.service.copier = copier
	fixture.service.config.Retries = 2
	go func() {
		<-copier.firstAttemptEnded
		time.Sleep(10 * time.Millisecond)
		_ = fixture.client.CoreV1().Pods(helper.Namespace).Delete(context.Background(), helper.Name, metav1.DeleteOptions{})
	}()
	session := appTestSession()
	transitionThrough(t, session, domain.PhaseReserving, domain.PhaseReserved)

	if err := fixture.service.WarmCopy(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if copier.calls != 2 || copier.secondSawHelper {
		t.Fatalf("copy calls=%d secondSawHelper=%t", copier.calls, copier.secondSawHelper)
	}
}

func TestCopyRetryCancellationLeavesRecoverableFailure(t *testing.T) {
	fixture := newRecoveryFixture(t)
	fixture.service.config.Retries = 3
	fixture.copier.failures["warm/data"] = 3
	fixture.service.sleep = func(context.Context, time.Duration) error { return context.Canceled }
	session := appTestSession()
	transitionThrough(t, session, domain.PhaseReserving, domain.PhaseReserved)

	err := fixture.service.WarmCopy(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorTimeout {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	if session.Status.Phase != domain.PhaseFailed || session.Status.ResumeFrom != domain.PhaseWarmCopying {
		t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
	}
	if session.Status.Volumes[0].Sync.Attempts != 1 || len(fixture.copier.requests) != 1 {
		t.Fatalf("attempts=%d requests=%d", session.Status.Volumes[0].Sync.Attempts, len(fixture.copier.requests))
	}
}

func TestCopyFailureAfterContextCancellationPreservesRootCause(t *testing.T) {
	fixture := newRecoveryFixture(t)
	cause := domain.NewError(domain.ErrorCopy, "copy", "helper deadline exceeded")
	ctx, cancel := context.WithCancel(context.Background())
	fixture.copier.copyError = cause
	fixture.copier.copyHook = cancel
	session := appTestSession()
	transitionThrough(t, session, domain.PhaseReserving, domain.PhaseReserved)

	err := fixture.service.WarmCopy(ctx, session)
	if !errors.Is(err, cause) {
		t.Fatalf("error=%v want root cause %v", err, cause)
	}
	if session.Status.Phase != domain.PhaseFailed || session.Status.ResumeFrom != domain.PhaseWarmCopying {
		t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
	}
	if !strings.Contains(session.Status.Volumes[0].Sync.LastError, cause.Error()) {
		t.Fatalf("lastError=%q want %q", session.Status.Volumes[0].Sync.LastError, cause.Error())
	}
}

func TestPauseIdempotencyVerifiesWorkloadState(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	transitionThrough(t, session, domain.PhaseReserving, domain.PhaseReserved, domain.PhasePausing, domain.PhasePaused)

	if err := fixture.service.Pause(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if fixture.controller.pauses != 0 || fixture.controller.verifies != 1 {
		t.Fatalf("pause calls=%d verify calls=%d", fixture.controller.pauses, fixture.controller.verifies)
	}
	fixture.controller.verifyErr = domain.NewError(domain.ErrorPrecondition, "verify", "workload is running")
	if err := fixture.service.Pause(context.Background(), session); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestAbortResumesWorkloadsThatReachedOrMayHavePartiallyEnteredPause(t *testing.T) {
	tests := []struct {
		name        string
		phase       domain.Phase
		resumeFrom  domain.Phase
		verifyErr   error
		wantResumes int
	}{
		{name: "reserved", phase: domain.PhaseReserved},
		{name: "paused", phase: domain.PhasePaused, wantResumes: 1},
		{name: "final syncing failure", phase: domain.PhaseFailed, resumeFrom: domain.PhaseFinalSyncing, wantResumes: 1},
		{name: "pausing with deleted pod", phase: domain.PhasePausing, wantResumes: 1},
		{name: "pausing with live pod", phase: domain.PhasePausing, verifyErr: errors.New("still running"), wantResumes: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			fixture.controller.verifyErr = test.verifyErr
			session := appTestSession()
			session.Status.Phase = test.phase
			session.Status.ResumeFrom = test.resumeFrom
			if err := fixture.service.Abort(context.Background(), session); err != nil {
				t.Fatal(err)
			}
			if session.Status.Phase != domain.PhaseAborted || fixture.controller.resumes != test.wantResumes {
				t.Fatalf("phase=%s resumes=%d want=%d", session.Status.Phase, fixture.controller.resumes, test.wantResumes)
			}
			if err := fixture.service.Abort(context.Background(), session); err != nil {
				t.Fatal(err)
			}
			if fixture.controller.resumes != test.wantResumes {
				t.Fatalf("idempotent abort resumes=%d want=%d", fixture.controller.resumes, test.wantResumes)
			}
		})
	}
}

func TestFailReturnsSessionPersistenceFailureWithOriginalCause(t *testing.T) {
	fixture := newRecoveryFixture(t)
	fixture.store.updateErrAt = 1
	session := appTestSession()
	cause := domain.NewError(domain.ErrorCopy, "copy", "injected copy failure")

	err := fixture.service.fail(context.Background(), session, cause)
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), "injected session update failure") {
		t.Fatalf("failure=%v", err)
	}
	if domain.CategoryOf(err) != domain.ErrorCopy || session.Status.Phase != domain.PhaseFailed {
		t.Fatalf("category=%s phase=%s", domain.CategoryOf(err), session.Status.Phase)
	}
}

func TestAbortRejectsCutoverSessions(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhaseActivated, domain.PhaseCompleted} {
		t.Run(string(phase), func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			session := appTestSession()
			session.Status.Phase = phase
			err := fixture.service.Abort(context.Background(), session)
			if domain.CategoryOf(err) != domain.ErrorPrecondition {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func TestAbortRejectsRollbackRecoveryChain(t *testing.T) {
	tests := []struct {
		name       string
		phase      domain.Phase
		resumeFrom domain.Phase
	}{
		{name: "rolling back", phase: domain.PhaseRollingBack},
		{name: "failed rollback", phase: domain.PhaseFailed, resumeFrom: domain.PhaseRollingBack},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			session := appTestSession()
			session.Status.Phase = test.phase
			session.Status.ResumeFrom = test.resumeFrom

			err := fixture.service.Abort(context.Background(), session)
			if domain.CategoryOf(err) != domain.ErrorPrecondition {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
			if fixture.controller.resumes != 0 || session.Status.Phase != test.phase {
				t.Fatalf("resumes=%d phase=%s", fixture.controller.resumes, session.Status.Phase)
			}
		})
	}
}

func TestActivateResumesAtFirstIncompleteVolume(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)
	session.Status.Phase = domain.PhaseFinalSynced
	completed := metav1.NewTime(time.Unix(400, 0))
	session.Status.Volumes[0].Activation.ActivatedAt = &completed

	if err := fixture.service.Activate(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if want := []string{"logs"}; !slices.Equal(fixture.switcher.activateCalls, want) {
		t.Fatalf("activation calls=%v want=%v", fixture.switcher.activateCalls, want)
	}
	if session.Status.Phase != domain.PhaseActivated || session.Status.Volumes[1].Activation.ActivatedAt == nil {
		t.Fatalf("phase=%s second activation=%+v", session.Status.Phase, session.Status.Volumes[1].Activation)
	}
}

func TestActivateIsIdempotentAfterCutover(t *testing.T) {
	tests := []struct {
		name       string
		phase      domain.Phase
		resumeFrom domain.Phase
	}{
		{name: "activated", phase: domain.PhaseActivated},
		{name: "resuming", phase: domain.PhaseResuming},
		{name: "completed", phase: domain.PhaseCompleted},
		{name: "failed resume", phase: domain.PhaseFailed, resumeFrom: domain.PhaseResuming},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			session := appTestSession()
			session.Status.Phase = test.phase
			session.Status.ResumeFrom = test.resumeFrom

			if err := fixture.service.Activate(context.Background(), session); err != nil {
				t.Fatal(err)
			}
			if len(fixture.switcher.activateCalls) != 0 || session.Status.Phase != test.phase {
				t.Fatalf("activate calls=%v phase=%s", fixture.switcher.activateCalls, session.Status.Phase)
			}
		})
	}
}

func TestResumeWorkloadPersistsRecreatedStandalonePodUID(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseActivated
	_ = session.Spec.SetWorkload(domain.WorkloadSpec{
		Adapter: domain.WorkloadStandalone,
		Pod:     domain.ObjectReference{Namespace: "app", Name: "application", UID: types.UID("old-pod-uid")},
	})
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{Name: "pv-destination", UID: types.UID("destination-pv-uid")}
	_, err := fixture.client.CoreV1().PersistentVolumeClaims("app").Create(context.Background(), &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid"),
			Annotations: map[string]string{kube.SessionAnnotation: session.ID},
		},
		Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fixture.controller.resumeHook = func(ctx context.Context, current *domain.Session) error {
		current.Spec.WorkloadPtr().Pod.UID = types.UID("new-pod-uid")
		current.Spec.WorkloadPtr().Pod.ResourceVersion = "44"
		_, createErr := fixture.client.CoreV1().Pods("app").Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "application", UID: types.UID("new-pod-uid")},
			Spec:       corev1.PodSpec{NodeName: "target-node"},
		}, metav1.CreateOptions{})
		return createErr
	}

	if err := fixture.service.ResumeWorkload(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if session.Status.Phase != domain.PhaseCompleted || session.Spec.Workload().Pod.UID != types.UID("new-pod-uid") {
		t.Fatalf("phase=%s pod=%+v", session.Status.Phase, session.Spec.Workload().Pod)
	}
	if got := fixture.store.podUIDUpdates[len(fixture.store.podUIDUpdates)-1]; got != types.UID("new-pod-uid") {
		t.Fatalf("last persisted Pod UID=%s", got)
	}
}

func TestRollbackMultiVolumeRunsInReverseAndRecoversFailure(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)
	session.Status.Phase = domain.PhaseCompleted
	session.Status.History = append(session.Status.History, domain.HistoryEntry{Phase: domain.PhaseCompleted, Time: metav1.Now()})
	fixture.switcher.rollbackErr["data"] = 1

	err := fixture.service.Rollback(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	if session.Status.Phase != domain.PhaseFailed || session.Status.ResumeFrom != domain.PhaseRollingBack {
		t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
	}
	if err := fixture.service.ResumeSession(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if session.Status.Phase != domain.PhaseRolledBack {
		t.Fatalf("phase=%s", session.Status.Phase)
	}
	if want := []string{"logs", "data", "logs", "data"}; !slices.Equal(fixture.switcher.rollbackCalls, want) {
		t.Fatalf("rollback calls=%v want=%v", fixture.switcher.rollbackCalls, want)
	}
	if fixture.controller.pauses != 2 || fixture.controller.resumes != 1 {
		t.Fatalf("pause calls=%d resume calls=%d", fixture.controller.pauses, fixture.controller.resumes)
	}
	before := len(fixture.switcher.rollbackCalls)
	if err := fixture.service.Rollback(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if len(fixture.switcher.rollbackCalls) != before {
		t.Fatalf("rolled-back session repeated switch calls=%v", fixture.switcher.rollbackCalls)
	}
}

func TestRollbackRejectsFailuresBeforeCutover(t *testing.T) {
	for _, resumeFrom := range []domain.Phase{domain.PhaseReserving, domain.PhaseWarmCopying, domain.PhasePausing, domain.PhaseFinalSyncing} {
		t.Run(string(resumeFrom), func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			session := appTestSession()
			session.Status.Phase = domain.PhaseFailed
			session.Status.ResumeFrom = resumeFrom

			err := fixture.service.Rollback(context.Background(), session)
			if domain.CategoryOf(err) != domain.ErrorPrecondition {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
			if len(fixture.switcher.rollbackCalls) != 0 || fixture.controller.pauses != 0 {
				t.Fatalf("rollback calls=%v pauses=%d", fixture.switcher.rollbackCalls, fixture.controller.pauses)
			}
		})
	}
}

func TestRenameAndRollbackPreservePVCIdentityDirection(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	setSessionOperation(session, domain.OperationRename)
	session.Spec.Volumes[0].DestinationPVC = domain.ObjectReference{Namespace: "app", Name: "renamed-data"}

	if err := fixture.service.Rename(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if session.Status.Phase != domain.PhaseCompleted || session.Status.Volumes[0].Activation.ActivePVC.UID != types.UID("renamed-pvc-uid") {
		t.Fatalf("phase=%s activation=%+v", session.Status.Phase, session.Status.Volumes[0].Activation)
	}
	if err := fixture.service.Rollback(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if session.Status.Phase != domain.PhaseRolledBack || len(fixture.switcher.renameCalls) != 2 {
		t.Fatalf("phase=%s rename calls=%d", session.Status.Phase, len(fixture.switcher.renameCalls))
	}
	reverse := fixture.switcher.renameCalls[1]
	if reverse.SourcePVC.Name != "renamed-data" || reverse.SourcePVC.UID != types.UID("renamed-pvc-uid") {
		t.Fatalf("rollback source=%+v", reverse.SourcePVC)
	}
	if reverse.DestinationPVC.Name != "data" || reverse.DestinationPVC.UID != "" || reverse.DestinationPVC.ResourceVersion != "" {
		t.Fatalf("rollback destination=%+v", reverse.DestinationPVC)
	}
	if session.Status.Volumes[0].Activation.RolledBackAt == nil || session.Status.Volumes[0].Activation.ActivePVC.Name != "data" {
		t.Fatalf("rollback activation=%+v", session.Status.Volumes[0].Activation)
	}
}

func TestDryRunRenameRollbackChecksCurrentActivePVC(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	setSessionOperation(session, domain.OperationRename)
	session.Spec.Volumes[0].DestinationPVC = domain.ObjectReference{Namespace: "app", Name: "renamed-data"}
	session.Status.Phase = domain.PhaseCompleted
	session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
		APIVersion: "v1",
		Kind:       "PersistentVolumeClaim",
		Namespace:  "app",
		Name:       "renamed-data",
		UID:        types.UID("renamed-pvc-uid"),
	}
	if err := fixture.service.ValidateRollback(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if len(fixture.switcher.offlineCalls) != 1 || fixture.switcher.offlineCalls[0] != "renamed-data" {
		t.Fatalf("offline calls=%v", fixture.switcher.offlineCalls)
	}
}

func TestPauseRejectsNonOrchestratedSessionBeforePhaseMutation(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	setSessionOperation(session, domain.OperationCopy)
	session.Status.Phase = domain.PhaseWarmCopied
	historyBefore := len(session.Status.History)
	if err := fixture.service.Pause(context.Background(), session); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	if session.Status.Phase != domain.PhaseWarmCopied || len(session.Status.History) != historyBefore || fixture.controller.pauses != 0 {
		t.Fatalf("phase=%s history=%v pauses=%d", session.Status.Phase, session.Status.History, fixture.controller.pauses)
	}
}

func TestStagePreconditionsPreserveSessionState(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Service, *domain.Session) error
	}{
		{name: "negative warm passes", run: func(service *Service, session *domain.Session) error {
			return service.Migrate(context.Background(), session, -1)
		}},
		{name: "warm copy from planned", run: func(service *Service, session *domain.Session) error {
			return service.WarmCopy(context.Background(), session)
		}},
		{name: "final sync from planned", run: func(service *Service, session *domain.Session) error {
			return service.FinalSync(context.Background(), session)
		}},
		{name: "activate from planned", run: func(service *Service, session *domain.Session) error {
			return service.Activate(context.Background(), session)
		}},
		{name: "resume workload from planned", run: func(service *Service, session *domain.Session) error {
			return service.ResumeWorkload(context.Background(), session)
		}},
		{name: "rollback from planned", run: func(service *Service, session *domain.Session) error {
			return service.Rollback(context.Background(), session)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			session := appTestSession()
			err := test.run(fixture.service, session)
			if category := domain.CategoryOf(err); category != domain.ErrorPrecondition && category != domain.ErrorValidation {
				t.Fatalf("category=%s error=%v", category, err)
			}
			if session.Status.Phase != domain.PhasePlanned || fixture.store.updates != 0 {
				t.Fatalf("phase=%s updates=%d", session.Status.Phase, fixture.store.updates)
			}
		})
	}
}

func TestResumeSessionCompletesEveryCompositeMigrationStage(t *testing.T) {
	type setupFunc func(*testing.T, *Service, *domain.Session)
	reserve := func(t *testing.T, service *Service, session *domain.Session) {
		t.Helper()
		if err := service.Reserve(context.Background(), session); err != nil {
			t.Fatal(err)
		}
	}
	pause := func(t *testing.T, service *Service, session *domain.Session) {
		t.Helper()
		reserve(t, service, session)
		if err := service.Pause(context.Background(), session); err != nil {
			t.Fatal(err)
		}
	}
	finalSync := func(t *testing.T, service *Service, session *domain.Session) {
		t.Helper()
		pause(t, service, session)
		if err := service.FinalSync(context.Background(), session); err != nil {
			t.Fatal(err)
		}
	}
	activate := func(t *testing.T, service *Service, session *domain.Session) {
		t.Helper()
		finalSync(t, service, session)
		if err := service.Activate(context.Background(), session); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name  string
		setup setupFunc
	}{
		{name: "planned"},
		{name: "reserving", setup: func(t *testing.T, _ *Service, session *domain.Session) {
			transitionThrough(t, session, domain.PhaseReserving)
		}},
		{name: "reserved", setup: reserve},
		{name: "warm copying", setup: func(t *testing.T, service *Service, session *domain.Session) {
			reserve(t, service, session)
			transitionThrough(t, session, domain.PhaseWarmCopying)
		}},
		{name: "warm copied", setup: func(t *testing.T, service *Service, session *domain.Session) {
			reserve(t, service, session)
			if err := service.WarmCopy(context.Background(), session); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "pausing", setup: func(t *testing.T, service *Service, session *domain.Session) {
			reserve(t, service, session)
			transitionThrough(t, session, domain.PhasePausing)
		}},
		{name: "paused", setup: pause},
		{name: "final syncing", setup: func(t *testing.T, service *Service, session *domain.Session) {
			pause(t, service, session)
			transitionThrough(t, session, domain.PhaseFinalSyncing)
		}},
		{name: "final synced", setup: finalSync},
		{name: "activating", setup: func(t *testing.T, service *Service, session *domain.Session) {
			finalSync(t, service, session)
			transitionThrough(t, session, domain.PhaseActivating)
		}},
		{name: "activated", setup: activate},
		{name: "resuming", setup: func(t *testing.T, service *Service, session *domain.Session) {
			activate(t, service, session)
			transitionThrough(t, session, domain.PhaseResuming)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, session, _, _ := appTestService(t, &fakeCopier{})
			if test.setup != nil {
				test.setup(t, service, session)
			}
			if err := service.ResumeSession(context.Background(), session); err != nil {
				t.Fatal(err)
			}
			if session.Status.Phase != domain.PhaseCompleted {
				t.Fatalf("phase=%s", session.Status.Phase)
			}
		})
	}
}

func TestResumeSessionDispatchesSingleOperationStages(t *testing.T) {
	tests := []struct {
		name      string
		operation domain.Operation
		phase     domain.Phase
		want      domain.Phase
	}{
		{name: "copy", operation: domain.OperationCopy, phase: domain.PhaseWarmCopying, want: domain.PhaseWarmCopied},
		{name: "rename", operation: domain.OperationRename, phase: domain.PhaseRenaming, want: domain.PhaseCompleted},
		{name: "move", operation: domain.OperationMove, phase: domain.PhaseMoving, want: domain.PhaseCompleted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			session := appTestSession()
			setSessionOperation(session, test.operation)
			session.Status.Phase = test.phase
			if err := fixture.service.ResumeSession(context.Background(), session); err != nil {
				t.Fatal(err)
			}
			if session.Status.Phase != test.want {
				t.Fatalf("phase=%s want=%s", session.Status.Phase, test.want)
			}
		})
	}
}

func TestResumeSessionDispatchesSingleOperationFirstStages(t *testing.T) {
	tests := []struct {
		name      string
		operation domain.Operation
		phase     domain.Phase
		want      domain.Phase
	}{
		{name: "reserve from planned", operation: domain.OperationReserve, phase: domain.PhasePlanned, want: domain.PhaseReserved},
		{name: "copy from reserved", operation: domain.OperationCopy, phase: domain.PhaseReserved, want: domain.PhaseWarmCopied},
		{name: "rename from planned", operation: domain.OperationRename, phase: domain.PhasePlanned, want: domain.PhaseCompleted},
		{name: "move from planned", operation: domain.OperationMove, phase: domain.PhasePlanned, want: domain.PhaseCompleted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			session := appTestSession()
			setSessionOperation(session, test.operation)
			session.Status.Phase = test.phase
			if err := fixture.service.ResumeSession(context.Background(), session); err != nil {
				t.Fatal(err)
			}
			if session.Status.Phase != test.want {
				t.Fatalf("phase=%s want=%s", session.Status.Phase, test.want)
			}
		})
	}

	fixture := newRecoveryFixture(t)
	session := appTestSession()
	setSessionOperation(session, domain.OperationReserve)
	session.Status.Phase = domain.Phase("Unknown")
	if err := fixture.service.ResumeSession(context.Background(), session); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("unknown phase category=%s error=%v", domain.CategoryOf(err), err)
	}

	fixture = newRecoveryFixture(t)
	session = appTestSession()
	setSessionOperation(session, domain.OperationRename)
	session.Status.Phase = domain.PhaseReserved
	if err := fixture.service.ResumeSession(context.Background(), session); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("mismatched phase category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestValidateResumeUsesOperationSpecificChecks(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	setSessionOperation(session, domain.OperationCopy)
	session.Status.Phase = domain.PhaseReserved
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "consumer"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
		}}}},
	}
	if _, err := fixture.client.CoreV1().Pods("app").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.ValidateResume(context.Background(), session); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("copy consumer category=%s error=%v", domain.CategoryOf(err), err)
	}

	fixture = newRecoveryFixture(t)
	session = appTestSession()
	setSessionOperation(session, domain.OperationRename)
	session.Status.Phase = domain.PhasePlanned
	fixture.switcher.offlineErr = domain.NewError(domain.ErrorPrecondition, "verify PVC offline", "source PVC has an active consumer")
	if err := fixture.service.ValidateResume(context.Background(), session); domain.CategoryOf(err) != domain.ErrorPrecondition || len(fixture.switcher.offlineCalls) != 1 {
		t.Fatalf("rename offline category=%s calls=%v error=%v", domain.CategoryOf(err), fixture.switcher.offlineCalls, err)
	}
}

func TestResumeSessionContinuesActivatedSingleOperationSession(t *testing.T) {
	service, session, _, _ := appTestService(t, &fakeCopier{})
	if err := service.Reserve(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if err := service.Pause(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if err := service.FinalSync(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if err := service.Activate(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if err := service.Activate(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if session.Status.Phase != domain.PhaseActivated {
		t.Fatalf("phase=%s", session.Status.Phase)
	}
}

func TestResumeSessionHandlesRecoveryAndTerminalPhases(t *testing.T) {
	t.Run("aborting", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		session.Status.Phase = domain.PhaseAborting
		session.Status.ResumeFrom = domain.PhaseReserved
		if err := fixture.service.ResumeSession(context.Background(), session); err != nil {
			t.Fatal(err)
		}
		if session.Status.Phase != domain.PhaseAborted {
			t.Fatalf("phase=%s", session.Status.Phase)
		}
	})

	t.Run("rolling back", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		session.Status.Phase = domain.PhaseRollingBack
		session.Status.ResumeFrom = domain.PhaseCompleted
		if err := fixture.service.ResumeSession(context.Background(), session); err != nil {
			t.Fatal(err)
		}
		if session.Status.Phase != domain.PhaseRolledBack {
			t.Fatalf("phase=%s", session.Status.Phase)
		}
	})

	for _, phase := range []domain.Phase{domain.PhaseCompleted, domain.PhaseAborted, domain.PhaseRolledBack} {
		t.Run(string(phase), func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			session := appTestSession()
			session.Status.Phase = phase
			if err := fixture.service.ResumeSession(context.Background(), session); err != nil {
				t.Fatal(err)
			}
			if fixture.store.updates != 0 || len(fixture.copier.requests) != 0 {
				t.Fatalf("terminal resume updates=%d copy requests=%d", fixture.store.updates, len(fixture.copier.requests))
			}
		})
	}

	t.Run("unknown phase", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		session.Status.Phase = domain.Phase("Unknown")
		err := fixture.service.ResumeSession(context.Background(), session)
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
}

func TestAbortRetryAfterResumeFailureStillResumesPausedWorkload(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseFailed
	session.Status.ResumeFrom = domain.PhaseAborting
	session.Status.History = []domain.HistoryEntry{
		{Phase: domain.PhasePlanned, Time: metav1.Now()},
		{Phase: domain.PhaseWarmCopied, Time: metav1.Now()},
		{Phase: domain.PhaseFinalSyncing, Time: metav1.Now()},
		{Phase: domain.PhaseFailed, Time: metav1.Now()},
		{Phase: domain.PhaseAborting, Time: metav1.Now()},
		{Phase: domain.PhaseFailed, Time: metav1.Now()},
	}
	if err := fixture.service.ResumeSession(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if session.Status.Phase != domain.PhaseAborted || fixture.controller.resumes != 1 {
		t.Fatalf("phase=%s resumes=%d", session.Status.Phase, fixture.controller.resumes)
	}
}

func TestFinalSyncResumesPartialVolumeAndRepeatsChecksumPass(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)
	session.Spec.WorkflowOptionsPtr().VerifyChecksum = true
	session.Status.Phase = domain.PhaseFinalSyncing
	completed := metav1.NewTime(time.Unix(500, 0))
	session.Status.Volumes[0].Sync.FinalCompletedAt = &completed

	if err := fixture.service.FinalSync(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if want := []string{"logs"}; !slices.Equal(requestSources(fixture.copier.requests), want) {
		t.Fatalf("recovery sources=%v want=%v", requestSources(fixture.copier.requests), want)
	}
	if err := fixture.service.FinalSync(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if want := []string{"logs", "data", "logs"}; !slices.Equal(requestSources(fixture.copier.requests), want) {
		t.Fatalf("final-sync sources=%v want=%v", requestSources(fixture.copier.requests), want)
	}
	for index, request := range fixture.copier.requests {
		if request.Mode != copyengine.ModeFinal || !request.VerifyChecksum {
			t.Fatalf("request %d mode=%s checksum=%v", index, request.Mode, request.VerifyChecksum)
		}
	}
	for index := range session.Status.Volumes {
		if session.Status.Volumes[index].Sync.FinalCompletedAt == nil || !session.Status.Volumes[index].Sync.ChecksumVerified {
			t.Fatalf("volume %d sync=%+v", index, session.Status.Volumes[index].Sync)
		}
	}
}

func TestHelmSchedulingValuesIncludeNodeTolerations(t *testing.T) {
	fixture := newRecoveryFixture(t)
	source, err := fixture.client.CoreV1().Nodes().Get(context.Background(), "source-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	source.Spec.Taints = []corev1.Taint{
		{Key: "dedicated", Value: "storage", Effect: corev1.TaintEffectNoSchedule},
		{Key: "draining", Effect: corev1.TaintEffectNoExecute},
		{Key: "preference", Effect: corev1.TaintEffectPreferNoSchedule},
	}
	if _, err := fixture.client.CoreV1().Nodes().Update(context.Background(), source, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	values, err := fixture.service.helmSchedulingValues(context.Background(), appTestSession())
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"sshd.nodeSelector.kubernetes\\.io/hostname=source-host",
		"sshd.tolerations[0].key=dedicated",
		"sshd.tolerations[0].effect=NoSchedule",
		"sshd.tolerations[0].operator=Equal",
		"sshd.tolerations[0].value=storage",
		"sshd.tolerations[1].key=draining",
		"sshd.tolerations[1].effect=NoExecute",
		"sshd.tolerations[1].operator=Exists",
		"rsync.nodeSelector.kubernetes\\.io/hostname=target-host",
	} {
		if !slices.Contains(values, expected) {
			t.Fatalf("missing value %q in %v", expected, values)
		}
	}
	for _, value := range values {
		if value == "sshd.tolerations[2].key=preference" {
			t.Fatalf("PreferNoSchedule taint emitted: %v", values)
		}
	}
	for _, expected := range kube.ZeroResourceHelmValues() {
		if !slices.Contains(values, expected) {
			t.Fatalf("missing zero-resource Helm value %q", expected)
		}
	}
}

func TestHelmSchedulingValuesRejectMissingNodeTopology(t *testing.T) {
	t.Run("node missing", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		session.Spec.WorkflowOptionsPtr().SourceNode = "missing-node"
		_, err := fixture.service.helmSchedulingValues(context.Background(), session)
		if domain.CategoryOf(err) != domain.ErrorKubernetes {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("hostname label missing", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		node, err := fixture.client.CoreV1().Nodes().Get(context.Background(), "target-node", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		node.Labels = nil
		if _, err := fixture.client.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		_, err = fixture.service.helmSchedulingValues(context.Background(), appTestSession())
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
}

func TestHelmSchedulingValuesLocalLetsPVTopologyPlaceBothSSHDPods(t *testing.T) {
	fixture := newRecoveryFixture(t)
	source, err := fixture.client.CoreV1().Nodes().Get(context.Background(), "source-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	source.Spec.Taints = []corev1.Taint{{Key: "source-storage", Value: "true", Effect: corev1.TaintEffectNoSchedule}}
	if _, err := fixture.client.CoreV1().Nodes().Update(context.Background(), source, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	target, err := fixture.client.CoreV1().Nodes().Get(context.Background(), "target-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	target.Spec.Taints = []corev1.Taint{{Key: "target-storage", Effect: corev1.TaintEffectNoExecute}}
	if _, err := fixture.client.CoreV1().Nodes().Update(context.Background(), target, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	session := appTestSession()
	session.Spec.WorkflowOptionsPtr().Strategies = []string{"local"}
	values, err := fixture.service.helmSchedulingValues(context.Background(), session)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if strings.HasPrefix(value, "sshd.nodeSelector.") {
			t.Fatalf("local strategy pinned both SSHD Pods to one node: %v", values)
		}
	}
	for _, expected := range []string{
		"sshd.tolerations[0].key=source-storage",
		"sshd.tolerations[0].value=true",
		"sshd.tolerations[1].key=target-storage",
		"sshd.tolerations[1].operator=Exists",
		"rsync.nodeSelector.kubernetes\\.io/hostname=target-host",
	} {
		if !slices.Contains(values, expected) {
			t.Fatalf("missing value %q in %v", expected, values)
		}
	}
}

func TestResumeWorkloadFailsWhenActiveResourcesDoNotMatchPlan(t *testing.T) {
	t.Run("PVC points to source", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		session.Status.Phase = domain.PhaseActivated
		session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{Name: "pv-destination"}
		_, err := fixture.client.CoreV1().PersistentVolumeClaims("app").Create(context.Background(), &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", Annotations: map[string]string{kube.SessionAnnotation: session.ID}},
			Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv-source"},
			Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		err = fixture.service.ResumeWorkload(context.Background(), session)
		if domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
		if session.Status.Phase != domain.PhaseFailed || session.Status.ResumeFrom != domain.PhaseResuming {
			t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
		}
	})

	t.Run("Pod lands on another node", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		session.Status.Phase = domain.PhaseActivated
		_ = session.Spec.SetWorkload(domain.WorkloadSpec{Adapter: domain.WorkloadStandalone, Pod: domain.ObjectReference{Namespace: "app", Name: "application"}})
		session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{Name: "pv-destination"}
		_, err := fixture.client.CoreV1().PersistentVolumeClaims("app").Create(context.Background(), &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", Annotations: map[string]string{kube.SessionAnnotation: session.ID}},
			Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"},
			Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		fixture.controller.resumeHook = func(ctx context.Context, _ *domain.Session) error {
			_, createErr := fixture.client.CoreV1().Pods("app").Create(ctx, &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "application"},
				Spec:       corev1.PodSpec{NodeName: "source-node"},
			}, metav1.CreateOptions{})
			return createErr
		}
		err = fixture.service.ResumeWorkload(context.Background(), session)
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
		if session.Status.Phase != domain.PhaseFailed || session.Status.ResumeFrom != domain.PhaseResuming {
			t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
		}
	})

	t.Run("managed workload may land on another node", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		session.Status.Phase = domain.PhaseActivated
		_ = session.Spec.SetWorkload(domain.WorkloadSpec{
			Adapter: domain.WorkloadStatefulSet,
			Pod:     domain.ObjectReference{Namespace: "app", Name: "application"},
		})
		session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{Name: "pv-destination", UID: types.UID("destination-pv-uid")}
		session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid")}
		_, err := fixture.client.CoreV1().PersistentVolumeClaims("app").Create(context.Background(), &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid"), Annotations: map[string]string{kube.SessionAnnotation: session.ID}},
			Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"},
			Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = fixture.client.CoreV1().PersistentVolumes().Create(context.Background(), &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-destination", UID: types.UID("destination-pv-uid")},
			Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
				Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid"),
			}},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = fixture.client.CoreV1().Pods("app").Create(context.Background(), &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "application"},
			Spec:       corev1.PodSpec{NodeName: "source-node"},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.service.ResumeWorkload(context.Background(), session); err != nil {
			t.Fatal(err)
		}
		if session.Status.Phase != domain.PhaseCompleted {
			t.Fatalf("phase=%s", session.Status.Phase)
		}
	})

	t.Run("managed workload does not require the helper target to be ready", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		session.Status.Phase = domain.PhaseResuming
		_ = session.Spec.SetWorkload(domain.WorkloadSpec{
			Adapter: domain.WorkloadStatefulSet,
			Pod:     domain.ObjectReference{Namespace: "app", Name: "application"},
		})
		session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{Name: "pv-destination", UID: types.UID("destination-pv-uid")}
		session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid")}
		_, err := fixture.client.CoreV1().PersistentVolumeClaims("app").Create(context.Background(), &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid"), Annotations: map[string]string{kube.SessionAnnotation: session.ID}},
			Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"},
			Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = fixture.client.CoreV1().PersistentVolumes().Create(context.Background(), &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-destination", UID: types.UID("destination-pv-uid")},
			Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
				Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid"),
			}},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.service.ValidateResume(context.Background(), session); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMigrateStopsAtEachFailedStageAndRecordsResumePoint(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(*recoveryFixture)
		warmPasses int
		resumeFrom domain.Phase
		category   domain.ErrorCategory
	}{
		{
			name: "reserve",
			configure: func(fixture *recoveryFixture) {
				fixture.reserver.failures["data"] = domain.NewError(domain.ErrorKubernetes, "reserve", "injected failure")
			},
			resumeFrom: domain.PhaseReserving,
			category:   domain.ErrorKubernetes,
		},
		{
			name: "warm copy",
			configure: func(fixture *recoveryFixture) {
				fixture.copier.failures["warm/data"] = 1
			},
			warmPasses: 1,
			resumeFrom: domain.PhaseWarmCopying,
			category:   domain.ErrorCopy,
		},
		{
			name: "pause",
			configure: func(fixture *recoveryFixture) {
				fixture.controller.pauseErr = domain.NewError(domain.ErrorKubernetes, "pause", "injected failure")
			},
			resumeFrom: domain.PhasePausing,
			category:   domain.ErrorKubernetes,
		},
		{
			name: "verify paused",
			configure: func(fixture *recoveryFixture) {
				fixture.controller.verifyErr = domain.NewError(domain.ErrorPrecondition, "verify paused", "injected failure")
			},
			resumeFrom: domain.PhasePausing,
			category:   domain.ErrorPrecondition,
		},
		{
			name: "verify volume offline",
			configure: func(fixture *recoveryFixture) {
				fixture.switcher.offlineErr = domain.NewError(domain.ErrorPrecondition, "verify offline", "injected failure")
			},
			resumeFrom: domain.PhaseFinalSyncing,
			category:   domain.ErrorPrecondition,
		},
		{
			name: "activate",
			configure: func(fixture *recoveryFixture) {
				fixture.switcher.activateErr = domain.NewError(domain.ErrorKubernetes, "activate", "injected failure")
			},
			resumeFrom: domain.PhaseActivating,
			category:   domain.ErrorKubernetes,
		},
		{
			name: "resume workload",
			configure: func(fixture *recoveryFixture) {
				fixture.controller.resumeErr = domain.NewError(domain.ErrorKubernetes, "resume", "injected failure")
			},
			resumeFrom: domain.PhaseResuming,
			category:   domain.ErrorKubernetes,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			test.configure(fixture)
			session := appTestSession()
			err := fixture.service.Migrate(context.Background(), session, test.warmPasses)
			if domain.CategoryOf(err) != test.category {
				t.Fatalf("category=%s want=%s error=%v", domain.CategoryOf(err), test.category, err)
			}
			if session.Status.Phase != domain.PhaseFailed || session.Status.ResumeFrom != test.resumeFrom {
				t.Fatalf("phase=%s resumeFrom=%s want=%s", session.Status.Phase, session.Status.ResumeFrom, test.resumeFrom)
			}
		})
	}
}

func TestReserveRecoversWhenCheckpointPersistenceFails(t *testing.T) {
	fixture := newRecoveryFixture(t)
	fixture.store.updateErrAt = 2
	session := appTestSession()

	err := fixture.service.Reserve(context.Background(), session)
	if err == nil || err.Error() != "injected session update failure" {
		t.Fatalf("error=%v", err)
	}
	if session.Status.Phase != domain.PhaseFailed || session.Status.ResumeFrom != domain.PhaseReserving || !session.Status.Volumes[0].Reserved {
		t.Fatalf("phase=%s volume=%+v", session.Status.Phase, session.Status.Volumes[0])
	}
	fixture.store.updateErrAt = 0
	if err := fixture.service.Reserve(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if session.Status.Phase != domain.PhaseReserved || len(fixture.reserver.calls) != 1 {
		t.Fatalf("phase=%s reservation calls=%v", session.Status.Phase, fixture.reserver.calls)
	}
}

func TestActivateRecoversAfterSwitcherCheckpointFailure(t *testing.T) {
	fixture := newRecoveryFixture(t)
	fixture.store.updateErrAt = 2
	session := appTestSession()
	setSessionOperation(session, domain.OperationMigrate)
	session.Status.Phase = domain.PhaseFinalSynced

	err := fixture.service.Activate(context.Background(), session)
	if err == nil || err.Error() != "injected session update failure" {
		t.Fatalf("error=%v", err)
	}
	if session.Status.Phase != domain.PhaseFailed || session.Status.ResumeFrom != domain.PhaseActivating || session.Status.Volumes[0].Activation.ActivatedAt == nil {
		t.Fatalf("phase=%s resumeFrom=%s activation=%+v", session.Status.Phase, session.Status.ResumeFrom, session.Status.Volumes[0].Activation)
	}
	fixture.store.updateErrAt = 0
	if err := fixture.service.Activate(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if session.Status.Phase != domain.PhaseActivated || len(fixture.switcher.activateCalls) != 1 {
		t.Fatalf("phase=%s activation calls=%v", session.Status.Phase, fixture.switcher.activateCalls)
	}
}

func TestDryRunRecoveryValidationUsesReadOnlyChecks(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	if err := fixture.service.ValidateResume(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if fixture.store.updates != 0 || len(fixture.reserver.calls) != 1 {
		t.Fatalf("resume dry-run mutated state: updates=%d reserveCalls=%v", fixture.store.updates, fixture.reserver.calls)
	}

	session.Status.Phase = domain.PhasePaused
	if err := fixture.service.ValidateAbort(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if fixture.controller.resumes != 0 || fixture.store.updates != 0 {
		t.Fatalf("abort dry-run mutated state: resumes=%d updates=%d", fixture.controller.resumes, fixture.store.updates)
	}
}

func TestDryRunResumeFromActivatedAcceptsPausedStandalonePod(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	setSessionOperation(session, domain.OperationMigrate)
	session.Status.Phase = domain.PhaseActivated
	_ = session.Spec.SetWorkload(domain.WorkloadSpec{
		Adapter: domain.WorkloadStandalone,
		Pod:     domain.ObjectReference{Namespace: "app", Name: "application"},
	})
	session.Spec.WorkflowOptionsPtr().TargetNode = "target-node"
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{Name: "pv-destination", UID: types.UID("destination-pv-uid")}
	session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid")}
	_, err := fixture.client.CoreV1().PersistentVolumeClaims("app").Create(context.Background(), &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid"), Annotations: map[string]string{kube.SessionAnnotation: session.ID}},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.client.CoreV1().PersistentVolumes().Create(context.Background(), &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-destination", UID: types.UID("destination-pv-uid")},
		Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
			Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid"),
		}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	node, err := fixture.client.CoreV1().Nodes().Get(context.Background(), "target-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	if _, err := fixture.client.CoreV1().Nodes().UpdateStatus(context.Background(), node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := fixture.service.ValidateResume(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if fixture.controller.verifies != 1 || fixture.controller.resumes != 0 || fixture.store.updates != 0 {
		t.Fatalf("dry-run side effects: verifies=%d resumes=%d updates=%d", fixture.controller.verifies, fixture.controller.resumes, fixture.store.updates)
	}

	node, err = fixture.client.CoreV1().Nodes().Get(context.Background(), "target-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	node.Spec.Unschedulable = true
	if _, err := fixture.client.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.ValidateResume(context.Background(), session); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("unschedulable target category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestDryRunRollbackRejectsUnactivatedSession(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	if err := fixture.service.ValidateRollback(context.Background(), session); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
	if fixture.store.updates != 0 || fixture.controller.pauses != 0 || fixture.controller.resumes != 0 {
		t.Fatalf("rollback dry-run mutated state: store=%d pauses=%d resumes=%d", fixture.store.updates, fixture.controller.pauses, fixture.controller.resumes)
	}
}

func TestDryRunCleanupEnforcesDestructivePrerequisites(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	if err := fixture.service.ValidateCleanup(context.Background(), session, CleanupOptions{}); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("active cleanup category=%s error=%v", domain.CategoryOf(err), err)
	}
	session.Status.Phase = domain.PhaseCompleted
	if err := fixture.service.ValidateCleanup(context.Background(), session, CleanupOptions{DeleteSession: true}); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("unfinalized delete category=%s error=%v", domain.CategoryOf(err), err)
	}
	if fixture.store.updates != 0 || fixture.store.deletes != 0 {
		t.Fatalf("cleanup dry-run mutated state: updates=%d deletes=%d", fixture.store.updates, fixture.store.deletes)
	}
}
