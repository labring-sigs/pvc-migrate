package app

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
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
	session.ResourceVersion = strconv.Itoa(s.updates + 1)

	return nil
}

func (s *snapshotStore) List(context.Context, string) ([]*domain.Session, error) { return nil, nil }

func (s *snapshotStore) Delete(context.Context, *domain.Session) error {
	s.deletes++
	return nil
}

type scriptedReserver struct {
	calls              []string
	dryRunCalls        []string
	failures           map[string]error
	validationFailures map[string]error
}

func (r *scriptedReserver) ReserveVolume(
	_ context.Context,
	_ *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	dryRun bool,
) error {
	if dryRun {
		r.dryRunCalls = append(r.dryRunCalls, volume.SourcePVC.Name)
		return r.validationFailures[volume.SourcePVC.Name]
	}

	r.calls = append(r.calls, volume.SourcePVC.Name)
	if err := r.failures[volume.SourcePVC.Name]; err != nil {
		return err
	}

	volume.DestinationPVC.UID = types.UID("dest-pvc-" + volume.SourcePVC.Name)
	volume.DestinationPV = domain.ObjectReference{
		Name: "dest-pv-" + volume.SourcePVC.Name,
		UID:  types.UID("dest-pv-uid-" + volume.SourcePVC.Name),
	}
	volume.DestinationPolicy = corev1.PersistentVolumeReclaimDelete
	status.Reserved = true

	return nil
}

type scriptedController struct {
	pauses              int
	resumes             int
	verifies            int
	pauseErr            error
	resumeErr           error
	validateResumeErr   error
	verifyErr           error
	currentRollbackPods []domain.ObjectReference
	rollbackPodsErr     error
	pauseHook           func(*domain.Session) error
	resumeHook          func(context.Context, *domain.Session) error
}

func (c *scriptedController) Pause(_ context.Context, session *domain.Session) error {
	c.pauses++
	if c.pauseHook != nil {
		return c.pauseHook(session)
	}

	return c.pauseErr
}

func (c *scriptedController) Resume(ctx context.Context, session *domain.Session) error {
	c.resumes++
	if c.resumeHook != nil {
		return c.resumeHook(ctx, session)
	}

	return c.resumeErr
}

func (c *scriptedController) ValidateResume(context.Context, *domain.Session) error {
	return c.validateResumeErr
}

func (c *scriptedController) VerifyPaused(context.Context, *domain.Session) error {
	c.verifies++
	return c.verifyErr
}

func (c *scriptedController) CurrentRollbackPods(
	context.Context,
	*domain.Session,
) ([]domain.ObjectReference, error) {
	return slices.Clone(c.currentRollbackPods), c.rollbackPodsErr
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
	toolNamespace     string
	toolName          string
	calls             int
	secondSawTool     bool
	firstAttemptEnded chan struct{}
}

func (c *cleanupAwareCopier) Copy(
	ctx context.Context,
	_ copyengine.Request,
	_ copyengine.ProgressFunc,
) error {
	c.calls++
	if c.calls == 1 {
		close(c.firstAttemptEnded)
		return domain.NewError(domain.ErrorCopy, "copy", "injected tool failure")
	}

	_, err := c.client.CoreV1().Pods(c.toolNamespace).Get(ctx, c.toolName, metav1.GetOptions{})
	c.secondSawTool = err == nil

	return nil
}

func (c *scriptedCopier) Copy(
	_ context.Context,
	request copyengine.Request,
	_ copyengine.ProgressFunc,
) error {
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
	client        kubernetes.Interface
	offlineCalls  []string
	activateCalls []string
	rollbackCalls []string
	renameCalls   []domain.VolumeSpec
	offlineErr    error
	offlineErrs   map[string]error
	activateErr   error
	rollbackErr   map[string]int
	rollbackHook  func(context.Context, *domain.VolumeSpec) error
	renameErr     error
}

func (s *scriptedSwitcher) VerifyVolumeOffline(_ context.Context, volume *domain.VolumeSpec) error {
	s.offlineCalls = append(s.offlineCalls, volume.SourcePVC.Name)
	if err := s.offlineErrs[volume.SourcePVC.Name]; err != nil {
		return err
	}

	return s.offlineErr
}

func (s *scriptedSwitcher) ActivateVolume(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	progress kube.ProgressFunc,
) error {
	s.activateCalls = append(s.activateCalls, volume.SourcePVC.Name)
	if s.activateErr != nil {
		return s.activateErr
	}

	now := metav1.Now()
	status.Activation.ActivatedAt = &now

	status.Activation.ActivePVC = volume.SourcePVC
	if s.client != nil && volume.DestinationPV.Name != "" && volume.DestinationPV.UID != "" {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: volume.SourcePVC.Namespace,
				Name:      volume.SourcePVC.Name,
				UID:       volume.SourcePVC.UID,
				Annotations: map[string]string{
					kube.SessionKey: session.ID,
				},
			},
			Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: volume.DestinationPV.Name},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}
		if _, err := s.client.CoreV1().
			PersistentVolumeClaims(pvc.Namespace).
			Create(ctx, pvc, metav1.CreateOptions{}); err != nil {
			return err
		}

		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: volume.DestinationPV.Name,
				UID:  volume.DestinationPV.UID,
			},
			Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
				Namespace: pvc.Namespace,
				Name:      pvc.Name,
				UID:       pvc.UID,
			}},
		}
		if _, err := s.client.CoreV1().
			PersistentVolumes().
			Create(ctx, pv, metav1.CreateOptions{}); err != nil {
			return err
		}
	}

	if progress != nil {
		return progress()
	}

	return nil
}

func (s *scriptedSwitcher) RollbackVolume(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	progress kube.ProgressFunc,
) error {
	s.rollbackCalls = append(s.rollbackCalls, volume.SourcePVC.Name)
	if s.rollbackErr[volume.SourcePVC.Name] > 0 {
		s.rollbackErr[volume.SourcePVC.Name]--
		return domain.NewError(domain.ErrorKubernetes, "rollback", "injected rollback failure")
	}

	activePVC := volume.SourcePVC
	if s.client != nil && volume.SourcePV.Name != "" && volume.SourcePV.UID != "" {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: volume.SourcePVC.Namespace,
				Name:      volume.SourcePVC.Name,
				UID:       volume.SourcePVC.UID,
				Annotations: map[string]string{
					kube.SessionKey: session.ID,
				},
			},
			Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: volume.SourcePV.Name},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}
		if current, err := s.client.CoreV1().
			PersistentVolumeClaims(pvc.Namespace).
			Get(ctx, pvc.Name, metav1.GetOptions{}); apierrors.IsNotFound(
			err,
		) {
			created, createErr := s.client.CoreV1().
				PersistentVolumeClaims(pvc.Namespace).
				Create(ctx, pvc, metav1.CreateOptions{})
			if createErr != nil {
				return createErr
			}

			activePVC.UID = created.UID
		} else if err != nil {
			return err
		} else {
			current.Spec.VolumeName = volume.SourcePV.Name

			current.Status.Phase = corev1.ClaimBound
			if current.Annotations == nil {
				current.Annotations = map[string]string{}
			}

			current.Annotations[kube.SessionKey] = session.ID

			updated, updateErr := s.client.CoreV1().
				PersistentVolumeClaims(current.Namespace).
				Update(ctx, current, metav1.UpdateOptions{})
			if updateErr != nil {
				return updateErr
			}

			activePVC.UID = updated.UID
		}

		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: volume.SourcePV.Name, UID: volume.SourcePV.UID},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{
					Namespace: pvc.Namespace,
					Name:      pvc.Name,
					UID:       activePVC.UID,
				},
			},
		}
		if _, err := s.client.CoreV1().
			PersistentVolumes().
			Get(ctx, pv.Name, metav1.GetOptions{}); apierrors.IsNotFound(
			err,
		) {
			if _, err := s.client.CoreV1().
				PersistentVolumes().
				Create(ctx, pv, metav1.CreateOptions{}); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}

	now := metav1.Now()
	status.Activation.RolledBackAt = &now
	status.Activation.ActivePVC = activePVC

	if s.rollbackHook != nil {
		if err := s.rollbackHook(ctx, volume); err != nil {
			return err
		}
	}

	if progress != nil {
		return progress()
	}

	return nil
}

func (s *scriptedSwitcher) RenamePVC(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	progress kube.ProgressFunc,
) (*corev1.PersistentVolumeClaim, error) {
	s.renameCalls = append(s.renameCalls, *volume)
	if s.renameErr != nil {
		return nil, s.renameErr
	}

	var renamed *corev1.PersistentVolumeClaim
	if s.client != nil {
		if source, err := s.client.CoreV1().
			PersistentVolumeClaims(volume.SourcePVC.Namespace).
			Get(ctx, volume.SourcePVC.Name, metav1.GetOptions{}); err == nil {
			uid := source.UID
			if err := s.client.CoreV1().
				PersistentVolumeClaims(source.Namespace).
				Delete(ctx, source.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil {
				return nil, err
			}
		} else if !apierrors.IsNotFound(
			err,
		) {
			return nil, err
		}

		renamed = &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:       volume.DestinationPVC.Namespace,
				Name:            volume.DestinationPVC.Name,
				UID:             types.UID("renamed-pvc-uid"),
				ResourceVersion: "23",
				Annotations:     map[string]string{kube.SessionKey: session.ID},
			},
			Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: volume.SourcePV.Name},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}

		created, err := s.client.CoreV1().
			PersistentVolumeClaims(renamed.Namespace).
			Create(ctx, renamed, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			created, err = s.client.CoreV1().
				PersistentVolumeClaims(renamed.Namespace).
				Get(ctx, renamed.Name, metav1.GetOptions{})
		}

		if err != nil {
			return nil, err
		}

		renamed = created

		pv, err := s.client.CoreV1().
			PersistentVolumes().
			Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		pv.Spec.ClaimRef = &corev1.ObjectReference{
			Namespace: renamed.Namespace,
			Name:      renamed.Name,
			UID:       renamed.UID,
		}
		if _, err := s.client.CoreV1().
			PersistentVolumes().
			Update(ctx, pv, metav1.UpdateOptions{}); err != nil {
			return nil, err
		}
	}

	if progress != nil {
		if err := progress(); err != nil {
			return nil, err
		}
	}

	if renamed != nil {
		return renamed, nil
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

	options := session.Spec.WorkflowOptions()
	switch operation {
	case domain.OperationMigrate:
		session.Spec = domain.NewOfflineMigrationSessionSpec(common, options)
	case domain.OperationMigratePod:
		session.Spec = domain.NewPodMigrationSessionSpec(
			common,
			workload,
			options,
			session.Spec.PrecopyPasses(),
			session.Spec.OpenEBSLVMSharedMountEnabled(),
		)
	default:
		session.Spec = domain.NewSessionSpec(
			operation,
			common,

			operation == domain.OperationCopy && session.Spec.Online(),
			options,
		)
	}
}

func newRecoveryFixture(t *testing.T) *recoveryFixture {
	t.Helper()

	client := fake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "source-node",
				Labels: map[string]string{corev1.LabelHostname: "source-host"},
			},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "target-node",
				Labels: map[string]string{corev1.LabelHostname: "target-host"},
			},
		},
	)
	store := &snapshotStore{}
	reserver := &scriptedReserver{failures: map[string]error{}}
	controller := &scriptedController{}
	copier := &scriptedCopier{failures: map[string]int{}}
	switcher := &scriptedSwitcher{client: client, rollbackErr: map[string]int{}}
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

func markNodeReady(t *testing.T, client kubernetes.Interface, name string) {
	t.Helper()

	node, err := client.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	node.Status.Conditions = []corev1.NodeCondition{
		{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
	}
	if _, err := client.CoreV1().
		Nodes().
		UpdateStatus(context.Background(), node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func addSecondVolume(session *domain.Session) {
	second := session.Spec.Volumes[0]
	second.SourcePVC = domain.ObjectReference{
		Namespace: "app",
		Name:      "logs",
		UID:       types.UID("source-logs-uid"),
	}
	second.SourcePV = domain.ObjectReference{
		Name: "pv-source-logs",
		UID:  types.UID("source-pv-logs-uid"),
	}
	second.DestinationPVC = domain.ObjectReference{Namespace: "system", Name: "logs-migrated"}
	second.DestinationPV = domain.ObjectReference{}
	session.Spec.Volumes = append(session.Spec.Volumes, second)
	session.Status.Volumes = append(
		session.Status.Volumes,
		domain.VolumeStatus{SourcePVCName: "logs"},
	)
}

func createSourceStorage(t *testing.T, fixture *recoveryFixture, session *domain.Session) {
	t.Helper()

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]

		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: volume.SourcePVC.Namespace,
				Name:      volume.SourcePVC.Name,
				UID:       volume.SourcePVC.UID,
			},
			Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: volume.SourcePV.Name},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}
		if _, err := fixture.client.CoreV1().
			PersistentVolumeClaims(pvc.Namespace).
			Create(context.Background(), pvc, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}

		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: volume.SourcePV.Name, UID: volume.SourcePV.UID},
			Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
				Namespace: pvc.Namespace,
				Name:      pvc.Name,
				UID:       pvc.UID,
			}},
		}
		if _, err := fixture.client.CoreV1().
			PersistentVolumes().
			Create(context.Background(), pv, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}

func createActiveDestinationStorage(
	t *testing.T,
	fixture *recoveryFixture,
	session *domain.Session,
) {
	t.Helper()

	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		status := &session.Status.Volumes[index]

		if volume.DestinationPV.Name == "" {
			volume.DestinationPV = domain.ObjectReference{
				Name: "dest-pv-" + volume.SourcePVC.Name,
				UID:  types.UID("dest-pv-uid-" + volume.SourcePVC.Name),
			}
		}

		if volume.DestinationPVC.UID == "" {
			volume.DestinationPVC.UID = types.UID("destination-pvc-uid-" + volume.SourcePVC.Name)
		}

		activeUID := types.UID("active-pvc-uid-" + volume.SourcePVC.Name)
		activatedAt := metav1.Now()
		status.Reserved = true
		status.Activation.ActivePVC = domain.ObjectReference{
			Namespace: volume.SourcePVC.Namespace,
			Name:      volume.SourcePVC.Name,
			UID:       activeUID,
		}
		status.Activation.ActivatedAt = &activatedAt

		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   volume.SourcePVC.Namespace,
				Name:        volume.SourcePVC.Name,
				UID:         activeUID,
				Annotations: map[string]string{kube.SessionKey: session.ID},
			},
			Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: volume.DestinationPV.Name},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}
		if _, err := fixture.client.CoreV1().
			PersistentVolumeClaims(pvc.Namespace).
			Create(context.Background(), pvc, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}

		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: volume.DestinationPV.Name,
				UID:  volume.DestinationPV.UID,
			},
			Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
				Namespace: pvc.Namespace,
				Name:      pvc.Name,
				UID:       pvc.UID,
			}},
		}
		if _, err := fixture.client.CoreV1().
			PersistentVolumes().
			Create(context.Background(), pv, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}

func transitionThrough(t *testing.T, session *domain.Session, phases ...domain.Phase) {
	t.Helper()

	for index, phase := range phases {
		if err := session.Transition(
			phase,
			"test transition",
			time.Unix(int64(200+index), 0),
		); err != nil {
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
	if _, err := fixture.service.CreateSession(
		context.Background(),
		nil,
		false,
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("nil plan category=%s error=%v", domain.CategoryOf(err), err)
	}

	if _, err := fixture.service.CreateSession(
		context.Background(),
		&domain.MigrationPlan{},
		false,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
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
		if _, err := fixture.client.CoreV1().
			Namespaces().
			Get(context.Background(), namespace, metav1.GetOptions{}); err != nil {
			t.Fatalf("namespace %s: %v", namespace, err)
		}
	}
}

func TestCreateSessionCreatesSessionNamespaceBeforeLease(t *testing.T) {
	client := fake.NewClientset()
	assignLeaseUIDs(client)
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
		if _, err := client.CoreV1().
			Namespaces().
			Get(context.Background(), namespace, metav1.GetOptions{}); err != nil {
			t.Fatalf("namespace %s: %v", namespace, err)
		}
	}

	if _, err := client.CoreV1().
		ConfigMaps("sessions").
		Get(context.Background(), kube.SessionConfigMapName(session.ID), metav1.GetOptions{}); err != nil {
		t.Fatalf("session ConfigMap: %v", err)
	}

	if _, err := client.CoordinationV1().
		Leases("sessions").
		Get(context.Background(), kube.SessionLockName(session.ID), metav1.GetOptions{}); err != nil {
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

	if want := []string{"data", "logs"}; !slices.Equal(fixture.reserver.dryRunCalls, want) {
		t.Fatalf("reservation validations=%v want=%v", fixture.reserver.dryRunCalls, want)
	}

	if fixture.store.creates != 0 || created.Status.Volumes[0].Reserved ||
		created.Spec.Volumes[0].DestinationPV.Name != "" {
		t.Fatalf(
			"dry-run persisted or mutated session: creates=%d session=%+v",
			fixture.store.creates,
			created,
		)
	}
}

func TestReserveValidatesEveryVolumeBeforeMutation(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)

	fixture.reserver.validationFailures = map[string]error{
		"logs": domain.NewError(
			domain.ErrorConflict,
			"reserve dry-run",
			"injected validation failure",
		),
	}

	err := fixture.service.Reserve(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if want := []string{"data", "logs"}; !slices.Equal(fixture.reserver.dryRunCalls, want) {
		t.Fatalf("reservation validations=%v want=%v", fixture.reserver.dryRunCalls, want)
	}

	if len(fixture.reserver.calls) != 0 {
		t.Fatalf(
			"reservation mutated before validation completed: calls=%v",
			fixture.reserver.calls,
		)
	}

	if session.Status.Phase != domain.PhasePlanned || session.Status.Volumes[0].Reserved ||
		session.Spec.Volumes[0].DestinationPV.Name != "" {
		t.Fatalf(
			"failed preflight mutated session: phase=%s volume=%+v status=%+v",
			session.Status.Phase,
			session.Spec.Volumes[0],
			session.Status.Volumes[0],
		)
	}
}

func TestValidateResumeDoesNotCheckpointInferredCopySourceNode(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	common := session.Spec.SessionCommon
	options := session.Spec.WorkflowOptions()
	options.SourceNode = ""
	session.Spec = domain.NewSessionSpec(
		domain.OperationCopy,
		common,

		true,
		options,
	)

	session.Status.Phase = domain.PhaseReserved

	if _, err := fixture.client.CoreV1().
		Pods("app").
		Create(context.Background(), activePVCConsumer("app", "data-writer", "data", "source-node"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := fixture.service.validateResumeWorkflowForTest(
		context.Background(),
		session,
	); err != nil {
		t.Fatal(err)
	}

	if got := session.Spec.WorkflowOptions().SourceNode; got != "" {
		t.Fatalf("resume dry-run mutated source node=%q", got)
	}

	if fixture.store.updates != 0 {
		t.Fatalf("resume dry-run persisted session %d time(s)", fixture.store.updates)
	}
}

func TestWarmCopyValidatesEveryConsumerBeforeCheckpointingSourceNode(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)
	common := session.Spec.SessionCommon
	options := session.Spec.WorkflowOptions()
	options.SourceNode = ""
	session.Spec = domain.NewSessionSpec(
		domain.OperationCopy,
		common,

		true,
		options,
	)

	session.Status.Phase = domain.PhaseReserved

	for _, pod := range []*corev1.Pod{
		activePVCConsumer("app", "data-writer", "data", "source-node"),
		activePVCConsumer("app", "logs-writer", "logs", "target-node"),
	} {
		if _, err := fixture.client.CoreV1().
			Pods(pod.Namespace).
			Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	err := fixture.service.WarmCopy(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if got := session.Spec.WorkflowOptions().SourceNode; got != "" {
		t.Fatalf("failed batch preflight mutated source node=%q", got)
	}

	if fixture.store.updates != 0 || session.Status.Phase != domain.PhaseReserved {
		t.Fatalf(
			"failed batch preflight persisted session: updates=%d phase=%s",
			fixture.store.updates,
			session.Status.Phase,
		)
	}
}

func activePVCConsumer(namespace, name, claim, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: corev1.PodSpec{
			NodeName: node,
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: claim,
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func TestReserveResumesMultiVolumePartialProgress(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)
	session.Status.Volumes[0].Reserved = true
	session.Spec.Volumes[0].DestinationPVC.UID = types.UID("existing-destination-pvc-uid")
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
		Name: "existing-destination",
		UID:  types.UID("existing-destination-uid"),
	}
	fixture.reserver.failures["logs"] = domain.NewError(
		domain.ErrorKubernetes,
		"reserve",
		"injected reservation failure",
	)

	err := fixture.service.Reserve(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if session.Status.Phase != domain.PhaseFailed ||
		session.Status.ResumeFrom != domain.PhaseReserving {
		t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
	}

	delete(fixture.reserver.failures, "logs")

	if err := fixture.service.Reserve(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != domain.PhaseReserved || !session.Status.Volumes[1].Reserved {
		t.Fatalf(
			"phase=%s second reserved=%v",
			session.Status.Phase,
			session.Status.Volumes[1].Reserved,
		)
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
	transitionThrough(
		t,
		session,
		domain.PhaseReserving,
		domain.PhaseReserved,
		domain.PhaseWarmCopying,
	)

	completed := metav1.NewTime(time.Unix(300, 0))
	session.Status.Volumes[0].Sync.WarmCompletedAt = &completed

	if err := fixture.service.WarmCopy(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if want := []string{"logs"}; !slices.Equal(requestSources(fixture.copier.requests), want) {
		t.Fatalf(
			"first recovery pass sources=%v want=%v",
			requestSources(fixture.copier.requests),
			want,
		)
	}

	if err := fixture.service.WarmCopy(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if want := []string{
		"logs",
		"data",
		"logs",
	}; !slices.Equal(
		requestSources(fixture.copier.requests),
		want,
	) {
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
	session.Spec.Volumes[0].TransferScope = &domain.TransferScope{
		SourcePath:      "mysql/current",
		DestinationPath: "restored/mysql",
	}
	transitionThrough(t, session, domain.PhaseReserving, domain.PhaseReserved)

	if err := fixture.service.WarmCopy(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Status.Volumes[0].Sync.Attempts != 3 ||
		session.Status.Volumes[0].Sync.LastError != "" {
		t.Fatalf("sync state=%+v", session.Status.Volumes[0].Sync)
	}

	if want := []time.Duration{
		5 * time.Millisecond,
		10 * time.Millisecond,
	}; !slices.Equal(
		delays,
		want,
	) {
		t.Fatalf("retry delays=%v want=%v", delays, want)
	}

	for index, request := range fixture.copier.requests {
		if request.Attempt != index+1 || request.Mode != copyengine.ModeWarm ||
			request.SourcePath != "mysql/current" ||
			request.DestinationPath != "restored/mysql" {
			t.Fatalf("request %d attempt=%d mode=%s", index, request.Attempt, request.Mode)
		}
	}
}

func TestDestinationENOSPCStopsSessionLevelRetries(t *testing.T) {
	fixture := newRecoveryFixture(t)
	fixture.service.config.Retries = 3
	fixture.copier.failures["warm/data"] = 3
	fixture.copier.failure = errors.New("rsync: write failed: No space left on device")
	session := appTestSession()
	transitionThrough(t, session, domain.PhaseReserving, domain.PhaseReserved)

	err := fixture.service.WarmCopy(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "copy capacity") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if len(fixture.copier.requests) != 1 || session.Status.Volumes[0].Sync.Attempts != 1 {
		t.Fatalf(
			"copy requests=%d attempts=%d",
			len(fixture.copier.requests),
			session.Status.Volumes[0].Sync.Attempts,
		)
	}

	if session.Status.Phase != domain.PhaseFailed ||
		session.Status.ResumeFrom != domain.PhaseWarmCopying {
		t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
	}
}

func TestCopyRetryRevalidatesReservedVolumeIdentity(t *testing.T) {
	fixture := newRecoveryFixture(t)
	fixture.service.config.Retries = 2
	fixture.copier.failures["warm/data"] = 1
	fixture.copier.copyHook = func() {
		if len(fixture.copier.requests) == 1 {
			fixture.reserver.validationFailures = map[string]error{
				"data": domain.NewError(
					domain.ErrorConflict,
					"reserve volume",
					"destination PVC UID changed",
				),
			}
		}
	}
	session := appTestSession()
	transitionThrough(t, session, domain.PhaseReserving, domain.PhaseReserved)

	err := fixture.service.WarmCopy(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if len(fixture.copier.requests) != 1 {
		t.Fatalf("copy attempts=%d want=1", len(fixture.copier.requests))
	}
}

func TestCopyFailureWaitsForToolReleaseBeforeRetry(t *testing.T) {
	fixture := newRecoveryFixture(t)

	tool := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "pv-migrate-tool",
			Labels: map[string]string{
				"app.kubernetes.io/instance":  "pv-migrate-pm-test-clusterip",
				"app.kubernetes.io/component": "sshd",
			},
		},
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
	if _, err := fixture.client.CoreV1().
		Pods(tool.Namespace).
		Create(context.Background(), tool, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	copier := &cleanupAwareCopier{
		client: fixture.client, toolNamespace: tool.Namespace, toolName: tool.Name,
		firstAttemptEnded: make(chan struct{}),
	}
	fixture.service.copier = copier

	fixture.service.config.Retries = 2
	go func() {
		<-copier.firstAttemptEnded
		time.Sleep(10 * time.Millisecond)

		_ = fixture.client.CoreV1().
			Pods(tool.Namespace).
			Delete(context.Background(), tool.Name, metav1.DeleteOptions{})
	}()

	session := appTestSession()
	transitionThrough(t, session, domain.PhaseReserving, domain.PhaseReserved)

	if err := fixture.service.WarmCopy(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if copier.calls != 2 || copier.secondSawTool {
		t.Fatalf("copy calls=%d secondSawTool=%t", copier.calls, copier.secondSawTool)
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

	if session.Status.Phase != domain.PhaseFailed ||
		session.Status.ResumeFrom != domain.PhaseWarmCopying {
		t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
	}

	if session.Status.Volumes[0].Sync.Attempts != 1 || len(fixture.copier.requests) != 1 {
		t.Fatalf(
			"attempts=%d requests=%d",
			session.Status.Volumes[0].Sync.Attempts,
			len(fixture.copier.requests),
		)
	}
}

func TestCopyFailureAfterContextCancellationPreservesRootCause(t *testing.T) {
	fixture := newRecoveryFixture(t)
	cause := domain.NewError(domain.ErrorCopy, "copy", "tool deadline exceeded")
	ctx, cancel := context.WithCancel(context.Background())
	fixture.copier.copyError = cause
	fixture.copier.copyHook = cancel
	session := appTestSession()
	transitionThrough(t, session, domain.PhaseReserving, domain.PhaseReserved)

	err := fixture.service.WarmCopy(ctx, session)
	if !errors.Is(err, cause) {
		t.Fatalf("error=%v want root cause %v", err, cause)
	}

	if session.Status.Phase != domain.PhaseFailed ||
		session.Status.ResumeFrom != domain.PhaseWarmCopying {
		t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
	}

	if !strings.Contains(session.Status.Volumes[0].Sync.LastError, cause.Error()) {
		t.Fatalf("lastError=%q want %q", session.Status.Volumes[0].Sync.LastError, cause.Error())
	}
}

func TestPauseIdempotencyVerifiesWorkloadState(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	transitionThrough(
		t,
		session,
		domain.PhaseReserving,
		domain.PhaseReserved,
		domain.PhasePausing,
		domain.PhasePaused,
	)

	if err := fixture.service.PodPause(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if fixture.controller.pauses != 0 || fixture.controller.verifies != 1 {
		t.Fatalf(
			"pause calls=%d verify calls=%d",
			fixture.controller.pauses,
			fixture.controller.verifies,
		)
	}

	fixture.controller.verifyErr = domain.NewError(
		domain.ErrorPrecondition,
		"verify",
		"workload is running",
	)
	if err := fixture.service.PodPause(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
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
		{
			name:        "final syncing failure",
			phase:       domain.PhaseFailed,
			resumeFrom:  domain.PhaseFinalSyncing,
			wantResumes: 1,
		},
		{name: "pausing with deleted pod", phase: domain.PhasePausing, wantResumes: 1},
		{
			name:        "pausing with live pod",
			phase:       domain.PhasePausing,
			verifyErr:   errors.New("still running"),
			wantResumes: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			fixture.controller.verifyErr = test.verifyErr
			session := appTestSession()
			session.Status.Phase = test.phase

			session.Status.ResumeFrom = test.resumeFrom
			if test.wantResumes > 0 {
				createSourceStorage(t, fixture, session)
			}

			if err := fixture.service.abortWorkflowForTest(
				context.Background(),
				session,
			); err != nil {
				t.Fatal(err)
			}

			if session.Status.Phase != domain.PhaseAborted ||
				fixture.controller.resumes != test.wantResumes {
				t.Fatalf(
					"phase=%s resumes=%d want=%d",
					session.Status.Phase,
					fixture.controller.resumes,
					test.wantResumes,
				)
			}

			if err := fixture.service.abortWorkflowForTest(
				context.Background(),
				session,
			); err != nil {
				t.Fatal(err)
			}

			if fixture.controller.resumes != test.wantResumes {
				t.Fatalf(
					"idempotent abort resumes=%d want=%d",
					fixture.controller.resumes,
					test.wantResumes,
				)
			}
		})
	}
}

func TestAbortRejectsSourceIdentityDriftBeforeResumingWorkload(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume)
	}{
		{
			name: "PVC UID",
			mutate: func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) {
				pvc.UID = types.UID("replacement-pvc-uid")
			},
		},
		{
			name: "PV UID",
			mutate: func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
				pv.UID = types.UID("replacement-pv-uid")
			},
		},
		{
			name: "PV claimRef",
			mutate: func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
				pv.Spec.ClaimRef.UID = types.UID("other-pvc-uid")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, execute := range []bool{false, true} {
				fixture := newRecoveryFixture(t)
				session := appTestSession()
				session.Status.Phase = domain.PhasePaused
				createSourceStorage(t, fixture, session)

				pvc, err := fixture.client.CoreV1().
					PersistentVolumeClaims("app").
					Get(context.Background(), "data", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}

				pv, err := fixture.client.CoreV1().
					PersistentVolumes().
					Get(context.Background(), "pv-source", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}

				test.mutate(pvc, pv)

				if _, err := fixture.client.CoreV1().
					PersistentVolumeClaims("app").
					Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}

				if _, err := fixture.client.CoreV1().
					PersistentVolumes().
					Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}

				var got error
				if execute {
					got = fixture.service.abortWorkflowForTest(context.Background(), session)
				} else {
					got = fixture.service.validateAbortWorkflowForTest(
						context.Background(),
						session,
					)
				}

				if domain.CategoryOf(got) != domain.ErrorConflict {
					t.Fatalf(
						"execute=%t category=%s error=%v",
						execute,
						domain.CategoryOf(got),
						got,
					)
				}

				if fixture.controller.resumes != 0 || session.Status.Phase != domain.PhasePaused {
					t.Fatalf(
						"execute=%t resumes=%d phase=%s",
						execute,
						fixture.controller.resumes,
						session.Status.Phase,
					)
				}
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

			err := fixture.service.abortWorkflowForTest(context.Background(), session)
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

			err := fixture.service.abortWorkflowForTest(context.Background(), session)
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
	createActiveDestinationStorage(t, fixture, session)
	addSecondVolume(session)
	session.Status.Phase = domain.PhaseFinalSynced

	completed := metav1.Now()
	for index := range session.Status.Volumes {
		session.Status.Volumes[index].Sync.FinalCompletedAt = &completed
	}

	if err := fixture.service.PodActivate(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if want := []string{"logs"}; !slices.Equal(fixture.switcher.activateCalls, want) {
		t.Fatalf("activation calls=%v want=%v", fixture.switcher.activateCalls, want)
	}

	if session.Status.Phase != domain.PhaseActivated ||
		session.Status.Volumes[1].Activation.ActivatedAt == nil {
		t.Fatalf(
			"phase=%s second activation=%+v",
			session.Status.Phase,
			session.Status.Volumes[1].Activation,
		)
	}
}

func TestActivatePreflightsAllVolumesBeforeSwitchingAnyPVC(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)
	session.Status.Phase = domain.PhaseFinalSynced

	completed := metav1.Now()
	for index := range session.Status.Volumes {
		session.Status.Volumes[index].Sync.FinalCompletedAt = &completed
	}

	conflict := domain.NewError(domain.ErrorConflict, "verify PVC offline", "logs binding changed")
	fixture.switcher.offlineErrs = map[string]error{"logs": conflict}

	err := fixture.service.PodActivate(context.Background(), session)
	if !errors.Is(err, conflict) {
		t.Fatalf("Activate() error=%v", err)
	}

	if len(fixture.switcher.activateCalls) != 0 || session.Status.Phase != domain.PhaseFinalSynced {
		t.Fatalf("activate calls=%v phase=%s", fixture.switcher.activateCalls, session.Status.Phase)
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

			if err := fixture.service.ValidatePodActivation(
				context.Background(),
				session,
			); err != nil {
				t.Fatal(err)
			}

			if err := fixture.service.PodActivate(context.Background(), session); err != nil {
				t.Fatal(err)
			}

			if len(fixture.switcher.activateCalls) != 0 || session.Status.Phase != test.phase {
				t.Fatalf(
					"activate calls=%v phase=%s",
					fixture.switcher.activateCalls,
					session.Status.Phase,
				)
			}
		})
	}
}

func TestValidateResumeRecognizesActivePVCBeforeCheckpointPersistence(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	createActiveDestinationStorage(t, fixture, session)

	completed := metav1.Now()
	session.Status.Phase = domain.PhaseFailed
	session.Status.ResumeFrom = domain.PhaseActivating
	session.Status.Volumes[0].Sync.FinalCompletedAt = &completed
	session.Status.Volumes[0].Activation.TemporaryPVCDeleted = true
	session.Status.Volumes[0].Activation.SourcePVCDeleted = true
	session.Status.Volumes[0].Activation.DestinationReserved = true
	session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{}
	session.Status.Volumes[0].Activation.ActivatedAt = nil

	if err := fixture.service.validateResumeWorkflowForTest(
		context.Background(),
		session,
	); err != nil {
		t.Fatal(err)
	}

	if session.Status.Volumes[0].Activation.ActivePVC.Name != "" ||
		session.Status.Volumes[0].Activation.ActivatedAt != nil {
		t.Fatalf("dry-run mutated activation state: %#v", session.Status.Volumes[0].Activation)
	}

	if want := []string{"data"}; !slices.Equal(fixture.switcher.offlineCalls, want) {
		t.Fatalf("offline checks=%v want=%v", fixture.switcher.offlineCalls, want)
	}
}

func TestValidateResumeRecognizesActivePVCBeforeQuotaProjection(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	createActiveDestinationStorage(t, fixture, session)

	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "storage"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceRequestsStorage: resource.MustParse("1Gi"),
		}},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceRequestsStorage: resource.MustParse("1Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceRequestsStorage: resource.MustParse("1Gi"),
			},
		},
	}
	if _, err := fixture.client.CoreV1().ResourceQuotas("app").Create(
		context.Background(), quota, metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	completed := metav1.Now()
	session.Status.Phase = domain.PhaseFailed
	session.Status.ResumeFrom = domain.PhaseActivating
	session.Status.Volumes[0].Sync.FinalCompletedAt = &completed
	session.Status.Volumes[0].Activation.TemporaryPVCDeleted = true
	session.Status.Volumes[0].Activation.SourcePVCDeleted = false
	session.Status.Volumes[0].Activation.DestinationReserved = true
	session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{}
	session.Status.Volumes[0].Activation.ActivatedAt = nil

	if err := fixture.service.validateResumeWorkflowForTest(
		context.Background(),
		session,
	); err != nil {
		t.Fatal(err)
	}

	if want := []string{"data"}; !slices.Equal(fixture.switcher.offlineCalls, want) {
		t.Fatalf("offline checks=%v want=%v", fixture.switcher.offlineCalls, want)
	}
}

func TestResumePodWorkloadPersistsRecreatedStandalonePodUID(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseActivated
	_ = session.Spec.SetWorkload(domain.WorkloadSpec{
		Adapter: domain.WorkloadStandalone,
		Pod: domain.ObjectReference{
			Namespace: "app",
			Name:      "application",
			UID:       types.UID("old-pod-uid"),
		},
	})

	markNodeReady(t, fixture.client, "target-node")

	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
		Name: "pv-destination",
		UID:  types.UID("destination-pv-uid"),
	}
	session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
		Namespace: "app",
		Name:      "data",
		UID:       types.UID("active-pvc-uid"),
	}

	_, err := fixture.client.CoreV1().
		PersistentVolumeClaims("app").
		Create(context.Background(), &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid"),
				Annotations: map[string]string{kube.SessionKey: session.ID},
			},
			Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = fixture.client.CoreV1().
		PersistentVolumes().
		Create(context.Background(), &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pv-destination",
				UID:  types.UID("destination-pv-uid"),
			},
			Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
				Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid"),
			}},
		}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	fixture.controller.resumeHook = func(ctx context.Context, current *domain.Session) error {
		current.Spec.WorkloadPtr().Pod.UID = types.UID("new-pod-uid")
		current.Spec.WorkloadPtr().Pod.ResourceVersion = "44"
		_, createErr := fixture.client.CoreV1().Pods("app").Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "app",
				Name:        "application",
				UID:         types.UID("new-pod-uid"),
				Annotations: map[string]string{kube.SessionKey: current.ID},
			},
			Spec: corev1.PodSpec{NodeName: "target-node"},
		}, metav1.CreateOptions{})

		return createErr
	}

	if err := fixture.service.ResumePodWorkload(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != domain.PhaseCompleted ||
		session.Spec.Workload().Pod.UID != types.UID("new-pod-uid") {
		t.Fatalf("phase=%s pod=%+v", session.Status.Phase, session.Spec.Workload().Pod)
	}

	if got := fixture.store.podUIDUpdates[len(fixture.store.podUIDUpdates)-1]; got != types.UID(
		"new-pod-uid",
	) {
		t.Fatalf("last persisted Pod UID=%s", got)
	}
}

func TestResumePodWorkloadRejectsReplacedStandalonePodAfterControllerResume(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseActivated
	_ = session.Spec.SetWorkload(domain.WorkloadSpec{
		Adapter: domain.WorkloadStandalone,
		Pod:     domain.ObjectReference{Namespace: "app", Name: "application", UID: "old-pod-uid"},
	})

	markNodeReady(t, fixture.client, "target-node")
	createActiveDestinationStorage(t, fixture, session)
	fixture.controller.resumeHook = func(ctx context.Context, current *domain.Session) error {
		current.Spec.WorkloadPtr().Pod.UID = "expected-resumed-uid"
		_, err := fixture.client.CoreV1().Pods("app").Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "app", Name: "application", UID: "replacement-uid",
				Annotations: map[string]string{kube.SessionKey: current.ID},
			},
			Spec: corev1.PodSpec{NodeName: "target-node"},
		}, metav1.CreateOptions{})

		return err
	}

	err := fixture.service.ResumePodWorkload(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if session.Status.Phase != domain.PhaseFailed ||
		session.Status.ResumeFrom != domain.PhaseResuming {
		t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
	}
}

func TestRollbackMultiVolumeRunsInReverseAndRecoversFailure(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)
	createActiveDestinationStorage(t, fixture, session)
	session.Status.Phase = domain.PhaseCompleted
	session.Status.History = append(
		session.Status.History,
		domain.HistoryEntry{Phase: domain.PhaseCompleted, Time: metav1.Now()},
	)
	fixture.switcher.rollbackErr["data"] = 1

	err := fixture.service.rollbackWorkflowForTest(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if session.Status.Phase != domain.PhaseFailed ||
		session.Status.ResumeFrom != domain.PhaseRollingBack {
		t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
	}

	if err := fixture.service.resumeWorkflowForTest(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != domain.PhaseRolledBack {
		t.Fatalf("phase=%s", session.Status.Phase)
	}

	if want := []string{
		"logs",
		"data",
		"logs",
		"data",
	}; !slices.Equal(
		fixture.switcher.rollbackCalls,
		want,
	) {
		t.Fatalf("rollback calls=%v want=%v", fixture.switcher.rollbackCalls, want)
	}

	if fixture.controller.pauses != 2 || fixture.controller.resumes != 1 {
		t.Fatalf(
			"pause calls=%d resume calls=%d",
			fixture.controller.pauses,
			fixture.controller.resumes,
		)
	}

	before := len(fixture.switcher.rollbackCalls)
	if err := fixture.service.validateRollbackWorkflowForTest(
		context.Background(),
		session,
	); err != nil {
		t.Fatal(err)
	}

	if err := fixture.service.rollbackWorkflowForTest(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if len(fixture.switcher.rollbackCalls) != before {
		t.Fatalf("rolled-back session repeated switch calls=%v", fixture.switcher.rollbackCalls)
	}
}

func TestRollbackPreservesRunningOriginAcrossRepeatedFailures(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	createActiveDestinationStorage(t, fixture, session)
	session.Status.Phase = domain.PhaseCompleted
	session.Status.History = append(
		session.Status.History,
		domain.HistoryEntry{Phase: domain.PhaseCompleted, Time: metav1.Now()},
	)
	fixture.switcher.rollbackErr["data"] = 1

	if err := fixture.service.rollbackWorkflowForTest(context.Background(), session); err == nil {
		t.Fatal("first rollback unexpectedly succeeded")
	}

	fixture.controller.pauseHook = func(*domain.Session) error {
		if fixture.controller.pauses == 2 {
			return domain.NewError(
				domain.ErrorPrecondition,
				"pause workload",
				"injected second pause failure",
			)
		}

		return nil
	}
	if err := fixture.service.rollbackWorkflowForTest(context.Background(), session); err == nil {
		t.Fatal("second rollback unexpectedly succeeded")
	}

	if err := fixture.service.rollbackWorkflowForTest(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != domain.PhaseRolledBack || fixture.controller.pauses != 3 {
		t.Fatalf(
			"phase=%s pause calls=%d",
			session.Status.Phase,
			fixture.controller.pauses,
		)
	}
}

func TestRollbackRejectsSourceIdentityDriftBeforeResumingWorkload(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	createActiveDestinationStorage(t, fixture, session)
	session.Status.Phase = domain.PhaseCompleted
	fixture.switcher.rollbackHook = func(ctx context.Context, volume *domain.VolumeSpec) error {
		pv, err := fixture.client.CoreV1().
			PersistentVolumes().
			Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		pv.UID = types.UID("replacement-source-pv-uid")
		_, err = fixture.client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})

		return err
	}

	err := fixture.service.rollbackWorkflowForTest(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if fixture.controller.resumes != 0 || session.Status.Phase != domain.PhaseFailed ||
		session.Status.ResumeFrom != domain.PhaseRollingBack {
		t.Fatalf(
			"resumes=%d phase=%s resumeFrom=%s",
			fixture.controller.resumes,
			session.Status.Phase,
			session.Status.ResumeFrom,
		)
	}
}

func TestRollbackCheckpointsCurrentControllerPodsBeforePause(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseCompleted
	createActiveDestinationStorage(t, fixture, session)

	replicas, ordinal := int32(1), int32(0)
	oldRef := domain.ObjectReference{
		Namespace: "app",
		Name:      "database-0",
		UID:       types.UID("old-pod-uid"),
	}

	currentRef := domain.ObjectReference{
		Namespace:       "app",
		Name:            "database-0",
		UID:             types.UID("current-pod-uid"),
		ResourceVersion: "99",
	}

	if err := session.Spec.SetWorkload(domain.WorkloadSpec{
		Adapter: domain.WorkloadStatefulSet,
		Pod:     oldRef,
		Controller: domain.ObjectReference{
			APIVersion: domain.AppsAPIVersion,
			Kind:       domain.KindStatefulSet,
			Namespace:  "app",
			Name:       "database",
			UID:        types.UID("statefulset-uid"),
		},
		OriginalReplicas: &replicas,
		Ordinal:          &ordinal,
		AffectedPods:     []domain.ObjectReference{oldRef},
	}); err != nil {
		t.Fatal(err)
	}

	fixture.controller.currentRollbackPods = []domain.ObjectReference{currentRef}
	fixture.controller.pauseHook = func(current *domain.Session) error {
		t.Helper()

		workload := current.Spec.Workload()
		if workload.Pod != currentRef || len(workload.AffectedPods) != 1 ||
			workload.AffectedPods[0] != currentRef {
			t.Fatalf("pause saw stale workload Pods: %+v", workload)
		}

		if len(fixture.store.podUIDUpdates) == 0 ||
			fixture.store.podUIDUpdates[len(fixture.store.podUIDUpdates)-1] != currentRef.UID {
			t.Fatal("current Pod identity was not checkpointed before pause")
		}

		return nil
	}

	if err := fixture.service.rollbackWorkflowForTest(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != domain.PhaseRolledBack {
		t.Fatalf("phase=%s", session.Status.Phase)
	}
}

func TestRefreshRollbackPodReferencesPreservesStableNames(t *testing.T) {
	oldPrimary := domain.ObjectReference{Namespace: "app", Name: "database-0", UID: "old-0"}
	oldReplica := domain.ObjectReference{Namespace: "app", Name: "database-1", UID: "old-1"}
	currentPrimary := domain.ObjectReference{
		Namespace: "app", Name: "database-0", UID: "current-0",
	}
	workload := domain.WorkloadSpec{
		Adapter:      domain.WorkloadStatefulSet,
		Pod:          oldPrimary,
		AffectedPods: []domain.ObjectReference{oldPrimary, oldReplica},
	}

	if !refreshRollbackPodReferences(&workload, []domain.ObjectReference{currentPrimary}) {
		t.Fatal("replacement Pod was not detected")
	}

	if workload.Pod != currentPrimary || workload.AffectedPods[0] != currentPrimary ||
		workload.AffectedPods[1] != oldReplica {
		t.Fatalf("stable Pod references=%+v", workload)
	}
}

func TestRefreshRollbackPodReferencesReplacesGeneratedNames(t *testing.T) {
	oldRef := domain.ObjectReference{Namespace: "app", Name: "web-old", UID: "old"}
	current := []domain.ObjectReference{
		{Namespace: "app", Name: "web-new-a", UID: "new-a"},
		{Namespace: "app", Name: "web-old", UID: "replacement"},
	}
	workload := domain.WorkloadSpec{
		Adapter:      domain.WorkloadDeployment,
		Pod:          oldRef,
		AffectedPods: []domain.ObjectReference{oldRef},
	}

	if !refreshRollbackPodReferences(&workload, current) {
		t.Fatal("replacement Deployment Pods were not detected")
	}

	if workload.Pod != current[0] || !slices.Equal(workload.AffectedPods, current) {
		t.Fatalf("generated Pod references=%+v", workload)
	}
}

func TestRollbackRejectsRunningStateConflictsBeforePausingWorkload(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*testing.T, *recoveryFixture, *domain.Session)
	}{
		{
			name: "active PV identity changed",
			configure: func(t *testing.T, fixture *recoveryFixture, session *domain.Session) {
				t.Helper()

				pv, err := fixture.client.CoreV1().
					PersistentVolumes().
					Get(context.Background(), session.Spec.Volumes[0].DestinationPV.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}

				pv.UID = types.UID("replacement-destination-pv-uid")
				if _, err := fixture.client.CoreV1().
					PersistentVolumes().
					Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "consumer outside pause scope",
			configure: func(t *testing.T, fixture *recoveryFixture, _ *domain.Session) {
				t.Helper()

				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "external-writer"},
					Spec: corev1.PodSpec{Volumes: []corev1.Volume{
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
				}
				if _, err := fixture.client.CoreV1().
					Pods(pod.Namespace).
					Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			session := appTestSession()
			session.Status.Phase = domain.PhaseCompleted
			createActiveDestinationStorage(t, fixture, session)
			test.configure(t, fixture, session)

			err := fixture.service.rollbackWorkflowForTest(context.Background(), session)
			if category := domain.CategoryOf(
				err,
			); category != domain.ErrorConflict &&
				category != domain.ErrorPrecondition {
				t.Fatalf("category=%s error=%v", category, err)
			}

			if fixture.controller.pauses != 0 || len(fixture.switcher.rollbackCalls) != 0 ||
				session.Status.Phase != domain.PhaseCompleted {
				t.Fatalf(
					"pauses=%d rollbackCalls=%v phase=%s",
					fixture.controller.pauses,
					fixture.switcher.rollbackCalls,
					session.Status.Phase,
				)
			}
		})
	}
}

func TestRollbackFromPausedCutoverPreflightsAllVolumesBeforeSwitchingAnyPVC(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)
	session.Status.Phase = domain.PhaseActivated
	createActiveDestinationStorage(t, fixture, session)

	pv, err := fixture.client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), session.Spec.Volumes[0].DestinationPV.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pv.UID = types.UID("replacement-destination-pv-uid")
	if _, err := fixture.client.CoreV1().
		PersistentVolumes().
		Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	err = fixture.service.rollbackWorkflowForTest(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if fixture.controller.pauses != 0 || len(fixture.switcher.rollbackCalls) != 0 ||
		session.Status.Phase != domain.PhaseActivated {
		t.Fatalf(
			"pauses=%d rollback calls=%v phase=%s",
			fixture.controller.pauses,
			fixture.switcher.rollbackCalls,
			session.Status.Phase,
		)
	}
}

func TestRollbackRejectsFailuresBeforeCutover(t *testing.T) {
	for _, resumeFrom := range []domain.Phase{domain.PhaseReserving, domain.PhaseWarmCopying, domain.PhasePausing, domain.PhaseFinalSyncing} {
		t.Run(string(resumeFrom), func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			session := appTestSession()
			session.Status.Phase = domain.PhaseFailed
			session.Status.ResumeFrom = resumeFrom

			err := fixture.service.rollbackWorkflowForTest(context.Background(), session)
			if domain.CategoryOf(err) != domain.ErrorPrecondition {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}

			if len(fixture.switcher.rollbackCalls) != 0 || fixture.controller.pauses != 0 {
				t.Fatalf(
					"rollback calls=%v pauses=%d",
					fixture.switcher.rollbackCalls,
					fixture.controller.pauses,
				)
			}
		})
	}
}

func TestRenameAndRollbackPreservePVCIdentityDirection(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	setSessionOperation(session, domain.OperationRename)
	session.Spec.Volumes[0].DestinationPVC = domain.ObjectReference{
		Namespace: "app",
		Name:      "renamed-data",
	}
	createSourceStorage(t, fixture, session)

	if err := fixture.service.Rename(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != domain.PhaseCompleted ||
		session.Status.Volumes[0].Activation.ActivePVC.UID != types.UID("renamed-pvc-uid") {
		t.Fatalf(
			"phase=%s activation=%+v",
			session.Status.Phase,
			session.Status.Volumes[0].Activation,
		)
	}

	if err := fixture.service.rollbackWorkflowForTest(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != domain.PhaseRolledBack || len(fixture.switcher.renameCalls) != 2 {
		t.Fatalf(
			"phase=%s rename calls=%d",
			session.Status.Phase,
			len(fixture.switcher.renameCalls),
		)
	}

	reverse := fixture.switcher.renameCalls[1]
	if reverse.SourcePVC.Name != "renamed-data" ||
		reverse.SourcePVC.UID != types.UID("renamed-pvc-uid") {
		t.Fatalf("rollback source=%+v", reverse.SourcePVC)
	}

	if reverse.DestinationPVC.Name != "data" || reverse.DestinationPVC.UID != "" ||
		reverse.DestinationPVC.ResourceVersion != "" {
		t.Fatalf("rollback destination=%+v", reverse.DestinationPVC)
	}

	if session.Status.Volumes[0].Activation.RolledBackAt == nil ||
		session.Status.Volumes[0].Activation.ActivePVC.Name != "data" {
		t.Fatalf("rollback activation=%+v", session.Status.Volumes[0].Activation)
	}
}

func TestDryRunRenameRollbackChecksCurrentActivePVC(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	setSessionOperation(session, domain.OperationRename)
	session.Spec.Volumes[0].DestinationPVC = domain.ObjectReference{
		Namespace: "app",
		Name:      "renamed-data",
	}
	session.Status.Phase = domain.PhaseCompleted

	session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
		APIVersion: "v1",
		Kind:       "PersistentVolumeClaim",
		Namespace:  "app",
		Name:       "renamed-data",
		UID:        types.UID("renamed-pvc-uid"),
	}
	if _, err := fixture.client.CoreV1().
		PersistentVolumeClaims("app").
		Create(context.Background(), &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "app",
				Name:        "renamed-data",
				UID:         types.UID("renamed-pvc-uid"),
				Annotations: map[string]string{kube.SessionKey: session.ID},
			},
			Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-source"},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := fixture.client.CoreV1().
		PersistentVolumes().
		Create(context.Background(), &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-source", UID: types.UID("source-pv-uid")},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{
					Namespace: "app",
					Name:      "renamed-data",
					UID:       types.UID("renamed-pvc-uid"),
				},
			},
		}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := fixture.service.validateRollbackWorkflowForTest(
		context.Background(),
		session,
	); err != nil {
		t.Fatal(err)
	}

	if len(fixture.switcher.offlineCalls) != 1 ||
		fixture.switcher.offlineCalls[0] != "renamed-data" {
		t.Fatalf("offline calls=%v", fixture.switcher.offlineCalls)
	}
}

func TestDryRunRenameResumeAcceptsOnlyOneValidEndpoint(t *testing.T) {
	t.Run("session-owned destination", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		setSessionOperation(session, domain.OperationRename)
		session.Spec.Volumes[0].DestinationPVC = domain.ObjectReference{
			Namespace: "app",
			Name:      "renamed-data",
			UID:       types.UID("renamed-pvc-uid"),
		}

		session.Status.Phase = domain.PhaseRenaming
		if _, err := fixture.client.CoreV1().
			PersistentVolumeClaims("app").
			Create(context.Background(), &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   "app",
					Name:        "renamed-data",
					UID:         types.UID("renamed-pvc-uid"),
					Annotations: map[string]string{kube.SessionKey: session.ID},
				},
				Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-source"},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
			}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}

		if _, err := fixture.client.CoreV1().
			PersistentVolumes().
			Create(context.Background(), &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: "pv-source", UID: types.UID("source-pv-uid")},
				Spec: corev1.PersistentVolumeSpec{
					ClaimRef: &corev1.ObjectReference{
						Namespace: "app",
						Name:      "renamed-data",
						UID:       types.UID("renamed-pvc-uid"),
					},
				},
			}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}

		if err := fixture.service.validateResumeWorkflowForTest(
			context.Background(),
			session,
		); err != nil {
			t.Fatal(err)
		}

		if want := []string{"renamed-data"}; !slices.Equal(fixture.switcher.offlineCalls, want) {
			t.Fatalf("offline calls=%v want=%v", fixture.switcher.offlineCalls, want)
		}
	})

	t.Run("both endpoints", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		setSessionOperation(session, domain.OperationRename)
		session.Spec.Volumes[0].DestinationPVC = domain.ObjectReference{
			Namespace: "app",
			Name:      "renamed-data",
		}
		session.Status.Phase = domain.PhaseRenaming
		createSourceStorage(t, fixture, session)

		if _, err := fixture.client.CoreV1().
			PersistentVolumeClaims("app").
			Create(context.Background(), &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   "app",
					Name:        "renamed-data",
					UID:         types.UID("renamed-pvc-uid"),
					Annotations: map[string]string{kube.SessionKey: session.ID},
				},
			}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}

		err := fixture.service.validateResumeWorkflowForTest(context.Background(), session)
		if domain.CategoryOf(err) != domain.ErrorConflict ||
			len(fixture.switcher.offlineCalls) != 0 {
			t.Fatalf(
				"category=%s offline=%v error=%v",
				domain.CategoryOf(err),
				fixture.switcher.offlineCalls,
				err,
			)
		}
	})
}

func TestPauseRejectsNonOrchestratedSessionBeforePhaseMutation(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	setSessionOperation(session, domain.OperationCopy)
	session.Status.Phase = domain.PhaseWarmCopied

	historyBefore := len(session.Status.History)
	if err := fixture.service.PodPause(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if session.Status.Phase != domain.PhaseWarmCopied ||
		len(session.Status.History) != historyBefore ||
		fixture.controller.pauses != 0 {
		t.Fatalf(
			"phase=%s history=%v pauses=%d",
			session.Status.Phase,
			session.Status.History,
			fixture.controller.pauses,
		)
	}
}

func TestStagePreconditionsPreserveSessionState(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Service, *domain.Session) error
	}{
		{name: "negative warm passes", run: func(service *Service, session *domain.Session) error {
			session.Spec.MigratePod.PrecopyPasses = -1
			return service.MigratePod(context.Background(), session)
		}},
		{
			name: "warm copy from planned",
			run: func(service *Service, session *domain.Session) error {
				return service.WarmCopy(context.Background(), session)
			},
		},
		{
			name: "final sync from planned",
			run: func(service *Service, session *domain.Session) error {
				return service.PodFinalSync(context.Background(), session)
			},
		},
		{name: "activate from planned", run: func(service *Service, session *domain.Session) error {
			return service.PodActivate(context.Background(), session)
		}},
		{
			name: "resume workload from planned",
			run: func(service *Service, session *domain.Session) error {
				return service.ResumePodWorkload(context.Background(), session)
			},
		},
		{name: "rollback from planned", run: func(service *Service, session *domain.Session) error {
			return service.rollbackWorkflowForTest(context.Background(), session)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			session := appTestSession()

			err := test.run(fixture.service, session)
			if category := domain.CategoryOf(
				err,
			); category != domain.ErrorPrecondition &&
				category != domain.ErrorValidation {
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

		if err := service.PodPause(context.Background(), session); err != nil {
			t.Fatal(err)
		}
	}
	finalSync := func(t *testing.T, service *Service, session *domain.Session) {
		t.Helper()
		pause(t, service, session)

		if err := service.PodFinalSync(context.Background(), session); err != nil {
			t.Fatal(err)
		}
	}
	activate := func(t *testing.T, service *Service, session *domain.Session) {
		t.Helper()
		finalSync(t, service, session)

		if err := service.PodActivate(context.Background(), session); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name  string
		setup setupFunc
	}{
		{name: "planned"},
		{name: "reserving", setup: func(t *testing.T, _ *Service, session *domain.Session) {
			t.Helper()
			transitionThrough(t, session, domain.PhaseReserving)
		}},
		{name: "reserved", setup: reserve},
		{
			name: "warm copying",
			setup: func(t *testing.T, service *Service, session *domain.Session) {
				t.Helper()

				reserve(t, service, session)
				transitionThrough(t, session, domain.PhaseWarmCopying)
			},
		},
		{name: "warm copied", setup: func(t *testing.T, service *Service, session *domain.Session) {
			t.Helper()

			reserve(t, service, session)

			if err := service.WarmCopy(context.Background(), session); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "pausing", setup: func(t *testing.T, service *Service, session *domain.Session) {
			t.Helper()

			reserve(t, service, session)
			transitionThrough(t, session, domain.PhasePausing)
		}},
		{name: "paused", setup: pause},
		{
			name: "final syncing",
			setup: func(t *testing.T, service *Service, session *domain.Session) {
				t.Helper()

				pause(t, service, session)
				transitionThrough(t, session, domain.PhaseFinalSyncing)
			},
		},
		{name: "final synced", setup: finalSync},
		{name: "activating", setup: func(t *testing.T, service *Service, session *domain.Session) {
			t.Helper()

			finalSync(t, service, session)
			transitionThrough(t, session, domain.PhaseActivating)
		}},
		{name: "activated", setup: activate},
		{name: "resuming", setup: func(t *testing.T, service *Service, session *domain.Session) {
			t.Helper()

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

			if err := service.resumeWorkflowForTest(context.Background(), session); err != nil {
				t.Fatal(err)
			}

			if session.Status.Phase != domain.PhaseCompleted {
				t.Fatalf("phase=%s", session.Status.Phase)
			}
		})
	}
}

func TestResumeFailedCompositePausingContinuesFromPause(t *testing.T) {
	service, session, controller, _ := appTestService(t, &fakeCopier{})
	if err := service.Reserve(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	transitionThrough(t, session, domain.PhaseWarmCopying, domain.PhaseWarmCopied)
	session.Status.Phase = domain.PhaseFailed
	session.Status.ResumeFrom = domain.PhasePausing

	if err := service.resumeWorkflowForTest(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != domain.PhaseCompleted {
		t.Fatalf("phase=%s", session.Status.Phase)
	}

	if controller.paused != 1 || controller.resumed != 1 {
		t.Fatalf("pause calls=%d resume calls=%d", controller.paused, controller.resumed)
	}
}

func TestResumeSessionDispatchesSingleOperationStages(t *testing.T) {
	tests := []struct {
		name      string
		operation domain.Operation
		phase     domain.Phase
		want      domain.Phase
	}{
		{
			name:      "copy",
			operation: domain.OperationCopy,
			phase:     domain.PhaseWarmCopying,
			want:      domain.PhaseWarmCopied,
		},
		{
			name:      "rename",
			operation: domain.OperationRename,
			phase:     domain.PhaseRenaming,
			want:      domain.PhaseCompleted,
		},
		{
			name:      "move",
			operation: domain.OperationMove,
			phase:     domain.PhaseMoving,
			want:      domain.PhaseCompleted,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			session := appTestSession()
			setSessionOperation(session, test.operation)

			session.Status.Phase = test.phase
			if test.operation.RebindsPVC() {
				createSourceStorage(t, fixture, session)
			}

			if err := fixture.service.resumeWorkflowForTest(
				context.Background(),
				session,
			); err != nil {
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
		{
			name:      "reserve from planned",
			operation: domain.OperationReserve,
			phase:     domain.PhasePlanned,
			want:      domain.PhaseReserved,
		},
		{
			name:      "copy from reserved",
			operation: domain.OperationCopy,
			phase:     domain.PhaseReserved,
			want:      domain.PhaseWarmCopied,
		},
		{
			name:      "rename from planned",
			operation: domain.OperationRename,
			phase:     domain.PhasePlanned,
			want:      domain.PhaseCompleted,
		},
		{
			name:      "move from planned",
			operation: domain.OperationMove,
			phase:     domain.PhasePlanned,
			want:      domain.PhaseCompleted,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			session := appTestSession()
			setSessionOperation(session, test.operation)

			session.Status.Phase = test.phase
			if test.operation.RebindsPVC() {
				createSourceStorage(t, fixture, session)
			}

			if err := fixture.service.resumeWorkflowForTest(
				context.Background(),
				session,
			); err != nil {
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
	if err := fixture.service.resumeWorkflowForTest(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("unknown phase category=%s error=%v", domain.CategoryOf(err), err)
	}

	fixture = newRecoveryFixture(t)
	session = appTestSession()
	setSessionOperation(session, domain.OperationRename)

	session.Status.Phase = domain.PhaseReserved
	if err := fixture.service.resumeWorkflowForTest(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("mismatched phase category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestValidateResumeUsesOperationSpecificChecks(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	setSessionOperation(session, domain.OperationCopy)
	session.Status.Phase = domain.PhaseReserved

	fixture.reserver.validationFailures = map[string]error{
		"data": domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			"destination PVC UID changed",
		),
	}
	if err := fixture.service.validateResumeWorkflowForTest(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("copy reservation category=%s error=%v", domain.CategoryOf(err), err)
	}

	fixture = newRecoveryFixture(t)
	session = appTestSession()
	setSessionOperation(session, domain.OperationCopy)
	session.Status.Phase = domain.PhaseReserved

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "consumer"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
		}}}},
	}
	if _, err := fixture.client.CoreV1().
		Pods("app").
		Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := fixture.service.validateResumeWorkflowForTest(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("copy consumer category=%s error=%v", domain.CategoryOf(err), err)
	}

	fixture = newRecoveryFixture(t)
	session = appTestSession()
	setSessionOperation(session, domain.OperationRename)
	session.Status.Phase = domain.PhasePlanned
	createSourceStorage(t, fixture, session)

	fixture.switcher.offlineErr = domain.NewError(
		domain.ErrorPrecondition,
		"verify PVC offline",
		"source PVC has an active consumer",
	)
	if err := fixture.service.validateResumeWorkflowForTest(
		context.Background(),
		session,
	); domain.CategoryOf(err) != domain.ErrorPrecondition ||
		len(fixture.switcher.offlineCalls) != 1 {
		t.Fatalf(
			"rename offline category=%s calls=%v error=%v",
			domain.CategoryOf(err),
			fixture.switcher.offlineCalls,
			err,
		)
	}
}

func TestValidateResumePropagatesWorkloadReplicaConflict(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseFailed
	session.Status.ResumeFrom = domain.PhaseResuming
	createActiveDestinationStorage(t, fixture, session)
	fixture.controller.validateResumeErr = domain.NewError(
		domain.ErrorConflict,
		"resume Deployment",
		"Deployment app/web replicas changed to 2 while restoring 1 replicas",
	)

	err := fixture.service.validateResumeWorkflowForTest(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "replicas changed to 2 while restoring 1") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if fixture.controller.resumes != 0 || fixture.store.updates != 0 {
		t.Fatalf(
			"dry-run mutated state: resumes=%d updates=%d",
			fixture.controller.resumes,
			fixture.store.updates,
		)
	}
}

func TestValidateResumeChecksReservationBeforeContinuingPause(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhasePausing
	fixture.reserver.validationFailures = map[string]error{
		"data": domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			"destination PVC UID changed",
		),
	}

	err := fixture.service.validateResumeWorkflowForTest(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if fixture.controller.pauses != 0 {
		t.Fatalf("pause calls=%d", fixture.controller.pauses)
	}
}

func TestPodPauseAndFinalSyncChecksReservationBeforePausing(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	fixture.reserver.validationFailures = map[string]error{
		"data": domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			"destination PVC UID changed",
		),
	}

	err := fixture.service.PodPauseAndFinalSync(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if fixture.controller.pauses != 0 || fixture.controller.verifies != 0 {
		t.Fatalf(
			"controller calls pause=%d verify=%d",
			fixture.controller.pauses,
			fixture.controller.verifies,
		)
	}
}

func TestPauseChecksReservationBeforeWorkloadMutation(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseReserved
	fixture.reserver.validationFailures = map[string]error{
		"data": domain.NewError(
			domain.ErrorConflict,
			"reserve volume",
			"destination PVC UID changed",
		),
	}

	err := fixture.service.PodPause(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if fixture.controller.pauses != 0 || fixture.store.updates != 0 ||
		session.Status.Phase != domain.PhaseReserved {
		t.Fatalf(
			"pause mutated before reservation validation: pauses=%d updates=%d phase=%s",
			fixture.controller.pauses,
			fixture.store.updates,
			session.Status.Phase,
		)
	}
}

func TestRenameChecksLiveEndpointsBeforeSessionMutation(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	setSessionOperation(session, domain.OperationRename)
	session.Spec.Volumes[0].DestinationPVC = domain.ObjectReference{
		Namespace: "app",
		Name:      "renamed-data",
	}
	createSourceStorage(t, fixture, session)
	fixture.switcher.offlineErr = domain.NewError(
		domain.ErrorPrecondition,
		"verify PVC offline",
		"source PVC has an active consumer",
	)

	err := fixture.service.Rename(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if len(fixture.switcher.renameCalls) != 0 || fixture.store.updates != 0 ||
		session.Status.Phase != domain.PhasePlanned {
		t.Fatalf(
			"rename mutated before endpoint validation: calls=%d updates=%d phase=%s",
			len(fixture.switcher.renameCalls),
			fixture.store.updates,
			session.Status.Phase,
		)
	}
}

func TestRenameRollbackChecksLiveEndpointsBeforeSessionMutation(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	setSessionOperation(session, domain.OperationRename)
	session.Spec.Volumes[0].DestinationPVC = domain.ObjectReference{
		Namespace: "app",
		Name:      "renamed-data",
	}
	session.Status.Phase = domain.PhaseCompleted
	session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
		Namespace: "app",
		Name:      "renamed-data",
		UID:       types.UID("renamed-pvc-uid"),
	}
	createSourceStorage(t, fixture, session)

	if _, err := fixture.client.CoreV1().
		PersistentVolumeClaims("app").
		Create(context.Background(), &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "app",
				Name:        "renamed-data",
				UID:         types.UID("renamed-pvc-uid"),
				Annotations: map[string]string{kube.SessionKey: session.ID},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName: session.Spec.Volumes[0].SourcePV.Name,
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	err := fixture.service.rollbackWorkflowForTest(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if len(fixture.switcher.renameCalls) != 0 || fixture.store.updates != 0 ||
		session.Status.Phase != domain.PhaseCompleted {
		t.Fatalf(
			"rollback mutated before endpoint validation: calls=%d updates=%d phase=%s",
			len(fixture.switcher.renameCalls),
			fixture.store.updates,
			session.Status.Phase,
		)
	}
}

func TestActivateRecoveryValidatesEveryVolumeBeforeMutation(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)
	session.Status.Phase = domain.PhaseActivating

	completed := metav1.Now()
	for index := range session.Status.Volumes {
		session.Status.Volumes[index].Reserved = true
		session.Spec.Volumes[index].DestinationPVC.UID = types.UID(
			"destination-pvc-uid-" + session.Spec.Volumes[index].SourcePVC.Name,
		)
		session.Spec.Volumes[index].DestinationPV = domain.ObjectReference{
			Name: "destination-pv-" + session.Spec.Volumes[index].SourcePVC.Name,
			UID:  types.UID("destination-pv-uid-" + session.Spec.Volumes[index].SourcePVC.Name),
		}
		session.Status.Volumes[index].Sync.FinalCompletedAt = &completed
	}

	fixture.switcher.offlineErrs = map[string]error{
		"logs": domain.NewError(
			domain.ErrorConflict,
			"verify PVC offline",
			"second volume binding changed",
		),
	}

	err := fixture.service.PodActivate(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if len(fixture.switcher.activateCalls) != 0 || fixture.store.updates != 0 ||
		session.Status.Phase != domain.PhaseActivating {
		t.Fatalf(
			"activation mutated before batch validation: calls=%v updates=%d phase=%s",
			fixture.switcher.activateCalls,
			fixture.store.updates,
			session.Status.Phase,
		)
	}
}

func TestRollbackRecoveryValidatesEveryVolumeBeforeMutation(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	addSecondVolume(session)
	session.Status.Phase = domain.PhaseFailed
	session.Status.ResumeFrom = domain.PhaseRollingBack
	session.Status.History = []domain.HistoryEntry{
		{Phase: domain.PhasePlanned, Time: metav1.Now()},
		{Phase: domain.PhaseFinalSynced, Time: metav1.Now()},
		{Phase: domain.PhaseRollingBack, Time: metav1.Now()},
		{Phase: domain.PhaseFailed, Time: metav1.Now()},
	}
	fixture.switcher.offlineErrs = map[string]error{
		"logs": domain.NewError(
			domain.ErrorConflict,
			"verify PVC offline",
			"second volume binding changed",
		),
	}

	err := fixture.service.rollbackWorkflowForTest(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if len(fixture.switcher.rollbackCalls) != 0 || fixture.store.updates != 0 ||
		session.Status.Phase != domain.PhaseFailed {
		t.Fatalf(
			"rollback mutated before batch validation: calls=%v updates=%d phase=%s",
			fixture.switcher.rollbackCalls,
			fixture.store.updates,
			session.Status.Phase,
		)
	}
}

func TestResumeSessionContinuesActivatedSingleOperationSession(t *testing.T) {
	service, session, _, _ := appTestService(t, &fakeCopier{})
	if err := service.Reserve(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if err := service.PodPause(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if err := service.PodFinalSync(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if err := service.PodActivate(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if err := service.PodActivate(context.Background(), session); err != nil {
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
		if err := fixture.service.resumeWorkflowForTest(context.Background(), session); err != nil {
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
		createActiveDestinationStorage(t, fixture, session)

		if err := fixture.service.resumeWorkflowForTest(context.Background(), session); err != nil {
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
			if err := fixture.service.resumeWorkflowForTest(
				context.Background(),
				session,
			); err != nil {
				t.Fatal(err)
			}

			if fixture.store.updates != 0 || len(fixture.copier.requests) != 0 {
				t.Fatalf(
					"terminal resume updates=%d copy requests=%d",
					fixture.store.updates,
					len(fixture.copier.requests),
				)
			}
		})
	}

	t.Run("unknown phase", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		session.Status.Phase = domain.Phase("Unknown")

		err := fixture.service.resumeWorkflowForTest(context.Background(), session)
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
}

func TestAbortRetryAfterResumeFailureStillResumesPausedWorkload(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	createSourceStorage(t, fixture, session)
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
	if err := fixture.service.resumeWorkflowForTest(context.Background(), session); err != nil {
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

	if err := fixture.service.PodFinalSync(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if want := []string{"logs"}; !slices.Equal(requestSources(fixture.copier.requests), want) {
		t.Fatalf("recovery sources=%v want=%v", requestSources(fixture.copier.requests), want)
	}

	if err := fixture.service.PodFinalSync(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if want := []string{
		"logs",
		"data",
		"logs",
	}; !slices.Equal(
		requestSources(fixture.copier.requests),
		want,
	) {
		t.Fatalf("final-sync sources=%v want=%v", requestSources(fixture.copier.requests), want)
	}

	for index, request := range fixture.copier.requests {
		if request.Mode != copyengine.ModeFinal || !request.VerifyChecksum {
			t.Fatalf("request %d mode=%s checksum=%v", index, request.Mode, request.VerifyChecksum)
		}
	}

	for index := range session.Status.Volumes {
		if session.Status.Volumes[index].Sync.FinalCompletedAt == nil ||
			!session.Status.Volumes[index].Sync.ChecksumVerified {
			t.Fatalf("volume %d sync=%+v", index, session.Status.Volumes[index].Sync)
		}
	}
}

func TestHelmSchedulingValuesIncludeNodeTolerations(t *testing.T) {
	fixture := newRecoveryFixture(t)

	source, err := fixture.client.CoreV1().
		Nodes().
		Get(context.Background(), "source-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	source.Spec.Taints = []corev1.Taint{
		{Key: "dedicated", Value: "storage", Effect: corev1.TaintEffectNoSchedule},
		{Key: "draining", Effect: corev1.TaintEffectNoExecute},
		{Key: "preference", Effect: corev1.TaintEffectPreferNoSchedule},
	}
	if _, err := fixture.client.CoreV1().
		Nodes().
		Update(context.Background(), source, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	values, err := fixture.service.helmSchedulingValues(context.Background(), appTestSession(), "")
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

		_, err := fixture.service.helmSchedulingValues(context.Background(), session, "")
		if domain.CategoryOf(err) != domain.ErrorKubernetes {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})

	t.Run("hostname label missing", func(t *testing.T) {
		fixture := newRecoveryFixture(t)

		node, err := fixture.client.CoreV1().
			Nodes().
			Get(context.Background(), "target-node", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}

		node.Labels = nil
		if _, err := fixture.client.CoreV1().
			Nodes().
			Update(context.Background(), node, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}

		_, err = fixture.service.helmSchedulingValues(context.Background(), appTestSession(), "")
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
}

func TestHelmSchedulingValuesRejectsEmptyNodeObjects(t *testing.T) {
	for _, test := range []struct {
		name   string
		object runtime.Object
	}{
		{name: "nil", object: nil},
		{name: "empty", object: &corev1.Node{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			testutil.MustType[*fake.Clientset](t, fixture.client).PrependReactor(
				"get",
				"nodes",
				func(clienttesting.Action) (bool, runtime.Object, error) {
					return true, test.object, nil
				},
			)

			_, err := fixture.service.helmSchedulingValues(
				context.Background(),
				appTestSession(),
				"",
			)
			if domain.CategoryOf(err) != domain.ErrorKubernetes {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}

			if !strings.Contains(err.Error(), "returned an empty object") {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestHelmSchedulingValuesLocalLetsPVTopologyPlaceBothSSHDPods(t *testing.T) {
	fixture := newRecoveryFixture(t)

	source, err := fixture.client.CoreV1().
		Nodes().
		Get(context.Background(), "source-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	source.Spec.Taints = []corev1.Taint{
		{Key: "source-storage", Value: "true", Effect: corev1.TaintEffectNoSchedule},
	}
	if _, err := fixture.client.CoreV1().
		Nodes().
		Update(context.Background(), source, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	target, err := fixture.client.CoreV1().
		Nodes().
		Get(context.Background(), "target-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	target.Spec.Taints = []corev1.Taint{
		{Key: "target-storage", Effect: corev1.TaintEffectNoExecute},
	}
	if _, err := fixture.client.CoreV1().
		Nodes().
		Update(context.Background(), target, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	session := appTestSession()
	session.Spec.WorkflowOptionsPtr().Strategies = []string{"local"}

	values, err := fixture.service.helmSchedulingValues(context.Background(), session, "")
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

func TestResumePodWorkloadFailsWhenActiveResourcesDoNotMatchPlan(t *testing.T) {
	t.Run("PVC points to source", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		session.Status.Phase = domain.PhaseActivated
		session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
			Name: "pv-destination",
			UID:  types.UID("destination-pv-uid"),
		}
		session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
			Namespace: "app",
			Name:      "data",
			UID:       types.UID("active-pvc-uid"),
		}

		_, err := fixture.client.CoreV1().
			PersistentVolumeClaims("app").
			Create(context.Background(), &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   "app",
					Name:        "data",
					UID:         types.UID("active-pvc-uid"),
					Annotations: map[string]string{kube.SessionKey: session.ID},
				},
				Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-source"},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
			}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}

		err = fixture.service.ResumePodWorkload(context.Background(), session)
		if domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}

		if fixture.controller.resumes != 0 || session.Status.Phase != domain.PhaseActivated {
			t.Fatalf("resumes=%d phase=%s", fixture.controller.resumes, session.Status.Phase)
		}
	})

	t.Run("Pod lands on another node", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		session.Status.Phase = domain.PhaseActivated
		_ = session.Spec.SetWorkload(
			domain.WorkloadSpec{
				Adapter: domain.WorkloadStandalone,
				Pod:     domain.ObjectReference{Namespace: "app", Name: "application"},
			},
		)

		markNodeReady(t, fixture.client, "target-node")

		session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
			Name: "pv-destination",
			UID:  types.UID("destination-pv-uid"),
		}
		session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
			Namespace: "app",
			Name:      "data",
			UID:       types.UID("active-pvc-uid"),
		}

		_, err := fixture.client.CoreV1().
			PersistentVolumeClaims("app").
			Create(context.Background(), &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   "app",
					Name:        "data",
					UID:         types.UID("active-pvc-uid"),
					Annotations: map[string]string{kube.SessionKey: session.ID},
				},
				Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
			}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}

		_, err = fixture.client.CoreV1().
			PersistentVolumes().
			Create(context.Background(), &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pv-destination",
					UID:  types.UID("destination-pv-uid"),
				},
				Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
					Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid"),
				}},
			}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}

		fixture.controller.resumeHook = func(ctx context.Context, current *domain.Session) error {
			current.Spec.WorkloadPtr().Pod.UID = "resumed-pod-uid"
			_, createErr := fixture.client.CoreV1().Pods("app").Create(ctx, &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   "app",
					Name:        "application",
					UID:         "resumed-pod-uid",
					Annotations: map[string]string{kube.SessionKey: current.ID},
				},
				Spec: corev1.PodSpec{NodeName: "source-node"},
			}, metav1.CreateOptions{})

			return createErr
		}

		err = fixture.service.ResumePodWorkload(context.Background(), session)
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
		}

		if session.Status.Phase != domain.PhaseFailed ||
			session.Status.ResumeFrom != domain.PhaseResuming {
			t.Fatalf("phase=%s resumeFrom=%s", session.Status.Phase, session.Status.ResumeFrom)
		}
	})

	t.Run("managed workload may land on another node", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		session.Status.Phase = domain.PhaseActivated
		_ = session.Spec.SetWorkload(domain.WorkloadSpec{
			Adapter: domain.WorkloadStatefulSet,
			Pod: domain.ObjectReference{
				Namespace: "app",
				Name:      "application",
				UID:       types.UID("application-uid"),
			},
			Controller: domain.ObjectReference{
				Namespace: "app",
				Name:      "database",
				UID:       types.UID("database-uid"),
			},
		})
		session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
			Name: "pv-destination",
			UID:  types.UID("destination-pv-uid"),
		}
		session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
			Namespace: "app",
			Name:      "data",
			UID:       types.UID("active-pvc-uid"),
		}

		_, err := fixture.client.CoreV1().
			PersistentVolumeClaims("app").
			Create(context.Background(), &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   "app",
					Name:        "data",
					UID:         types.UID("active-pvc-uid"),
					Annotations: map[string]string{kube.SessionKey: session.ID},
				},
				Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
			}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}

		_, err = fixture.client.CoreV1().
			PersistentVolumes().
			Create(context.Background(), &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pv-destination",
					UID:  types.UID("destination-pv-uid"),
				},
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

		if err := fixture.service.ResumePodWorkload(context.Background(), session); err != nil {
			t.Fatal(err)
		}

		if session.Status.Phase != domain.PhaseCompleted {
			t.Fatalf("phase=%s", session.Status.Phase)
		}
	})

	t.Run("managed workload does not require the tool target to be ready", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		session.Status.Phase = domain.PhaseResuming
		_ = session.Spec.SetWorkload(domain.WorkloadSpec{
			Adapter: domain.WorkloadStatefulSet,
			Pod: domain.ObjectReference{
				Namespace: "app",
				Name:      "application",
				UID:       types.UID("application-uid"),
			},
			Controller: domain.ObjectReference{
				Namespace: "app",
				Name:      "database",
				UID:       types.UID("database-uid"),
			},
		})
		session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
			Name: "pv-destination",
			UID:  types.UID("destination-pv-uid"),
		}
		session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
			Namespace: "app",
			Name:      "data",
			UID:       types.UID("active-pvc-uid"),
		}

		_, err := fixture.client.CoreV1().
			PersistentVolumeClaims("app").
			Create(context.Background(), &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   "app",
					Name:        "data",
					UID:         types.UID("active-pvc-uid"),
					Annotations: map[string]string{kube.SessionKey: session.ID},
				},
				Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
			}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}

		_, err = fixture.client.CoreV1().
			PersistentVolumes().
			Create(context.Background(), &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pv-destination",
					UID:  types.UID("destination-pv-uid"),
				},
				Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
					Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid"),
				}},
			}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}

		if err := fixture.service.validateResumeWorkflowForTest(
			context.Background(),
			session,
		); err != nil {
			t.Fatal(err)
		}
	})
}

func TestResumePodWorkloadRejectsActiveIdentityDriftBeforeControllerResume(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, fixture *recoveryFixture, session *domain.Session)
	}{
		{
			name: "PVC UID changed",
			setup: func(t *testing.T, fixture *recoveryFixture, session *domain.Session) {
				t.Helper()

				session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
					Name: "pv-destination",
					UID:  types.UID("destination-pv-uid"),
				}
				session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
					Namespace: "app",
					Name:      "data",
					UID:       types.UID("recorded-pvc-uid"),
				}

				_, err := fixture.client.CoreV1().
					PersistentVolumeClaims("app").
					Create(context.Background(), &corev1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Namespace:   "app",
							Name:        "data",
							UID:         types.UID("replacement-pvc-uid"),
							Annotations: map[string]string{kube.SessionKey: session.ID},
						},
						Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"},
						Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
					}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "destination PV UID changed",
			setup: func(t *testing.T, fixture *recoveryFixture, session *domain.Session) {
				t.Helper()

				session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
					Name: "pv-destination",
					UID:  types.UID("recorded-pv-uid"),
				}
				session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
					Namespace: "app",
					Name:      "data",
					UID:       types.UID("active-pvc-uid"),
				}

				_, err := fixture.client.CoreV1().
					PersistentVolumeClaims("app").
					Create(context.Background(), &corev1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Namespace:   "app",
							Name:        "data",
							UID:         types.UID("active-pvc-uid"),
							Annotations: map[string]string{kube.SessionKey: session.ID},
						},
						Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"},
						Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
					}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}

				_, err = fixture.client.CoreV1().
					PersistentVolumes().
					Create(context.Background(), &corev1.PersistentVolume{
						ObjectMeta: metav1.ObjectMeta{
							Name: "pv-destination",
							UID:  types.UID("replacement-pv-uid"),
						},
						Spec: corev1.PersistentVolumeSpec{
							ClaimRef: &corev1.ObjectReference{
								Namespace: "app",
								Name:      "data",
								UID:       types.UID("active-pvc-uid"),
							},
						},
					}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "destination PV claimRef changed",
			setup: func(t *testing.T, fixture *recoveryFixture, session *domain.Session) {
				t.Helper()

				session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
					Name: "pv-destination",
					UID:  types.UID("destination-pv-uid"),
				}
				session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
					Namespace: "app",
					Name:      "data",
					UID:       types.UID("active-pvc-uid"),
				}

				_, err := fixture.client.CoreV1().
					PersistentVolumeClaims("app").
					Create(context.Background(), &corev1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Namespace:   "app",
							Name:        "data",
							UID:         types.UID("active-pvc-uid"),
							Annotations: map[string]string{kube.SessionKey: session.ID},
						},
						Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"},
						Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
					}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}

				_, err = fixture.client.CoreV1().
					PersistentVolumes().
					Create(context.Background(), &corev1.PersistentVolume{
						ObjectMeta: metav1.ObjectMeta{
							Name: "pv-destination",
							UID:  types.UID("destination-pv-uid"),
						},
						Spec: corev1.PersistentVolumeSpec{
							ClaimRef: &corev1.ObjectReference{
								Namespace: "other",
								Name:      "data",
								UID:       types.UID("active-pvc-uid"),
							},
						},
					}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			session := appTestSession()
			session.Status.Phase = domain.PhaseActivated
			test.setup(t, fixture, session)

			err := fixture.service.ResumePodWorkload(context.Background(), session)
			if domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}

			if fixture.controller.resumes != 0 || session.Status.Phase != domain.PhaseActivated {
				t.Fatalf("resumes=%d phase=%s", fixture.controller.resumes, session.Status.Phase)
			}
		})
	}
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
				fixture.reserver.failures["data"] = domain.NewError(
					domain.ErrorKubernetes,
					"reserve",
					"injected failure",
				)
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
				fixture.controller.pauseErr = domain.NewError(
					domain.ErrorKubernetes,
					"pause",
					"injected failure",
				)
			},
			resumeFrom: domain.PhasePausing,
			category:   domain.ErrorKubernetes,
		},
		{
			name: "verify paused",
			configure: func(fixture *recoveryFixture) {
				fixture.controller.verifyErr = domain.NewError(
					domain.ErrorPrecondition,
					"verify paused",
					"injected failure",
				)
			},
			resumeFrom: domain.PhasePausing,
			category:   domain.ErrorPrecondition,
		},
		{
			name: "verify volume offline",
			configure: func(fixture *recoveryFixture) {
				fixture.switcher.offlineErr = domain.NewError(
					domain.ErrorPrecondition,
					"verify offline",
					"injected failure",
				)
			},
			resumeFrom: domain.PhaseFinalSyncing,
			category:   domain.ErrorPrecondition,
		},
		{
			name: "activate",
			configure: func(fixture *recoveryFixture) {
				fixture.switcher.activateErr = domain.NewError(
					domain.ErrorKubernetes,
					"activate",
					"injected failure",
				)
			},
			resumeFrom: domain.PhaseActivating,
			category:   domain.ErrorKubernetes,
		},
		{
			name: "resume workload",
			configure: func(fixture *recoveryFixture) {
				fixture.controller.resumeErr = domain.NewError(
					domain.ErrorKubernetes,
					"resume",
					"injected failure",
				)
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
			session.Spec.MigratePod.PrecopyPasses = test.warmPasses

			err := fixture.service.MigratePod(context.Background(), session)
			if domain.CategoryOf(err) != test.category {
				t.Fatalf("category=%s want=%s error=%v", domain.CategoryOf(err), test.category, err)
			}

			if session.Status.Phase != domain.PhaseFailed ||
				session.Status.ResumeFrom != test.resumeFrom {
				t.Fatalf(
					"phase=%s resumeFrom=%s want=%s",
					session.Status.Phase,
					session.Status.ResumeFrom,
					test.resumeFrom,
				)
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

	if session.Status.Phase != domain.PhaseFailed ||
		session.Status.ResumeFrom != domain.PhaseReserving ||
		!session.Status.Volumes[0].Reserved {
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
	session.Status.Phase = domain.PhaseFinalSynced
	session.Spec.Volumes[0].DestinationPVC.UID = types.UID("destination-pvc-uid")
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
		Name: "dest-pv-data",
		UID:  types.UID("dest-pv-uid-data"),
	}
	session.Status.Volumes[0].Reserved = true
	completed := metav1.Now()
	session.Status.Volumes[0].Sync.FinalCompletedAt = &completed

	err := fixture.service.PodActivate(context.Background(), session)
	if err == nil || err.Error() != "injected session update failure" {
		t.Fatalf("error=%v", err)
	}

	if session.Status.Phase != domain.PhaseFailed ||
		session.Status.ResumeFrom != domain.PhaseActivating ||
		session.Status.Volumes[0].Activation.ActivatedAt == nil {
		t.Fatalf(
			"phase=%s resumeFrom=%s activation=%+v",
			session.Status.Phase,
			session.Status.ResumeFrom,
			session.Status.Volumes[0].Activation,
		)
	}

	fixture.store.updateErrAt = 0
	if err := fixture.service.PodActivate(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != domain.PhaseActivated || len(fixture.switcher.activateCalls) != 1 {
		t.Fatalf(
			"phase=%s activation calls=%v",
			session.Status.Phase,
			fixture.switcher.activateCalls,
		)
	}
}

func TestDryRunActivationAcceptsCheckpointedActiveStorage(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	completed := metav1.Now()
	session.Status.Phase = domain.PhaseFailed
	session.Status.ResumeFrom = domain.PhaseActivating
	session.Status.Volumes[0].Sync.FinalCompletedAt = &completed
	createActiveDestinationStorage(t, fixture, session)

	if err := fixture.service.validateResumeWorkflowForTest(
		context.Background(),
		session,
	); err != nil {
		t.Fatal(err)
	}

	if len(fixture.switcher.offlineCalls) != 0 || fixture.controller.resumes != 0 ||
		fixture.store.updates != 0 {
		t.Fatalf(
			"offline=%v resumes=%d updates=%d",
			fixture.switcher.offlineCalls,
			fixture.controller.resumes,
			fixture.store.updates,
		)
	}
}

func TestDryRunRollbackAcceptsActivatedAndPartiallyRestoredStorage(t *testing.T) {
	t.Run("activated", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		session.Status.Phase = domain.PhaseActivated
		createActiveDestinationStorage(t, fixture, session)

		if err := fixture.service.validateRollbackWorkflowForTest(
			context.Background(),
			session,
		); err != nil {
			t.Fatal(err)
		}

		if len(fixture.switcher.offlineCalls) != 0 || fixture.controller.resumes != 0 ||
			fixture.store.updates != 0 {
			t.Fatalf(
				"offline=%v resumes=%d updates=%d",
				fixture.switcher.offlineCalls,
				fixture.controller.resumes,
				fixture.store.updates,
			)
		}
	})

	t.Run("partial rollback", func(t *testing.T) {
		fixture := newRecoveryFixture(t)
		session := appTestSession()
		addSecondVolume(session)
		createActiveDestinationStorage(t, fixture, session)

		if err := fixture.switcher.RollbackVolume(
			context.Background(),
			session,
			&session.Spec.Volumes[1],
			&session.Status.Volumes[1],
			nil,
		); err != nil {
			t.Fatal(err)
		}

		session.Status.Phase = domain.PhaseFailed
		session.Status.ResumeFrom = domain.PhaseRollingBack
		session.Status.History = []domain.HistoryEntry{
			{Phase: domain.PhasePlanned, Time: metav1.Now()},
			{Phase: domain.PhaseCompleted, Time: metav1.Now()},
			{Phase: domain.PhaseRollingBack, Time: metav1.Now()},
			{Phase: domain.PhaseFailed, Time: metav1.Now()},
		}

		if err := fixture.service.validateResumeWorkflowForTest(
			context.Background(),
			session,
		); err != nil {
			t.Fatal(err)
		}

		if len(fixture.switcher.offlineCalls) != 0 || fixture.controller.resumes != 0 ||
			fixture.store.updates != 0 {
			t.Fatalf(
				"offline=%v resumes=%d updates=%d",
				fixture.switcher.offlineCalls,
				fixture.controller.resumes,
				fixture.store.updates,
			)
		}
	})
}

func TestDryRunRecoveryValidationUsesReadOnlyChecks(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()

	session.Status.Phase = domain.PhaseReserved
	if err := fixture.service.validateResumeWorkflowForTest(
		context.Background(),
		session,
	); err != nil {
		t.Fatal(err)
	}

	if fixture.store.updates != 0 || len(fixture.reserver.calls) != 0 {
		t.Fatalf(
			"resume dry-run mutated state: updates=%d reserveCalls=%v",
			fixture.store.updates,
			fixture.reserver.calls,
		)
	}

	if want := []string{"data"}; !slices.Equal(fixture.reserver.dryRunCalls, want) {
		t.Fatalf("resume dry-run validations=%v want=%v", fixture.reserver.dryRunCalls, want)
	}

	session.Status.Phase = domain.PhasePaused
	createSourceStorage(t, fixture, session)

	if err := fixture.service.validateAbortWorkflowForTest(
		context.Background(),
		session,
	); err != nil {
		t.Fatal(err)
	}

	if fixture.controller.resumes != 0 || fixture.store.updates != 0 {
		t.Fatalf(
			"abort dry-run mutated state: resumes=%d updates=%d",
			fixture.controller.resumes,
			fixture.store.updates,
		)
	}
}

func TestDryRunResumeFromActivatedAcceptsPausedStandalonePod(t *testing.T) {
	fixture := newRecoveryFixture(t)
	session := appTestSession()
	session.Status.Phase = domain.PhaseActivated
	_ = session.Spec.SetWorkload(domain.WorkloadSpec{
		Adapter: domain.WorkloadStandalone,
		Pod: domain.ObjectReference{
			Namespace: "app",
			Name:      "application",
			UID:       types.UID("application-uid"),
		},
	})
	session.Spec.WorkflowOptionsPtr().TargetNode = "target-node"
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
		Name: "pv-destination",
		UID:  types.UID("destination-pv-uid"),
	}
	session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
		Namespace: "app",
		Name:      "data",
		UID:       types.UID("active-pvc-uid"),
	}

	_, err := fixture.client.CoreV1().
		PersistentVolumeClaims("app").
		Create(context.Background(), &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "app",
				Name:        "data",
				UID:         types.UID("active-pvc-uid"),
				Annotations: map[string]string{kube.SessionKey: session.ID},
			},
			Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = fixture.client.CoreV1().
		PersistentVolumes().
		Create(context.Background(), &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pv-destination",
				UID:  types.UID("destination-pv-uid"),
			},
			Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
				Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid"),
			}},
		}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	node, err := fixture.client.CoreV1().
		Nodes().
		Get(context.Background(), "target-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	node.Status.Conditions = []corev1.NodeCondition{
		{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
	}
	if _, err := fixture.client.CoreV1().
		Nodes().
		UpdateStatus(context.Background(), node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := fixture.service.validateResumeWorkflowForTest(
		context.Background(),
		session,
	); err != nil {
		t.Fatal(err)
	}

	if fixture.controller.verifies != 1 || fixture.controller.resumes != 0 ||
		fixture.store.updates != 0 {
		t.Fatalf(
			"dry-run side effects: verifies=%d resumes=%d updates=%d",
			fixture.controller.verifies,
			fixture.controller.resumes,
			fixture.store.updates,
		)
	}

	node, err = fixture.client.CoreV1().
		Nodes().
		Get(context.Background(), "target-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	node.Spec.Unschedulable = true
	if _, err := fixture.client.CoreV1().
		Nodes().
		Update(context.Background(), node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := fixture.service.validateResumeWorkflowForTest(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("unschedulable target category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestDryRunRollbackRejectsUnactivatedSession(t *testing.T) {
	fixture := newRecoveryFixture(t)

	session := appTestSession()
	if err := fixture.service.validateRollbackWorkflowForTest(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if fixture.store.updates != 0 || fixture.controller.pauses != 0 ||
		fixture.controller.resumes != 0 {
		t.Fatalf(
			"rollback dry-run mutated state: store=%d pauses=%d resumes=%d",
			fixture.store.updates,
			fixture.controller.pauses,
			fixture.controller.resumes,
		)
	}
}

func TestBackupSessionRollbackIsRejected(t *testing.T) {
	fixture := newRecoveryFixture(t)
	spec := domain.NewSessionSpec(
		domain.OperationBackup,
		domain.SessionCommon{SourceNamespace: "app", SessionNamespace: "sessions"},

		true,
		domain.SessionWorkflowOptions{},
	)

	spec.Backup.SourcePVC = domain.ObjectReference{Namespace: "app", Name: "data", UID: "pvc-uid"}
	spec.Backup.SourcePV = domain.ObjectReference{Name: "pv-data", UID: "pv-uid"}
	spec.Backup.Backend = "s3"
	spec.Backup.Bucket = "backups"
	spec.Backup.Name = "daily"
	session := domain.NewSession("backup-session", spec, time.Now())
	session.Status.Phase = domain.PhaseCompleted

	if err := fixture.service.validateRollbackWorkflowForTest(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("dry-run category=%s error=%v", domain.CategoryOf(err), err)
	}

	if err := fixture.service.rollbackWorkflowForTest(
		context.Background(),
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("execution category=%s error=%v", domain.CategoryOf(err), err)
	}

	if session.Status.Phase != domain.PhaseCompleted || fixture.store.updates != 0 {
		t.Fatalf(
			"rollback mutated backup session: phase=%s updates=%d",
			session.Status.Phase,
			fixture.store.updates,
		)
	}
}

func TestBackupSessionAbortUsesBackupMessage(t *testing.T) {
	fixture := newRecoveryFixture(t)
	spec := domain.NewSessionSpec(
		domain.OperationBackup,
		domain.SessionCommon{SourceNamespace: "app", SessionNamespace: "sessions"},

		true,
		domain.SessionWorkflowOptions{},
	)

	spec.Backup.SourcePVC = domain.ObjectReference{Namespace: "app", Name: "data", UID: "pvc-uid"}
	spec.Backup.SourcePV = domain.ObjectReference{Name: "pv-data", UID: "pv-uid"}
	spec.Backup.Backend = "s3"
	spec.Backup.Bucket = "backups"
	spec.Backup.Name = "daily"
	session := domain.NewSession("backup-session", spec, time.Now())
	session.Status.Phase = domain.PhaseFailed
	session.Status.ResumeFrom = domain.PhaseWarmCopied

	if err := fixture.service.abortWorkflowForTest(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != domain.PhaseAborted ||
		session.Status.Message != "backup aborted; no recovery point was published" {
		t.Fatalf("phase=%s message=%q", session.Status.Phase, session.Status.Message)
	}
}

func TestDryRunRollbackChecksConsumersOutsideTheWorkloadPauseScope(t *testing.T) {
	originalDeploymentReplicas := int32(1)

	tests := []struct {
		name                string
		workload            domain.WorkloadSpec
		consumer            string
		consumerUID         types.UID
		phase               corev1.PodPhase
		currentRollbackPods []domain.ObjectReference
		want                domain.ErrorCategory
	}{
		{
			name:     "plain migrate rejects an active consumer",
			workload: domain.WorkloadSpec{Adapter: domain.WorkloadNone},
			consumer: "writer",
			phase:    corev1.PodRunning,
			want:     domain.ErrorPrecondition,
		},
		{
			name: "migrate-pod allows its controlled consumer",
			workload: domain.WorkloadSpec{
				Adapter: domain.WorkloadStandalone,
				Pod: domain.ObjectReference{
					Namespace: "app",
					Name:      "application",
					UID:       "application-uid",
				},
			},
			consumer: "application",
			phase:    corev1.PodRunning,
		},
		{
			name: "migrate-pod rejects a same-name replacement consumer",
			workload: domain.WorkloadSpec{
				Adapter: domain.WorkloadStandalone,
				Pod: domain.ObjectReference{
					Namespace: "app",
					Name:      "application",
					UID:       "application-uid",
				},
			},
			consumer:    "application",
			consumerUID: "replacement-uid",
			phase:       corev1.PodRunning,
			want:        domain.ErrorConflict,
		},
		{
			name: "migrate-pod allows a replacement Deployment consumer",
			workload: domain.WorkloadSpec{
				Adapter: domain.WorkloadDeployment,
				Pod: domain.ObjectReference{
					Namespace: "app",
					Name:      "web-old",
					UID:       types.UID("web-old-uid"),
				},
				Controller: domain.ObjectReference{
					APIVersion: domain.AppsAPIVersion,
					Kind:       domain.KindDeployment,
					Namespace:  "app",
					Name:       "web",
					UID:        types.UID("web-uid"),
				},
				OriginalReplicas: &originalDeploymentReplicas,
				AffectedPods: []domain.ObjectReference{
					{Namespace: "app", Name: "web-old", UID: types.UID("web-old-uid")},
				},
			},
			consumer: "web-new",
			phase:    corev1.PodRunning,
			currentRollbackPods: []domain.ObjectReference{
				{Namespace: "app", Name: "web-new", UID: types.UID("web-new-uid")},
			},
		},
		{
			name: "migrate-pod allows a replacement StatefulSet consumer",
			workload: domain.WorkloadSpec{
				Adapter: domain.WorkloadStatefulSet,
				Pod: domain.ObjectReference{
					Namespace: "app",
					Name:      "db-0",
					UID:       types.UID("db-0-old-uid"),
				},
				Controller: domain.ObjectReference{
					APIVersion: domain.AppsAPIVersion,
					Kind:       domain.KindStatefulSet,
					Namespace:  "app",
					Name:       "db",
					UID:        types.UID("db-controller-uid"),
				},
				AffectedPods: []domain.ObjectReference{
					{Namespace: "app", Name: "db-0", UID: types.UID("db-0-old-uid")},
				},
			},
			consumer:    "db-0",
			consumerUID: "db-0-new-uid",
			phase:       corev1.PodRunning,
			currentRollbackPods: []domain.ObjectReference{
				{Namespace: "app", Name: "db-0", UID: types.UID("db-0-new-uid")},
			},
		},
		{
			name: "migrate-pod rejects a terminal external consumer like execution",
			workload: domain.WorkloadSpec{
				Adapter: domain.WorkloadStatefulSet,
				Pod: domain.ObjectReference{
					Namespace: "app",
					Name:      "db-0",
					UID:       types.UID("db-0-uid"),
				},
				Controller: domain.ObjectReference{
					Namespace: "app",
					Name:      "db",
					UID:       types.UID("db-controller-uid"),
				},
				AffectedPods: []domain.ObjectReference{
					{Namespace: "app", Name: "db-0", UID: types.UID("db-0-uid")},
				},
			},
			consumer: "stale-reader",
			phase:    corev1.PodSucceeded,
			want:     domain.ErrorPrecondition,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRecoveryFixture(t)
			fixture.controller.currentRollbackPods = slices.Clone(test.currentRollbackPods)
			session := appTestSession()
			session.Status.Phase = domain.PhaseCompleted

			operation := domain.OperationMigratePod
			if test.workload.Adapter == domain.WorkloadNone {
				operation = domain.OperationMigrate
			}

			setSessionOperation(session, operation)

			if operation == domain.OperationMigratePod {
				if err := session.Spec.SetWorkload(test.workload); err != nil {
					t.Fatal(err)
				}
			}

			session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
				Name: "pv-destination",
				UID:  types.UID("destination-pv-uid"),
			}

			session.Status.Volumes[0].Activation.ActivePVC = domain.ObjectReference{
				Namespace: "app",
				Name:      "data",
				UID:       types.UID("active-pvc-uid"),
			}
			if _, err := fixture.client.CoreV1().
				PersistentVolumeClaims("app").
				Create(context.Background(), &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Namespace:   "app",
						Name:        "data",
						UID:         types.UID("active-pvc-uid"),
						Annotations: map[string]string{kube.SessionKey: session.ID},
					},
					Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"},
					Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
				}, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}

			if _, err := fixture.client.CoreV1().
				PersistentVolumes().
				Create(context.Background(), &corev1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pv-destination",
						UID:  types.UID("destination-pv-uid"),
					},
					Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
						Namespace: "app", Name: "data", UID: types.UID("active-pvc-uid"),
					}},
				}, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}

			podUID := test.consumerUID
			if podUID == "" {
				podUID = types.UID(test.consumer + "-uid")
			}

			annotations := map[string]string{}
			if test.workload.Adapter == domain.WorkloadStandalone &&
				test.consumer == test.workload.Pod.Name && test.consumerUID == "" {
				podUID = test.workload.Pod.UID
				annotations[kube.SessionKey] = session.ID
			}

			if _, err := fixture.client.CoreV1().
				Pods("app").
				Create(context.Background(), &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Namespace:   "app",
						Name:        test.consumer,
						UID:         podUID,
						Annotations: annotations,
					},
					Spec: corev1.PodSpec{Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "data",
								},
							},
						},
					}, NodeName: "target-node"},
					Status: corev1.PodStatus{Phase: test.phase},
				}, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}

			err := fixture.service.validateRollbackWorkflowForTest(context.Background(), session)
			if test.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}

			if domain.CategoryOf(err) != test.want {
				t.Fatalf("category=%s error=%v want=%s", domain.CategoryOf(err), err, test.want)
			}
		})
	}
}

func TestDryRunCleanupEnforcesDestructivePrerequisites(t *testing.T) {
	fixture := newRecoveryFixture(t)

	session := appTestSession()
	if err := fixture.service.validateCleanupWorkflowForTest(
		context.Background(),
		session,
		CleanupOptions{},
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("active cleanup category=%s error=%v", domain.CategoryOf(err), err)
	}

	session.Status.Phase = domain.PhaseCompleted
	if err := fixture.service.validateCleanupWorkflowForTest(
		context.Background(),
		session,
		CleanupOptions{DeleteSession: true},
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("unfinalized delete category=%s error=%v", domain.CategoryOf(err), err)
	}

	if fixture.store.updates != 0 || fixture.store.deletes != 0 {
		t.Fatalf(
			"cleanup dry-run mutated state: updates=%d deletes=%d",
			fixture.store.updates,
			fixture.store.deletes,
		)
	}
}
