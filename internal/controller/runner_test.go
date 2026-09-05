package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientfake "k8s.io/client-go/kubernetes/fake"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type runnerSessionStore struct {
	listed     *domain.Session
	latest     *domain.Session
	updates    []*domain.Session
	deleted    []*domain.Session
	getErr     error
	acquireErr error
	updateErr  error
	lock       *runnerSessionLock
}

func (*runnerSessionStore) StorageBackend() string { return kube.SessionBackendCRD }

func (s *runnerSessionStore) Create(context.Context, *domain.Session) error { return nil }

func (s *runnerSessionStore) Get(context.Context, string, string) (*domain.Session, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return cloneRunnerSession(s.latest), nil
}

func (s *runnerSessionStore) GetByType(
	ctx context.Context,
	namespace, id string,
	_ domain.SessionType,
) (*domain.Session, error) {
	return s.Get(ctx, namespace, id)
}

func (s *runnerSessionStore) GetByKind(
	ctx context.Context,
	namespace, id string,
	_ domain.ControllerKind,
) (*domain.Session, error) {
	return s.Get(ctx, namespace, id)
}

func (*runnerSessionStore) CheckWorkflowNameCollision(
	context.Context,
	*domain.Session,
) error {
	return nil
}

func (*runnerSessionStore) EnsureSessionProtection(context.Context, *domain.Session) error {
	return nil
}

func (s *runnerSessionStore) Update(_ context.Context, session *domain.Session) error {
	if s.updateErr != nil {
		return s.updateErr
	}

	s.updates = append(s.updates, cloneRunnerSession(session))
	s.latest = cloneRunnerSession(session)

	return nil
}

func (s *runnerSessionStore) List(context.Context, string) ([]*domain.Session, error) {
	return []*domain.Session{cloneRunnerSession(s.listed)}, nil
}

func (s *runnerSessionStore) Delete(_ context.Context, session *domain.Session) error {
	s.deleted = append(s.deleted, cloneRunnerSession(session))
	return nil
}

func (*runnerSessionStore) DeleteSessionLease(context.Context, string, string) error { return nil }

func (s *runnerSessionStore) AcquireSessionLock(
	context.Context,
	string,
	string,
) (kube.SessionLock, error) {
	if s.acquireErr != nil {
		return nil, s.acquireErr
	}
	return s.lock, nil
}

type runnerSessionLock struct {
	err        error
	releaseErr error
	released   bool
}

func (l *runnerSessionLock) Bind(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(ctx)
}

func (l *runnerSessionLock) Err() error { return l.err }

func (l *runnerSessionLock) Release(context.Context) error {
	l.released = true
	return l.releaseErr
}

func (l *runnerSessionLock) Delete(context.Context) error { return nil }

func cloneRunnerSession(in *domain.Session) *domain.Session {
	if in == nil {
		return nil
	}

	out := *in
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)

	return &out
}

func newRunnerSession(id string) *domain.Session {
	spec := domain.NewSessionSpec(
		domain.OperationCopy,
		domain.SessionCommon{
			SourceNamespace:      "system",
			TemporaryNamespace:   "system",
			DestinationNamespace: "system",
			SessionNamespace:     "system",
			Volumes: []domain.VolumeSpec{
				{
					SourcePVC: domain.ObjectReference{
						Namespace: "system",
						Name:      "data",
						UID:       "pvc-uid",
					},
					DestinationPVC: domain.ObjectReference{Namespace: "system", Name: "data"},
				},
			},
		},
		false,
		domain.SessionWorkflowOptions{},
	)

	session := domain.NewSession(id, spec, time.Unix(1, 0))
	session.Backend = kube.SessionBackendCRD
	session.BackendResource = domain.ControllerKindCopy

	return session
}

func TestRunnerUsesTrustedToolImageForEveryWorkflowValue(t *testing.T) {
	runner := NewRunner(&recordingWorkflowResumer{}, nil, "system").
		WithTrustedToolImage("registry.example/pvc-migrate:trusted")

	for _, requested := range []string{
		"registry.example/pvc-migrate:trusted",
		"registry.example/tenant:arbitrary",
		"registry.example/tenant",
		"",
	} {
		session := newRunnerSession("image-policy")
		session.Spec.Copy.ToolImage = requested

		if err := runner.validateTrustedToolImage(session); err != nil {
			t.Fatalf("requested image %q rejected: %v", requested, err)
		}

		got := runner.executionToolImage(session)
		if got != "registry.example/pvc-migrate:trusted" {
			t.Fatalf("execution image=%q, want trusted image", got)
		}
	}

	runner = NewRunner(&recordingWorkflowResumer{}, nil, "system").
		WithTrustedToolImage("registry.example/pvc-migrate")

	err := runner.validateTrustedToolImage(newRunnerSession("invalid-trusted"))
	if domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("invalid trusted image category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRunnerSkipsToolImagePolicyForMetadataOnlyWorkflows(t *testing.T) {
	runner := NewRunner(&recordingWorkflowResumer{}, nil, "system").
		WithTrustedToolImage("registry.example/pvc-migrate:0.3.0")

	for _, operation := range []domain.Operation{domain.OperationRename, domain.OperationMove} {
		destinationNamespace := "destination"
		if operation == domain.OperationRename {
			destinationNamespace = "source"
		}

		session := domain.NewSession(
			"metadata-only",
			domain.NewSessionSpec(
				operation,
				domain.SessionCommon{
					SourceNamespace:      "source",
					DestinationNamespace: destinationNamespace,
					SessionNamespace:     "system",
				},
				false,
				domain.SessionWorkflowOptions{},
			),
			time.Unix(1, 0),
		)

		if err := runner.validateTrustedToolImage(session); err != nil {
			t.Fatalf("operation %s rejected without a tool image: %v", operation, err)
		}
	}
}

type recordingWorkflowResumer struct {
	called string
}

func TestRunnerBackupRepositoryUsesTenantLocationAndLocalSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	repository := &v1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "custom",
			Namespace:  "tenant",
			UID:        types.UID("repo-uid"),
			Generation: 4,
		},
		Spec: v1alpha1.BackupRepositorySpec{
			Type: v1alpha1.BackupRepositoryTypeS3,
			S3: &v1alpha1.S3BackupRepositorySpec{
				Bucket:            "tenant-bucket",
				Prefix:            "team-a",
				Endpoint:          "https://s3.example.test",
				CredentialsSecret: v1alpha1.BackupRepositorySecretReference{Name: "s3"},
			},
		},
	}
	runtimeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(repository).Build()
	kubeClient := clientfake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s3",
			Namespace: "tenant",
			UID:       types.UID("secret-uid"),
		},
		Data: map[string][]byte{
			kube.BackupAccessKeyDataKey: []byte("access"),
			kube.BackupSecretKeyDataKey: []byte("secret"),
		},
	})
	spec := domain.NewSessionSpec(
		domain.OperationBackup,
		domain.SessionCommon{SourceNamespace: "tenant", SessionNamespace: "tenant"},
		false,
		domain.SessionWorkflowOptions{},
	)
	spec.Backup.BackupRepository = "custom"
	spec.Backup.Name = "daily"
	runner := NewRunner(
		&recordingWorkflowResumer{},
		nil,
		"tenant",
	).WithKubernetesClient(kubeClient).
		WithControllerClient(runtimeClient)

	config, err := runner.objectStoreConfig(
		context.Background(),
		domain.NewSession("backup", spec, time.Now()),
		"",
		"",
		"daily",
		"",
		"",
		"",
		false,
		"",
		"",
	)
	if err != nil {
		t.Fatal(err)
	}

	if config.Bucket != "tenant-bucket" || config.AccessKey != "access" ||
		config.SecretKey != "secret" ||
		config.Prefix != "team-a/namespaces/tenant" ||
		config.RepositoryUID != "repo-uid" ||
		config.RepositoryGeneration != 4 {
		t.Fatalf("repository config=%#v", config)
	}

	other := cloneRunnerSession(domain.NewSession("other", spec, time.Now()))

	other.Spec.SessionNamespace = "other"
	if _, err := runner.objectStoreConfig(
		context.Background(),
		other,
		"",
		"",
		"daily",
		"",
		"",
		"",
		false,
		"",
		"",
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf(
			"cross-namespace repository lookup category=%q error=%v",
			domain.CategoryOf(err),
			err,
		)
	}
}

func TestRunnerRejectsPVCBackupRepositoryUntilDataPlaneIsAvailable(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	repository := &v1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-repo", Namespace: "tenant"},
		Spec: v1alpha1.BackupRepositorySpec{
			Type: v1alpha1.BackupRepositoryTypePVC,
			PVC: &v1alpha1.PVCBackupRepositorySpec{
				ClaimRef: v1alpha1.LocalObjectReference{Name: "backup-volume"},
			},
		},
	}
	runtimeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(repository).Build()
	spec := domain.NewSessionSpec(
		domain.OperationBackup,
		domain.SessionCommon{SourceNamespace: "tenant", SessionNamespace: "tenant"},
		false,
		domain.SessionWorkflowOptions{},
	)
	spec.Backup.BackupRepository = "pvc-repo"
	spec.Backup.Name = "daily"
	runner := NewRunner(
		&recordingWorkflowResumer{},
		nil,
		"tenant",
	).WithControllerClient(runtimeClient)

	_, err := runner.objectStoreConfig(
		context.Background(),
		domain.NewSession("backup-pvc", spec, time.Now()),
		"",
		"",
		"daily",
		"",
		"",
		"",
		false,
		"",
		"",
	)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "backend pvc is not supported") {
		t.Fatalf("PVC repository error category=%q error=%v", domain.CategoryOf(err), err)
	}
}

func (r *recordingWorkflowResumer) call(name string) error {
	r.called = name
	return fmt.Errorf("%s dispatched", name)
}

func (r *recordingWorkflowResumer) ResumeReserve(context.Context, *domain.Session) error {
	return r.call("reserve")
}

func (r *recordingWorkflowResumer) ResumeOfflineMigration(context.Context, *domain.Session) error {
	return r.call("migrate")
}

func (r *recordingWorkflowResumer) ResumePodMigration(context.Context, *domain.Session) error {
	return r.call("migrate-pod")
}

func (r *recordingWorkflowResumer) ResumeCopy(context.Context, *domain.Session) error {
	return r.call("copy")
}

func (r *recordingWorkflowResumer) ResumeRename(context.Context, *domain.Session) error {
	return r.call("rename")
}

func (r *recordingWorkflowResumer) ResumeMove(context.Context, *domain.Session) error {
	return r.call("move")
}

func TestRunnerRequiresServiceBeforeReconcilingCrossNamespaceMigration(t *testing.T) {
	spec := domain.NewOfflineMigrationSessionSpec(domain.SessionCommon{
		SourceNamespace:      "source",
		TemporaryNamespace:   "system",
		DestinationNamespace: "destination",
		SessionNamespace:     "system",
		Volumes: []domain.VolumeSpec{{
			SourcePVC:      domain.ObjectReference{Name: "source"},
			DestinationPVC: domain.ObjectReference{Name: "destination"},
		}},
	}, domain.SessionWorkflowOptions{})
	session := domain.NewSession("move-session", spec, time.Now())
	runner := NewRunner(nil, nil, "system")

	err := runner.reconcileSession(context.Background(), session)
	if domain.CategoryOf(err) != domain.ErrorValidation ||
		err.Error() != "controller reconcile: service is required" {
		t.Fatalf("error category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRunnerDispatchesEveryControllerWorkflow(t *testing.T) {
	common := domain.SessionCommon{
		SourceNamespace:      "system",
		TemporaryNamespace:   "system",
		DestinationNamespace: "system",
		SessionNamespace:     "system",
		Volumes: []domain.VolumeSpec{
			{
				SourcePVC: domain.ObjectReference{
					Namespace: "system",
					Name:      "source",
					UID:       "pvc-uid",
				},
				SourcePV: domain.ObjectReference{Name: "pv-source", UID: "pv-uid"},
				DestinationPVC: domain.ObjectReference{
					Namespace: "system",
					Name:      "destination",
				},
			},
		},
	}

	tests := []struct {
		name     string
		typeName domain.SessionType
		make     func() domain.SessionSpec
		want     string
	}{
		{name: "reserve", typeName: domain.SessionTypeReserve, make: func() domain.SessionSpec {
			return domain.NewSessionSpec(
				domain.OperationReserve,
				common,
				false,
				domain.SessionWorkflowOptions{},
			)
		}, want: "reserve"},
		{name: "migration", typeName: domain.SessionTypeMigrate, make: func() domain.SessionSpec {
			return domain.NewOfflineMigrationSessionSpec(common, domain.SessionWorkflowOptions{})
		}, want: "migrate"},
		{
			name:     "pod migration",
			typeName: domain.SessionTypeMigratePod,
			make: func() domain.SessionSpec {
				return domain.NewPodMigrationSessionSpec(
					common,
					domain.WorkloadSpec{Adapter: domain.WorkloadNone},
					domain.SessionWorkflowOptions{},
					1,
					false,
				)
			},
			want: "migrate-pod",
		},
		{name: "copy", typeName: domain.SessionTypeCopy, make: func() domain.SessionSpec {
			return domain.NewSessionSpec(
				domain.OperationCopy,
				common,
				false,
				domain.SessionWorkflowOptions{},
			)
		}, want: "copy"},
		{name: "rename", typeName: domain.SessionTypeRename, make: func() domain.SessionSpec {
			c := common
			c.DestinationNamespace = c.SourceNamespace

			return domain.NewSessionSpec(
				domain.OperationRename,
				c,
				false,
				domain.SessionWorkflowOptions{},
			)
		}, want: "rename"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resumer := &recordingWorkflowResumer{}
			runner := NewRunner(resumer, nil, "system")
			session := domain.NewSession(test.name, test.make(), time.Now())

			err := runner.reconcileSession(context.Background(), session)
			if resumer.called != test.want {
				t.Fatalf("dispatch=%q, want %q (error=%v)", resumer.called, test.want, err)
			}

			if err == nil || err.Error() != test.want+" dispatched" {
				t.Fatalf("error=%v, want %q", err, test.want+" dispatched")
			}
		})
	}

	for _, test := range []struct {
		name string
		make func() domain.SessionSpec
	}{
		{name: "backup", make: func() domain.SessionSpec {
			spec := domain.NewSessionSpec(domain.OperationBackup, common, false, domain.SessionWorkflowOptions{})
			spec.Backup.SourcePVC = domain.ObjectReference{Namespace: "system", Name: "source", UID: "pvc-uid"}
			return spec
		}},
		{name: "restore", make: func() domain.SessionSpec {
			spec := domain.NewSessionSpec(domain.OperationRestore, common, false, domain.SessionWorkflowOptions{})
			spec.Restore.DestinationPVC = domain.ObjectReference{Namespace: "system", Name: "destination"}
			return spec
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := NewRunner(&recordingWorkflowResumer{}, nil, "system")

			session := domain.NewSession(test.name, test.make(), time.Now())
			if session.Spec.Backup != nil {
				session.Spec.Backup.BackupRepository = "default"
			}

			if session.Spec.Restore != nil {
				session.Spec.Restore.BackupRepository = "default"
			}

			err := runner.reconcileSession(context.Background(), session)
			if err == nil ||
				err.Error() != "controller reconcile: "+test.name+" controller execution requires a Kubernetes client" {
				t.Fatalf("error=%v, want missing Kubernetes client dispatch error", err)
			}
		})
	}
}

func TestTerminalSessionUsesOperationSpecificCompletionPhase(t *testing.T) {
	tests := []struct {
		name     string
		typeName domain.SessionType
		phase    domain.Phase
		want     bool
	}{
		{
			name: "migration completed", typeName: domain.SessionTypeMigrate,
			phase: domain.PhaseCompleted, want: true,
		},
		{
			name: "pod migration completed", typeName: domain.SessionTypeMigratePod,
			phase: domain.PhaseCompleted, want: true,
		},
		{
			name: "reservation reserved", typeName: domain.SessionTypeReserve,
			phase: domain.PhaseReserved, want: true,
		},
		{
			name: "copy warm copied", typeName: domain.SessionTypeCopy,
			phase: domain.PhaseWarmCopied, want: true,
		},
		{
			name: "migration reserved", typeName: domain.SessionTypeMigrate,
			phase: domain.PhaseReserved, want: false,
		},
		{
			name: "pod migration warm copied", typeName: domain.SessionTypeMigratePod,
			phase: domain.PhaseWarmCopied, want: false,
		},
		{
			name: "reservation reserving", typeName: domain.SessionTypeReserve,
			phase: domain.PhaseReserving, want: false,
		},
		{
			name: "copy warm copying", typeName: domain.SessionTypeCopy,
			phase: domain.PhaseWarmCopying, want: false,
		},
		{
			name: "aborted copy", typeName: domain.SessionTypeCopy,
			phase: domain.PhaseAborted, want: true,
		},
		{
			name: "failed copy", typeName: domain.SessionTypeCopy,
			phase: domain.PhaseFailed, want: true,
		},
		{
			name: "rolled back move", typeName: domain.SessionTypeMove,
			phase: domain.PhaseRolledBack, want: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := &domain.Session{
				Spec:   domain.SessionSpec{Type: test.typeName},
				Status: domain.SessionStatus{Phase: test.phase},
			}
			if got := terminalSession(session); got != test.want {
				t.Fatalf("terminalSession()=%t, want %t", got, test.want)
			}
		})
	}

	if !terminalSession(nil) {
		t.Fatal("nil session must be ignored as terminal")
	}
}

func TestRunnerCheckpointFailureUsesLatestSessionState(t *testing.T) {
	for _, test := range []struct {
		name            string
		latestPhase     domain.Phase
		wantUpdate      bool
		wantLatestPhase domain.Phase
	}{
		{name: "active session is failed", latestPhase: domain.PhaseReserved, wantUpdate: true, wantLatestPhase: domain.PhaseFailed},
		{name: "completed session wins", latestPhase: domain.PhaseCompleted, wantUpdate: false, wantLatestPhase: domain.PhaseCompleted},
		{name: "aborted session wins", latestPhase: domain.PhaseAborted, wantUpdate: false, wantLatestPhase: domain.PhaseAborted},
		{name: "already failed is unchanged", latestPhase: domain.PhaseFailed, wantUpdate: false, wantLatestPhase: domain.PhaseFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			listed := newRunnerSession(test.name)
			latest := cloneRunnerSession(listed)
			latest.Status.Phase = test.latestPhase
			store := &runnerSessionStore{
				listed: listed,
				latest: latest,
				lock:   &runnerSessionLock{},
			}
			runner := NewRunner(&recordingWorkflowResumer{}, store, "system")

			var logs bytes.Buffer
			runner.WithLogger(slog.New(slog.NewTextHandler(&logs, nil)))

			err := runner.ReconcileOnce(context.Background())
			if err == nil || !strings.Contains(err.Error(), "copy dispatched") {
				t.Fatalf("reconcile error=%v", err)
			}

			if (len(store.updates) > 0) != test.wantUpdate {
				t.Fatalf("updates=%d, want update=%t", len(store.updates), test.wantUpdate)
			}

			if strings.Contains(logs.String(), "workflow entered failed state") != test.wantUpdate {
				t.Fatalf("failure log must reflect the persisted transition: %s", logs.String())
			}

			if store.latest.Status.Phase != test.wantLatestPhase {
				t.Fatalf(
					"latest phase=%s, want %s",
					store.latest.Status.Phase,
					test.wantLatestPhase,
				)
			}

			if !store.lock.released {
				t.Fatal("session lock was not released")
			}
		})
	}
}

func TestRunnerControllerFailureCheckpointIsQuietAfterPersistence(t *testing.T) {
	listed := newRunnerSession("controller-failure")
	store := &runnerSessionStore{
		listed: listed,
		latest: cloneRunnerSession(listed),
		lock:   &runnerSessionLock{},
	}
	runner := NewRunner(&recordingWorkflowResumer{}, store, "system")

	if err := runner.checkpointFailureForController(
		context.Background(),
		listed,
		errors.New("destination PVC is unavailable"),
	); err != nil {
		t.Fatalf("controller failure checkpoint returned error: %v", err)
	}

	if store.latest.Status.Phase != domain.PhaseFailed {
		t.Fatalf("phase=%s, want %s", store.latest.Status.Phase, domain.PhaseFailed)
	}
}

func TestRunnerDoesNotCheckpointControllerShutdown(t *testing.T) {
	session := newRunnerSession("interrupted")
	store := &runnerSessionStore{listed: session, latest: cloneRunnerSession(session)}
	runner := NewRunner(&recordingWorkflowResumer{}, store, "system")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := runner.checkpointFailureForController(
		ctx,
		session,
		context.Canceled,
	); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf("error=%v", err)
	}

	if len(store.updates) != 0 || store.latest.Status.Phase != session.Status.Phase {
		t.Fatal("controller shutdown changed the execution checkpoint")
	}
}

func TestRunnerControllerFailureCheckpointReturnsPersistenceError(t *testing.T) {
	listed := newRunnerSession("controller-checkpoint-error")
	checkpointErr := errors.New("status update unavailable")
	store := &runnerSessionStore{
		listed:    listed,
		latest:    cloneRunnerSession(listed),
		updateErr: checkpointErr,
		lock:      &runnerSessionLock{},
	}
	runner := NewRunner(&recordingWorkflowResumer{}, store, "system")

	err := runner.checkpointFailureForController(
		context.Background(),
		listed,
		errors.New("destination PVC is unavailable"),
	)
	if !errors.Is(err, checkpointErr) {
		t.Fatalf("error=%v, want checkpoint error", err)
	}
}

func TestRunnerCheckpointFailurePreservesLockAcquisitionError(t *testing.T) {
	listed := newRunnerSession("lock-error")
	lockErr := errors.New("lock unavailable")
	store := &runnerSessionStore{listed: listed, latest: listed, acquireErr: lockErr}
	runner := NewRunner(&recordingWorkflowResumer{}, store, "system")

	err := runner.ReconcileOnce(context.Background())
	if !strings.Contains(err.Error(), "copy dispatched") || !errors.Is(err, lockErr) {
		t.Fatalf("error=%v, want dispatch and lock errors", err)
	}

	if len(store.updates) != 0 {
		t.Fatalf("session was updated after lock acquisition failed: %d", len(store.updates))
	}
}

func TestRunnerCheckpointFailureRequiresWorkflowKind(t *testing.T) {
	session := newRunnerSession("missing-kind")
	session.BackendResource = ""
	cause := errors.New("reconcile failed")
	runner := NewRunner(&recordingWorkflowResumer{}, &runnerSessionStore{}, "system")

	err := runner.checkpointFailure(context.Background(), session, cause)
	if !errors.Is(err, cause) ||
		!strings.Contains(err.Error(), "backend resource kind is required") {
		t.Fatalf("checkpoint error=%v", err)
	}
}
