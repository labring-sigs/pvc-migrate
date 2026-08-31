package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

type runnerSessionStore struct {
	listed     *domain.Session
	latest     *domain.Session
	updates    []*domain.Session
	getErr     error
	acquireErr error
	lock       *runnerSessionLock
}

func (s *runnerSessionStore) Create(context.Context, *domain.Session) error { return nil }

func (s *runnerSessionStore) Get(context.Context, string, string) (*domain.Session, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return cloneRunnerSession(s.latest), nil
}

func (s *runnerSessionStore) Update(_ context.Context, session *domain.Session) error {
	s.updates = append(s.updates, cloneRunnerSession(session))
	s.latest = cloneRunnerSession(session)
	return nil
}

func (s *runnerSessionStore) List(context.Context, string) ([]*domain.Session, error) {
	return []*domain.Session{cloneRunnerSession(s.listed)}, nil
}

func (s *runnerSessionStore) Delete(context.Context, *domain.Session) error { return nil }

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
			SourceNamespace:      "source",
			TemporaryNamespace:   "system",
			DestinationNamespace: "destination",
			SessionNamespace:     "system",
			Volumes: []domain.VolumeSpec{
				{
					SourcePVC: domain.ObjectReference{
						Namespace: "source",
						Name:      "data",
						UID:       "pvc-uid",
					},
					DestinationPVC: domain.ObjectReference{Namespace: "destination", Name: "data"},
				},
			},
		},
		false,
		domain.SessionWorkflowOptions{},
	)

	return domain.NewSession(id, spec, time.Unix(1, 0))
}

type recordingWorkflowResumer struct {
	called string
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
		SourceNamespace:      "source",
		TemporaryNamespace:   "system",
		DestinationNamespace: "destination",
		SessionNamespace:     "system",
		Volumes: []domain.VolumeSpec{
			{
				SourcePVC: domain.ObjectReference{
					Namespace: "source",
					Name:      "source",
					UID:       "pvc-uid",
				},
				SourcePV: domain.ObjectReference{Name: "pv-source", UID: "pv-uid"},
				DestinationPVC: domain.ObjectReference{
					Namespace: "destination",
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
		{name: "move", typeName: domain.SessionTypeMove, make: func() domain.SessionSpec {
			return domain.NewSessionSpec(
				domain.OperationMove,
				common,
				false,
				domain.SessionWorkflowOptions{},
			)
		}, want: "move"},
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
			spec.Backup.SourcePVC = domain.ObjectReference{Namespace: "source", Name: "source", UID: "pvc-uid"}
			return spec
		}},
		{name: "restore", make: func() domain.SessionSpec {
			spec := domain.NewSessionSpec(domain.OperationRestore, common, false, domain.SessionWorkflowOptions{})
			spec.Restore.DestinationPVC = domain.ObjectReference{Namespace: "destination", Name: "destination"}
			return spec
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := NewRunner(&recordingWorkflowResumer{}, nil, "system")
			session := domain.NewSession(test.name, test.make(), time.Now())

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

			err := runner.ReconcileOnce(context.Background())
			if err == nil || !strings.Contains(err.Error(), "copy dispatched") {
				t.Fatalf("reconcile error=%v", err)
			}

			if (len(store.updates) > 0) != test.wantUpdate {
				t.Fatalf("updates=%d, want update=%t", len(store.updates), test.wantUpdate)
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
