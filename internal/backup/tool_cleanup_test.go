package backup

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

type uninstallCall struct {
	namespace   string
	release     string
	ctxErr      error
	deadline    time.Duration
	hasDeadline bool
}

type cleanupToolRunner struct {
	runErr       error
	uninstallErr error

	backupID    string
	uninstalled []uninstallCall
}

func (r *cleanupToolRunner) RunBackup(_ context.Context, request pvmigrate.Backup) error {
	r.backupID = request.ID
	return r.runErr
}

func (*cleanupToolRunner) RunRestore(context.Context, pvmigrate.Restore) error {
	return nil
}

func (r *cleanupToolRunner) UninstallTool(
	ctx context.Context,
	_, _, namespace, release string,
) error {
	deadline, hasDeadline := ctx.Deadline()
	r.uninstalled = append(r.uninstalled, uninstallCall{
		namespace:   namespace,
		release:     release,
		ctxErr:      ctx.Err(),
		deadline:    time.Until(deadline),
		hasDeadline: hasDeadline,
	})

	return r.uninstallErr
}

type transferRecordingStore struct {
	s3RepositoryStoreStub

	released []string
}

func newTransferRecordingStore() *transferRecordingStore {
	return &transferRecordingStore{
		s3RepositoryStoreStub: s3RepositoryStoreStub{
			repositoryStoreStub: repositoryStoreStub{backend: "s3"},
		},
	}
}

func (s *transferRecordingStore) AcquireLock(
	context.Context,
	string,
	time.Duration,
) (string, error) {
	return "etag-1", nil
}

func (s *transferRecordingStore) RenewLock(
	context.Context,
	string,
	string,
	time.Duration,
) (string, error) {
	return "etag-1", nil
}

func (s *transferRecordingStore) ReleaseLock(_ context.Context, etag string) error {
	s.released = append(s.released, etag)
	return nil
}

func (transferRecordingStore) RcloneConfig() string { return "[remote]" }

func TestOperationLockReleaseBlocked(t *testing.T) {
	t.Parallel()

	transferErr := errors.New("transfer failed")
	cleanupErr := errors.New("cleanup failed")

	for _, tc := range []struct {
		name       string
		retErr     error
		cleanupErr error
		blocked    bool
	}{
		{name: "success releases", retErr: nil, cleanupErr: nil, blocked: false},
		{name: "transfer error alone releases", retErr: transferErr, cleanupErr: nil, blocked: false},
		{name: "cleanup error alone releases", retErr: nil, cleanupErr: cleanupErr, blocked: false},
		{name: "uncertain tool blocks release", retErr: transferErr, cleanupErr: cleanupErr, blocked: true},
	} {
		if got := operationLockReleaseBlocked(tc.retErr, tc.cleanupErr); got != tc.blocked {
			t.Fatalf("%s: operationLockReleaseBlocked = %v", tc.name, got)
		}
	}
}

func TestCleanupInterruptedToolDetachesFromParentContext(t *testing.T) {
	t.Parallel()

	parent, cancel := context.WithCancel(context.Background())
	cancel()

	runner := &cleanupToolRunner{}
	if err := cleanupInterruptedTool(
		parent,
		runner,
		ToolRuntime{Logger: discardLogger()},
		"backup",
		"app",
		"pv-migrate-pm-x-backup",
	); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}

	if len(runner.uninstalled) != 1 {
		t.Fatalf("uninstall calls = %d", len(runner.uninstalled))
	}

	call := runner.uninstalled[0]
	if call.release != "pv-migrate-pm-x-backup" || call.namespace != "app" {
		t.Fatalf("uninstalled %q in %q", call.release, call.namespace)
	}

	// The parent context is already canceled; the uninstall must still run on
	// a live, bounded context or a cancellation-triggered failure could never
	// converge its own tool.
	if call.ctxErr != nil {
		t.Fatalf("uninstall context inherited parent cancellation: %v", call.ctxErr)
	}

	if !call.hasDeadline || call.deadline <= 0 || call.deadline > toolCleanupTimeout {
		t.Fatalf("uninstall deadline = %v (has %v)", call.deadline, call.hasDeadline)
	}
}

func TestCleanupInterruptedToolWrapsFailure(t *testing.T) {
	t.Parallel()

	runner := &cleanupToolRunner{uninstallErr: errors.New("helm uninstall failed")}
	err := cleanupInterruptedTool(
		context.Background(),
		runner,
		ToolRuntime{Logger: discardLogger()},
		"backup",
		"app",
		"pv-migrate-pm-x-backup",
	)

	if domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("category = %v, error = %v", domain.CategoryOf(err), err)
	}
}

func backupTransferTestClient(consumer bool) *fake.Clientset {
	objects := []runtime.Object{
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv", UID: types.UID("pv")},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{
					Namespace: "app", Name: "data", UID: types.UID("pvc"),
				},
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "data", Namespace: "app", UID: types.UID("pvc"),
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName: "pv",
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
			},
			Status: corev1.PersistentVolumeClaimStatus{
				Phase: corev1.ClaimBound,
			},
		},
	}

	if consumer {
		objects = append(objects, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "app"},
			Spec: corev1.PodSpec{
				NodeName: "node-1",
				Volumes: []corev1.Volume{{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "data",
						},
					},
				}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		})
	}

	return fake.NewClientset(objects...)
}

func backupTransferPlan() *v1alpha1.BackupPlan {
	plan := plannedBackupObject().Status.Plan.DeepCopy()
	plan.Online = true
	return plan
}

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func newBackupTransferFixture(
	client *fake.Clientset,
	store *transferRecordingStore,
	runner ToolRunner,
) backupTransfer {
	return backupTransfer{
		client: client,
		locker: backupExecutorLocker{&recordingBackupSessionLock{}},
		store:  store,
		tools: ToolRuntime{
			HelmTimeout:            time.Minute,
			ToolServiceAccountName: "pvc-migrate",
			Writer:                 io.Discard,
			Logger:                 discardLogger(),
		},
		runner: runner,
	}
}

func TestBackupTransferFailureConvergesToolThenReleasesLock(t *testing.T) {
	transferErr := errors.New("helm wait timeout")
	runner := &cleanupToolRunner{runErr: transferErr}
	store := newTransferRecordingStore()
	transfer := newBackupTransferFixture(backupTransferTestClient(true), store, runner)

	err := transfer.run(t.Context(), "app", "wf", "app", "", *backupTransferPlan(), false)
	if !errors.Is(err, transferErr) {
		t.Fatalf("error = %v", err)
	}

	if len(runner.uninstalled) != 1 {
		t.Fatalf("uninstall calls = %d", len(runner.uninstalled))
	}

	call := runner.uninstalled[0]
	if want := "pv-migrate-" + runner.backupID + "-backup"; call.release != want {
		t.Fatalf("uninstalled %q, want %q", call.release, want)
	}

	if call.namespace != "app" {
		t.Fatalf("uninstall namespace = %q", call.namespace)
	}

	// The tool is confirmed gone, so the operation lock must be released and
	// the next attempt may retry immediately instead of waiting out the TTL.
	if len(store.released) != 1 || store.released[0] != "etag-1" {
		t.Fatalf("released = %v", store.released)
	}
}

func TestBackupTransferCleanupFailureKeepsOperationLock(t *testing.T) {
	transferErr := errors.New("helm wait timeout")
	cleanupErr := errors.New("helm uninstall failed")
	runner := &cleanupToolRunner{runErr: transferErr, uninstallErr: cleanupErr}
	store := newTransferRecordingStore()
	transfer := newBackupTransferFixture(backupTransferTestClient(true), store, runner)

	err := transfer.run(t.Context(), "app", "wf", "app", "", *backupTransferPlan(), false)
	if !errors.Is(err, transferErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("error = %v", err)
	}

	if len(runner.uninstalled) != 1 {
		t.Fatalf("uninstall calls = %d", len(runner.uninstalled))
	}

	// The tool may still be writing, so the lock must stay held and age out by
	// TTL rather than admit a second writer against the same recovery point.
	if len(store.released) != 0 {
		t.Fatalf("lock released despite uncertain tool: %v", store.released)
	}
}

func TestBackupTransferFailureBeforeToolLaunchReleasesWithoutCleanup(t *testing.T) {
	// No consumer Pod: online scheduling fails in prepareTool, before the tool
	// release name exists.
	runner := &cleanupToolRunner{runErr: errors.New("unreachable")}
	store := newTransferRecordingStore()
	transfer := newBackupTransferFixture(backupTransferTestClient(false), store, runner)

	if err := transfer.run(
		t.Context(),
		"app",
		"wf",
		"app",
		"",
		*backupTransferPlan(),
		false,
	); err == nil {
		t.Fatal("scheduling without a consumer unexpectedly succeeded")
	}

	if len(runner.uninstalled) != 0 {
		t.Fatalf("uninstall calls = %d", len(runner.uninstalled))
	}

	// Nothing was launched, so the lock follows the normal release path.
	if len(store.released) != 1 || store.released[0] != "etag-1" {
		t.Fatalf("released = %v", store.released)
	}
}
