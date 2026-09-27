package backup

import (
	"errors"
	"io"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

// restoreTransferFixture drives restoreTransfer.run against a fake cluster
// whose destination PVC carries the annotation lock the run installs. The
// lock annotation is the durable state the deferred finalizer owns, so the
// tests assert on it directly.
func restoreTransferFixture(t *testing.T) (*restoreTransfer, *fake.Clientset, *cleanupToolRunner) {
	t.Helper()

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv", UID: types.UID("pv")},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{
				Namespace: "app", Name: "dest", UID: types.UID("pvc"),
			},
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "kubernetes.io/hostname",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"node-1"},
					}},
				}}},
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "dest", Namespace: "app", UID: types.UID("pvc")},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv",
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	client := fake.NewClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
		pv,
		pvc,
	)

	runner := &cleanupToolRunner{}

	return &restoreTransfer{
		client: client,
		store:  newTransferRecordingStore(),
		tools: ToolRuntime{
			HelmTimeout:            time.Minute,
			ToolServiceAccountName: "pvc-migrate",
			Writer:                 io.Discard,
			Logger:                 discardLogger(),
		},
		runner: runner,
	}, client, runner
}

func restoreRunPlan() v1alpha1.RestorePlan {
	return v1alpha1.RestorePlan{
		DestinationPVC: v1alpha1.LocalResourceReference{Name: "dest", UID: "pvc"},
		Path:           ".",
		ToolImage:      "example/tool:v1",
	}
}

func restoreLockRetained(t *testing.T, client *fake.Clientset) bool {
	t.Helper()

	pvc, err := client.CoreV1().
		PersistentVolumeClaims("app").
		Get(t.Context(), "dest", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	return pvc.Annotations[restoreLockAnnotation] != ""
}

func TestRestoreTransferFailureConvergesToolThenUnlocks(t *testing.T) {
	transfer, client, runner := restoreTransferFixture(t)
	transferErr := errors.New("helm wait timeout")
	runner.runErr = transferErr

	err := transfer.run(
		t.Context(),
		"app",
		"wf",
		restoreRunPlan(),
		"pv",
		objectstore.Manifest{},
	)
	if !errors.Is(err, transferErr) {
		t.Fatalf("error = %v", err)
	}

	if len(runner.uninstalled) != 1 {
		t.Fatalf("uninstall calls = %d", len(runner.uninstalled))
	}

	call := runner.uninstalled[0]
	if want := restoreToolReleaseName(runner.restoreID); call.release != want {
		t.Fatalf("uninstalled %q, want %q", call.release, want)
	}

	// The tool is confirmed gone, so the destination lock must be removed.
	if restoreLockRetained(t, client) {
		t.Fatal("destination lock retained after converged tool")
	}
}

func TestRestoreTransferCleanupFailureKeepsDestinationLock(t *testing.T) {
	transfer, client, runner := restoreTransferFixture(t)
	transferErr := errors.New("helm wait timeout")
	cleanupErr := errors.New("helm uninstall failed")
	runner.runErr = transferErr
	runner.uninstallErr = cleanupErr

	err := transfer.run(
		t.Context(),
		"app",
		"wf",
		restoreRunPlan(),
		"pv",
		objectstore.Manifest{},
	)
	if !errors.Is(err, transferErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("error = %v", err)
	}

	// The tool may still write into the destination, so the annotation lock
	// must stay and age out instead of admitting a second writer.
	if !restoreLockRetained(t, client) {
		t.Fatal("destination lock released despite uncertain tool")
	}
}

func TestRestoreTransferFailureBeforeToolLaunchUnlocksWithoutCleanup(t *testing.T) {
	transfer, client, runner := restoreTransferFixture(t)

	// A wrong PV identity fails inside verifyPVCIdentity, before the tool
	// release name exists.
	err := transfer.run(
		t.Context(),
		"app",
		"wf",
		restoreRunPlan(),
		"other-pv",
		objectstore.Manifest{},
	)
	if err == nil {
		t.Fatal("identity mismatch unexpectedly succeeded")
	}

	if len(runner.uninstalled) != 0 {
		t.Fatalf("uninstall calls = %d", len(runner.uninstalled))
	}

	if restoreLockRetained(t, client) {
		t.Fatal("destination lock retained though no tool was launched")
	}
}
