package app

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// hangingEngine fails a copy attempt only when its attempt context expires.
type hangingEngine struct {
	cancelled int
}

func (e *hangingEngine) Copy(
	ctx context.Context,
	_ copyengine.Request,
	_ copyengine.ProgressFunc,
) error {
	<-ctx.Done()

	e.cancelled++

	return ctx.Err()
}

func (e *hangingEngine) Cleanup(context.Context, copyengine.CleanupRequest) error {
	return nil
}

func TestCopyWithRetryHonorsPerAttemptCopyTimeout(t *testing.T) {
	engine := &hangingEngine{}
	client := fake.NewClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name:   "node-a",
			Labels: map[string]string{"kubernetes.io/hostname": "node-a"},
		}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name:   "node-b",
			Labels: map[string]string{"kubernetes.io/hostname": "node-b"},
		}},
	)

	runner := newVolumeCopyRunner(client, engine, VolumeCopyConfig{
		Retries:      2,
		RetryBackoff: time.Millisecond,
		HelmTimeout:  time.Second,
		CopyTimeout:  30 * time.Millisecond,
		Writer:       io.Discard,
	})
	runner.sleep = func(context.Context, time.Duration) error { return nil }

	request := copyengine.Request{
		SessionID: "copy-timeout-test",
		Mode:      copyengine.ModeWarm,
		Source: v1alpha1.ObjectReference{
			Kind: "PersistentVolumeClaim", Namespace: "source", Name: "a", UID: "a",
		},
		Destination: v1alpha1.ObjectReference{
			Kind: "PersistentVolumeClaim", Namespace: "destination", Name: "b", UID: "b",
		},
	}

	attempts := 0
	lastError := ""

	err := runner.copyWithRetry(
		context.Background(),
		request,
		"node-a", "node-b", "",
		&attempts,
		&lastError,
		[]kube.ToolImageProbeResult{},
		func(context.Context) error { return nil },
		func(context.Context) error { return nil },
		func(context.Context) (bool, error) { return false, nil },
	)
	if err == nil {
		t.Fatal("expected the exhausted retries to surface the timeout failure")
	}

	if !strings.Contains(err.Error(), "--copy-timeout") {
		t.Fatalf("error should attribute the failure to --copy-timeout: %v", err)
	}

	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (one per retry)", attempts)
	}

	if engine.cancelled != 2 {
		t.Fatalf("copier attempts = %d, want 2", engine.cancelled)
	}
}
