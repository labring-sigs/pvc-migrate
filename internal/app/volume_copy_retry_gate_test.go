package app

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// A failed attempt whose tool Pods could not be confirmed released must not
// spend the in-process retry budget beside a possibly-live writer: the copy
// returns and the owner reconciles it again once the tools converge.
func TestCopyWithRetryStopsWhenToolReleaseUncertain(t *testing.T) {
	engine := &concreteCopyEngine{}
	calls := 0
	engine.copy = func(copyengine.CopyRequest) error {
		calls++
		return errors.New("rsync exited 23")
	}

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
	client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("pod listing unavailable")
	})

	runner := newVolumeCopyRunner(client, engine, VolumeCopyConfig{
		Retries:      3,
		RetryBackoff: time.Millisecond,
		HelmTimeout:  time.Second,
		Writer:       io.Discard,
	})
	runner.sleep = func(context.Context, time.Duration) error { return nil }

	request := copyengine.CopyRequest{
		AttemptIdentity: copyengine.AttemptIdentity{
			SessionID: "retry-gate-test",
			Mode:      copyengine.ModeWarm,
			Source: v1alpha1.ObjectReference{
				Kind: "PersistentVolumeClaim", Namespace: "source", Name: "a", UID: "a",
			},
		},
		Destination: copyengine.CopyDestination{Reference: v1alpha1.ObjectReference{
			Kind: "PersistentVolumeClaim", Namespace: "destination", Name: "b", UID: "b",
		}},
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
	if err == nil || !strings.Contains(err.Error(), "list copy tool Pods") {
		t.Fatalf("error = %v, want the unconverged-tool failure", err)
	}

	if attempts != 1 || calls != 1 {
		t.Fatalf(
			"attempts = %d, engine calls = %d; the retry must not run beside an unconverged tool",
			attempts,
			calls,
		)
	}
}
