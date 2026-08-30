package kube

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
)

type fakeToolLogSource struct {
	mu          sync.Mutex
	list        *corev1.PodList
	podWatch    watch.Interface
	pod         *corev1.Pod
	streams     []string
	streamCalls []*corev1.PodLogOptions
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func (b *lockedBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Len()
}

func (s *fakeToolLogSource) listPods(
	context.Context,
	string,
	metav1.ListOptions,
) (*corev1.PodList, error) {
	if s.list == nil {
		return &corev1.PodList{}, nil
	}
	return s.list.DeepCopy(), nil
}

func (s *fakeToolLogSource) watchPods(
	context.Context,
	string,
	metav1.ListOptions,
) (watch.Interface, error) {
	if s.podWatch == nil {
		s.podWatch = watch.NewRaceFreeFake()
	}
	return s.podWatch, nil
}

func (s *fakeToolLogSource) getPod(context.Context, string, string) (*corev1.Pod, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pod := s.pod.DeepCopy()
	if len(s.streamCalls) >= len(s.streams) {
		pod.Status.Phase = corev1.PodSucceeded
	} else {
		pod.Status.Phase = corev1.PodRunning
	}

	return pod, nil
}

func (s *fakeToolLogSource) streamPodLogs(
	_ context.Context,
	_, _ string,
	options *corev1.PodLogOptions,
) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	copyOptions := options.DeepCopy()
	s.streamCalls = append(s.streamCalls, copyOptions)

	index := len(s.streamCalls) - 1
	if index >= len(s.streams) {
		return io.NopCloser(strings.NewReader("")), nil
	}

	return io.NopCloser(strings.NewReader(s.streams[index])), nil
}

func TestToolLogStreamerFollowsAndPrefixesEveryLine(t *testing.T) {
	pod := upstreamToolPod("pm-operation")
	source := &fakeToolLogSource{
		list: &corev1.PodList{
			ListMeta: metav1.ListMeta{ResourceVersion: "7"},
			Items:    []corev1.Pod{*pod},
		},
		podWatch: watch.NewRaceFreeFake(),
		pod:      pod,
		streams:  []string{"10% copied\r20% copied\r\n100% copied\n"},
	}

	var output lockedBuffer

	stream := startToolLogStream(t.Context(), source, ToolLogOptions{
		Namespaces:  []string{"staging", "staging"},
		OperationID: "pm-operation",
		Writer:      &output,
	}, nil)
	waitForText(t, &output, "100% copied")
	stream.Stop()

	text := output.String()
	for _, want := range []string{
		"Following tool logs for staging/pv-migrate-pm-operation-mount-rsync-abc (container rsync)",
		"[tool staging/pv-migrate-pm-operation-mount-rsync-abc rsync] 10% copied",
		"[tool staging/pv-migrate-pm-operation-mount-rsync-abc rsync] 20% copied",
		"[tool staging/pv-migrate-pm-operation-mount-rsync-abc rsync] 100% copied",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("tool log output lacks %q: %s", want, text)
		}
	}

	if len(source.streamCalls) != 1 || source.streamCalls[0].TailLines != nil ||
		!source.streamCalls[0].Follow {
		t.Fatalf("initial log options=%#v", source.streamCalls)
	}
}

func TestToolLogStreamerReconnectsWithoutReplayingHistory(t *testing.T) {
	pod := upstreamToolPod("pm-retry")
	source := &fakeToolLogSource{pod: pod, streams: []string{"first\n", "second\n"}}

	var output lockedBuffer

	stream := startToolLogStream(t.Context(), source, ToolLogOptions{Writer: &output}, pod)
	waitForText(t, &output, "second")
	stream.Stop()

	if len(source.streamCalls) != 2 {
		t.Fatalf("stream calls=%d", len(source.streamCalls))
	}

	if source.streamCalls[1].TailLines == nil || *source.streamCalls[1].TailLines != 0 {
		t.Fatalf("reconnect options=%#v", source.streamCalls[1])
	}
}

func TestToolLogStreamerKeepsStructuredOutputParseable(t *testing.T) {
	pod := upstreamToolPod("pm-json")
	source := &fakeToolLogSource{pod: pod, streams: []string{"copied 42 bytes\n"}}

	var raw, records lockedBuffer

	logger := slog.New(slog.NewJSONHandler(&records, nil))
	stream := startToolLogStream(t.Context(), source, ToolLogOptions{
		Writer:     &raw,
		Logger:     logger,
		Structured: true,
	}, pod)
	waitForText(t, &records, "copied 42 bytes")
	stream.Stop()

	if raw.Len() != 0 {
		t.Fatalf("structured tool logs wrote raw output: %q", raw.String())
	}

	for _, want := range []string{`"msg":"tool Pod log"`, `"namespace":"staging"`, `"container":"rsync"`, `"line":"copied 42 bytes"`} {
		if !strings.Contains(records.String(), want) {
			t.Fatalf("structured records lack %q: %s", want, records.String())
		}
	}
}

func TestToolLogStreamerDetectsNoSpaceWhenOutputIsDiscarded(t *testing.T) {
	pod := upstreamToolPod("pm-no-space")
	source := &fakeToolLogSource{
		pod:     pod,
		streams: []string{"rsync: write failed: No space left on device (28)\n"},
	}
	stream := startToolLogStream(t.Context(), source, ToolLogOptions{}, pod)

	deadline := time.Now().Add(time.Second)
	for !errors.Is(stream.ObservedError(), ErrToolPodNoSpace) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	stream.Stop()

	if !errors.Is(stream.ObservedError(), ErrToolPodNoSpace) {
		t.Fatalf("observed error=%v", stream.ObservedError())
	}
}

func TestToolLogStreamStopIsBoundedWhenLogStreamIgnoresCancellation(t *testing.T) {
	previousTimeout := toolLogStopTimeout
	toolLogStopTimeout = 10 * time.Millisecond
	t.Cleanup(func() { toolLogStopTimeout = previousTimeout })

	release := make(chan struct{})
	started := make(chan struct{})
	source := &blockingToolLogSource{
		pod:     upstreamToolPod("pm-blocked"),
		release: release,
		started: started,
	}
	stream := startToolLogStream(t.Context(), source, ToolLogOptions{}, source.pod)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("log stream did not start")
	}

	start := time.Now()
	stream.Stop()
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Stop took %s", elapsed)
	}

	close(release)
	select {
	case <-stream.done:
	case <-time.After(time.Second):
		t.Fatal("log stream did not finish after the blocked reader was released")
	}
}

type blockingToolLogSource struct {
	pod     *corev1.Pod
	release chan struct{}
	started chan struct{}
}

func (s *blockingToolLogSource) listPods(context.Context, string, metav1.ListOptions) (*corev1.PodList, error) {
	return &corev1.PodList{}, nil
}

func (s *blockingToolLogSource) watchPods(context.Context, string, metav1.ListOptions) (watch.Interface, error) {
	return watch.NewRaceFreeFake(), nil
}

func (s *blockingToolLogSource) getPod(context.Context, string, string) (*corev1.Pod, error) {
	return s.pod.DeepCopy(), nil
}

func (s *blockingToolLogSource) streamPodLogs(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
	select {
	case <-s.started:
	default:
		close(s.started)
	}

	return blockingToolLogReader{release: s.release}, nil
}

type blockingToolLogReader struct {
	release <-chan struct{}
}

func (r blockingToolLogReader) Read([]byte) (int, error) {
	<-r.release
	return 0, io.EOF
}

func (r blockingToolLogReader) Close() error { return nil }

func TestToolLogMatcherUsesExactOperationSegmentAndKnownComponents(t *testing.T) {
	streamer := &toolLogStreamer{options: ToolLogOptions{OperationID: "pm-abc"}}
	for _, test := range []struct {
		name      string
		instance  string
		component string
		want      bool
	}{
		{name: "rsync", instance: "pv-migrate-pm-abc-mount", component: "rsync", want: true},
		{name: "rclone", instance: "pv-migrate-pm-abc-backup", component: "rclone", want: true},
		{name: "operation prefix collision", instance: "pv-migrate-pm-abcd-mount", component: "rsync"},
		{name: "operation suffix collision", instance: "pv-migrate-old-pm-abcde-mount", component: "rsync"},
		{name: "unrelated component", instance: "pv-migrate-pm-abc-mount", component: "sidecar"},
	} {
		t.Run(test.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				AppInstanceLabel: test.instance, AppComponentLabel: test.component,
			}}}
			if got := streamer.matches(pod); got != test.want {
				t.Fatalf("matches()=%t, want %t", got, test.want)
			}
		})
	}
}

func upstreamToolPod(operationID string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "staging",
			Name:      "pv-migrate-" + operationID + "-mount-rsync-abc",
			UID:       types.UID("pod-uid-" + operationID),
			Labels: map[string]string{
				AppInstanceLabel:  "pv-migrate-" + operationID + "-mount",
				AppComponentLabel: "rsync",
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "rsync"}}},
	}
}

func waitForText(t *testing.T, buffer *lockedBuffer, text string) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buffer.String(), text) {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %q in %q", text, buffer.String())
}
