package kube

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

const (
	toolLogRetryDelay = 250 * time.Millisecond
	toolLogMaxLine    = 1024 * 1024
)

// Kubernetes Pod log streams are expected to close when their context is
// cancelled. A broken or stalled API proxy can leave a stream blocked, though;
// never let that prevent the workflow from deleting the tool Pods.
var toolLogStopTimeout = 5 * time.Second

// ErrToolPodNoSpace records destination exhaustion reported by a data-mover
// Pod when upstream pv-migrate omits the Pod log tail from its returned error.
var ErrToolPodNoSpace = errors.New("tool Pod reported no space left on device")

// ToolLogOptions controls how logs from short-lived tool Pods are surfaced.
// Writer is used for text logs; structured logs are emitted through Logger.
type ToolLogOptions struct {
	Namespaces  []string
	OperationID string
	Writer      io.Writer
	Logger      *slog.Logger
	Structured  bool
}

// ToolLogStream owns the background watches and Pod log requests for one
// tool operation.
type ToolLogStream struct {
	cancel context.CancelFunc
	done   chan struct{}
	logger *slog.Logger

	mu            sync.Mutex
	observedError error
}

// Stop closes active watches and log streams. The bounded wait keeps a stalled
// Kubernetes log endpoint from blocking the migration's resource cleanup.
func (s *ToolLogStream) Stop() {
	if s == nil {
		return
	}

	s.cancel()

	timer := time.NewTimer(toolLogStopTimeout)
	defer timer.Stop()

	select {
	case <-s.done:
	case <-timer.C:
		if s.logger != nil {
			s.logger.Warn(
				"tool Pod log stream did not stop before timeout; continuing cleanup",
				"timeout",
				toolLogStopTimeout,
			)
		}
	}
}

// ObservedError returns a terminal storage error seen in the tool Pod logs.
func (s *ToolLogStream) ObservedError() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.observedError
}

func (s *ToolLogStream) observeLine(line string) {
	if s == nil {
		return
	}

	lower := strings.ToLower(line)
	if !strings.Contains(lower, "no space left on device") && !strings.Contains(lower, "enospc") {
		return
	}

	s.mu.Lock()
	s.observedError = ErrToolPodNoSpace
	s.mu.Unlock()
}

// StartPVMigrateToolLogs follows every upstream rsync, sshd, and rclone Pod
// whose Helm release contains the exact operation ID.
func StartPVMigrateToolLogs(
	ctx context.Context,
	client kubernetes.Interface,
	options ToolLogOptions,
) *ToolLogStream {
	if client == nil || options.OperationID == "" || len(options.Namespaces) == 0 {
		return nil
	}
	return startToolLogStream(ctx, kubernetesToolLogSource{client: client}, options, nil)
}

// StartPodLogs follows an already discovered tool Pod. It is used for the
// reservation consumer, whose name and labels are owned by this project.
func StartPodLogs(
	ctx context.Context,
	client kubernetes.Interface,
	pod *corev1.Pod,
	options ToolLogOptions,
) *ToolLogStream {
	if client == nil || pod == nil {
		return nil
	}
	return startToolLogStream(ctx, kubernetesToolLogSource{client: client}, options, pod)
}

type toolLogSource interface {
	listPods(
		ctx context.Context,
		namespace string,
		options metav1.ListOptions,
	) (*corev1.PodList, error)
	watchPods(
		ctx context.Context,
		namespace string,
		options metav1.ListOptions,
	) (watch.Interface, error)
	getPod(ctx context.Context, namespace, name string) (*corev1.Pod, error)
	streamPodLogs(
		ctx context.Context,
		namespace, name string,
		options *corev1.PodLogOptions,
	) (io.ReadCloser, error)
}

type kubernetesToolLogSource struct {
	client kubernetes.Interface
}

func (s kubernetesToolLogSource) listPods(
	ctx context.Context,
	namespace string,
	options metav1.ListOptions,
) (*corev1.PodList, error) {
	return s.client.CoreV1().Pods(namespace).List(ctx, options)
}

func (s kubernetesToolLogSource) watchPods(
	ctx context.Context,
	namespace string,
	options metav1.ListOptions,
) (watch.Interface, error) {
	return s.client.CoreV1().Pods(namespace).Watch(ctx, options)
}

func (s kubernetesToolLogSource) getPod(
	ctx context.Context,
	namespace, name string,
) (*corev1.Pod, error) {
	return s.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
}

func (s kubernetesToolLogSource) streamPodLogs(
	ctx context.Context,
	namespace, name string,
	options *corev1.PodLogOptions,
) (io.ReadCloser, error) {
	return s.client.CoreV1().Pods(namespace).GetLogs(name, options).Stream(ctx)
}

type toolLogStreamer struct {
	source  toolLogSource
	options ToolLogOptions
	stream  *ToolLogStream

	mu      sync.Mutex
	seen    map[string]struct{}
	writers sync.WaitGroup
}

func startToolLogStream(
	parent context.Context,
	source toolLogSource,
	options ToolLogOptions,
	exactPod *corev1.Pod,
) *ToolLogStream {
	if options.Writer == nil {
		options.Writer = io.Discard
	}

	if options.Logger == nil {
		options.Logger = slog.New(slog.DiscardHandler)
	}

	ctx, cancel := context.WithCancel(parent)
	stream := &ToolLogStream{
		cancel: cancel,
		done:   make(chan struct{}),
		logger: options.Logger,
	}

	streamer := &toolLogStreamer{
		source:  source,
		options: options,
		stream:  stream,
		seen:    make(map[string]struct{}),
	}
	go func() {
		defer close(stream.done)

		if exactPod != nil {
			streamer.startPod(ctx, exactPod)
		} else {
			streamer.watchNamespaces(ctx)
		}

		streamer.writers.Wait()
	}()

	return stream
}

func (s *toolLogStreamer) watchNamespaces(ctx context.Context) {
	namespaces := append([]string(nil), s.options.Namespaces...)
	slices.Sort(namespaces)
	namespaces = slices.Compact(namespaces)

	var watchers sync.WaitGroup
	for _, namespace := range namespaces {
		if namespace == "" {
			continue
		}

		watchers.Go(func() {
			s.watchNamespace(ctx, namespace)
		})
	}

	watchers.Wait()
}

func (s *toolLogStreamer) watchNamespace(ctx context.Context, namespace string) {
	for ctx.Err() == nil {
		list, err := s.source.listPods(ctx, namespace, metav1.ListOptions{})
		if err != nil {
			s.logWatchError(ctx, namespace, "list", err)

			if !waitToolLogRetry(ctx) {
				return
			}

			continue
		}

		for index := range list.Items {
			s.startPod(ctx, &list.Items[index])
		}

		podWatch, err := s.source.watchPods(ctx, namespace, metav1.ListOptions{
			ResourceVersion: list.ResourceVersion,
		})
		if err != nil {
			s.logWatchError(ctx, namespace, "watch", err)

			if !waitToolLogRetry(ctx) {
				return
			}

			continue
		}

		s.consumeWatch(ctx, podWatch)
		podWatch.Stop()

		if !waitToolLogRetry(ctx) {
			return
		}
	}
}

func (s *toolLogStreamer) consumeWatch(ctx context.Context, podWatch watch.Interface) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-podWatch.ResultChan():
			if !ok {
				return
			}

			pod, ok := event.Object.(*corev1.Pod)
			if ok && (event.Type == watch.Added || event.Type == watch.Modified) {
				s.startPod(ctx, pod)
			}
		}
	}
}

func (s *toolLogStreamer) startPod(ctx context.Context, pod *corev1.Pod) {
	if pod == nil || !s.matches(pod) {
		return
	}

	for _, container := range pod.Spec.Containers {
		key := string(pod.UID) + "/" + container.Name
		if pod.UID == "" {
			key = pod.Namespace + "/" + pod.Name + "/" + container.Name
		}

		s.mu.Lock()

		_, exists := s.seen[key]
		if !exists {
			s.seen[key] = struct{}{}
		}
		s.mu.Unlock()

		if exists {
			continue
		}

		podNamespace, podName, containerName := pod.Namespace, pod.Name, container.Name
		s.writers.Go(func() {
			s.followContainer(ctx, podNamespace, podName, containerName)
		})
	}
}

func (s *toolLogStreamer) matches(pod *corev1.Pod) bool {
	if s.options.OperationID == "" {
		return true
	}

	instance := pod.Labels[AppInstanceLabel]
	if instance == "" || !containsNameSegment(instance, s.options.OperationID) {
		return false
	}

	switch pod.Labels[AppComponentLabel] {
	case "rsync", "sshd", "rclone":
		return true
	default:
		return false
	}
}

func containsNameSegment(value, segment string) bool {
	return strings.Contains("-"+value+"-", "-"+segment+"-")
}

func (s *toolLogStreamer) followContainer(ctx context.Context, namespace, pod, container string) {
	s.emitStart(ctx, namespace, pod, container)

	streamed := false
	for ctx.Err() == nil {
		options := &corev1.PodLogOptions{Container: container, Follow: true}
		if streamed {
			tail := int64(0)
			options.TailLines = &tail
		}

		stream, err := s.source.streamPodLogs(ctx, namespace, pod, options)
		if err == nil {
			streamed = true
			err = s.consumeLogStream(ctx, stream, namespace, pod, container)
		}

		if ctx.Err() != nil {
			return
		}

		current, getErr := s.source.getPod(ctx, namespace, pod)
		if apierrors.IsNotFound(getErr) || (getErr == nil && podLogsComplete(current, container)) {
			return
		}

		if apierrors.IsForbidden(err) {
			s.options.Logger.Warn(
				"tool Pod logs are forbidden",
				"namespace",
				namespace,
				"pod",
				pod,
				"container",
				container,
				"error",
				err,
			)

			return
		}

		if err != nil {
			s.options.Logger.Debug(
				"tool Pod log stream ended; retrying",
				"namespace",
				namespace,
				"pod",
				pod,
				"container",
				container,
				"error",
				err,
			)
		}

		if getErr != nil {
			s.options.Logger.Debug(
				"failed to inspect tool Pod after log stream ended",
				"namespace",
				namespace,
				"pod",
				pod,
				"error",
				getErr,
			)
		}

		if !waitToolLogRetry(ctx) {
			return
		}
	}
}

func (s *toolLogStreamer) consumeLogStream(
	ctx context.Context,
	stream io.ReadCloser,
	namespace, pod, container string,
) error {
	defer stream.Close()

	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 64*1024), toolLogMaxLine)
	scanner.Split(scanToolLogLines)

	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		s.emitLine(ctx, namespace, pod, container, scanner.Text())
	}

	return scanner.Err()
}

// scanToolLogLines treats rsync's carriage-return progress updates as
// individual lines, keeping each update visible after the CLI adds its Pod
// and container prefix.
func scanToolLogLines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if index := bytes.IndexAny(data, "\r\n"); index >= 0 {
		if data[index] == '\r' && index+1 < len(data) && data[index+1] == '\n' {
			return index + 2, data[:index], nil
		}
		return index + 1, data[:index], nil
	}

	if atEOF {
		return len(data), data, nil
	}

	return 0, nil, nil
}

func podLogsComplete(pod *corev1.Pod, container string) bool {
	if pod == nil {
		return false
	}

	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return true
	}

	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == container && status.State.Terminated != nil {
			return true
		}
	}

	return false
}

func (s *toolLogStreamer) emitStart(ctx context.Context, namespace, pod, container string) {
	if s.options.Structured {
		s.options.Logger.InfoContext(
			ctx,
			"following tool Pod logs",
			"namespace",
			namespace,
			"pod",
			pod,
			"container",
			container,
		)

		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, _ = fmt.Fprintf(
		s.options.Writer,
		"Following tool logs for %s/%s (container %s)\n",
		namespace,
		pod,
		container,
	)
}

func (s *toolLogStreamer) emitLine(ctx context.Context, namespace, pod, container, line string) {
	s.stream.observeLine(line)

	if s.options.Structured {
		s.options.Logger.InfoContext(
			ctx,
			"tool Pod log",
			"namespace",
			namespace,
			"pod",
			pod,
			"container",
			container,
			"line",
			line,
		)

		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, _ = fmt.Fprintf(s.options.Writer, "[tool %s/%s %s] %s\n", namespace, pod, container, line)
}

func (s *toolLogStreamer) logWatchError(ctx context.Context, namespace, action string, err error) {
	if ctx.Err() != nil {
		return
	}

	s.options.Logger.Debug(
		"tool Pod log discovery failed; retrying",
		"namespace",
		namespace,
		"action",
		action,
		"error",
		err,
	)
}

func waitToolLogRetry(ctx context.Context) bool {
	timer := time.NewTimer(toolLogRetryDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
