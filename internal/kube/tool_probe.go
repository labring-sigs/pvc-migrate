package kube

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

const (
	ToolComponentShell  = "sh"
	ToolComponentRsync  = "rsync"
	ToolComponentSSHD   = "sshd"
	ToolComponentRclone = "rclone"
	toolProbeRole       = "tool-probe"
	toolProbeOperation  = "pvc-migrate.io/probe-operation"
	toolProbeTimeout    = 2 * time.Minute
	toolProbePoll       = 250 * time.Millisecond
	toolProbeCleanupTTL = 10 * time.Second
)

// ToolProbeTarget describes one node and namespace where a real tool Pod will
// run. An empty NodeName lets the scheduler choose a node. PVCName optionally
// applies the real volume's scheduling and mount constraints to that choice.
type ToolProbeTarget struct {
	Namespace  string
	NodeName   string
	PVCName    string
	Components []string
}

// ToolImageProbeOptions controls a preflight Pod that verifies the image can
// be pulled, scheduled, started, and provide the commands used by the
// selected tool roles.
type ToolImageProbeOptions struct {
	OperationID string
	Image       string
	Targets     []ToolProbeTarget
	Timeout     time.Duration
	Poll        time.Duration
	Writer      io.Writer
	Logger      *slog.Logger
}

// ToolImageProbeResult records the scheduling and image-pull settings accepted
// by the API server for one probe target. Callers reuse these values for the
// real tool Pod so a scheduler-selected probe remains representative.
type ToolImageProbeResult struct {
	Target           ToolProbeTarget
	NodeName         string
	ImagePullSecrets []corev1.LocalObjectReference
}

// ToolImageProber is injected into higher-level workflows so unit tests can
// model probe outcomes without creating Kubernetes Pods.
type ToolImageProber interface {
	Probe(context.Context, ToolImageProbeOptions) ([]ToolImageProbeResult, error)
}

type KubernetesToolImageProber struct {
	client   kubernetes.Interface
	poll     time.Duration
	outputMu sync.Mutex
}

func NewToolImageProber(client kubernetes.Interface) *KubernetesToolImageProber {
	return &KubernetesToolImageProber{client: client, poll: toolProbePoll}
}

func (p *KubernetesToolImageProber) Probe(ctx context.Context, options ToolImageProbeOptions) ([]ToolImageProbeResult, error) {
	if p == nil || p.client == nil {
		return nil, domain.NewError(domain.ErrorInternal, "tool image probe", "Kubernetes client is required")
	}
	image, err := NormalizeToolImage(options.Image)
	if err != nil {
		return nil, err
	}
	targets, err := normalizeToolProbeTargets(options.Targets)
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, nil
	}
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = toolProbeTimeout
	}
	poll := options.Poll
	if poll <= 0 {
		poll = p.poll
	}
	if poll <= 0 {
		poll = toolProbePoll
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	results := make([]ToolImageProbeResult, len(targets))
	group, groupCtx := errgroup.WithContext(probeCtx)
	for index := range targets {
		index := index
		group.Go(func() error {
			result, probeErr := p.probeTarget(groupCtx, image, options.OperationID, targets[index], timeout, poll, options)
			if probeErr != nil {
				return probeErr
			}
			results[index] = result
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

func (p *KubernetesToolImageProber) probeTarget(ctx context.Context, image, operationID string, target ToolProbeTarget, timeout, poll time.Duration, options ToolImageProbeOptions) (result ToolImageProbeResult, retErr error) {
	var node *corev1.Node
	if target.NodeName != "" {
		var err error
		node, err = p.client.CoreV1().Nodes().Get(ctx, target.NodeName, metav1.GetOptions{})
		if err != nil {
			return result, domain.WrapError(domain.ErrorKubernetes, "tool image probe", fmt.Sprintf("read node %s", target.NodeName), err)
		}
		if node == nil || node.Name == "" {
			return result, domain.NewError(domain.ErrorKubernetes, "tool image probe", fmt.Sprintf("read node %s returned an empty object", target.NodeName))
		}
		if !nodeReadyAndSchedulable(node) {
			return result, domain.NewError(domain.ErrorPrecondition, "tool image probe", fmt.Sprintf("node %s is not Ready and schedulable", node.Name))
		}
		if node.Labels[corev1.LabelHostname] == "" {
			return result, domain.NewError(domain.ErrorPrecondition, "tool image probe", fmt.Sprintf("node %s lacks %s", node.Name, corev1.LabelHostname))
		}
	}
	pod := toolProbePod(image, operationID, target, node, timeout)
	created, err := p.client.CoreV1().Pods(target.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return result, domain.WrapError(domain.ErrorPrecondition, "tool image probe", fmt.Sprintf("create probe Pod %s/%s: %v", target.Namespace, pod.Name, err), err)
	}
	defer func() {
		if cleanupErr := p.cleanupProbePod(target.Namespace, created.Name, created.UID, poll); cleanupErr != nil {
			retErr = errors.Join(retErr, cleanupErr)
		}
	}()
	observedTarget := target
	imagePullSecrets := slices.Clone(created.Spec.ImagePullSecrets)
	if err := WaitFor(ctx, poll, fmt.Sprintf("tool image probe Pod %s/%s", target.Namespace, created.Name), func(waitCtx context.Context) (bool, error) {
		current, getErr := p.client.CoreV1().Pods(target.Namespace).Get(waitCtx, created.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(getErr) {
			return false, domain.NewError(domain.ErrorConflict, "tool image probe", fmt.Sprintf("probe Pod %s/%s disappeared before completion", target.Namespace, created.Name))
		}
		if getErr != nil {
			return false, domain.WrapError(domain.ErrorKubernetes, "tool image probe", fmt.Sprintf("read probe Pod %s/%s", target.Namespace, created.Name), getErr)
		}
		if current.Spec.NodeName != "" {
			observedTarget.NodeName = current.Spec.NodeName
		}
		imagePullSecrets = slices.Clone(current.Spec.ImagePullSecrets)
		switch current.Status.Phase {
		case corev1.PodSucceeded:
			return true, nil
		case corev1.PodFailed:
			return false, toolProbePodFailure(current, image, observedTarget)
		default:
			if reason, message, fatal := toolProbePendingFailure(current); fatal {
				return false, toolProbePodFailureWithMessage(current, image, observedTarget, reason, message)
			}
			return false, nil
		}
	}); err != nil {
		if domain.CategoryOf(err) == domain.ErrorTimeout {
			if details := p.probeDiagnostics(target.Namespace, created.Name, created.UID); details != "" {
				return result, domain.WrapError(domain.ErrorTimeout, "tool image probe", fmt.Sprintf("probe Pod %s/%s did not complete: %s", target.Namespace, created.Name, details), err)
			}
		}
		return result, err
	}
	p.outputMu.Lock()
	logProbeSuccess(options, image, observedTarget, created.Name)
	p.outputMu.Unlock()
	return ToolImageProbeResult{
		Target:           observedTarget,
		NodeName:         observedTarget.NodeName,
		ImagePullSecrets: imagePullSecrets,
	}, nil
}

func (p *KubernetesToolImageProber) cleanupProbePod(namespace, name string, uid types.UID, poll time.Duration) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), toolProbeCleanupTTL)
	defer cancel()
	deleteErr := p.client.CoreV1().Pods(namespace).Delete(cleanupCtx, name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	})
	if apierrors.IsNotFound(deleteErr) {
		return nil
	}
	if deleteErr != nil {
		return domain.WrapError(domain.ErrorKubernetes, "tool image probe", fmt.Sprintf("delete probe Pod %s/%s", namespace, name), deleteErr)
	}
	return WaitFor(cleanupCtx, poll, fmt.Sprintf("probe Pod %s/%s deletion", namespace, name), func(waitCtx context.Context) (bool, error) {
		current, err := p.client.CoreV1().Pods(namespace).Get(waitCtx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, domain.WrapError(domain.ErrorKubernetes, "tool image probe", fmt.Sprintf("confirm probe Pod %s/%s deletion", namespace, name), err)
		}
		if current == nil || current.UID != uid {
			return false, domain.NewError(domain.ErrorConflict, "tool image probe", fmt.Sprintf("Pod %s/%s was replaced while waiting for probe cleanup", namespace, name))
		}
		return false, nil
	})
}

func (p *KubernetesToolImageProber) probeDiagnostics(namespace, name string, uid types.UID) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	parts := make([]string, 0)
	pod, err := p.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil && pod != nil {
		parts = append(parts, toolProbePodStatusParts(pod)...)
	}
	selector := fields.OneTermEqualSelector("involvedObject.name", name).String()
	events, err := p.client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{FieldSelector: selector})
	if err == nil && events != nil {
		for _, event := range events.Items {
			if event.InvolvedObject.Name != name || (uid != "" && event.InvolvedObject.UID != "" && event.InvolvedObject.UID != uid) {
				continue
			}
			parts = append(parts, strings.TrimSpace(event.Reason+": "+event.Message))
		}
	}
	return joinProbeFailure(parts...)
}

func normalizeToolProbeTargets(targets []ToolProbeTarget) ([]ToolProbeTarget, error) {
	merged := make(map[string]ToolProbeTarget, len(targets))
	for _, target := range targets {
		if target.Namespace == "" {
			return nil, domain.NewError(domain.ErrorValidation, "tool image probe", "target namespace is required")
		}
		key := target.Namespace + "\x00" + target.NodeName + "\x00" + target.PVCName
		current := merged[key]
		current.Namespace = target.Namespace
		current.NodeName = target.NodeName
		current.PVCName = target.PVCName
		for _, component := range append([]string{ToolComponentShell}, target.Components...) {
			if !validToolProbeComponent(component) {
				return nil, domain.NewError(domain.ErrorValidation, "tool image probe", fmt.Sprintf("unsupported tool component %q", component))
			}
			if !slices.Contains(current.Components, component) {
				current.Components = append(current.Components, component)
			}
		}
		merged[key] = current
	}
	result := make([]ToolProbeTarget, 0, len(merged))
	for _, target := range merged {
		slices.Sort(target.Components)
		result = append(result, target)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Namespace != result[j].Namespace {
			return result[i].Namespace < result[j].Namespace
		}
		if result[i].NodeName != result[j].NodeName {
			return result[i].NodeName < result[j].NodeName
		}
		return result[i].PVCName < result[j].PVCName
	})
	return result, nil
}

func validToolProbeComponent(component string) bool {
	switch component {
	case ToolComponentShell, ToolComponentRsync, ToolComponentSSHD, ToolComponentRclone:
		return true
	default:
		return false
	}
}

// ToolImagePullSecretHelmValues forwards image-pull credentials admitted onto
// probe Pods to the matching upstream chart components. A single Helm release
// can deploy a component into more than one namespace (for example local SSHD
// releases), while its values contain one shared secret list. Every namespace
// used by that component therefore must expose the same list; accepting a
// union would reference Secret objects that do not exist in some namespaces.
func ToolImagePullSecretHelmValues(results []ToolImageProbeResult) ([]string, error) {
	secretNames := map[string]map[string][]string{
		ToolComponentRsync:  {},
		ToolComponentSSHD:   {},
		ToolComponentRclone: {},
	}
	for _, result := range results {
		for _, component := range result.Target.Components {
			namespaces, ok := secretNames[component]
			if !ok {
				continue
			}
			secrets := make([]string, 0, len(result.ImagePullSecrets))
			for _, secret := range result.ImagePullSecrets {
				if secret.Name != "" && !slices.Contains(secrets, secret.Name) {
					secrets = append(secrets, secret.Name)
				}
			}
			slices.Sort(secrets)
			if previous, exists := namespaces[result.Target.Namespace]; exists && !slices.Equal(previous, secrets) {
				return nil, domain.NewError(domain.ErrorPrecondition, "tool image probe", fmt.Sprintf("imagePullSecrets for tool component %s differ between probe Pods in namespace %s", component, result.Target.Namespace))
			}
			namespaces[result.Target.Namespace] = secrets
		}
	}
	values := make([]string, 0)
	for _, component := range []string{ToolComponentRsync, ToolComponentSSHD, ToolComponentRclone} {
		var selected []string
		for namespace, names := range secretNames[component] {
			if selected == nil {
				selected = names
				continue
			}
			if !slices.Equal(selected, names) {
				return nil, domain.NewError(domain.ErrorPrecondition, "tool image probe", fmt.Sprintf("imagePullSecrets for tool component %s differ between namespaces (including %s); create the same Secret names in every namespace or provide a shared pull credential", component, namespace))
			}
		}
		for index, name := range selected {
			values = append(values, fmt.Sprintf("%s.imagePullSecrets[%d].name=%s", component, index, name))
		}
	}
	return values, nil
}

func toolProbePod(image, operationID string, target ToolProbeTarget, node *corev1.Node, timeout time.Duration) *corev1.Pod {
	name := BoundedName("pvc-migrate-probe", operationID, target.Namespace, target.NodeName, target.PVCName, string(uuid.NewUUID()))
	labels := map[string]string{
		ManagedByLabel:    ManagedByValue,
		ResourceRoleLabel: toolProbeRole,
	}
	if operationID != "" && len(utilvalidation.IsValidLabelValue(operationID)) == 0 {
		labels[SessionKey] = operationID
	}
	command := toolProbeCommand(target.Components)
	securityContext := &corev1.SecurityContext{RunAsUser: int64Pointer(0), RunAsGroup: int64Pointer(0)}
	if slices.Contains(target.Components, ToolComponentSSHD) {
		securityContext.Capabilities = &corev1.Capabilities{Add: []corev1.Capability{"SYS_CHROOT"}}
	}
	activeDeadline := int64(timeout / time.Second)
	if timeout%time.Second != 0 {
		activeDeadline++
	}
	activeDeadline = max(activeDeadline, 1)
	automountServiceAccountToken := false
	spec := corev1.PodSpec{
		RestartPolicy:                 corev1.RestartPolicyNever,
		TerminationGracePeriodSeconds: int64Pointer(1),
		ActiveDeadlineSeconds:         &activeDeadline,
		AutomountServiceAccountToken:  &automountServiceAccountToken,
		Containers: []corev1.Container{{
			Name:                     "tool-probe",
			Image:                    image,
			ImagePullPolicy:          corev1.PullIfNotPresent,
			SecurityContext:          securityContext,
			Command:                  []string{"sh", "-c", command},
			Resources:                ZeroResourceRequirements(),
			TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
		}},
	}
	if node != nil {
		spec.NodeSelector = map[string]string{corev1.LabelHostname: node.Labels[corev1.LabelHostname]}
		spec.Tolerations = nodeTolerations(node)
	}
	if target.PVCName != "" {
		spec.Volumes = []corev1.Volume{{
			Name: "source-pvc",
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: target.PVCName,
				ReadOnly:  true,
			}},
		}}
		spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "source-pvc", MountPath: "/probe-volume", ReadOnly: true}}
	}
	annotations := map[string]string{
		"pvc-migrate.io/tool-components": strings.Join(target.Components, ","),
	}
	if target.PVCName != "" {
		annotations["pvc-migrate.io/probe-pvc"] = target.PVCName
	}
	if operationID != "" {
		annotations[toolProbeOperation] = operationID
	}
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:        name,
		Namespace:   target.Namespace,
		Labels:      labels,
		Annotations: annotations,
	}, Spec: spec}
}

func toolProbeCommand(components []string) string {
	requiredCommands := make([]string, 0)
	for _, component := range components {
		for _, command := range toolProbeComponentCommands(component) {
			if !slices.Contains(requiredCommands, command) {
				requiredCommands = append(requiredCommands, command)
			}
		}
	}
	commands := make([]string, 0, len(requiredCommands)+len(components))
	for _, command := range requiredCommands {
		commands = append(commands, fmt.Sprintf("command -v %s >/dev/null 2>&1 || { echo 'required tool command %s is missing' >&2; exit 127; }", command, command))
	}
	for _, component := range components {
		switch component {
		case ToolComponentRsync:
			commands = append(commands, "rsync --version >/dev/null", "ssh -V >/dev/null 2>&1")
		case ToolComponentSSHD:
			commands = append(commands,
				"rsync --version >/dev/null",
				"test -x /usr/sbin/sshd || { echo 'required tool path /usr/sbin/sshd is unavailable' >&2; exit 127; }",
				"test -r /etc/ssh/sshd_config || { echo 'required tool file /etc/ssh/sshd_config is unavailable' >&2; exit 127; }",
				"ssh-keygen -q -t ed25519 -f /tmp/pvc-migrate-probe-host-key -N ''",
				"ssh-keygen -q -t ecdsa -f /tmp/pvc-migrate-probe-host-key-ecdsa -N ''",
				"/usr/sbin/sshd -t -f /etc/ssh/sshd_config -o HostKey=/tmp/pvc-migrate-probe-host-key -o HostKey=/tmp/pvc-migrate-probe-host-key-ecdsa",
			)
		case ToolComponentRclone:
			commands = append(commands, "rclone version >/dev/null")
		}
	}
	return "set -eu; " + strings.Join(commands, "; ")
}

func toolProbeComponentCommands(component string) []string {
	switch component {
	case ToolComponentShell:
		return []string{"sh", "sleep", "test"}
	case ToolComponentRsync:
		return []string{"rsync", "ssh", "awk", "id", "basename", "mkdir", "chmod", "cp"}
	case ToolComponentSSHD:
		return []string{"sshd", "ssh-keygen", "rsync", "awk", "id", "basename", "mkdir", "chmod", "cp"}
	case ToolComponentRclone:
		return []string{"rclone", "base64", "sleep"}
	default:
		return nil
	}
}

func toolProbePodFailure(pod *corev1.Pod, image string, target ToolProbeTarget) error {
	return toolProbePodFailureWithMessage(pod, image, target, "", "")
}

func toolProbePodFailureWithMessage(pod *corev1.Pod, image string, target ToolProbeTarget, reason, message string) error {
	parts := append([]string{message, reason}, toolProbePodStatusParts(pod)...)
	details := joinProbeFailure(parts...)
	if details == "" {
		details = "Pod entered Failed phase"
	}
	node := target.NodeName
	if node == "" {
		node = "scheduler-selected node"
	}
	return domain.NewError(domain.ErrorPrecondition, "tool image probe", fmt.Sprintf("tool image %s failed on %s/%s for components %s: %s", image, target.Namespace, node, strings.Join(target.Components, ","), details))
}

func toolProbePodStatusParts(pod *corev1.Pod) []string {
	if pod == nil {
		return nil
	}
	parts := []string{pod.Status.Message, pod.Status.Reason}
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Waiting != nil {
			parts = append(parts, status.State.Waiting.Reason, status.State.Waiting.Message)
		}
		if status.State.Terminated != nil {
			parts = append(parts, status.State.Terminated.Reason, status.State.Terminated.Message)
			if status.State.Terminated.ExitCode != 0 {
				parts = append(parts, fmt.Sprintf("exit code %d", status.State.Terminated.ExitCode))
			}
		}
	}
	return parts
}

func toolProbePendingFailure(pod *corev1.Pod) (reason, message string, fatal bool) {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse {
			return condition.Reason, condition.Message, true
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Waiting == nil {
			continue
		}
		reason = status.State.Waiting.Reason
		switch reason {
		case "ErrImagePull", "ImagePullBackOff", "InvalidImageName", "CreateContainerConfigError", "CreateContainerError", "RunContainerError":
			return reason, status.State.Waiting.Message, true
		}
	}
	return "", "", false
}

func joinProbeFailure(parts ...string) string {
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" && !slices.Contains(filtered, value) {
			filtered = append(filtered, value)
		}
	}
	return strings.Join(filtered, "; ")
}

func logProbeSuccess(options ToolImageProbeOptions, image string, target ToolProbeTarget, podName string) {
	nodeName := target.NodeName
	if nodeName == "" {
		nodeName = "scheduler-selected"
	}
	if options.Logger != nil {
		options.Logger.Info("tool image probe succeeded", "image", image, "namespace", target.Namespace, "node", nodeName, "components", target.Components, "pod", podName)
	}
	if options.Writer != nil {
		_, _ = fmt.Fprintf(options.Writer, "tool image probe succeeded: namespace=%s node=%s image=%s components=%s\n", target.Namespace, nodeName, image, strings.Join(target.Components, ","))
	}
}

func nodeReadyAndSchedulable(node *corev1.Node) bool {
	if node == nil || node.Spec.Unschedulable {
		return false
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

var _ ToolImageProber = (*KubernetesToolImageProber)(nil)
