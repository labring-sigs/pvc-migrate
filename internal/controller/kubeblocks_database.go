package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func kubeBlocksMongoDBNativeSwitchoverCommand(
	namespace, cluster, component, selected, candidate string,
) string {
	headlessService := fmt.Sprintf("%s-%s-headless", cluster, component)

	return fmt.Sprintf(
		"kubectl --namespace %s exec %s -c mongodb -- env KB_CONSENSUS_LEADER_POD_FQDN=%s.%s KB_SWITCHOVER_CANDIDATE_FQDN=%s.%s /scripts/switchover-with-candidate.sh",
		namespace,
		selected,
		selected,
		headlessService,
		candidate,
		headlessService,
	)
}

func isKubeBlocksMongoDB(pod *corev1.Pod) bool {
	return isKubeBlocksApplication(pod, "mongodb")
}

func isKubeBlocksRedis(pod *corev1.Pod) bool {
	return isKubeBlocksApplication(pod, "redis")
}

func isKubeBlocksApplication(pod *corev1.Pod, name string) bool {
	if pod == nil {
		return false
	}

	for _, value := range []string{pod.Labels[kube.AppNameLabel], pod.Labels[kube.AppComponentLabel], pod.Labels[kubeBlocksComponentLabel]} {
		if strings.EqualFold(value, name) {
			return true
		}
	}

	return false
}

func mongoDBContainer(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}

	for _, container := range pod.Spec.Containers {
		if container.Name == "mongodb" {
			return container.Name
		}
	}

	return ""
}

func (m *Manager) preflightMongoDBNativeSwitchover(
	ctx context.Context,
	pod *corev1.Pod,
) (string, error) {
	container := mongoDBContainer(pod)
	if container == "" {
		return "", fmt.Errorf("MongoDB Pod %s has no mongodb container", pod.Name)
	}

	if m.commandExecutor == nil {
		return "", errors.New(
			"pod exec is unavailable; configure Kubernetes REST access for the MongoDB native switchover",
		)
	}

	if m.logger != nil {
		m.logger.Info(
			"checking MongoDB native switchover script",
			"namespace",
			pod.Namespace,
			"pod",
			pod.Name,
			"container",
			container,
		)
	}

	result, err := m.commandExecutor.Execute(ctx, podCommandRequest{
		Namespace: pod.Namespace,
		Pod:       pod.Name,
		Container: container,
		Command:   []string{"sh", "-c", "test -x /scripts/switchover-with-candidate.sh"},
	})
	if err != nil {
		return "", podCommandError("check MongoDB native switchover script", result, err)
	}

	return container, nil
}

// runMongoDBNativeSwitchover is intentionally kept with the database adapter.
// KubeBlocks pause orchestration selects this strategy, while this method owns
// the MongoDB script invocation and role-convergence check.
func (m *Manager) runMongoDBNativeSwitchover(ctx context.Context, session *domain.Session) error {
	kb := session.Spec.Workload().KubeBlocks
	if kb == nil {
		return domain.NewError(domain.ErrorInternal, "pause KubeBlocks", "session lacks KubeBlocks state")
	}

	if kb.SwitchoverContainer == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pause KubeBlocks",
			"MongoDB native switchover session lacks the validated container",
		)
	}

	if m.commandExecutor == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pause KubeBlocks",
			"Pod exec is unavailable for the MongoDB native switchover; manual MongoDB switchover: "+kubeBlocksMongoDBNativeSwitchoverCommand(
				session.Spec.Workload().Pod.Namespace,
				kb.Cluster,
				kb.Component,
				kb.Instance,
				kb.SwitchoverCandidate,
			),
		)
	}

	namespace := session.Spec.Workload().Pod.Namespace
	selected, err := m.typed.CoreV1().Pods(namespace).Get(ctx, kb.Instance, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "pause KubeBlocks", "read MongoDB switchover source Pod", err)
	}

	if selected.UID != session.Spec.Workload().Pod.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"pause KubeBlocks",
			fmt.Sprintf("Pod %s/%s UID changed", selected.Namespace, selected.Name),
		)
	}
	if err := validatePodController(selected, session.Spec.Workload().Controller, "pause KubeBlocks"); err != nil {
		return err
	}

	headlessService := fmt.Sprintf("%s-%s-headless", kb.Cluster, kb.Component)
	leaderFQDN := fmt.Sprintf("%s.%s", kb.Instance, headlessService)
	candidateFQDN := fmt.Sprintf("%s.%s", kb.SwitchoverCandidate, headlessService)
	if m.logger != nil {
		m.logger.Info(
			"starting KubeBlocks MongoDB native switchover",
			"namespace", namespace,
			"cluster", kb.Cluster,
			"workload_component", kb.Component,
			"instance", kb.Instance,
			"candidate", kb.SwitchoverCandidate,
		)
	}

	result, err := m.commandExecutor.Execute(ctx, podCommandRequest{
		Namespace: namespace,
		Pod:       kb.Instance,
		Container: kb.SwitchoverContainer,
		Command: []string{
			"env",
			"KB_CONSENSUS_LEADER_POD_FQDN=" + leaderFQDN,
			"KB_SWITCHOVER_CANDIDATE_FQDN=" + candidateFQDN,
			"/scripts/switchover-with-candidate.sh",
		},
	})
	if err != nil {
		executionErr := podCommandError("run MongoDB native candidate switchover", result, err)
		return domain.WrapError(
			domain.ErrorPrecondition,
			"pause KubeBlocks",
			fmt.Sprintf(
				"%v; manual MongoDB switchover: %s",
				executionErr,
				kubeBlocksMongoDBNativeSwitchoverCommand(namespace, kb.Cluster, kb.Component, kb.Instance, kb.SwitchoverCandidate),
			),
			executionErr,
		)
	}

	return m.waitFor(
		ctx,
		fmt.Sprintf("KubeBlocks MongoDB switchover from %s to %s", kb.Instance, kb.SwitchoverCandidate),
		func(waitCtx context.Context) (bool, error) {
			leader, leaderErr := m.typed.CoreV1().Pods(namespace).Get(waitCtx, kb.Instance, metav1.GetOptions{})
			if leaderErr != nil {
				return false, leaderErr
			}
			if err := validatePodController(leader, session.Spec.Workload().Controller, "pause KubeBlocks"); err != nil {
				return false, err
			}

			candidate, candidateErr := m.typed.CoreV1().Pods(namespace).Get(waitCtx, kb.SwitchoverCandidate, metav1.GetOptions{})
			if candidateErr != nil {
				return false, candidateErr
			}
			if err := validatePodController(candidate, session.Spec.Workload().Controller, "pause KubeBlocks"); err != nil {
				return false, err
			}
			return !isLeaderRole(podRole(leader)) && isLeaderRole(podRole(candidate)), nil
		},
	)
}

func podCommandError(action string, result podCommandResult, err error) error {
	output := strings.TrimSpace(result.Stderr)
	if output == "" {
		output = strings.TrimSpace(result.Stdout)
	}

	if output == "" {
		return fmt.Errorf("%s: %w", action, err)
	}

	output = strings.Join(strings.Fields(output), " ")
	if len(output) > 512 {
		output = output[:512] + "..."
	}

	return fmt.Errorf("%s: %w (output: %s)", action, err, output)
}
