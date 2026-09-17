package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// VolumeCopyConfig configures transfer execution infrastructure. Workflow input
// and durable progress belong to the operation's CRD and are passed per copy.
type VolumeCopyConfig struct {
	KubeconfigPath string
	Context        string
	Retries        int
	RetryBackoff   time.Duration
	HelmTimeout    time.Duration
	// CopyTimeout bounds a single data-transfer attempt. Zero disables the
	// per-attempt bound; the operation context remains the only limit.
	CopyTimeout      time.Duration
	// RsyncMaxRetries overrides the rsync job's internal retry count within
	// one transfer attempt. Zero keeps the upstream default (10).
	RsyncMaxRetries  int
	NoCompress       bool
	StreamToolLogs   bool
	StructuredLogs   bool
	Writer           io.Writer
	Logger           *slog.Logger
	TrustedToolImage string
}

type volumeCopyRunner struct {
	client kubernetes.Interface
	copier copyengine.Engine
	config VolumeCopyConfig
	sleep  func(context.Context, time.Duration) error
}

func newVolumeCopyRunner(
	client kubernetes.Interface,
	copier copyengine.Engine,
	config VolumeCopyConfig,
) *volumeCopyRunner {
	if config.Retries <= 0 {
		config.Retries = 3
	}

	if config.RetryBackoff <= 0 {
		config.RetryBackoff = 2 * time.Second
	}

	if config.HelmTimeout <= 0 {
		config.HelmTimeout = 10 * time.Minute
	}

	if config.Writer == nil {
		config.Writer = io.Discard
	}

	config.Writer = kube.NewSynchronizedWriter(config.Writer)

	return &volumeCopyRunner{client: client, copier: copier, config: config, sleep: sleepContext}
}

func (s *volumeCopyRunner) logInfo(message string, args ...any) {
	if s.config.Logger != nil {
		s.config.Logger.Info(message, args...)
	}
}

func (s *volumeCopyRunner) toolImage(requested string) string {
	if trusted := strings.TrimSpace(s.config.TrustedToolImage); trusted != "" {
		return trusted
	}
	return requested
}

func (s *volumeCopyRunner) waitForCopyToolRelease(
	ctx context.Context,
	source, destination v1alpha1.ObjectReference,
) error {
	claims := map[string]map[string]struct{}{}
	for _, ref := range []v1alpha1.ObjectReference{source, destination} {
		if claims[ref.Namespace] == nil {
			claims[ref.Namespace] = map[string]struct{}{}
		}

		claims[ref.Namespace][ref.Name] = struct{}{}
	}

	return kube.WaitFor(
		ctx,
		time.Second,
		fmt.Sprintf(
			"pv-migrate tools to release PVC %s/%s",
			source.Namespace,
			source.Name,
		),
		func(waitCtx context.Context) (bool, error) {
			for namespace, namespaceClaims := range claims {
				pods, err := s.client.CoreV1().Pods(namespace).List(waitCtx, metav1.ListOptions{})
				if err != nil {
					return false, domain.WrapError(
						domain.ErrorKubernetes,
						"copy cleanup",
						"list Pods in "+namespace,
						err,
					)
				}

				for i := range pods.Items {
					if isPVMigrateToolForClaims(&pods.Items[i], namespaceClaims) {
						return false, nil
					}
				}
			}

			return true, nil
		},
	)
}

func (s *volumeCopyRunner) cleanupCopyToolPods(
	ctx context.Context,
	source, destination v1alpha1.ObjectReference,
	operationID string,
) error {
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		copyToolCleanupTimeout,
	)
	defer cancel()

	// Helm uninstall only starts asynchronous garbage collection. Delete Pods
	// from this operation directly, then wait for every tool Pod to release the
	// claims even when the copy context has already expired.
	return errors.Join(
		s.deleteCopyToolPods(cleanupCtx, source, destination, operationID),
		s.waitForCopyToolRelease(cleanupCtx, source, destination),
	)
}

func (s *volumeCopyRunner) deleteCopyToolPods(
	ctx context.Context,
	source, destination v1alpha1.ObjectReference,
	operationID string,
) error {
	claims := map[string]map[string]struct{}{}
	for _, ref := range []v1alpha1.ObjectReference{source, destination} {
		if claims[ref.Namespace] == nil {
			claims[ref.Namespace] = map[string]struct{}{}
		}

		claims[ref.Namespace][ref.Name] = struct{}{}
	}

	var candidates []corev1.Pod
	for namespace, namespaceClaims := range claims {
		pods, err := s.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"copy cleanup",
				"list copy tool Pods in "+namespace,
				err,
			)
		}

		for index := range pods.Items {
			pod := &pods.Items[index]
			if !isPVMigrateToolForClaims(pod, namespaceClaims) ||
				!isPVMigrateToolForOperation(pod, operationID) {
				continue
			}

			if pod.UID == "" {
				return domain.NewError(
					domain.ErrorKubernetes,
					"copy cleanup",
					fmt.Sprintf("copy tool Pod %s/%s has no UID", namespace, pod.Name),
				)
			}

			candidates = append(candidates, *pod.DeepCopy())
		}
	}

	for _, pod := range candidates {
		if err := checkpointFenceError(ctx); err != nil {
			return err
		}

		uid, version := pod.UID, pod.ResourceVersion

		deleteErr := s.client.CoreV1().Pods(pod.Namespace).Delete(
			ctx,
			pod.Name,
			metav1.DeleteOptions{
				Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version},
			},
		)
		if deleteErr != nil && !apierrors.IsNotFound(deleteErr) {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"copy cleanup",
				"delete copy tool Pod "+pod.Namespace+"/"+pod.Name,
				deleteErr,
			)
		}
	}

	return nil
}

func (s *volumeCopyRunner) startCopyToolLogs(
	ctx context.Context,
	sourceNamespace, destinationNamespace string,
	operationID string,
) *kube.ToolLogStream {
	if s == nil || !s.config.StreamToolLogs || operationID == "" {
		return nil
	}

	return kube.StartPVMigrateToolLogs(ctx, s.client, kube.ToolLogOptions{
		Namespaces:  []string{sourceNamespace, destinationNamespace},
		OperationID: operationID,
		Writer:      s.config.Writer,
		Logger:      s.config.Logger,
		Structured:  s.config.StructuredLogs,
	})
}

func isPVMigrateToolForClaims(pod *corev1.Pod, claims map[string]struct{}) bool {
	if _, tool := pvmigrateToolInstance(pod); !tool {
		return false
	}

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		if _, exists := claims[volume.PersistentVolumeClaim.ClaimName]; exists {
			return true
		}
	}

	return false
}

func pvmigrateToolInstance(pod *corev1.Pod) (string, bool) {
	if pod == nil {
		return "", false
	}

	instance := pod.Labels[kube.AppInstanceLabel]
	if !strings.HasPrefix(instance, "pv-migrate-") {
		return "", false
	}

	switch pod.Labels[kube.AppComponentLabel] {
	case kube.ToolComponentSSHD, kube.ToolComponentRsync, kube.ToolComponentRclone:
		return instance, true
	default:
		return "", false
	}
}

func isPVMigrateToolForOperation(pod *corev1.Pod, operationID string) bool {
	instance, ok := pvmigrateToolInstance(pod)
	return ok && operationID != "" && strings.HasPrefix(instance, "pv-migrate-"+operationID+"-")
}

func (s *volumeCopyRunner) helmSchedulingValues(
	ctx context.Context,
	sourceNode, targetNode string,
	strategies []string,
) ([]string, error) {
	values := kube.ZeroResourceHelmValues()

	type schedulingTarget struct {
		component string
		nodes     []string
		pinNode   bool
	}

	targets := []schedulingTarget{
		{component: "rsync", nodes: []string{targetNode}, pinNode: true},
	}
	if slices.Contains(strategies, domain.StrategyLocal) {
		// The local strategy deploys an SSHD on both sides. PVC topology places
		// each Pod on its volume's node, while the combined tolerations allow
		// both source and destination nodes to accept their respective Pod.
		targets = append(
			targets,
			schedulingTarget{component: "sshd", nodes: []string{sourceNode, targetNode}},
		)
	} else {
		targets = append(
			targets,
			schedulingTarget{component: "sshd", nodes: []string{sourceNode}, pinNode: true},
		)
	}

	nodeNames := make([]string, 0, 2)

	nodeIndexes := map[string]int{}
	for _, target := range targets {
		for _, nodeName := range target.nodes {
			if nodeName == "" {
				continue
			}

			if _, exists := nodeIndexes[nodeName]; exists {
				continue
			}

			nodeIndexes[nodeName] = len(nodeNames)
			nodeNames = append(nodeNames, nodeName)
		}
	}

	type nodeResult struct {
		node *corev1.Node
		err  error
	}

	nodes := make([]nodeResult, len(nodeNames))
	parallel.For(len(nodeNames), func(index int) {
		nodes[index].node, nodes[index].err = s.client.CoreV1().
			Nodes().
			Get(ctx, nodeNames[index], metav1.GetOptions{})
	})

	for _, target := range targets {
		seenNodes := map[string]struct{}{}

		componentNodes := make([]*corev1.Node, 0, len(target.nodes))
		for _, nodeName := range target.nodes {
			if nodeName == "" {
				continue
			}

			if _, seen := seenNodes[nodeName]; seen {
				continue
			}

			seenNodes[nodeName] = struct{}{}

			result := nodes[nodeIndexes[nodeName]]
			if result.err != nil {
				return nil, domain.WrapError(
					domain.ErrorKubernetes,
					"copy scheduling",
					"read node "+nodeName,
					result.err,
				)
			}

			node := result.node
			if node == nil || node.Name == "" {
				return nil, domain.NewError(
					domain.ErrorKubernetes,
					"copy scheduling",
					fmt.Sprintf("read node %s returned an empty object", nodeName),
				)
			}

			componentNodes = append(componentNodes, node)
			if target.pinNode {
				hostname := node.Labels[corev1.LabelHostname]
				if hostname == "" {
					return nil, domain.NewError(
						domain.ErrorPrecondition,
						"copy scheduling",
						fmt.Sprintf("node %s lacks %s", nodeName, corev1.LabelHostname),
					)
				}

				values = append(
					values,
					fmt.Sprintf(
						"%s.nodeSelector.kubernetes\\.io/hostname=%s",
						target.component,
						hostname,
					),
				)
			}
		}

		values = append(
			values,
			kube.ToolComponentTolerationHelmValues(target.component, componentNodes...)...,
		)
	}

	return values, nil
}
