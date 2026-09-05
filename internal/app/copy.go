package app

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const copyToolCleanupTimeout = 10 * time.Second

func (s *Service) validateCopyConsumers(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
) error {
	if session.Spec.Operation() != domain.OperationCopy {
		return nil
	}

	pods, err := s.client.CoreV1().Pods(volume.SourcePVC.Namespace).List(ctx, metav1.ListOptions{})
	if err == nil && pods == nil {
		err = fmt.Errorf(
			"list PVC consumers in %s returned an empty object",
			volume.SourcePVC.Namespace,
		)
	}

	var items []corev1.Pod
	if pods != nil {
		items = pods.Items
	}

	_, err = s.validateCopyConsumersFromPods(
		session,
		volume,
		items,
		err,
		session.Spec.WorkflowOptions().SourceNode,
	)

	return err
}

func (s *Service) validateCopyConsumersBatch(
	ctx context.Context,
	session *domain.Session,
	checkpointSourceNode bool,
) error {
	if session.Spec.Operation() != domain.OperationCopy {
		return nil
	}

	options := session.Spec.WorkflowOptionsPtr()
	if options == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"copy preflight",
			"copy session workflow options are missing",
		)
	}

	namespaces := make([]string, 0)

	seen := map[string]struct{}{}
	for index := range session.Spec.Volumes {
		namespace := session.Spec.Volumes[index].SourcePVC.Namespace
		if _, exists := seen[namespace]; exists {
			continue
		}

		seen[namespace] = struct{}{}
		namespaces = append(namespaces, namespace)
	}

	sort.Strings(namespaces)

	type result struct {
		pods []corev1.Pod
		err  error
	}

	results := make([]result, len(namespaces))
	parallel.For(len(namespaces), func(index int) {
		pods, err := s.client.CoreV1().Pods(namespaces[index]).List(ctx, metav1.ListOptions{})
		if err == nil && pods == nil {
			err = fmt.Errorf("list PVC consumers in %s returned an empty object", namespaces[index])
		}

		if pods != nil {
			results[index].pods = pods.Items
		}

		results[index].err = err
	})

	resolvedSourceNode := options.SourceNode
	for index := range session.Spec.Volumes {
		volume := &session.Spec.Volumes[index]
		namespaceIndex := sort.SearchStrings(namespaces, volume.SourcePVC.Namespace)
		result := results[namespaceIndex]

		inferredSourceNode, err := s.validateCopyConsumersFromPods(
			session,
			volume,
			result.pods,
			result.err,
			resolvedSourceNode,
		)
		if err != nil {
			return err
		}

		if resolvedSourceNode == "" {
			resolvedSourceNode = inferredSourceNode
		}
	}

	if !checkpointSourceNode || options.SourceNode != "" || resolvedSourceNode == "" {
		return nil
	}

	options.SourceNode = resolvedSourceNode
	if s.store == nil {
		options.SourceNode = ""

		return domain.NewError(
			domain.ErrorInternal,
			"copy preflight",
			"session store is required to persist the inferred source node",
		)
	}

	if err := s.store.Update(ctx, session); err != nil {
		options.SourceNode = ""

		return domain.WrapError(
			domain.ErrorKubernetes,
			"copy preflight",
			"persist inferred source node",
			err,
		)
	}

	return nil
}

func (s *Service) validateCopyConsumersFromPods(
	session *domain.Session,
	volume *domain.VolumeSpec,
	pods []corev1.Pod,
	listErr error,
	sourceNode string,
) (string, error) {
	if listErr != nil {
		return "", domain.WrapError(
			domain.ErrorKubernetes,
			"copy preflight",
			"list PVC consumers in "+volume.SourcePVC.Namespace,
			listErr,
		)
	}

	active := make([]*corev1.Pod, 0)
	nodes := map[string]struct{}{}

	scheduledCount := 0
	for index := range pods {
		pod := &pods[index]
		if !kube.ActivePodUsesPVC(pod, volume.SourcePVC.Name) {
			continue
		}

		active = append(active, pod)
		if pod.Spec.NodeName != "" {
			scheduledCount++
			nodes[pod.Spec.NodeName] = struct{}{}
		}
	}

	if len(active) == 0 {
		return "", nil
	}

	options := session.Spec.WorkflowOptionsPtr()
	if options == nil {
		return "", domain.NewError(
			domain.ErrorValidation,
			"copy preflight",
			"copy session workflow options are missing",
		)
	}

	if !session.Spec.Online() {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"copy preflight",
			fmt.Sprintf(
				"offline copy requires PVC %s/%s to have zero active Pod consumers",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if kube.HasAccessMode(volume.AccessModes, corev1.ReadWriteOncePod) {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"copy preflight",
			fmt.Sprintf(
				"active RWOP PVC %s/%s cannot be warm-copied",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if kube.HasAccessMode(volume.AccessModes, corev1.ReadWriteOnce) &&
		scheduledCount != len(active) {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"copy preflight",
			fmt.Sprintf(
				"every active consumer of RWO PVC %s/%s must be scheduled before online copy",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	if len(nodes) > 1 {
		return "", domain.NewError(
			domain.ErrorPrecondition,
			"copy preflight",
			fmt.Sprintf(
				"online copy consumers for PVC %s/%s moved across multiple nodes",
				volume.SourcePVC.Namespace,
				volume.SourcePVC.Name,
			),
		)
	}

	inferredSourceNode := ""
	for node := range nodes {
		inferredSourceNode = node
		if sourceNode != "" && node != sourceNode {
			return "", domain.NewError(
				domain.ErrorConflict,
				"copy preflight",
				fmt.Sprintf(
					"PVC %s/%s consumer runs on %s, session source node is %s",
					volume.SourcePVC.Namespace,
					volume.SourcePVC.Name,
					node,
					sourceNode,
				),
			)
		}
	}

	return inferredSourceNode, nil
}

func (s *Service) copyWithRetry(
	ctx context.Context,
	session *domain.Session,
	volume *domain.VolumeSpec,
	status *domain.VolumeStatus,
	mode copyengine.Mode,
	probeResults []kube.ToolImageProbeResult,
) error {
	values, err := s.helmSchedulingValues(
		ctx,
		session,
		probedSourceNode(session, volume, probeResults),
	)
	if err != nil {
		return err
	}

	pullSecretValues, err := kube.ToolImagePullSecretHelmValues(probeResults)
	if err != nil {
		return err
	}

	values = append(values, pullSecretValues...)

	// The upstream transfer chart does not expose PodSpec token automount. Use
	// a namespace-local, project-managed account whose automount setting is
	// explicitly disabled for sshd/rsync transfer Pods.
	seenNamespaces := map[string]struct{}{}
	for _, namespace := range []string{
		volume.SourcePVC.Namespace,
		volume.DestinationPVC.Namespace,
	} {
		if _, seen := seenNamespaces[namespace]; seen {
			continue
		}

		if err := kube.EnsureTransferServiceAccount(ctx, s.client, namespace); err != nil {
			return err
		}

		seenNamespaces[namespace] = struct{}{}
	}

	identityValues := kube.TransferServiceAccountHelmValues()
	values = append(values, identityValues.StringValues...)

	var last error

	options := session.Spec.WorkflowOptions()
	for retryIndex := range s.config.Retries {
		if err := s.validateReservedVolume(ctx, session, volume, status); err != nil {
			return err
		}

		sourceMountReadWrite := false
		if mode == copyengine.ModeWarm {
			_, sourceMountReadWrite, err = s.sharedOpenEBSLVMSource(ctx, session, volume)
			if err != nil {
				return err
			}
		}

		status.Sync.Attempts++
		status.Sync.LastError = ""

		if err := s.store.Update(ctx, session); err != nil {
			return err
		}

		request := copyengine.Request{
			SessionID:             session.ID,
			ToolImage:             s.toolImage(session),
			Source:                volume.SourcePVC,
			Destination:           volume.DestinationPVC,
			SourcePath:            domain.SourceTransferPath(volume.TransferScope),
			DestinationPath:       domain.DestinationTransferPath(volume.TransferScope),
			Mode:                  mode,
			Attempt:               status.Sync.Attempts,
			KubeconfigPath:        s.config.KubeconfigPath,
			Context:               s.config.Context,
			Strategies:            options.Strategies,
			DeleteExtraneousFiles: options.DeleteExtraneous,
			VerifyChecksum:        mode == copyengine.ModeFinal && options.VerifyChecksum,
			SourceMountReadWrite:  sourceMountReadWrite,
			IgnoreSizes:           volumeCapacityIsSmaller(volume),
			NoCompress:            s.config.NoCompress,
			HelmTimeout:           s.config.HelmTimeout,
			HelmValues:            identityValues.Values,
			HelmStringValues:      values,
			Writer:                s.config.Writer,
			Logger:                s.config.Logger,
		}
		s.logInfo(
			"copy started",
			"session",
			session.ID,
			"pvc",
			volume.SourcePVC.Name,
			"mode",
			mode,
			"attempt",
			status.Sync.Attempts,
			"source",
			volume.SourcePVC.Namespace+"/"+volume.SourcePVC.Name,
			"sourcePath",
			request.SourcePath,
			"destination",
			volume.DestinationPVC.Namespace+"/"+volume.DestinationPVC.Name,
			"destinationPath",
			request.DestinationPath,
		)

		toolLogs := s.startCopyToolLogs(
			ctx,
			volume,
			copyengine.OperationID(request),
		)
		copyErr := s.copier.Copy(ctx, request, func(progress copyengine.Progress) {
			s.logInfo(
				"copy progress",
				"session",
				session.ID,
				"pvc",
				volume.SourcePVC.Name,
				"mode",
				progress.Mode,
				"attempt",
				progress.Attempt,
				"state",
				progress.State,
				"message",
				progress.Message,
			)
		})

		toolLogs.Stop()
		copyErr = mergeToolLogError(copyErr, toolLogs.ObservedError())

		s.logInfo(
			"waiting for copy tool Pods to release PVCs",
			"session",
			session.ID,
			"pvc",
			volume.SourcePVC.Name,
		)

		operationID := copyengine.OperationID(request)

		last = errors.Join(
			copyErr,
			s.cleanupCopyToolPods(ctx, volume, operationID),
		)
		if last == nil {
			return nil
		}

		status.Sync.LastError = last.Error()
		if err := s.store.Update(ctx, session); err != nil {
			// Preserve the copy failure when the operation context was canceled;
			// failContext checkpoints the updated status with an independent context.
			if ctx.Err() != nil {
				return last
			}
			return err
		}

		if isDestinationNoSpaceError(last) {
			message := fmt.Sprintf(
				"destination PVC %s/%s ran out of space; abort and clean up this session, then create a new session with a larger --destination-capacity",
				volume.DestinationPVC.Namespace,
				volume.DestinationPVC.Name,
			)
			if kubeblocks, ok := session.Spec.KubeBlocksPodMigration(); ok {
				message = fmt.Sprintf(
					"destination PVC %s/%s ran out of space during KubeBlocks Cluster %s component %s real-time migration; update the component volumeClaimTemplates storage request, abort and clean up this session, then create a new migrate-pod session",
					volume.DestinationPVC.Namespace,
					volume.DestinationPVC.Name,
					kubeblocks.Cluster,
					kubeblocks.Component,
				)
			}

			return domain.WrapError(
				domain.ErrorConflict,
				domain.ErrorOperationCopyCapacity,
				message,
				last,
			)
		}

		if retryIndex+1 < s.config.Retries {
			delay := time.Duration(math.Pow(2, float64(retryIndex))) * s.config.RetryBackoff
			s.logInfo(
				"copy retry scheduled",
				"session",
				session.ID,
				"pvc",
				volume.SourcePVC.Name,
				"mode",
				mode,
				"attempt",
				status.Sync.Attempts,
				"nextAttempt",
				status.Sync.Attempts+1,
				"backoff",
				delay,
				"error",
				last,
			)

			if err := s.sleep(ctx, delay); err != nil {
				return domain.WrapError(
					domain.ErrorTimeout,
					"copy retry",
					"context ended during retry backoff",
					err,
				)
			}
		}
	}

	return last
}

func mergeToolLogError(copyErr, observedErr error) error {
	if copyErr == nil {
		return nil
	}
	return errors.Join(copyErr, observedErr)
}

func isDestinationNoSpaceError(err error) bool {
	if err == nil {
		return false
	}

	if copyengine.IsDestinationNoSpaceError(err) {
		return true
	}

	var visit func(error) bool

	visit = func(candidate error) bool {
		if candidate == nil {
			return false
		}

		if errors.Is(candidate, kube.ErrToolPodNoSpace) {
			return true
		}

		message := strings.ToLower(candidate.Error())
		if strings.Contains(message, "no space left on device") ||
			strings.Contains(message, "enospc") {
			return true
		}

		if joined, ok := candidate.(interface{ Unwrap() []error }); ok {
			return slices.ContainsFunc(joined.Unwrap(), visit)
		}

		return visit(errors.Unwrap(candidate))
	}

	return visit(err)
}

func volumeCapacityIsSmaller(volume *domain.VolumeSpec) bool {
	if volume == nil || volume.SourceCapacity == "" || volume.Capacity == "" {
		return false
	}

	source, sourceErr := resource.ParseQuantity(volume.SourceCapacity)
	destination, destinationErr := resource.ParseQuantity(volume.Capacity)

	return sourceErr == nil && destinationErr == nil && destination.Cmp(source) < 0
}

func (s *Service) waitForCopyToolRelease(ctx context.Context, volume *domain.VolumeSpec) error {
	claims := map[string]map[string]struct{}{}
	for _, ref := range []domain.ObjectReference{volume.SourcePVC, volume.DestinationPVC} {
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
			volume.SourcePVC.Namespace,
			volume.SourcePVC.Name,
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

func (s *Service) cleanupCopyToolPods(
	ctx context.Context,
	volume *domain.VolumeSpec,
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
		s.deleteCopyToolPods(cleanupCtx, volume, operationID),
		s.waitForCopyToolRelease(cleanupCtx, volume),
	)
}

func (s *Service) deleteCopyToolPods(
	ctx context.Context,
	volume *domain.VolumeSpec,
	operationID string,
) error {
	claims := map[string]map[string]struct{}{}
	for _, ref := range []domain.ObjectReference{volume.SourcePVC, volume.DestinationPVC} {
		if claims[ref.Namespace] == nil {
			claims[ref.Namespace] = map[string]struct{}{}
		}

		claims[ref.Namespace][ref.Name] = struct{}{}
	}

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

			uid := pod.UID

			deleteErr := s.client.CoreV1().Pods(namespace).Delete(
				ctx,
				pod.Name,
				metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}},
			)
			if apierrors.IsNotFound(deleteErr) {
				continue
			}

			if deleteErr != nil {
				return domain.WrapError(
					domain.ErrorKubernetes,
					"copy cleanup",
					"delete copy tool Pod "+namespace+"/"+pod.Name,
					deleteErr,
				)
			}
		}
	}

	return nil
}

func (s *Service) startCopyToolLogs(
	ctx context.Context,
	volume *domain.VolumeSpec,
	operationID string,
) *kube.ToolLogStream {
	if s == nil || !s.config.StreamToolLogs || volume == nil || operationID == "" {
		return nil
	}

	return kube.StartPVMigrateToolLogs(ctx, s.client, kube.ToolLogOptions{
		Namespaces:  []string{volume.SourcePVC.Namespace, volume.DestinationPVC.Namespace},
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

func (s *Service) helmSchedulingValues(
	ctx context.Context,
	session *domain.Session,
	sourceNode string,
) ([]string, error) {
	values := kube.ZeroResourceHelmValues()

	options := session.Spec.WorkflowOptions()
	if sourceNode == "" {
		sourceNode = options.SourceNode
	}

	type schedulingTarget struct {
		component string
		nodes     []string
		pinNode   bool
	}

	targets := []schedulingTarget{
		{component: "rsync", nodes: []string{options.TargetNode}, pinNode: true},
	}
	if slices.Contains(options.Strategies, domain.StrategyLocal) {
		// The local strategy deploys an SSHD on both sides. PVC topology places
		// each Pod on its volume's node, while the combined tolerations allow
		// both source and destination nodes to accept their respective Pod.
		targets = append(
			targets,
			schedulingTarget{component: "sshd", nodes: []string{sourceNode, options.TargetNode}},
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

func probedSourceNode(
	session *domain.Session,
	volume *domain.VolumeSpec,
	results []kube.ToolImageProbeResult,
) string {
	if session == nil || volume == nil {
		return ""
	}

	if sourceNode := session.Spec.WorkflowOptions().SourceNode; sourceNode != "" {
		return sourceNode
	}

	for _, result := range results {
		if result.Target.Namespace == volume.SourcePVC.Namespace &&
			result.Target.PVCName == volume.SourcePVC.Name &&
			slices.Contains(result.Target.Components, kube.ToolComponentSSHD) {
			return result.NodeName
		}
	}

	return ""
}
