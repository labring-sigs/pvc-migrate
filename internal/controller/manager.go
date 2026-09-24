package controller

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	kubeBlocksClusterAPIVersion = domain.KubeBlocksClusterAPIVersion
	kubeBlocksOpsAPIVersion     = domain.KubeBlocksOperationsAPIVersion
	kubeBlocksGroupSuffix       = "kubeblocks.io"
	vmClusterAPIVersion         = domain.VictoriaMetricsAPIVersion
	grafanaAPIVersion           = domain.GrafanaAPIVersion
	grafanaAPIGroup             = "grafana.integreatly.org"
	clusterResource             = "clusters"
	instanceSetResource         = "instancesets"
	vmClusterResource           = "vmclusters"
	grafanaResource             = "grafanas"
	pauseSessionAnnotation      = kube.PauseSessionAnnotation
	kubeBlocksComponentLabel    = "apps.kubeblocks.io/component-name"
	kubeBlocksRoleLabel         = "kubeblocks.io/role"
	kubeBlocksAppsRoleLabel     = "apps.kubeblocks.io/role"
	genericRoleLabel            = "role"
	validateRollbackConsumers   = "validate rollback consumers"
)

type kubeBlocksInstanceSetState struct {
	Paused           bool
	PausedConfigured bool
	UID              types.UID
	Role             string
	LeaderRoles      map[string]bool
	HasLeaderRole    bool
}

type Manager struct {
	typed           kubernetes.Interface
	dynamic         dynamic.Interface
	discovery       discovery.DiscoveryInterface
	commandExecutor podCommandExecutor
	poll            time.Duration
	logger          *slog.Logger
}

func NewManager(
	typed kubernetes.Interface,
	dynamicClient dynamic.Interface,
	discoveryClient discovery.DiscoveryInterface,
) *Manager {
	return &Manager{
		typed:     typed,
		dynamic:   dynamicClient,
		discovery: discoveryClient,
		poll:      time.Second,
	}
}

// WithLogger enables progress logs for controller reconciliation waits.
func (m *Manager) WithLogger(logger *slog.Logger) *Manager {
	m.logger = logger
	return m
}

// WithRESTConfig enables Pod exec for controller adapters that require a native workload command.
func (m *Manager) WithRESTConfig(config *rest.Config) *Manager {
	if config == nil {
		m.commandExecutor = nil
		return m
	}

	m.commandExecutor = kubernetesPodCommandExecutor{
		client: m.typed,
		config: rest.CopyConfig(config),
	}

	return m
}

func (m *Manager) waitFor(
	ctx context.Context,
	description string,
	condition func(context.Context) (bool, error),
) error {
	if m.logger != nil {
		m.logger.Info("waiting for workload controller", "description", description)
	}
	return kube.WaitFor(ctx, m.poll, description, condition)
}

// waitForPodDeletion keeps the deletion wait fenced to the Pod observed by
// discovery. A controller can delete and recreate a same-name Pod while the
// wait is in progress; treating that replacement as the original Pod would
// let a pause stage cross its offline boundary without deleting the intended
// workload instance.
func (m *Manager) waitForPodDeletion(
	ctx context.Context,
	ref v1alpha1.ObjectReference,
	operation string,
) error {
	if ref.Namespace == "" || ref.Name == "" || ref.UID == "" {
		return domain.NewError(
			domain.ErrorValidation,
			operation,
			"Pod namespace, name, and UID are required",
		)
	}

	return m.waitFor(
		ctx,
		fmt.Sprintf("%s Pod %s/%s deletion", operation, ref.Namespace, ref.Name),
		func(waitCtx context.Context) (bool, error) {
			current, getErr := m.typed.CoreV1().
				Pods(ref.Namespace).
				Get(waitCtx, ref.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				return true, nil
			}

			if getErr != nil {
				return false, getErr
			}

			if current.UID != ref.UID {
				return false, domain.NewError(
					domain.ErrorConflict,
					operation,
					fmt.Sprintf(
						"Pod %s/%s was replaced while waiting for deletion",
						ref.Namespace,
						ref.Name,
					),
				)
			}

			return false, nil
		},
	)
}

// DiscoverPod resolves a workload from a caller-owned Pod snapshot. The
// presentation argument renders field-spelling guidance for the planning
// audience: flags for the CLI, spec fields for controller-recorded checks.
func (m *Manager) DiscoverPod(
	ctx context.Context,
	pod *corev1.Pod,
	namespace string,
	expected v1alpha1.LocalResourceReference,
	switchoverCandidate string,
	allowLeaderDowntime bool,
	presentation domain.Presentation,
) (v1alpha1.WorkloadSpec, error) {
	if err := validateDiscoverPodInput(pod, namespace, expected); err != nil {
		return v1alpha1.WorkloadSpec{}, err
	}

	owner := controllerOwner(pod.OwnerReferences)
	if owner == nil {
		if err := requireReadyPod(pod, pod.Namespace, pod.Name); err != nil {
			return v1alpha1.WorkloadSpec{}, err
		}
		return standaloneWorkload(pod)
	}

	return m.discoverOwnedWorkload(
		ctx,
		pod,
		owner,
		switchoverCandidate,
		allowLeaderDowntime,
		presentation,
	)
}

func validateDiscoverPodInput(
	pod *corev1.Pod,
	namespace string,
	expected v1alpha1.LocalResourceReference,
) error {
	if pod == nil {
		return domain.NewError(domain.ErrorValidation, "discover workload", "Pod is nil")
	}

	if (namespace != "" && pod.Namespace != namespace) ||
		(expected.Name != "" && pod.Name != expected.Name) ||
		(expected.UID != "" && pod.UID != expected.UID) ||
		(expected.ResourceVersion != "" && pod.ResourceVersion != expected.ResourceVersion) ||
		(expected.Kind != "" && expected.Kind != domain.KindPod) ||
		(expected.APIVersion != "" && expected.APIVersion != corev1.SchemeGroupVersion.String()) {
		return domain.NewError(
			domain.ErrorConflict,
			"discover workload",
			fmt.Sprintf(
				"Pod snapshot %s/%s does not match requested identity",
				pod.Namespace,
				pod.Name,
			),
		)
	}

	if pod.Namespace == "" || pod.Name == "" || pod.UID == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			"discover workload",
			"Kubernetes returned an incomplete Pod identity",
		)
	}

	if pod.Annotations[corev1.MirrorPodAnnotationKey] != "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"discover workload",
			"static mirror Pods are unsupported",
		)
	}

	if owner := pod.Annotations[kube.SessionKey]; owner != "" {
		return domain.NewError(
			domain.ErrorConflict,
			"discover workload",
			fmt.Sprintf(
				"Pod %s/%s is still owned by migration session %s; finish or clean up that session before starting another migration",
				pod.Namespace,
				pod.Name,
				owner,
			),
		)
	}

	return nil
}

func (m *Manager) discoverOwnedWorkload(
	ctx context.Context,
	pod *corev1.Pod,
	owner *metav1.OwnerReference,
	candidate string,
	allowLeaderDowntime bool,
	presentation domain.Presentation,
) (v1alpha1.WorkloadSpec, error) {
	if owner.UID == "" {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover workload",
			fmt.Sprintf("Pod %s/%s controller reference has no UID", pod.Namespace, pod.Name),
		)
	}

	groupVersion, err := schema.ParseGroupVersion(owner.APIVersion)
	if err != nil {
		return v1alpha1.WorkloadSpec{}, domain.WrapError(
			domain.ErrorPrecondition,
			"discover workload",
			"parse controller apiVersion",
			err,
		)
	}

	switch {
	case owner.Kind == domain.KindStatefulSet && groupVersion.Group == appsv1.GroupName:
		return m.discoverStatefulSetOwner(
			ctx,
			pod,
			owner,
			candidate,
			allowLeaderDowntime,
			presentation,
		)
	case owner.Kind == domain.KindJob && groupVersion.Group == batchv1.GroupName:
		return m.discoverJobOwner(ctx, pod, owner)
	case owner.Kind == domain.KindReplicaSet && groupVersion.Group == appsv1.GroupName:
		return m.discoverReplicaSetOwner(ctx, pod, owner)
	case owner.Kind == domain.KindInstanceSet &&
		strings.Contains(groupVersion.Group, kubeBlocksGroupSuffix):
		return m.kubeBlocksWorkload(ctx, pod, owner, candidate, allowLeaderDowntime, presentation)
	default:
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover workload",
			fmt.Sprintf("controller %s/%s has no safe pause adapter", owner.APIVersion, owner.Kind),
		)
	}
}

func (m *Manager) discoverStatefulSetOwner(
	ctx context.Context,
	pod *corev1.Pod,
	owner *metav1.OwnerReference,
	candidate string,
	allowLeaderDowntime bool,
	presentation domain.Presentation,
) (v1alpha1.WorkloadSpec, error) {
	sts, err := m.typed.AppsV1().StatefulSets(pod.Namespace).Get(
		ctx,
		owner.Name,
		metav1.GetOptions{},
	)
	if err != nil {
		return v1alpha1.WorkloadSpec{}, domain.WrapError(
			domain.ErrorKubernetes,
			"discover workload",
			"read StatefulSet",
			err,
		)
	}

	if sts.UID == "" || sts.UID != owner.UID {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorConflict,
			"discover workload",
			fmt.Sprintf("Pod %s/%s StatefulSet owner UID changed", pod.Namespace, pod.Name),
		)
	}

	if reason := unsupportedStatefulSetReason(sts); reason != "" {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover workload",
			reason,
		)
	}

	if isVictoriaLogsStatefulSet(sts) {
		return m.victoriaLogsWorkload(ctx, pod, sts)
	}

	if err := requireReadyPod(pod, pod.Namespace, pod.Name); err != nil {
		return v1alpha1.WorkloadSpec{}, err
	}

	parent := controllerOwner(sts.OwnerReferences)
	if parent == nil {
		return m.statefulSetWorkload(ctx, pod, sts, allowLeaderDowntime)
	}

	return m.discoverStatefulSetParent(
		ctx,
		pod,
		sts,
		parent,
		candidate,
		allowLeaderDowntime,
		presentation,
	)
}

func (m *Manager) discoverStatefulSetParent(
	ctx context.Context,
	pod *corev1.Pod,
	sts *appsv1.StatefulSet,
	parent *metav1.OwnerReference,
	candidate string,
	allowLeaderDowntime bool,
	presentation domain.Presentation,
) (v1alpha1.WorkloadSpec, error) {
	if parent.UID == "" {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover workload",
			fmt.Sprintf(
				"StatefulSet %s/%s controller reference has no UID",
				sts.Namespace,
				sts.Name,
			),
		)
	}

	parentGV, err := schema.ParseGroupVersion(parent.APIVersion)
	if err != nil {
		return v1alpha1.WorkloadSpec{}, domain.WrapError(
			domain.ErrorPrecondition,
			"discover workload",
			"parse parent controller apiVersion",
			err,
		)
	}

	switch parent.Kind {
	case domain.KindVMCluster:
		if parentGV.Group == "operator.victoriametrics.com" {
			return m.vmClusterWorkload(ctx, pod, parent, sts, allowLeaderDowntime)
		}
	case domain.KindComponent:
		if strings.Contains(parentGV.Group, "kubeblocks.io") {
			return m.kubeBlocksWorkload(
				ctx,
				pod,
				controllerOwner(pod.OwnerReferences),
				candidate,
				allowLeaderDowntime,
				presentation,
			)
		}
	}

	if strings.Contains(parentGV.Group, "kubeblocks.io") {
		return m.kubeBlocksWorkload(
			ctx,
			pod,
			controllerOwner(pod.OwnerReferences),
			candidate,
			allowLeaderDowntime,
			presentation,
		)
	}

	return v1alpha1.WorkloadSpec{}, domain.NewError(
		domain.ErrorPrecondition,
		"discover workload",
		fmt.Sprintf(
			"StatefulSet is generated by unsupported controller %s/%s",
			parent.APIVersion,
			parent.Kind,
		),
	)
}

func (m *Manager) discoverJobOwner(
	ctx context.Context,
	pod *corev1.Pod,
	owner *metav1.OwnerReference,
) (v1alpha1.WorkloadSpec, error) {
	job, err := m.typed.BatchV1().Jobs(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return v1alpha1.WorkloadSpec{}, domain.WrapError(
			domain.ErrorKubernetes,
			"discover workload",
			"read Job",
			err,
		)
	}

	if job.UID == "" || job.UID != owner.UID {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorConflict,
			"discover workload",
			fmt.Sprintf("Pod %s/%s Job owner UID changed", pod.Namespace, pod.Name),
		)
	}

	if parent := controllerOwner(
		job.OwnerReferences,
	); parent != nil &&
		parent.Kind == domain.KindBackup {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover workload",
			fmt.Sprintf(
				"Backup-owned archive-WAL Job %s/%s is a backup workload and cannot be migrated",
				pod.Namespace,
				job.Name,
			),
		)
	}

	if err := requireReadyPod(pod, pod.Namespace, pod.Name); err != nil {
		return v1alpha1.WorkloadSpec{}, err
	}

	return v1alpha1.WorkloadSpec{}, domain.NewError(
		domain.ErrorPrecondition,
		"discover workload",
		fmt.Sprintf("controller %s/%s has no safe pause adapter", owner.APIVersion, owner.Kind),
	)
}

func (m *Manager) discoverReplicaSetOwner(
	ctx context.Context,
	pod *corev1.Pod,
	owner *metav1.OwnerReference,
) (v1alpha1.WorkloadSpec, error) {
	rs, err := m.typed.AppsV1().
		ReplicaSets(pod.Namespace).
		Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return v1alpha1.WorkloadSpec{}, domain.WrapError(
			domain.ErrorKubernetes,
			"discover workload",
			"read ReplicaSet",
			err,
		)
	}

	if rs.UID == "" || rs.UID != owner.UID {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorConflict,
			"discover workload",
			fmt.Sprintf("Pod %s/%s ReplicaSet owner UID changed", pod.Namespace, pod.Name),
		)
	}

	deployment := controllerOwner(rs.OwnerReferences)
	if deployment == nil || deployment.Kind != domain.KindDeployment {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover workload",
			"ReplicaSet has no Deployment controller",
		)
	}

	if deployment.UID == "" {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover workload",
			fmt.Sprintf("ReplicaSet %s/%s Deployment reference has no UID", rs.Namespace, rs.Name),
		)
	}

	deploymentObject, err := m.typed.AppsV1().
		Deployments(pod.Namespace).
		Get(ctx, deployment.Name, metav1.GetOptions{})
	if err != nil {
		return v1alpha1.WorkloadSpec{}, domain.WrapError(
			domain.ErrorKubernetes,
			"discover workload",
			"read Deployment",
			err,
		)
	}

	if deploymentObject.UID == "" || deploymentObject.UID != deployment.UID {
		return v1alpha1.WorkloadSpec{}, domain.NewError(
			domain.ErrorConflict,
			"discover workload",
			fmt.Sprintf("ReplicaSet %s/%s Deployment owner UID changed", rs.Namespace, rs.Name),
		)
	}

	grafanaOwner := controllerOwner(deploymentObject.OwnerReferences)
	if grafanaOwner != nil {
		if grafanaOwner.UID == "" {
			return v1alpha1.WorkloadSpec{}, domain.NewError(
				domain.ErrorPrecondition,
				"discover workload",
				fmt.Sprintf(
					"Deployment %s/%s Grafana reference has no UID",
					deploymentObject.Namespace,
					deploymentObject.Name,
				),
			)
		}

		grafanaGV, _ := schema.ParseGroupVersion(grafanaOwner.APIVersion)
		if grafanaOwner.Kind == domain.KindGrafana && grafanaGV.Group == grafanaAPIGroup {
			return m.grafanaWorkload(ctx, pod, deploymentObject, grafanaOwner)
		}
	}

	if err := requireReadyPod(pod, pod.Namespace, pod.Name); err != nil {
		return v1alpha1.WorkloadSpec{}, err
	}

	return m.deploymentWorkload(ctx, pod, deploymentObject)
}

func (m *Manager) ValidateResume(
	ctx context.Context,
	owner, namespace string,
	workload v1alpha1.WorkloadSpec,
	phase, resumeFrom v1alpha1.WorkflowPhase,
) error {
	if err := validateWorkloadScope(namespace, workload.Adapter); err != nil {
		return err
	}

	controller := qualifiedWorkloadReference(workload.Controller, namespace)
	pod := qualifiedWorkloadReference(workload.Pod, namespace)

	switch workload.Adapter {
	case v1alpha1.WorkloadNone:
		return nil
	case v1alpha1.WorkloadStandalone:
		return m.validateStandaloneResume(ctx, owner, pod)
	case v1alpha1.WorkloadDeployment:
		return m.validateDeploymentResume(ctx, controller,
			workload.OriginalReplicas)
	case v1alpha1.WorkloadStatefulSet:
		return m.validateStatefulSetTransitionReplicas(ctx, controller,
			workload.OriginalReplicas, workload.Ordinal, "resume StatefulSet")
	case v1alpha1.WorkloadVictoriaLogs:
		return m.validateVictoriaLogsResume(ctx, owner, controller,
			workload.OriginalReplicas)
	case v1alpha1.WorkloadVMCluster:
		return m.validateVMClusterResume(ctx, owner, namespace,
			controller, workload.OriginalReplicas,
			workload.Ordinal, workload.VMCluster)
	case v1alpha1.WorkloadKubeBlocks:
		return m.validateKubeBlocksResume(ctx, owner, namespace,
			controller, workload.KubeBlocks,
			phase, resumeFrom)
	case v1alpha1.WorkloadGrafana:
		return m.validateGrafanaResume(
			ctx,
			owner,
			namespace,
			controller,
			workload.OriginalReplicas,
			workload.Grafana,
		)
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"validate workload resume",
			fmt.Sprintf("adapter %q is unsupported", workload.Adapter),
		)
	}
}

func (m *Manager) CurrentRollbackPods(
	ctx context.Context,
	owner, namespace string,
	workload v1alpha1.WorkloadSpec,
) ([]v1alpha1.ObjectReference, error) {
	if err := validateWorkloadScope(namespace, workload.Adapter); err != nil {
		return nil, err
	}

	pod := qualifiedWorkloadReference(workload.Pod, namespace)
	controller := qualifiedWorkloadReference(workload.Controller, namespace)

	switch workload.Adapter {
	case v1alpha1.WorkloadNone:
		return nil, nil
	case v1alpha1.WorkloadStandalone:
		return m.currentStandaloneRollbackPods(ctx, owner, pod)
	case v1alpha1.WorkloadDeployment:
		return m.currentDeploymentRollbackPods(ctx, controller, workload.OriginalReplicas)
	case v1alpha1.WorkloadStatefulSet,
		v1alpha1.WorkloadVictoriaLogs,
		v1alpha1.WorkloadVMCluster:
		if err := m.validateStatefulSetTransitionReplicas(ctx, controller,
			workload.OriginalReplicas, workload.Ordinal, validateRollbackConsumers); err != nil {
			return nil, err
		}

		references := qualifiedWorkloadReferences(workload.AffectedPods, namespace)
		if len(references) == 0 {
			references = []v1alpha1.ObjectReference{pod}
		}

		return m.currentControllerPods(
			ctx,
			references,
			controller,
			validateRollbackConsumers,
		)
	case v1alpha1.WorkloadKubeBlocks:
		if workload.KubeBlocks == nil {
			return nil, domain.NewError(domain.ErrorInternal, validateRollbackConsumers,
				"session lacks KubeBlocks state")
		}

		return m.currentKubeBlocksRollbackPods(
			ctx,
			pod,
			controller,
		)
	case v1alpha1.WorkloadGrafana:
		return m.currentGrafanaRollbackPods(ctx, controller,
			workload.OriginalReplicas, workload.Grafana)
	default:
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			validateRollbackConsumers,
			fmt.Sprintf("adapter %q is unsupported", workload.Adapter),
		)
	}
}

func (m *Manager) currentControllerPods(
	ctx context.Context,
	references []v1alpha1.ObjectReference,
	controller v1alpha1.ObjectReference,
	operation string,
) ([]v1alpha1.ObjectReference, error) {
	pods, errors := m.readPodReferences(ctx, references)
	current := make([]v1alpha1.ObjectReference, 0, len(references))

	for index, ref := range references {
		err := errors[index]
		if apierrors.IsNotFound(err) {
			continue
		}

		if err != nil {
			return nil, domain.WrapError(
				domain.ErrorKubernetes,
				operation,
				fmt.Sprintf("read Pod %s/%s", ref.Namespace, ref.Name),
				err,
			)
		}

		if err := validatePodController(pods[index], controller, operation); err != nil {
			return nil, err
		}

		current = append(current, podReference(pods[index]))
	}

	return current, nil
}

func (m *Manager) VerifyPaused(
	ctx context.Context,
	owner, namespace string,
	workload v1alpha1.WorkloadSpec,
	phase, resumeFrom v1alpha1.WorkflowPhase,
) error {
	if err := validateWorkloadScope(namespace, workload.Adapter); err != nil {
		return err
	}

	if workload.Adapter == v1alpha1.WorkloadNone {
		return nil
	}

	if err := m.verifyPauseControl(ctx, owner, namespace, workload, phase, resumeFrom); err != nil {
		return err
	}

	references := qualifiedWorkloadReferences(workload.AffectedPods, namespace)
	if len(references) == 0 {
		references = []v1alpha1.ObjectReference{qualifiedWorkloadReference(workload.Pod, namespace)}
	}

	seen := make(map[string]struct{}, len(references))

	uniqueReferences := make([]v1alpha1.ObjectReference, 0, len(references))
	for _, reference := range references {
		key := reference.Namespace + "/" + reference.Name
		if _, ok := seen[key]; ok || reference.Name == "" {
			continue
		}

		seen[key] = struct{}{}

		uniqueReferences = append(uniqueReferences, reference)
	}

	// Workload operators can transiently recreate a paused component's Pod
	// while they reconcile around the pause (VMCluster rolling updates
	// recreate from the highest ordinal first). The reduced replicaCount
	// guarantees any recreation is reaped, so wait the Pod out -- by name,
	// tolerating replacements -- instead of failing the workflow on the
	// first sighting.
	for _, reference := range uniqueReferences {
		if err := m.waitForPodNameGone(
			ctx,
			reference.Namespace,
			reference.Name,
			"verify paused",
		); err != nil {
			return err
		}
	}

	return nil
}

// waitForPodNameGone polls until no Pod with the given name exists,
// regardless of UID. Unlike waitForPodDeletion, a same-name replacement is
// not an error: while the reduced replicaCount (or the operator's pause) is
// held, the recreated Pod is guaranteed to be reaped.
func (m *Manager) waitForPodNameGone(
	ctx context.Context,
	namespace, name, operation string,
) error {
	return m.waitFor(
		ctx,
		fmt.Sprintf("%s Pod %s/%s removal", operation, namespace, name),
		func(waitCtx context.Context) (bool, error) {
			_, getErr := m.typed.CoreV1().
				Pods(namespace).
				Get(waitCtx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				return true, nil
			}

			if getErr != nil {
				return false, getErr
			}

			return false, nil
		},
	)
}

func (m *Manager) readPodReferences(
	ctx context.Context,
	references []v1alpha1.ObjectReference,
) ([]*corev1.Pod, []error) {
	pods := make([]*corev1.Pod, len(references))
	errors := make([]error, len(references))
	parallel.For(len(references), func(index int) {
		reference := references[index]

		pods[index], errors[index] = m.typed.CoreV1().
			Pods(reference.Namespace).
			Get(ctx, reference.Name, metav1.GetOptions{})
		if errors[index] == nil && (pods[index] == nil || pods[index].Name == "") {
			errors[index] = domain.NewError(
				domain.ErrorKubernetes,
				"read Pod",
				fmt.Sprintf(
					"Pod %s/%s returned an empty object",
					reference.Namespace,
					reference.Name,
				),
			)
		}
	})

	return pods, errors
}

func (m *Manager) readPods(
	ctx context.Context,
	namespace string,
	names []string,
) ([]*corev1.Pod, []error) {
	references := make([]v1alpha1.ObjectReference, len(names))
	for index, name := range names {
		references[index] = v1alpha1.ObjectReference{Namespace: namespace, Name: name}
	}

	return m.readPodReferences(ctx, references)
}

func (m *Manager) verifyPauseControl(
	ctx context.Context,
	owner, namespace string,
	workload v1alpha1.WorkloadSpec,
	phase, resumeFrom v1alpha1.WorkflowPhase,
) error {
	controller := qualifiedWorkloadReference(workload.Controller, namespace)
	pod := qualifiedWorkloadReference(workload.Pod, namespace)

	switch workload.Adapter {
	case v1alpha1.WorkloadDeployment:
		return m.verifyDeploymentPaused(ctx, controller, workload.OriginalReplicas)
	case v1alpha1.WorkloadStatefulSet:
		return m.verifyStatefulSetPaused(
			ctx,
			controller,
			workload.OriginalReplicas,
			workload.Ordinal,
		)
	case v1alpha1.WorkloadVictoriaLogs:
		return m.verifyVictoriaLogsPaused(ctx, owner, controller)
	case v1alpha1.WorkloadVMCluster:
		return m.verifyVMClusterPaused(
			ctx,
			owner, namespace, controller,
			workload.Ordinal,
			workload.VMCluster,
		)
	case v1alpha1.WorkloadGrafana:
		return m.verifyGrafanaPaused(
			ctx,
			owner, controller, pod,
			workload.Grafana,
		)
	case v1alpha1.WorkloadKubeBlocks:
		return m.verifyKubeBlocksPaused(
			ctx,
			owner, namespace,
			kubeBlocksOperationName(
				owner, phase, resumeFrom,
				"pause",
			),
			controller,
			workload.KubeBlocks,
		)
	default:
		return nil
	}
}

func statefulSetReplicas(sts *appsv1.StatefulSet) int32 {
	if sts.Spec.Replicas == nil {
		return 1
	}
	return *sts.Spec.Replicas
}

func deploymentReplicas(deployment *appsv1.Deployment) int32 {
	if deployment.Spec.Replicas == nil {
		return 1
	}
	return *deployment.Spec.Replicas
}

func isCockroachStatefulSet(sts *appsv1.StatefulSet) bool {
	return strings.EqualFold(sts.Labels[kube.AppNameLabel], "cockroachdb") ||
		strings.EqualFold(sts.Labels["app"], "cockroachdb")
}

func isMinIOStatefulSet(sts *appsv1.StatefulSet) bool {
	for _, value := range []string{
		sts.Labels["app"],
		sts.Labels[kube.AppNameLabel],
		sts.Labels[kube.AppComponentLabel],
	} {
		if strings.EqualFold(value, "minio") {
			return true
		}
	}

	return false
}

func isVictoriaLogsStatefulSet(sts *appsv1.StatefulSet) bool {
	if !strings.EqualFold(sts.Labels[kube.AppNameLabel], "victoria-logs-cluster") {
		return false
	}

	return strings.EqualFold(sts.Labels[kube.AppComponentLabel], "vlstorage") ||
		strings.EqualFold(sts.Labels["app"], "vlstorage") ||
		strings.Contains(strings.ToLower(sts.Name), "-vlstorage")
}

func requireReadyPod(pod *corev1.Pod, namespace, name string) error {
	if pod.Status.Phase != corev1.PodRunning || !kube.PodReady(pod) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"discover workload",
			fmt.Sprintf("Pod %s/%s must be Running and Ready", namespace, name),
		)
	}

	return nil
}

// unsupportedStatefulSetReason rejects workloads whose reconciliation or
// storage lifecycle requires an application-specific migration procedure.
// Ordinary StatefulSets, including Helm-rendered ones, use the native
// StatefulSet adapter below. Helm metadata alone does not imply a controller
// that will reconcile replica changes during the migration window.
func unsupportedStatefulSetReason(sts *appsv1.StatefulSet) string {
	if isCockroachStatefulSet(sts) {
		return fmt.Sprintf(
			"CockroachDB StatefulSet %s/%s requires CockroachDB drain and decommission",
			sts.Namespace,
			sts.Name,
		)
	}

	if isMinIOStatefulSet(sts) {
		return fmt.Sprintf(
			"MinIO StatefulSet %s/%s requires MinIO drive or pool maintenance",
			sts.Namespace,
			sts.Name,
		)
	}

	parent := controllerOwner(sts.OwnerReferences)
	if parent != nil {
		parentGV, err := schema.ParseGroupVersion(parent.APIVersion)
		if err == nil {
			switch parent.Kind {
			case domain.KindBackup:
				return fmt.Sprintf(
					"Backup-owned archive-WAL StatefulSet %s/%s is a backup workload and cannot be migrated",
					sts.Namespace,
					sts.Name,
				)
			case "Tenant":
				if parentGV.Group == "minio.min.io" {
					return fmt.Sprintf(
						"MinIO Tenant StatefulSet %s/%s requires MinIO drive or pool maintenance",
						sts.Namespace,
						sts.Name,
					)
				}
			case domain.KindVMCluster:
				if parentGV.Group == "operator.victoriametrics.com" {
					return ""
				}
			case domain.KindComponent, domain.KindInstanceSet:
				if strings.Contains(parentGV.Group, "kubeblocks.io") {
					return ""
				}
			}
		}
	}

	return ""
}

func controllerOwner(owners []metav1.OwnerReference) *metav1.OwnerReference {
	for i := range owners {
		if owners[i].Controller != nil && *owners[i].Controller {
			return &owners[i]
		}
	}

	return nil
}

func sameControllerOwner(current, expected *metav1.OwnerReference) bool {
	if current == nil || expected == nil {
		return false
	}

	return expected.UID != "" && current.APIVersion == expected.APIVersion &&
		current.Kind == expected.Kind &&
		current.Name == expected.Name &&
		current.UID == expected.UID
}

func validatePodController(
	pod *corev1.Pod,
	expected v1alpha1.ObjectReference,
	operation string,
) error {
	if pod == nil || pod.Namespace == "" || pod.Name == "" || pod.UID == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			operation,
			"Kubernetes returned an incomplete Pod identity",
		)
	}

	if expected.Namespace == "" || expected.Name == "" || expected.UID == "" {
		return domain.NewError(
			domain.ErrorValidation,
			operation,
			"controller namespace, name, and UID are required",
		)
	}

	owner := controllerOwner(pod.OwnerReferences)
	if owner != nil && owner.APIVersion == expected.APIVersion && owner.Kind == expected.Kind &&
		owner.Name == expected.Name &&
		owner.UID == expected.UID {
		return nil
	}

	return domain.NewError(
		domain.ErrorConflict,
		operation,
		fmt.Sprintf(
			"Pod %s/%s is not controlled by the expected %s %s/%s",
			pod.Namespace,
			pod.Name,
			expected.Kind,
			expected.Namespace,
			expected.Name,
		),
	)
}

func podReference(pod *corev1.Pod) v1alpha1.ObjectReference {
	return objectReference(
		domain.CoreAPIVersion,
		domain.KindPod,
		pod.Namespace,
		pod.Name,
		pod.UID,
		pod.ResourceVersion,
	)
}

// waitForResumedPod returns the ready Pod identity fenced to its controller.
// The caller owns checkpointing replacements before a later pause or rollback.
func (m *Manager) waitForResumedPod(
	ctx context.Context,
	ref, controller v1alpha1.ObjectReference,
	operation string,
) (v1alpha1.ObjectReference, error) {
	var ready *corev1.Pod
	if err := m.waitFor(
		ctx,
		fmt.Sprintf("Pod %s/%s readiness", ref.Namespace, ref.Name),
		func(waitCtx context.Context) (bool, error) {
			pod, err := m.typed.CoreV1().
				Pods(ref.Namespace).
				Get(waitCtx, ref.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return false, nil
			}

			if err != nil {
				return false, err
			}

			if err := validatePodController(pod, controller, operation); err != nil {
				return false, err
			}

			if !kube.PodReady(pod) {
				return false, nil
			}

			ready = pod.DeepCopy()

			return true, nil
		},
	); err != nil {
		return v1alpha1.ObjectReference{}, err
	}

	if ready == nil {
		return v1alpha1.ObjectReference{}, domain.NewError(
			domain.ErrorKubernetes,
			operation,
			fmt.Sprintf("Pod %s/%s readiness wait returned no Pod", ref.Namespace, ref.Name),
		)
	}

	return podReference(ready), nil
}

func (m *Manager) waitForResumedPods(
	ctx context.Context,
	references []v1alpha1.ObjectReference,
	controller v1alpha1.ObjectReference,
	operation string,
) ([]v1alpha1.ObjectReference, error) {
	observed := make([]v1alpha1.ObjectReference, 0, len(references))
	for _, ref := range references {
		updated, err := m.waitForResumedPod(ctx, ref, controller, operation)
		if err != nil {
			return observed, err
		}

		observed = append(observed, updated)
	}

	return observed, nil
}

func objectReference(
	apiVersion, kind, namespace, name string,
	uid types.UID,
	resourceVersion string,
) v1alpha1.ObjectReference {
	return v1alpha1.ObjectReference{
		APIVersion:      apiVersion,
		Kind:            kind,
		Namespace:       namespace,
		Name:            name,
		UID:             uid,
		ResourceVersion: resourceVersion,
	}
}

func podRole(pod *corev1.Pod) string {
	for _, key := range []string{kubeBlocksRoleLabel, kubeBlocksAppsRoleLabel, genericRoleLabel} {
		if value := strings.ToLower(pod.Labels[key]); value != "" {
			return value
		}
	}

	return ""
}

func isLeaderRole(role string) bool {
	switch strings.ToLower(role) {
	case "leader", "primary", "master":
		return true
	default:
		return false
	}
}

func podOrdinal(pod *corev1.Pod, statefulSetName string) (int32, error) {
	value := pod.Labels[appsv1.PodIndexLabel]
	if value == "" {
		value = strings.TrimPrefix(pod.Name, statefulSetName+"-")
	}

	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil || parsed < 0 || pod.Name != fmt.Sprintf("%s-%d", statefulSetName, parsed) {
		return 0, domain.NewError(
			domain.ErrorPrecondition,
			"discover StatefulSet",
			"cannot derive ordinal from Pod "+pod.Name,
		)
	}

	return int32(parsed), nil
}
