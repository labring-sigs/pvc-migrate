package controller

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

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
	kubeBlocksClusterAPIVersion = domain.KubeBlocksAppsGroup + "/v1alpha1"
	kubeBlocksOpsAPIVersion     = kubeBlocksOperationsAPIGroup + "/v1alpha1"
	kubeBlocksGroupSuffix       = "kubeblocks.io"
	vmClusterAPIVersion         = "operator.victoriametrics.com/v1beta1"
	grafanaAPIVersion           = "grafana.integreatly.org/v1beta1"
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

type DiscoverOptions struct {
	Namespace           string
	PodName             string
	SwitchoverCandidate string
	AllowLeaderDowntime bool
}

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
	ref domain.ObjectReference,
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

func (m *Manager) Discover(
	ctx context.Context,
	options DiscoverOptions,
) (domain.WorkloadSpec, error) {
	pod, err := m.typed.CoreV1().
		Pods(options.Namespace).
		Get(ctx, options.PodName, metav1.GetOptions{})
	if err != nil {
		return domain.WorkloadSpec{}, domain.WrapError(
			domain.ErrorKubernetes,
			"discover workload",
			fmt.Sprintf("read Pod %s/%s", options.Namespace, options.PodName),
			err,
		)
	}

	return m.DiscoverPod(ctx, pod, options)
}

// DiscoverPod resolves a workload from a caller-owned Pod snapshot.
func (m *Manager) DiscoverPod(
	ctx context.Context,
	pod *corev1.Pod,
	options DiscoverOptions,
) (domain.WorkloadSpec, error) {
	options, err := normalizeDiscoverPodInput(pod, options)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}

	owner := controllerOwner(pod.OwnerReferences)
	if owner == nil {
		if err := requireReadyPod(pod, options.Namespace, options.PodName); err != nil {
			return domain.WorkloadSpec{}, err
		}
		return standaloneWorkload(pod)
	}

	return m.discoverOwnedWorkload(ctx, pod, owner, options)
}

func normalizeDiscoverPodInput(
	pod *corev1.Pod,
	options DiscoverOptions,
) (DiscoverOptions, error) {
	if pod == nil {
		return options, domain.NewError(
			domain.ErrorValidation,
			"discover workload",
			"Pod is nil",
		)
	}

	if options.Namespace == "" {
		options.Namespace = pod.Namespace
	}

	if options.PodName == "" {
		options.PodName = pod.Name
	}

	if pod.Namespace != options.Namespace || pod.Name != options.PodName {
		return options, domain.NewError(
			domain.ErrorConflict,
			"discover workload",
			fmt.Sprintf(
				"Pod snapshot %s/%s does not match requested %s/%s",
				pod.Namespace,
				pod.Name,
				options.Namespace,
				options.PodName,
			),
		)
	}

	if pod.Namespace == "" || pod.Name == "" || pod.UID == "" {
		return options, domain.NewError(
			domain.ErrorKubernetes,
			"discover workload",
			"Kubernetes returned an incomplete Pod identity",
		)
	}

	if pod.Annotations[corev1.MirrorPodAnnotationKey] != "" {
		return options, domain.NewError(
			domain.ErrorPrecondition,
			"discover workload",
			"static mirror Pods are unsupported",
		)
	}

	if owner := pod.Annotations[kube.SessionKey]; owner != "" {
		return options, domain.NewError(
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

	return options, nil
}

func (m *Manager) discoverOwnedWorkload(
	ctx context.Context,
	pod *corev1.Pod,
	owner *metav1.OwnerReference,
	options DiscoverOptions,
) (domain.WorkloadSpec, error) {
	if owner.UID == "" {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover workload",
			fmt.Sprintf("Pod %s/%s controller reference has no UID", pod.Namespace, pod.Name),
		)
	}

	groupVersion, err := schema.ParseGroupVersion(owner.APIVersion)
	if err != nil {
		return domain.WorkloadSpec{}, domain.WrapError(
			domain.ErrorPrecondition,
			"discover workload",
			"parse controller apiVersion",
			err,
		)
	}

	switch {
	case owner.Kind == domain.KindStatefulSet && groupVersion.Group == appsv1.GroupName:
		return m.discoverStatefulSetOwner(ctx, pod, owner, options)
	case owner.Kind == domain.KindJob && groupVersion.Group == batchv1.GroupName:
		return m.discoverJobOwner(ctx, pod, owner, options)
	case owner.Kind == domain.KindReplicaSet && groupVersion.Group == appsv1.GroupName:
		return m.discoverReplicaSetOwner(ctx, pod, owner, options)
	case owner.Kind == domain.KindInstanceSet &&
		strings.Contains(groupVersion.Group, kubeBlocksGroupSuffix):
		return m.kubeBlocksWorkload(ctx, pod, owner, options)
	default:
		return domain.WorkloadSpec{}, domain.NewError(
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
	options DiscoverOptions,
) (domain.WorkloadSpec, error) {
	sts, err := m.typed.AppsV1().StatefulSets(options.Namespace).Get(
		ctx,
		owner.Name,
		metav1.GetOptions{},
	)
	if err != nil {
		return domain.WorkloadSpec{}, domain.WrapError(
			domain.ErrorKubernetes,
			"discover workload",
			"read StatefulSet",
			err,
		)
	}

	if sts.UID == "" || sts.UID != owner.UID {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorConflict,
			"discover workload",
			fmt.Sprintf("Pod %s/%s StatefulSet owner UID changed", pod.Namespace, pod.Name),
		)
	}

	if reason := unsupportedStatefulSetReason(sts); reason != "" {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover workload",
			reason,
		)
	}

	if isVictoriaLogsStatefulSet(sts) {
		return m.victoriaLogsWorkload(ctx, pod, sts)
	}

	if err := requireReadyPod(pod, options.Namespace, options.PodName); err != nil {
		return domain.WorkloadSpec{}, err
	}

	parent := controllerOwner(sts.OwnerReferences)
	if parent == nil {
		return m.statefulSetWorkload(ctx, pod, sts, options)
	}

	return m.discoverStatefulSetParent(ctx, pod, sts, parent, options)
}

func (m *Manager) discoverStatefulSetParent(
	ctx context.Context,
	pod *corev1.Pod,
	sts *appsv1.StatefulSet,
	parent *metav1.OwnerReference,
	options DiscoverOptions,
) (domain.WorkloadSpec, error) {
	if parent.UID == "" {
		return domain.WorkloadSpec{}, domain.NewError(
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
		return domain.WorkloadSpec{}, domain.WrapError(
			domain.ErrorPrecondition,
			"discover workload",
			"parse parent controller apiVersion",
			err,
		)
	}

	switch parent.Kind {
	case domain.KindVMCluster:
		if parentGV.Group == "operator.victoriametrics.com" {
			return m.vmClusterWorkload(ctx, pod, parent, sts, options)
		}
	case domain.KindComponent:
		if strings.Contains(parentGV.Group, "kubeblocks.io") {
			return m.kubeBlocksWorkload(ctx, pod, controllerOwner(pod.OwnerReferences), options)
		}
	}

	if strings.Contains(parentGV.Group, "kubeblocks.io") {
		return m.kubeBlocksWorkload(ctx, pod, controllerOwner(pod.OwnerReferences), options)
	}

	return domain.WorkloadSpec{}, domain.NewError(
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
	options DiscoverOptions,
) (domain.WorkloadSpec, error) {
	job, err := m.typed.BatchV1().Jobs(options.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WorkloadSpec{}, domain.WrapError(
			domain.ErrorKubernetes,
			"discover workload",
			"read Job",
			err,
		)
	}

	if job.UID == "" || job.UID != owner.UID {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorConflict,
			"discover workload",
			fmt.Sprintf("Pod %s/%s Job owner UID changed", pod.Namespace, pod.Name),
		)
	}

	if parent := controllerOwner(
		job.OwnerReferences,
	); parent != nil &&
		parent.Kind == domain.KindBackup {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover workload",
			fmt.Sprintf(
				"Backup-owned archive-WAL Job %s/%s is a backup workload and cannot be migrated",
				options.Namespace,
				job.Name,
			),
		)
	}

	if err := requireReadyPod(pod, options.Namespace, options.PodName); err != nil {
		return domain.WorkloadSpec{}, err
	}

	return domain.WorkloadSpec{}, domain.NewError(
		domain.ErrorPrecondition,
		"discover workload",
		fmt.Sprintf("controller %s/%s has no safe pause adapter", owner.APIVersion, owner.Kind),
	)
}

func (m *Manager) discoverReplicaSetOwner(
	ctx context.Context,
	pod *corev1.Pod,
	owner *metav1.OwnerReference,
	options DiscoverOptions,
) (domain.WorkloadSpec, error) {
	rs, err := m.typed.AppsV1().
		ReplicaSets(options.Namespace).
		Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WorkloadSpec{}, domain.WrapError(
			domain.ErrorKubernetes,
			"discover workload",
			"read ReplicaSet",
			err,
		)
	}

	if rs.UID == "" || rs.UID != owner.UID {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorConflict,
			"discover workload",
			fmt.Sprintf("Pod %s/%s ReplicaSet owner UID changed", pod.Namespace, pod.Name),
		)
	}

	deployment := controllerOwner(rs.OwnerReferences)
	if deployment == nil || deployment.Kind != domain.KindDeployment {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover workload",
			"ReplicaSet has no Deployment controller",
		)
	}

	if deployment.UID == "" {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover workload",
			fmt.Sprintf("ReplicaSet %s/%s Deployment reference has no UID", rs.Namespace, rs.Name),
		)
	}

	deploymentObject, err := m.typed.AppsV1().
		Deployments(options.Namespace).
		Get(ctx, deployment.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WorkloadSpec{}, domain.WrapError(
			domain.ErrorKubernetes,
			"discover workload",
			"read Deployment",
			err,
		)
	}

	if deploymentObject.UID == "" || deploymentObject.UID != deployment.UID {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorConflict,
			"discover workload",
			fmt.Sprintf("ReplicaSet %s/%s Deployment owner UID changed", rs.Namespace, rs.Name),
		)
	}

	grafanaOwner := controllerOwner(deploymentObject.OwnerReferences)
	if grafanaOwner != nil {
		if grafanaOwner.UID == "" {
			return domain.WorkloadSpec{}, domain.NewError(
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

	if err := requireReadyPod(pod, options.Namespace, options.PodName); err != nil {
		return domain.WorkloadSpec{}, err
	}

	return m.deploymentWorkload(ctx, pod, deploymentObject)
}

func (m *Manager) Pause(ctx context.Context, session *domain.Session) error {
	switch session.Spec.Workload().Adapter {
	case domain.WorkloadNone:
		return nil
	case domain.WorkloadStandalone:
		return m.pauseStandalone(ctx, session)
	case domain.WorkloadDeployment:
		return m.pauseDeployment(ctx, session)
	case domain.WorkloadStatefulSet:
		return m.pauseStatefulSet(ctx, session)
	case domain.WorkloadVictoriaLogs:
		return m.pauseVictoriaLogs(ctx, session)
	case domain.WorkloadKubeBlocks:
		return m.pauseKubeBlocks(ctx, session)
	case domain.WorkloadVMCluster:
		return m.pauseVMCluster(ctx, session)
	case domain.WorkloadGrafana:
		return m.pauseGrafana(ctx, session)
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"pause workload",
			fmt.Sprintf("adapter %q is unsupported", session.Spec.Workload().Adapter),
		)
	}
}

func (m *Manager) Resume(ctx context.Context, session *domain.Session) error {
	switch session.Spec.Workload().Adapter {
	case domain.WorkloadNone:
		return nil
	case domain.WorkloadStandalone:
		return m.resumeStandalone(ctx, session)
	case domain.WorkloadDeployment:
		return m.resumeDeployment(ctx, session)
	case domain.WorkloadStatefulSet:
		return m.resumeStatefulSet(ctx, session)
	case domain.WorkloadVictoriaLogs:
		return m.resumeVictoriaLogs(ctx, session)
	case domain.WorkloadKubeBlocks:
		return m.resumeKubeBlocks(ctx, session)
	case domain.WorkloadVMCluster:
		return m.resumeVMCluster(ctx, session)
	case domain.WorkloadGrafana:
		return m.resumeGrafana(ctx, session)
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume workload",
			fmt.Sprintf("adapter %q is unsupported", session.Spec.Workload().Adapter),
		)
	}
}

func (m *Manager) ValidateResume(ctx context.Context, session *domain.Session) error {
	if session == nil {
		return domain.NewError(
			domain.ErrorValidation,
			"validate workload resume",
			"session is nil",
		)
	}

	switch session.Spec.Workload().Adapter {
	case domain.WorkloadNone:
		return nil
	case domain.WorkloadStandalone:
		return m.validateStandaloneResume(ctx, session)
	case domain.WorkloadDeployment:
		return m.validateDeploymentResume(ctx, session)
	case domain.WorkloadStatefulSet:
		return m.validateStatefulSetResume(ctx, session)
	case domain.WorkloadVictoriaLogs:
		return m.validateVictoriaLogsResume(ctx, session)
	case domain.WorkloadVMCluster:
		return m.validateVMClusterResume(ctx, session)
	case domain.WorkloadKubeBlocks:
		return m.validateKubeBlocksResume(ctx, session)
	case domain.WorkloadGrafana:
		return m.validateGrafanaResume(ctx, session)
	default:
		return domain.NewError(
			domain.ErrorPrecondition,
			"validate workload resume",
			fmt.Sprintf("adapter %q is unsupported", session.Spec.Workload().Adapter),
		)
	}
}

func (m *Manager) CurrentRollbackPods(
	ctx context.Context,
	session *domain.Session,
) ([]domain.ObjectReference, error) {
	if session == nil {
		return nil, domain.NewError(
			domain.ErrorValidation,
			validateRollbackConsumers,
			"session is nil",
		)
	}

	switch session.Spec.Workload().Adapter {
	case domain.WorkloadNone:
		return nil, nil
	case domain.WorkloadStandalone:
		return m.currentStandaloneRollbackPods(ctx, session)
	case domain.WorkloadDeployment:
		return m.currentDeploymentRollbackPods(ctx, session)
	case domain.WorkloadStatefulSet,
		domain.WorkloadVictoriaLogs,
		domain.WorkloadVMCluster:
		return m.currentStatefulSetRollbackPods(ctx, session)
	case domain.WorkloadKubeBlocks:
		return m.currentKubeBlocksRollbackPods(ctx, session)
	case domain.WorkloadGrafana:
		return m.currentGrafanaRollbackPods(ctx, session)
	default:
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			validateRollbackConsumers,
			fmt.Sprintf("adapter %q is unsupported", session.Spec.Workload().Adapter),
		)
	}
}

func (m *Manager) currentControllerPods(
	ctx context.Context,
	references []domain.ObjectReference,
	controller domain.ObjectReference,
	operation string,
) ([]domain.ObjectReference, error) {
	pods, errors := m.readPodReferences(ctx, references)
	current := make([]domain.ObjectReference, 0, len(references))

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

func (m *Manager) VerifyPaused(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.Adapter == domain.WorkloadNone {
		return nil
	}

	if err := m.verifyPauseControl(ctx, session); err != nil {
		return err
	}

	references := workload.AffectedPods
	if len(references) == 0 {
		references = []domain.ObjectReference{workload.Pod}
	}

	seen := make(map[string]struct{}, len(references))

	uniqueReferences := make([]domain.ObjectReference, 0, len(references))
	for _, reference := range references {
		key := reference.Namespace + "/" + reference.Name
		if _, ok := seen[key]; ok || reference.Name == "" {
			continue
		}

		seen[key] = struct{}{}

		uniqueReferences = append(uniqueReferences, reference)
	}

	_, errors := m.readPodReferences(ctx, uniqueReferences)
	for index, reference := range uniqueReferences {
		err := errors[index]
		if apierrors.IsNotFound(err) {
			continue
		}

		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"verify paused",
				"read workload Pod",
				err,
			)
		}

		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			fmt.Sprintf("Pod %s/%s is still present", reference.Namespace, reference.Name),
		)
	}

	return nil
}

func (m *Manager) readPodReferences(
	ctx context.Context,
	references []domain.ObjectReference,
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
	references := make([]domain.ObjectReference, len(names))
	for index, name := range names {
		references[index] = domain.ObjectReference{Namespace: namespace, Name: name}
	}

	return m.readPodReferences(ctx, references)
}

func (m *Manager) verifyPauseControl(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	switch workload.Adapter {
	case domain.WorkloadDeployment:
		return m.verifyDeploymentPaused(ctx, workload)
	case domain.WorkloadStatefulSet:
		return m.verifyStatefulSetPaused(ctx, workload)
	case domain.WorkloadVictoriaLogs:
		return m.verifyVictoriaLogsPaused(ctx, session, workload)
	case domain.WorkloadVMCluster:
		return m.verifyVMClusterPaused(ctx, session, workload)
	case domain.WorkloadGrafana:
		return m.verifyGrafanaPaused(ctx, session, workload)
	case domain.WorkloadKubeBlocks:
		return m.verifyKubeBlocksPaused(ctx, session, workload)
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
	expected domain.ObjectReference,
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

func podReference(pod *corev1.Pod) domain.ObjectReference {
	return objectReference(
		domain.CoreAPIVersion,
		domain.KindPod,
		pod.Namespace,
		pod.Name,
		pod.UID,
		pod.ResourceVersion,
	)
}

// waitForResumedPod fences readiness to the controller-owned Pod name and
// refreshes every session reference for that name. Controllers commonly
// recreate a Pod during resume, so retaining the paused Pod UID would make a
// later pause or rollback reject the healthy replacement as an unsafe drift.
func (m *Manager) waitForResumedPod(
	ctx context.Context,
	session *domain.Session,
	ref, controller domain.ObjectReference,
	operation string,
) error {
	if session == nil || session.Spec.WorkloadPtr() == nil {
		return domain.NewError(domain.ErrorValidation, operation, "session workload is required")
	}

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
		return err
	}

	if ready == nil {
		return domain.NewError(
			domain.ErrorKubernetes,
			operation,
			fmt.Sprintf("Pod %s/%s readiness wait returned no Pod", ref.Namespace, ref.Name),
		)
	}

	refreshResumedPodReference(session.Spec.WorkloadPtr(), ref, ready)

	return nil
}

func refreshResumedPodReference(
	workload *domain.WorkloadSpec,
	previous domain.ObjectReference,
	pod *corev1.Pod,
) {
	if workload == nil || pod == nil {
		return
	}

	updated := podReference(pod)
	if workload.Pod.Namespace == previous.Namespace && workload.Pod.Name == previous.Name {
		workload.Pod = updated
	}

	for index := range workload.AffectedPods {
		if workload.AffectedPods[index].Namespace == previous.Namespace &&
			workload.AffectedPods[index].Name == previous.Name {
			workload.AffectedPods[index] = updated
		}
	}
}

func objectReference(
	apiVersion, kind, namespace, name string,
	uid types.UID,
	resourceVersion string,
) domain.ObjectReference {
	return domain.ObjectReference{
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
