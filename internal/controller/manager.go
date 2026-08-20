package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sort"
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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
)

const (
	kubeBlocksClusterAPIVersion = domain.KubeBlocksAppsGroup + "/v1alpha1"
	kubeBlocksOpsAPIVersion     = "operations.kubeblocks.io/v1alpha1"
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

	return domain.WorkloadSpec{}, domain.NewError(
		domain.ErrorPrecondition,
		"discover workload",
		fmt.Sprintf(
			"Deployment %s/%s has no safe pause adapter",
			options.Namespace,
			deployment.Name,
		),
	)
}

func (m *Manager) Pause(ctx context.Context, session *domain.Session) error {
	switch session.Spec.Workload().Adapter {
	case domain.WorkloadNone:
		return nil
	case domain.WorkloadStandalone:
		return m.pauseStandalone(ctx, session)
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

func (m *Manager) verifyStatefulSetPaused(
	ctx context.Context,
	workload domain.WorkloadSpec,
) error {
	if workload.Controller.Kind != domain.KindStatefulSet || workload.OriginalReplicas == nil ||
		workload.Ordinal == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"verify paused",
			"StatefulSet session lacks controller and replica state",
		)
	}

	sts, err := m.typed.AppsV1().
		StatefulSets(workload.Controller.Namespace).
		Get(ctx, workload.Controller.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "verify paused", "read StatefulSet", err)
	}

	if sts.UID != workload.Controller.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf("StatefulSet %s/%s UID changed", sts.Namespace, sts.Name),
		)
	}

	if replicas := statefulSetReplicas(sts); replicas != *workload.Ordinal {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			fmt.Sprintf(
				"StatefulSet %s/%s replicas=%d, expected %d while paused",
				sts.Namespace,
				sts.Name,
				replicas,
				*workload.Ordinal,
			),
		)
	}

	return nil
}

func (m *Manager) verifyVictoriaLogsPaused(
	ctx context.Context,
	session *domain.Session,
	workload domain.WorkloadSpec,
) error {
	if workload.Controller.Kind != domain.KindStatefulSet {
		return domain.NewError(
			domain.ErrorInternal,
			"verify paused",
			"Victoria Logs session lacks StatefulSet controller state",
		)
	}

	sts, err := m.typed.AppsV1().
		StatefulSets(workload.Controller.Namespace).
		Get(ctx, workload.Controller.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify paused",
			"read Victoria Logs StatefulSet",
			err,
		)
	}

	if sts.UID != workload.Controller.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf("Victoria Logs StatefulSet %s/%s UID changed", sts.Namespace, sts.Name),
		)
	}

	if sts.Annotations[pauseSessionAnnotation] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf(
				"Victoria Logs StatefulSet %s/%s pause ownership changed",
				sts.Namespace,
				sts.Name,
			),
		)
	}

	if replicas := statefulSetReplicas(sts); replicas != 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			fmt.Sprintf(
				"Victoria Logs StatefulSet %s/%s replicas=%d",
				sts.Namespace,
				sts.Name,
				replicas,
			),
		)
	}

	return nil
}

func (m *Manager) verifyVMClusterPaused(
	ctx context.Context,
	session *domain.Session,
	workload domain.WorkloadSpec,
) error {
	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			"dynamic client is required for VMCluster pause verification",
		)
	}

	vm := workload.VMCluster
	if vm == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"verify paused",
			"session lacks VMCluster state",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(vm.APIVersion, vmClusterResource)
	if err != nil {
		return err
	}

	object, err := m.dynamic.Resource(gvr).
		Namespace(workload.Pod.Namespace).
		Get(ctx, vm.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "verify paused", "read VMCluster", err)
	}

	if object.GetUID() != vm.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf("VMCluster %s/%s UID changed", object.GetNamespace(), object.GetName()),
		)
	}

	if object.GetAnnotations()[pauseSessionAnnotation] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf(
				"VMCluster %s/%s pause ownership changed",
				object.GetNamespace(),
				object.GetName(),
			),
		)
	}

	component, _, nestedErr := unstructured.NestedMap(object.Object, "spec", vm.Component)
	if nestedErr != nil {
		return nestedErr
	}

	paused, _, _ := unstructured.NestedBool(component, "paused")
	if !paused {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			fmt.Sprintf("VMCluster component %s is not paused", vm.Component),
		)
	}

	return nil
}

func (m *Manager) verifyGrafanaPaused(
	ctx context.Context,
	session *domain.Session,
	workload domain.WorkloadSpec,
) error {
	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			"dynamic client is required for Grafana pause verification",
		)
	}

	grafana := workload.Grafana
	if grafana == nil {
		return domain.NewError(domain.ErrorInternal, "verify paused", "session lacks Grafana state")
	}

	gvr, err := kube.ParseGroupVersionResource(grafana.APIVersion, grafanaResource)
	if err != nil {
		return err
	}

	object, err := m.dynamic.Resource(gvr).
		Namespace(workload.Pod.Namespace).
		Get(ctx, grafana.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "verify paused", "read Grafana", err)
	}

	if object.GetUID() != grafana.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf("Grafana %s/%s UID changed", object.GetNamespace(), object.GetName()),
		)
	}

	if object.GetAnnotations()[pauseSessionAnnotation] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf(
				"Grafana %s/%s suspend ownership changed",
				object.GetNamespace(),
				object.GetName(),
			),
		)
	}

	suspended, _, _ := unstructured.NestedBool(object.Object, "spec", "suspend")
	if !suspended {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			"Grafana reconciliation is not suspended",
		)
	}

	if workload.Controller.Kind != domain.KindDeployment {
		return domain.NewError(
			domain.ErrorInternal,
			"verify paused",
			"Grafana session lacks Deployment controller state",
		)
	}

	deployment, deploymentErr := m.typed.AppsV1().
		Deployments(workload.Controller.Namespace).
		Get(ctx, workload.Controller.Name, metav1.GetOptions{})
	if deploymentErr != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify paused",
			"read Grafana Deployment",
			deploymentErr,
		)
	}

	if deployment.UID != workload.Controller.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf(
				"Grafana Deployment %s/%s UID changed",
				deployment.Namespace,
				deployment.Name,
			),
		)
	}

	if replicas := deploymentReplicas(deployment); replicas != 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			fmt.Sprintf(
				"Grafana Deployment %s/%s replicas=%d while reconciliation is suspended",
				deployment.Namespace,
				deployment.Name,
				replicas,
			),
		)
	}

	return nil
}

func (m *Manager) verifyKubeBlocksPaused(
	ctx context.Context,
	session *domain.Session,
	workload domain.WorkloadSpec,
) error {
	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			"dynamic client is required for KubeBlocks pause verification",
		)
	}

	kb := workload.KubeBlocks
	if kb == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"verify paused",
			"session lacks KubeBlocks state",
		)
	}

	if workload.Controller.Kind == domain.KindInstanceSet {
		return m.verifyKubeBlocksInstanceSetPaused(ctx, session)
	}

	if kb.ClusterUID == "" {
		return domain.NewError(
			domain.ErrorInternal,
			"verify paused",
			"session lacks KubeBlocks Cluster identity",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(kubeBlocksClusterAPIVersion, clusterResource)
	if err != nil {
		return err
	}

	object, err := m.dynamic.Resource(gvr).
		Namespace(workload.Pod.Namespace).
		Get(ctx, kb.Cluster, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"verify paused",
			"read KubeBlocks Cluster",
			err,
		)
	}

	if object.GetUID() != kb.ClusterUID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf(
				"KubeBlocks Cluster %s/%s UID changed",
				object.GetNamespace(),
				object.GetName(),
			),
		)
	}

	if object.GetAnnotations()[pauseSessionAnnotation] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf(
				"KubeBlocks Cluster %s/%s pause ownership changed",
				object.GetNamespace(),
				object.GetName(),
			),
		)
	}

	components, ok, nestedErr := unstructured.NestedSlice(object.Object, "spec", "componentSpecs")
	if nestedErr != nil || !ok {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			"KubeBlocks Cluster has no componentSpecs",
		)
	}

	for index := range components {
		component, ok := components[index].(map[string]any)
		if !ok {
			return domain.NewError(
				domain.ErrorPrecondition,
				"verify paused",
				fmt.Sprintf("KubeBlocks componentSpecs[%d] is malformed", index),
			)
		}

		name, _, _ := unstructured.NestedString(component, "name")
		if name != kb.Component {
			continue
		}

		stopped, _, _ := unstructured.NestedBool(component, "stop")
		if !stopped {
			return domain.NewError(
				domain.ErrorPrecondition,
				"verify paused",
				fmt.Sprintf("KubeBlocks component %s is not stopped", name),
			)
		}

		return nil
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		"verify paused",
		"KubeBlocks Cluster has no component "+kb.Component,
	)
}

func standaloneWorkload(pod *corev1.Pod) (domain.WorkloadSpec, error) {
	raw, err := json.Marshal(pod)
	if err != nil {
		return domain.WorkloadSpec{}, domain.WrapError(
			domain.ErrorInternal,
			"discover standalone Pod",
			"encode Pod",
			err,
		)
	}

	return domain.WorkloadSpec{
		Adapter:        domain.WorkloadStandalone,
		Pod:            podReference(pod),
		OriginalObject: raw,
	}, nil
}

func (m *Manager) statefulSetWorkload(
	ctx context.Context,
	pod *corev1.Pod,
	sts *appsv1.StatefulSet,
	options DiscoverOptions,
) (domain.WorkloadSpec, error) {
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}

	ordinal, err := podOrdinal(pod, sts.Name)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}

	if ordinal >= replicas {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover StatefulSet",
			fmt.Sprintf("Pod ordinal %d is outside replicas %d", ordinal, replicas),
		)
	}

	if policy := sts.Spec.PersistentVolumeClaimRetentionPolicy; policy != nil &&
		policy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover StatefulSet",
			fmt.Sprintf("PVC retention whenScaled is %s", policy.WhenScaled),
		)
	}

	affected := make([]domain.ObjectReference, 0, replicas-ordinal)

	names := make([]string, 0, replicas-ordinal)
	for current := ordinal; current < replicas; current++ {
		names = append(names, fmt.Sprintf("%s-%d", sts.Name, current))
	}

	candidates, getErrors := m.readPods(ctx, pod.Namespace, names)
	for index, name := range names {
		candidate, getErr := candidates[index], getErrors[index]
		if getErr != nil {
			return domain.WorkloadSpec{}, domain.WrapError(
				domain.ErrorPrecondition,
				"discover StatefulSet",
				fmt.Sprintf("affected Pod %s/%s is unavailable", pod.Namespace, name),
				getErr,
			)
		}

		if candidate.Status.Phase != corev1.PodRunning || !podReady(candidate) {
			return domain.WorkloadSpec{}, domain.NewError(
				domain.ErrorPrecondition,
				"discover StatefulSet",
				fmt.Sprintf("affected Pod %s/%s must be Running and Ready", pod.Namespace, name),
			)
		}

		if err := validatePodController(
			candidate,
			objectReference(
				domain.AppsAPIVersion,
				domain.KindStatefulSet,
				sts.Namespace,
				sts.Name,
				sts.UID,
				sts.ResourceVersion,
			),
			"discover StatefulSet",
		); err != nil {
			return domain.WorkloadSpec{}, err
		}

		if isLeaderRole(podRole(candidate)) && !options.AllowLeaderDowntime {
			return domain.WorkloadSpec{}, domain.NewError(
				domain.ErrorPrecondition,
				"discover StatefulSet",
				fmt.Sprintf(
					"scale-down affects %s with role %s; complete an application switchover and pass --allow-leader-downtime",
					name,
					podRole(candidate),
				),
			)
		}

		affected = append(affected, podReference(candidate))
	}

	return domain.WorkloadSpec{
		Adapter: domain.WorkloadStatefulSet,
		Pod:     podReference(pod),
		Controller: objectReference(
			domain.AppsAPIVersion,
			domain.KindStatefulSet,
			sts.Namespace,
			sts.Name,
			sts.UID,
			sts.ResourceVersion,
		),
		OriginalReplicas: &replicas,
		Ordinal:          &ordinal,
		AffectedPods:     affected,
	}, nil
}

func (m *Manager) victoriaLogsWorkload(
	ctx context.Context,
	pod *corev1.Pod,
	sts *appsv1.StatefulSet,
) (domain.WorkloadSpec, error) {
	replicas := statefulSetReplicas(sts)
	if policy := sts.Spec.PersistentVolumeClaimRetentionPolicy; policy != nil &&
		policy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover Victoria Logs",
			fmt.Sprintf("PVC retention whenScaled is %s", policy.WhenScaled),
		)
	}

	affected := make([]domain.ObjectReference, 0, replicas)

	names := make([]string, 0, replicas)
	for ordinal := range replicas {
		names = append(names, fmt.Sprintf("%s-%d", sts.Name, ordinal))
	}

	candidates, getErrors := m.readPods(ctx, pod.Namespace, names)
	for index, name := range names {
		candidate, err := candidates[index], getErrors[index]
		if err != nil {
			return domain.WorkloadSpec{}, domain.WrapError(
				domain.ErrorPrecondition,
				"discover Victoria Logs",
				fmt.Sprintf("affected Pod %s/%s is unavailable", pod.Namespace, name),
				err,
			)
		}

		if candidate.Status.Phase != corev1.PodRunning || !podReady(candidate) {
			return domain.WorkloadSpec{}, domain.NewError(
				domain.ErrorPrecondition,
				"discover Victoria Logs",
				fmt.Sprintf("affected Pod %s/%s must be Running and Ready", pod.Namespace, name),
			)
		}

		if err := validatePodController(
			candidate,
			objectReference(
				domain.AppsAPIVersion,
				domain.KindStatefulSet,
				sts.Namespace,
				sts.Name,
				sts.UID,
				sts.ResourceVersion,
			),
			"discover Victoria Logs",
		); err != nil {
			return domain.WorkloadSpec{}, err
		}

		affected = append(affected, podReference(candidate))
	}

	zero := int32(0)

	return domain.WorkloadSpec{
		Adapter: domain.WorkloadVictoriaLogs,
		Pod:     podReference(pod),
		Controller: objectReference(
			domain.AppsAPIVersion,
			domain.KindStatefulSet,
			sts.Namespace,
			sts.Name,
			sts.UID,
			sts.ResourceVersion,
		),
		OriginalReplicas: &replicas,
		Ordinal:          &zero,
		AffectedPods:     affected,
	}, nil
}

func (m *Manager) kubeBlocksWorkload(
	ctx context.Context,
	pod *corev1.Pod,
	owner *metav1.OwnerReference,
	options DiscoverOptions,
) (domain.WorkloadSpec, error) {
	state, err := m.prepareKubeBlocksDiscovery(ctx, pod, owner, options)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}

	instanceSet, err := m.discoverKubeBlocksInstanceSet(ctx, pod.Namespace, owner, pod.Name)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}

	role, roleIsLeader, err := resolveKubeBlocksRole(pod, owner, options, state.role, instanceSet)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}

	if roleIsLeader && state.switchoverCandidate == nil && !options.AllowLeaderDowntime {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			m.kubeBlocksLeaderGuidance(
				ctx,
				pod,
				state.cluster,
				state.component,
				role,
				state.opsAPIVersion,
			),
		)
	}

	switchoverStrategy := domain.KubeBlocksSwitchoverOpsRequest

	switchoverContainer := ""
	if state.switchoverCandidate != nil {
		switchoverStrategy, switchoverContainer, err = m.resolveKubeBlocksSwitchover(
			ctx,
			pod,
			owner,
			state,
			instanceSet,
			roleIsLeader,
		)
		if err != nil {
			return domain.WorkloadSpec{}, err
		}
	}

	if owner.Kind == domain.KindInstanceSet && instanceSet.Paused {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			fmt.Sprintf(
				"InstanceSet %s/%s is already paused; set spec.paused=false and wait for Pod %s/%s to become Ready before retrying",
				pod.Namespace,
				owner.Name,
				pod.Namespace,
				pod.Name,
			),
		)
	}

	controllerUID := owner.UID
	if owner.Kind == domain.KindInstanceSet {
		controllerUID = instanceSet.UID
	}

	return domain.WorkloadSpec{
		Adapter: domain.WorkloadKubeBlocks,
		Pod:     podReference(pod),
		Controller: objectReference(
			owner.APIVersion,
			owner.Kind,
			pod.Namespace,
			owner.Name,
			controllerUID,
			"",
		),
		KubeBlocks: &domain.KubeBlocksSpec{
			Cluster:                  state.cluster,
			Component:                state.component,
			Instance:                 pod.Name,
			Role:                     role,
			SwitchoverCandidate:      options.SwitchoverCandidate,
			SwitchoverStrategy:       switchoverStrategy,
			SwitchoverContainer:      switchoverContainer,
			OpsAPIVersion:            state.opsAPIVersion,
			ClusterUID:               state.clusterObject.GetUID(),
			OriginalStops:            state.originalStops,
			OriginalPaused:           instanceSet.Paused,
			OriginalPausedConfigured: instanceSet.PausedConfigured,
		},
	}, nil
}

type kubeBlocksDiscoveryState struct {
	cluster             string
	component           string
	role                string
	opsAPIVersion       string
	switchoverCandidate *corev1.Pod
	clusterObject       *unstructured.Unstructured
	originalStops       map[string]bool
}

func (m *Manager) prepareKubeBlocksDiscovery(
	ctx context.Context,
	pod *corev1.Pod,
	owner *metav1.OwnerReference,
	options DiscoverOptions,
) (kubeBlocksDiscoveryState, error) {
	state := kubeBlocksDiscoveryState{
		cluster:   pod.Labels[kube.AppInstanceLabel],
		component: kubeBlocksComponent(pod),
		role:      podRole(pod),
	}
	if state.cluster == "" || state.component == "" {
		return state, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"Pod lacks cluster or component identity labels",
		)
	}

	if reason := unsupportedKubeBlocksReason(pod, nil, state.component); reason != "" {
		return state, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", reason)
	}

	state.opsAPIVersion = servedKubeBlocksOpsAPIVersion(m.discovery)
	if state.opsAPIVersion == "" {
		return state, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"no served OpsRequest API was found",
		)
	}

	candidate, err := m.discoverKubeBlocksCandidate(ctx, pod, owner, options, state)
	if err != nil {
		return state, err
	}

	state.switchoverCandidate = candidate
	if m.dynamic == nil {
		return state, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"dynamic client is required for Cluster pause control",
		)
	}

	if isLeaderRole(state.role) && !options.AllowLeaderDowntime && candidate == nil {
		return state, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			m.kubeBlocksLeaderGuidance(
				ctx,
				pod,
				state.cluster,
				state.component,
				state.role,
				state.opsAPIVersion,
			),
		)
	}

	clusterObject, components, err := m.loadKubeBlocksCluster(ctx, pod, state.cluster)
	if err != nil {
		return state, err
	}

	state.clusterObject = clusterObject

	stops, err := parseKubeBlocksStops(components, state.component)
	if err != nil {
		return state, err
	}

	state.originalStops = stops

	return state, nil
}

func servedKubeBlocksOpsAPIVersion(discovery discovery.DiscoveryInterface) string {
	for _, candidate := range []string{kubeBlocksClusterAPIVersion, kubeBlocksOpsAPIVersion} {
		if kube.HasAPIResource(discovery, candidate, "opsrequests") {
			return candidate
		}
	}

	return ""
}

func (m *Manager) discoverKubeBlocksCandidate(
	ctx context.Context,
	pod *corev1.Pod,
	owner *metav1.OwnerReference,
	options DiscoverOptions,
	state kubeBlocksDiscoveryState,
) (*corev1.Pod, error) {
	if options.SwitchoverCandidate == "" {
		return nil, nil
	}

	if options.SwitchoverCandidate == pod.Name {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			fmt.Sprintf(
				"--kubeblocks-candidate %s refers to the selected source Pod; choose a different Ready non-leader Pod in cluster %s component %s%s",
				pod.Name,
				state.cluster,
				state.component,
				m.kubeBlocksCandidateSuggestion(ctx, pod, state.cluster, state.component),
			),
		)
	}

	candidate, err := m.typed.CoreV1().Pods(pod.Namespace).Get(
		ctx,
		options.SwitchoverCandidate,
		metav1.GetOptions{},
	)
	if err != nil {
		message := fmt.Sprintf(
			"read switchover candidate Pod %s/%s: %v",
			pod.Namespace,
			options.SwitchoverCandidate,
			err,
		)
		if apierrors.IsNotFound(err) {
			message = fmt.Sprintf(
				"switchover candidate Pod %s/%s does not exist; verify --kubeblocks-candidate%s",
				pod.Namespace,
				options.SwitchoverCandidate,
				m.kubeBlocksCandidateSuggestion(ctx, pod, state.cluster, state.component),
			)
		}

		return nil, domain.WrapError(domain.ErrorPrecondition, "discover KubeBlocks", message, err)
	}

	candidateCluster := candidate.Labels[kube.AppInstanceLabel]

	candidateComponent := kubeBlocksComponent(candidate)
	if candidateCluster != state.cluster || candidateComponent != state.component {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			fmt.Sprintf(
				"switchover candidate Pod %s/%s belongs to cluster %s component %s; expected cluster %s component %s%s",
				pod.Namespace,
				candidate.Name,
				candidateCluster,
				candidateComponent,
				state.cluster,
				state.component,
				m.kubeBlocksCandidateSuggestion(ctx, pod, state.cluster, state.component),
			),
		)
	}

	if err := validatePodController(
		candidate,
		objectReference(owner.APIVersion, owner.Kind, pod.Namespace, owner.Name, owner.UID, ""),
		"discover KubeBlocks",
	); err != nil {
		return nil, err
	}

	if !podReady(candidate) {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			fmt.Sprintf(
				"switchover candidate Pod %s/%s must be Running and Ready%s",
				pod.Namespace,
				candidate.Name,
				m.kubeBlocksCandidateSuggestion(ctx, pod, state.cluster, state.component),
			),
		)
	}

	return candidate, nil
}

func (m *Manager) loadKubeBlocksCluster(
	ctx context.Context,
	pod *corev1.Pod,
	cluster string,
) (*unstructured.Unstructured, []any, error) {
	clusterGVR, err := kube.ParseGroupVersionResource(kubeBlocksClusterAPIVersion, clusterResource)
	if err != nil {
		return nil, nil, err
	}

	clusterObject, err := m.dynamic.Resource(clusterGVR).Namespace(pod.Namespace).Get(
		ctx,
		cluster,
		metav1.GetOptions{},
	)
	if err != nil {
		return nil, nil, domain.WrapError(
			domain.ErrorKubernetes,
			"discover KubeBlocks",
			"read Cluster",
			err,
		)
	}

	if clusterObject.GetUID() == "" {
		return nil, nil, domain.NewError(
			domain.ErrorKubernetes,
			"discover KubeBlocks",
			fmt.Sprintf("Cluster %s/%s has an incomplete identity", pod.Namespace, cluster),
		)
	}

	components, ok, err := unstructured.NestedSlice(clusterObject.Object, "spec", "componentSpecs")
	if err != nil || !ok || len(components) == 0 {
		return nil, nil, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"Cluster has no componentSpecs",
		)
	}

	if reason := unsupportedKubeBlocksReason(
		pod,
		components,
		kubeBlocksComponent(pod),
	); reason != "" {
		return nil, nil, domain.NewError(domain.ErrorPrecondition, "discover KubeBlocks", reason)
	}

	return clusterObject, components, nil
}

func parseKubeBlocksStops(components []any, selected string) (map[string]bool, error) {
	stops := make(map[string]bool, 1)

	found := false
	for index := range components {
		componentSpec, ok := components[index].(map[string]any)
		if !ok {
			return nil, domain.NewError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("componentSpecs[%d] is malformed", index),
			)
		}

		name, nameOK, err := unstructured.NestedString(componentSpec, "name")
		if err != nil || !nameOK || name == "" {
			return nil, domain.NewError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("componentSpecs[%d] has no name", index),
			)
		}

		if _, _, err := unstructured.NestedString(componentSpec, "componentDefRef"); err != nil {
			return nil, domain.WrapError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("read component %s definition", name),
				err,
			)
		}

		stopped, _, err := unstructured.NestedBool(componentSpec, "stop")
		if err != nil {
			return nil, domain.WrapError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("read component %s stop state", name),
				err,
			)
		}

		if name == selected {
			stops[name] = stopped
			found = true
		}
	}

	if !found {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"Cluster componentSpecs has no component "+selected,
		)
	}

	return stops, nil
}

func resolveKubeBlocksRole(
	pod *corev1.Pod,
	owner *metav1.OwnerReference,
	options DiscoverOptions,
	role string,
	instanceSet kubeBlocksInstanceSetState,
) (string, bool, error) {
	if role != "" && instanceSet.Role != "" && !strings.EqualFold(role, instanceSet.Role) {
		return "", false, domain.NewError(
			domain.ErrorConflict,
			"discover KubeBlocks",
			fmt.Sprintf(
				"selected instance role changed from %s to %s during discovery; rerun the plan",
				role,
				instanceSet.Role,
			),
		)
	}

	if role == "" {
		role = instanceSet.Role
	}

	roleIsLeader := isLeaderRole(role)
	if !instanceSet.HasLeaderRole {
		return role, roleIsLeader, nil
	}

	if role == "" {
		if !options.AllowLeaderDowntime {
			return "", false, domain.NewError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf(
					"selected instance %s role is unavailable while InstanceSet %s declares leader roles; wait for the KubeBlocks role probe to recover, or use --allow-leader-downtime to acknowledge a possible leader outage",
					pod.Name,
					owner.Name,
				),
			)
		}

		return "unknown", false, nil
	}

	if knownLeader, knownRole := instanceSet.LeaderRoles[strings.ToLower(role)]; knownRole {
		return role, knownLeader, nil
	}

	if !options.AllowLeaderDowntime {
		return "", false, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			fmt.Sprintf(
				"selected instance %s reports role %s, which is absent from InstanceSet %s role definitions; wait for role status to converge, or use --allow-leader-downtime",
				pod.Name,
				role,
				owner.Name,
			),
		)
	}

	return role, roleIsLeader, nil
}

func (m *Manager) resolveKubeBlocksSwitchover(
	ctx context.Context,
	pod *corev1.Pod,
	owner *metav1.OwnerReference,
	state kubeBlocksDiscoveryState,
	instanceSet kubeBlocksInstanceSetState,
	roleIsLeader bool,
) (domain.KubeBlocksSwitchoverStrategy, string, error) {
	if roleIsLeader && instanceSet.HasLeaderRole {
		candidateRole := podRole(state.switchoverCandidate)

		candidateIsLeader, knownRole := instanceSet.LeaderRoles[strings.ToLower(candidateRole)]
		if !knownRole || candidateIsLeader {
			return "", "", domain.NewError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf(
					"switchover candidate %s must have a known non-leader role in InstanceSet %s",
					state.switchoverCandidate.Name,
					owner.Name,
				),
			)
		}
	}

	strategy, container, err := m.kubeBlocksSwitchoverStrategy(
		ctx,
		pod,
		state.cluster,
		state.component,
		state.switchoverCandidate.Name,
		state.opsAPIVersion,
	)
	if err != nil {
		return "", "", domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			fmt.Sprintf(
				"automatic switchover for selected instance %s is unavailable: %v; use --allow-leader-downtime to acknowledge the leader outage",
				pod.Name,
				err,
			),
		)
	}

	return strategy, container, nil
}

func (m *Manager) discoverKubeBlocksInstanceSet(
	ctx context.Context,
	namespace string,
	owner *metav1.OwnerReference,
	podName string,
) (kubeBlocksInstanceSetState, error) {
	state := kubeBlocksInstanceSetState{}
	if owner.Kind != domain.KindInstanceSet {
		return state, nil
	}

	if m.dynamic == nil {
		return state, domain.NewError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"dynamic client is required for InstanceSet reconciliation control",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(owner.APIVersion, instanceSetResource)
	if err != nil {
		return state, err
	}

	resource := m.dynamic.Resource(gvr).Namespace(namespace)

	object, err := resource.Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return state, domain.WrapError(
			domain.ErrorKubernetes,
			"discover KubeBlocks",
			"read InstanceSet",
			err,
		)
	}

	if object.GetUID() == "" || object.GetUID() != owner.UID {
		return state, domain.NewError(
			domain.ErrorConflict,
			"discover KubeBlocks",
			fmt.Sprintf("InstanceSet %s/%s UID changed", namespace, owner.Name),
		)
	}

	state.UID = object.GetUID()

	state.LeaderRoles, state.HasLeaderRole, err = kubeBlocksLeaderRoles(object)
	if err != nil {
		return state, err
	}

	state.Role, err = kubeBlocksMemberRole(object, podName)
	if err != nil {
		return state, err
	}

	paused, found, err := unstructured.NestedBool(object.Object, "spec", "paused")
	if err != nil {
		return state, domain.WrapError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"read InstanceSet paused state",
			err,
		)
	}

	state.Paused = paused

	state.PausedConfigured = found
	if !found {
		probe := object.DeepCopy()
		if err := unstructured.SetNestedField(probe.Object, true, "spec", "paused"); err != nil {
			return state, err
		}

		result, updateErr := resource.Update(
			ctx,
			probe,
			metav1.UpdateOptions{DryRun: []string{metav1.DryRunAll}},
		)
		if updateErr != nil {
			return state, domain.WrapError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				"probe InstanceSet spec.paused support",
				updateErr,
			)
		}

		if _, supported, nestedErr := unstructured.NestedBool(
			result.Object,
			"spec",
			"paused",
		); nestedErr != nil ||
			!supported {
			return state, domain.NewError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf(
					"InstanceSet %s/%s does not support spec.paused",
					namespace,
					owner.Name,
				),
			)
		}
	}

	return state, nil
}

func kubeBlocksLeaderRoles(instanceSet *unstructured.Unstructured) (map[string]bool, bool, error) {
	roles, found, err := unstructured.NestedSlice(instanceSet.Object, "spec", "roles")
	if err != nil {
		return nil, false, domain.WrapError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"read InstanceSet role definitions",
			err,
		)
	}

	if !found || len(roles) == 0 {
		return nil, false, nil
	}

	result := make(map[string]bool, len(roles))

	hasLeader := false
	for index, value := range roles {
		role, ok := value.(map[string]any)
		if !ok {
			return nil, false, domain.NewError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("InstanceSet spec.roles[%d] is malformed", index),
			)
		}

		name, _, nameErr := unstructured.NestedString(role, "name")

		leader, _, leaderErr := unstructured.NestedBool(role, "isLeader")
		if nameErr != nil || leaderErr != nil || name == "" {
			return nil, false, domain.NewError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("InstanceSet spec.roles[%d] has invalid role identity", index),
			)
		}

		result[strings.ToLower(name)] = leader
		hasLeader = hasLeader || leader
	}

	return result, hasLeader, nil
}

func kubeBlocksMemberRole(instanceSet *unstructured.Unstructured, podName string) (string, error) {
	members, found, err := unstructured.NestedSlice(instanceSet.Object, "status", "membersStatus")
	if err != nil {
		return "", domain.WrapError(
			domain.ErrorPrecondition,
			"discover KubeBlocks",
			"read InstanceSet member roles",
			err,
		)
	}

	if !found {
		return "", nil
	}

	for index, value := range members {
		member, ok := value.(map[string]any)
		if !ok {
			return "", domain.NewError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("InstanceSet status.membersStatus[%d] is malformed", index),
			)
		}

		name, _, nameErr := unstructured.NestedString(member, "podName")
		if nameErr != nil {
			return "", domain.WrapError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("read InstanceSet member %d Pod name", index),
				nameErr,
			)
		}

		if name != podName {
			continue
		}

		role, _, roleErr := unstructured.NestedString(member, "role", "name")
		if roleErr != nil {
			return "", domain.WrapError(
				domain.ErrorPrecondition,
				"discover KubeBlocks",
				fmt.Sprintf("read InstanceSet member %s role", podName),
				roleErr,
			)
		}

		return strings.ToLower(role), nil
	}

	return "", nil
}

func kubeBlocksComponent(pod *corev1.Pod) string {
	component := pod.Labels[kubeBlocksComponentLabel]
	if component == "" {
		component = pod.Labels[kube.AppComponentLabel]
	}

	return component
}

func (m *Manager) kubeBlocksLeaderGuidance(
	ctx context.Context,
	selected *corev1.Pod,
	cluster, component, role, opsAPIVersion string,
) string {
	candidate := m.readyKubeBlocksCandidate(ctx, selected, cluster, component)
	if candidate == "" {
		candidate = "REPLACE_WITH_READY_SECONDARY_POD"
	}

	if candidate != "REPLACE_WITH_READY_SECONDARY_POD" {
		if err := m.validateKubeBlocksSwitchover(
			ctx,
			selected.Namespace,
			cluster,
			component,
			selected.Name,
			candidate,
			opsAPIVersion,
		); err != nil {
			if isKubeBlocksMongoDB(selected) && kubeBlocksSwitchoverUnsupported(err) {
				return fmt.Sprintf(
					"selected instance %s has role %s; the served OpsRequest API has no MongoDB switchover handler. Use --kubeblocks-candidate %s and pvc-migrate will validate and run the MongoDB native candidate switchover script. Manual MongoDB switchover: %s. The candidate must remain Ready and caught up; --allow-leader-downtime acknowledges a leader outage",
					selected.Name,
					role,
					candidate,
					kubeBlocksMongoDBNativeSwitchoverCommand(
						selected.Namespace,
						cluster,
						component,
						selected.Name,
						candidate,
					),
				)
			}

			return fmt.Sprintf(
				"selected instance %s has role %s; the served OpsRequest API rejected automatic switchover to %s: %v. Use the component's native switchover procedure, or use --allow-leader-downtime to acknowledge the leader outage",
				selected.Name,
				role,
				candidate,
				err,
			)
		}
	}

	return fmt.Sprintf(
		"selected instance %s has role %s; use --kubeblocks-candidate %s for an automatic switchover, or complete a native switchover first and rerun the plan. Use --allow-leader-downtime to acknowledge the leader outage. KubeBlocks commands: kbcli cluster promote %s --namespace %s --instance %s --candidate %s; or %s",
		selected.Name,
		role,
		candidate,
		cluster,
		selected.Namespace,
		selected.Name,
		candidate,
		kubeBlocksSwitchoverCommand(
			selected.Namespace,
			cluster,
			component,
			selected.Name,
			candidate,
			opsAPIVersion,
		),
	)
}

func (m *Manager) readyKubeBlocksCandidate(
	ctx context.Context,
	selected *corev1.Pod,
	cluster, component string,
) string {
	pods, err := m.typed.CoreV1().Pods(selected.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil || pods == nil {
		return ""
	}

	candidates := make([]string, 0, len(pods.Items))

	selectedOwner := controllerOwner(selected.OwnerReferences)
	for index := range pods.Items {
		candidate := &pods.Items[index]
		if candidate.Name == selected.Name || candidate.Labels[kube.AppInstanceLabel] != cluster ||
			kubeBlocksComponent(candidate) != component ||
			!podReady(candidate) ||
			isLeaderRole(podRole(candidate)) ||
			!sameControllerOwner(controllerOwner(candidate.OwnerReferences), selectedOwner) {
			continue
		}

		candidates = append(candidates, candidate.Name)
	}

	sort.Strings(candidates)

	if len(candidates) == 0 {
		return ""
	}

	return candidates[0]
}

func (m *Manager) kubeBlocksCandidateSuggestion(
	ctx context.Context,
	selected *corev1.Pod,
	cluster, component string,
) string {
	candidate := m.readyKubeBlocksCandidate(ctx, selected, cluster, component)
	if candidate == "" {
		return ""
	}

	return "; available Ready non-leader candidate: --kubeblocks-candidate " + candidate
}

func kubeBlocksSwitchoverCommand(
	namespace, cluster, component, selected, candidate, opsAPIVersion string,
) string {
	var builder strings.Builder
	fmt.Fprintf(
		&builder,
		"kubectl create -f - <<'YAML'\napiVersion: %s\nkind: OpsRequest\nmetadata:\n  generateName: %s-switchover-\n  namespace: %s\nspec:\n  clusterName: %s\n  type: Switchover\n  switchover:\n  - componentName: %s\n",
		opsAPIVersion,
		cluster,
		namespace,
		cluster,
		component,
	)

	if strings.HasPrefix(opsAPIVersion, "operations.kubeblocks.io/") {
		fmt.Fprintf(&builder, "    instanceName: %s\n    candidateName: %s\n", selected, candidate)
	} else {
		fmt.Fprintf(&builder, "    instanceName: %s\n", candidate)
	}

	builder.WriteString("YAML")

	return builder.String()
}

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

func (m *Manager) validateKubeBlocksSwitchover(
	ctx context.Context,
	namespace, cluster, component, selected, candidate, opsAPIVersion string,
) error {
	if m.logger != nil {
		m.logger.Info(
			"checking KubeBlocks automatic switchover",
			"namespace",
			namespace,
			"cluster",
			cluster,
			"workload_component",
			component,
			"instance",
			selected,
			"candidate",
			candidate,
		)
	}

	gvr, err := opsGVR(opsAPIVersion)
	if err != nil {
		return err
	}

	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": opsAPIVersion,
		"kind":       "OpsRequest",
		"metadata": map[string]any{
			"generateName": "pvc-migrate-switchover-",
			"namespace":    namespace,
		},
		"spec": kubeBlocksSwitchoverSpec(opsAPIVersion, cluster, component, selected, candidate),
	}}
	_, err = m.dynamic.Resource(gvr).
		Namespace(namespace).
		Create(ctx, object, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})

	return err
}

func (m *Manager) kubeBlocksSwitchoverStrategy(
	ctx context.Context,
	selected *corev1.Pod,
	cluster, component, candidate, opsAPIVersion string,
) (domain.KubeBlocksSwitchoverStrategy, string, error) {
	err := m.validateKubeBlocksSwitchover(
		ctx,
		selected.Namespace,
		cluster,
		component,
		selected.Name,
		candidate,
		opsAPIVersion,
	)
	if err == nil {
		return domain.KubeBlocksSwitchoverOpsRequest, "", nil
	}

	if !isKubeBlocksMongoDB(selected) || !kubeBlocksSwitchoverUnsupported(err) {
		return "", "", fmt.Errorf(
			"the served OpsRequest API rejected the request: %w; use the component's native switchover procedure",
			err,
		)
	}

	container, nativeErr := m.preflightMongoDBNativeSwitchover(ctx, selected)
	if nativeErr != nil {
		return "", "", fmt.Errorf(
			"the served OpsRequest API has no MongoDB switchover handler: %w; native switchover preflight failed: %w; verify /scripts/switchover-with-candidate.sh is executable in the mongodb container, then rerun the plan",
			err,
			nativeErr,
		)
	}

	return domain.KubeBlocksSwitchoverMongoDBNative, container, nil
}

func isKubeBlocksMongoDB(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}

	for _, value := range []string{pod.Labels[kube.AppNameLabel], pod.Labels[kube.AppComponentLabel], pod.Labels[kubeBlocksComponentLabel]} {
		if strings.EqualFold(value, "mongodb") {
			return true
		}
	}

	return false
}

func kubeBlocksSwitchoverUnsupported(err error) bool {
	if err == nil {
		return false
	}

	message := strings.ToLower(err.Error())

	return strings.Contains(message, "does not support switchover") ||
		strings.Contains(message, "doesn't support switchover") ||
		strings.Contains(message, "not support switchover")
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

func kubeBlocksSwitchoverSpec(
	opsAPIVersion, cluster, component, selected, candidate string,
) map[string]any {
	switchover := map[string]any{"componentName": component, "instanceName": candidate}
	if strings.HasPrefix(opsAPIVersion, "operations.kubeblocks.io/") {
		switchover["instanceName"] = selected
		switchover["candidateName"] = candidate
	}

	return map[string]any{
		"clusterName": cluster,
		"type":        "Switchover",
		"switchover":  []any{switchover},
	}
}

func unsupportedKubeBlocksReason(
	pod *corev1.Pod,
	components []any,
	selectedComponent string,
) string {
	values := []string{
		selectedComponent,
		pod.Labels[kube.AppNameLabel],
		pod.Labels[kube.AppComponentLabel],
		pod.Labels[kubeBlocksComponentLabel],
	}
	for _, container := range append(append([]corev1.Container(nil), pod.Spec.InitContainers...), pod.Spec.Containers...) {
		values = append(values, container.Name, container.Image)
	}

	for index := range components {
		component, ok := components[index].(map[string]any)
		if !ok {
			continue
		}

		name, _, _ := unstructured.NestedString(component, "name")
		if name != selectedComponent {
			continue
		}

		definition, _, _ := unstructured.NestedString(component, "componentDefRef")
		values = append(values, name, definition)
	}

	for _, value := range values {
		lower := strings.ToLower(value)
		switch {
		case strings.Contains(lower, "minio"):
			return fmt.Sprintf(
				"KubeBlocks MinIO component %s requires MinIO's native drive or pool maintenance",
				selectedComponent,
			)
		case strings.Contains(lower, "cockroach"), strings.Contains(lower, "crdb"):
			return fmt.Sprintf(
				"KubeBlocks CockroachDB component %s requires CockroachDB drain and decommission",
				selectedComponent,
			)
		case strings.Contains(lower, "archive-wal"),
			lower == "wal",
			strings.Contains(lower, "wal-tool"):
			return fmt.Sprintf(
				"KubeBlocks archive-WAL component %s is a backup workload and cannot be migrated",
				selectedComponent,
			)
		}
	}

	return ""
}

func (m *Manager) vmClusterWorkload(
	ctx context.Context,
	pod *corev1.Pod,
	parent *metav1.OwnerReference,
	sts *appsv1.StatefulSet,
	options DiscoverOptions,
) (domain.WorkloadSpec, error) {
	if sts == nil {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorInternal,
			"discover VMCluster",
			"StatefulSet is required",
		)
	}

	base, err := m.statefulSetWorkload(ctx, pod, sts, options)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}

	component := ""
	for _, candidate := range []string{"vmstorage", "vmselect", "vminsert"} {
		if strings.Contains(strings.ToLower(sts.Name), candidate) {
			component = candidate
			break
		}
	}

	if component == "" {
		component = sts.Labels[kube.AppComponentLabel]
	}

	if component != "vmstorage" && component != "vmselect" && component != "vminsert" {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover VMCluster",
			fmt.Sprintf(
				"StatefulSet %s/%s has no supported VMCluster component",
				pod.Namespace,
				sts.Name,
			),
		)
	}

	originalPaused := false
	originalPausedConfigured := false
	originalClusterPaused := false
	originalClusterPausedConfigured := false
	originalReplicas := *base.OriginalReplicas
	originalReplicasConfigured := false

	var vmUID types.UID
	if m.dynamic != nil {
		gvr, parseErr := kube.ParseGroupVersionResource(vmClusterAPIVersion, vmClusterResource)
		if parseErr != nil {
			return domain.WorkloadSpec{}, parseErr
		}

		vm, getErr := m.dynamic.Resource(gvr).
			Namespace(pod.Namespace).
			Get(ctx, parent.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WorkloadSpec{}, domain.WrapError(
				domain.ErrorKubernetes,
				"discover VMCluster",
				"read VMCluster",
				getErr,
			)
		}

		if vm.GetUID() == "" || vm.GetUID() != parent.UID {
			return domain.WorkloadSpec{}, domain.NewError(
				domain.ErrorConflict,
				"discover VMCluster",
				fmt.Sprintf(
					"StatefulSet %s/%s VMCluster owner UID changed",
					sts.Namespace,
					sts.Name,
				),
			)
		}

		vmUID = vm.GetUID()

		componentObject, found, nestedErr := unstructured.NestedMap(vm.Object, "spec", component)
		if nestedErr != nil {
			return domain.WorkloadSpec{}, domain.WrapError(
				domain.ErrorPrecondition,
				"discover VMCluster",
				"read component state",
				nestedErr,
			)
		}

		if !found {
			return domain.WorkloadSpec{}, domain.NewError(
				domain.ErrorPrecondition,
				"discover VMCluster",
				fmt.Sprintf("VMCluster component %s is absent", component),
			)
		}

		originalPaused, originalPausedConfigured, nestedErr = unstructured.NestedBool(
			componentObject,
			"paused",
		)
		if nestedErr != nil {
			return domain.WorkloadSpec{}, domain.WrapError(
				domain.ErrorPrecondition,
				"discover VMCluster",
				"read component pause state",
				nestedErr,
			)
		}

		configuredReplicas, replicasFound, replicasErr := unstructured.NestedInt64(
			componentObject,
			"replicaCount",
		)
		if replicasErr != nil {
			return domain.WorkloadSpec{}, domain.WrapError(
				domain.ErrorPrecondition,
				"discover VMCluster",
				"read component replica count",
				replicasErr,
			)
		}

		if replicasFound {
			if configuredReplicas <= 0 || configuredReplicas > math.MaxInt32 {
				return domain.WorkloadSpec{}, domain.NewError(
					domain.ErrorPrecondition,
					"discover VMCluster",
					fmt.Sprintf(
						"VMCluster component %s has invalid replicaCount %d",
						component,
						configuredReplicas,
					),
				)
			}

			if configuredReplicas != int64(*base.OriginalReplicas) {
				return domain.WorkloadSpec{}, domain.NewError(
					domain.ErrorPrecondition,
					"discover VMCluster",
					fmt.Sprintf(
						"VMCluster component %s replicaCount %d has not converged to StatefulSet replicas %d",
						component,
						configuredReplicas,
						*base.OriginalReplicas,
					),
				)
			}

			originalReplicas = int32(configuredReplicas)
			originalReplicasConfigured = true
		}

		originalClusterPaused, originalClusterPausedConfigured, nestedErr = unstructured.NestedBool(
			vm.Object,
			"spec",
			"paused",
		)
		if nestedErr != nil {
			return domain.WorkloadSpec{}, domain.WrapError(
				domain.ErrorPrecondition,
				"discover VMCluster",
				"read top-level pause state",
				nestedErr,
			)
		}
	}

	return domain.WorkloadSpec{
		Adapter:          domain.WorkloadVMCluster,
		Pod:              base.Pod,
		Controller:       base.Controller,
		OriginalReplicas: base.OriginalReplicas,
		Ordinal:          base.Ordinal,
		AffectedPods:     base.AffectedPods,
		VMCluster: &domain.VMClusterSpec{
			APIVersion:                      vmClusterAPIVersion,
			Name:                            parent.Name,
			UID:                             vmUID,
			Component:                       component,
			OriginalPaused:                  originalPaused,
			OriginalPausedConfigured:        originalPausedConfigured,
			OriginalClusterPaused:           originalClusterPaused,
			OriginalClusterPausedConfigured: originalClusterPausedConfigured,
			OriginalReplicas:                originalReplicas,
			OriginalReplicasConfigured:      originalReplicasConfigured,
		},
	}, nil
}

func (m *Manager) grafanaWorkload(
	ctx context.Context,
	pod *corev1.Pod,
	deployment *appsv1.Deployment,
	owner *metav1.OwnerReference,
) (domain.WorkloadSpec, error) {
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas <= 0 {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover Grafana",
			fmt.Sprintf(
				"Deployment %s/%s has no positive replica count",
				deployment.Namespace,
				deployment.Name,
			),
		)
	}

	if m.dynamic == nil {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorPrecondition,
			"discover Grafana",
			"dynamic client is required for Grafana pause control",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(grafanaAPIVersion, grafanaResource)
	if err != nil {
		return domain.WorkloadSpec{}, err
	}

	grafana, err := m.dynamic.Resource(gvr).
		Namespace(pod.Namespace).
		Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WorkloadSpec{}, domain.WrapError(
			domain.ErrorKubernetes,
			"discover Grafana",
			"read Grafana",
			err,
		)
	}

	if grafana.GetUID() == "" || grafana.GetUID() != owner.UID {
		return domain.WorkloadSpec{}, domain.NewError(
			domain.ErrorConflict,
			"discover Grafana",
			fmt.Sprintf(
				"Deployment %s/%s Grafana owner UID changed",
				deployment.Namespace,
				deployment.Name,
			),
		)
	}

	suspended, suspendConfigured, nestedErr := unstructured.NestedBool(
		grafana.Object,
		"spec",
		"suspend",
	)
	if nestedErr != nil {
		return domain.WorkloadSpec{}, domain.WrapError(
			domain.ErrorPrecondition,
			"discover Grafana",
			"read reconciliation suspend state",
			nestedErr,
		)
	}

	return domain.WorkloadSpec{
		Adapter: domain.WorkloadGrafana,
		Pod:     podReference(pod),
		Controller: objectReference(
			domain.AppsAPIVersion,
			domain.KindDeployment,
			deployment.Namespace,
			deployment.Name,
			deployment.UID,
			deployment.ResourceVersion,
		),
		OriginalReplicas: deployment.Spec.Replicas,
		AffectedPods:     []domain.ObjectReference{podReference(pod)},
		Grafana: &domain.GrafanaSpec{
			APIVersion:                grafanaAPIVersion,
			Name:                      owner.Name,
			UID:                       grafana.GetUID(),
			OriginalSuspend:           suspended,
			OriginalSuspendConfigured: suspendConfigured,
			OriginalReplicas:          *deployment.Spec.Replicas,
		},
	}, nil
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
	if pod.Status.Phase != corev1.PodRunning || !podReady(pod) {
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

			if !podReady(pod) {
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

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
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

func opsGVR(apiVersion string) (schema.GroupVersionResource, error) {
	return kube.ParseGroupVersionResource(apiVersion, "opsrequests")
}

func (m *Manager) createAndWaitOps(
	ctx context.Context,
	session *domain.Session,
	action string,
	spec map[string]any,
) error {
	kb := session.Spec.Workload().KubeBlocks

	gvr, err := opsGVR(kb.OpsAPIVersion)
	if err != nil {
		return err
	}

	name := operationName(session.ID, action)
	resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)
	existing, getErr := resource.Get(ctx, name, metav1.GetOptions{})
	create := apierrors.IsNotFound(getErr)

	var expectedUID types.UID
	if getErr == nil {
		labels := existing.GetLabels()
		if labels[kube.ManagedByLabel] != kube.ManagedByValue ||
			labels[kube.SessionKey] != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"KubeBlocks operation",
				fmt.Sprintf("OpsRequest %s belongs to another operation", name),
			)
		}

		expectedUID = existing.GetUID()
		if expectedUID == "" {
			return domain.NewError(
				domain.ErrorKubernetes,
				"KubeBlocks operation",
				fmt.Sprintf("OpsRequest %s has an incomplete identity", name),
			)
		}

		phase, _, _ := unstructured.NestedString(existing.Object, "status", "phase")
		if phase == "Failed" || phase == "Cancelled" || phase == "Aborted" {
			uid := existing.GetUID()
			if err := resource.Delete(
				ctx,
				name,
				metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}},
			); err != nil &&
				!apierrors.IsNotFound(err) {
				return domain.WrapError(
					domain.ErrorKubernetes,
					"KubeBlocks operation",
					"delete failed OpsRequest "+name,
					err,
				)
			}

			if err := m.waitFor(
				ctx,
				fmt.Sprintf("failed OpsRequest %s deletion", name),
				func(waitCtx context.Context) (bool, error) {
					current, err := resource.Get(waitCtx, name, metav1.GetOptions{})
					if apierrors.IsNotFound(err) {
						return true, nil
					}

					if err == nil && current.GetUID() != uid {
						return false, domain.NewError(
							domain.ErrorConflict,
							"KubeBlocks operation",
							fmt.Sprintf(
								"OpsRequest %s was replaced while waiting for deletion",
								name,
							),
						)
					}

					return false, err
				},
			); err != nil {
				return err
			}

			create = true
			expectedUID = ""
		}
	}

	if create {
		object := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": kb.OpsAPIVersion,
			"kind":       "OpsRequest",
			"metadata": map[string]any{
				"name":      name,
				"namespace": session.Spec.Workload().Pod.Namespace,
				"labels": map[string]any{
					kube.ManagedByLabel: kube.ManagedByValue,
					kube.SessionKey:     session.ID,
				},
			},
			"spec": spec,
		}}
		created, createErr := resource.Create(ctx, object, metav1.CreateOptions{})

		err = createErr
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"KubeBlocks operation",
				"create OpsRequest "+name,
				err,
			)
		}

		if created == nil || created.GetName() == "" || created.GetUID() == "" {
			return domain.NewError(
				domain.ErrorKubernetes,
				"KubeBlocks operation",
				fmt.Sprintf("create OpsRequest %s returned an empty object", name),
			)
		}

		expectedUID = created.GetUID()
	} else if getErr != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"KubeBlocks operation",
			"read OpsRequest "+name,
			getErr,
		)
	}

	return m.waitFor(
		ctx,
		"KubeBlocks OpsRequest "+name,
		func(waitCtx context.Context) (bool, error) {
			current, readErr := resource.Get(waitCtx, name, metav1.GetOptions{})
			if readErr != nil {
				return false, domain.WrapError(
					domain.ErrorKubernetes,
					"KubeBlocks operation",
					"read OpsRequest status",
					readErr,
				)
			}

			labels := current.GetLabels()
			if labels[kube.ManagedByLabel] != kube.ManagedByValue ||
				labels[kube.SessionKey] != session.ID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"KubeBlocks operation",
					fmt.Sprintf("OpsRequest %s ownership changed while waiting", name),
				)
			}

			if current.GetUID() != expectedUID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"KubeBlocks operation",
					fmt.Sprintf("OpsRequest %s was replaced while waiting", name),
				)
			}

			phase, _, _ := unstructured.NestedString(current.Object, "status", "phase")
			switch phase {
			case "Succeed":
				return true, nil
			case "Failed", "Cancelled", "Aborted":
				return false, domain.NewError(
					domain.ErrorPrecondition,
					"KubeBlocks operation",
					fmt.Sprintf("OpsRequest %s ended in phase %s", name, phase),
				)
			default:
				return false, nil
			}
		},
	)
}

func operationName(sessionID, action string) string {
	return kube.BoundedName("pvc-migrate", sessionID, action)
}

func (m *Manager) patchStatefulSetReplicas(
	ctx context.Context,
	ref domain.ObjectReference,
	replicas int32,
	allowedCurrent ...int32,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sts, err := m.typed.AppsV1().
			StatefulSets(ref.Namespace).
			Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if sts.UID != ref.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"scale StatefulSet",
				fmt.Sprintf("StatefulSet %s/%s UID changed", ref.Namespace, ref.Name),
			)
		}

		current := int32(1)
		if sts.Spec.Replicas != nil {
			current = *sts.Spec.Replicas
		}

		if current == replicas {
			return nil
		}

		allowed := slices.Contains(allowedCurrent, current)

		if !allowed {
			return domain.NewError(
				domain.ErrorConflict,
				"scale StatefulSet",
				fmt.Sprintf(
					"StatefulSet %s/%s replicas changed to %d",
					ref.Namespace,
					ref.Name,
					current,
				),
			)
		}

		sts.Spec.Replicas = &replicas
		_, err = m.typed.AppsV1().
			StatefulSets(ref.Namespace).
			Update(ctx, sts, metav1.UpdateOptions{})

		return err
	})
}

func (m *Manager) pauseStatefulSet(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.Ordinal == nil || workload.OriginalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause StatefulSet",
			"session lacks replica state",
		)
	}

	if err := m.patchStatefulSetReplicas(
		ctx,
		workload.Controller,
		*workload.Ordinal,
		*workload.OriginalReplicas,
	); err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}
		return domain.WrapError(domain.ErrorKubernetes, "pause StatefulSet", "scale down", err)
	}

	for _, pod := range workload.AffectedPods {
		if err := m.waitForPodDeletion(ctx, pod, "pause StatefulSet"); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) pauseVictoriaLogs(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.Controller.Kind != domain.KindStatefulSet || workload.OriginalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause Victoria Logs",
			"session lacks StatefulSet replica state",
		)
	}

	if err := m.patchVictoriaLogsReplicas(ctx, session, 0, false); err != nil {
		return err
	}

	for _, pod := range workload.AffectedPods {
		if err := m.waitForPodDeletion(ctx, pod, "pause Victoria Logs"); err != nil {
			return err
		}
	}

	return m.VerifyPaused(ctx, session)
}

func (m *Manager) resumeStatefulSet(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.OriginalReplicas == nil || workload.Ordinal == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume StatefulSet",
			"session lacks replica state",
		)
	}

	if err := m.patchStatefulSetReplicas(
		ctx,
		workload.Controller,
		*workload.OriginalReplicas,
		*workload.Ordinal,
	); err != nil {
		if domain.CategoryOf(err) == domain.ErrorConflict {
			return err
		}

		return domain.WrapError(
			domain.ErrorKubernetes,
			"resume StatefulSet",
			"restore replicas",
			err,
		)
	}

	for _, ref := range workload.AffectedPods {
		if err := m.waitForResumedPod(
			ctx,
			session,
			ref,
			workload.Controller,
			"resume StatefulSet",
		); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) resumeVictoriaLogs(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	if workload.Controller.Kind != domain.KindStatefulSet || workload.OriginalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume Victoria Logs",
			"session lacks StatefulSet replica state",
		)
	}

	if err := m.patchVictoriaLogsReplicas(
		ctx,
		session,
		*workload.OriginalReplicas,
		true,
	); err != nil {
		return err
	}

	for _, ref := range workload.AffectedPods {
		if err := m.waitForResumedPod(
			ctx,
			session,
			ref,
			workload.Controller,
			"resume Victoria Logs",
		); err != nil {
			return err
		}
	}

	return m.clearVictoriaLogsPauseOwner(ctx, session)
}

func (m *Manager) patchVictoriaLogsReplicas(
	ctx context.Context,
	session *domain.Session,
	replicas int32,
	resuming bool,
) error {
	ref := session.Spec.Workload().Controller

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sts, err := m.typed.AppsV1().
			StatefulSets(ref.Namespace).
			Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"Victoria Logs pause",
				"read StatefulSet",
				err,
			)
		}

		if sts.UID != ref.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"Victoria Logs pause",
				fmt.Sprintf("StatefulSet %s/%s UID changed", ref.Namespace, ref.Name),
			)
		}

		annotations := sts.GetAnnotations()

		owner := annotations[pauseSessionAnnotation]
		if owner != "" && owner != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"Victoria Logs pause",
				fmt.Sprintf(
					"StatefulSet %s/%s pause is owned by session %s",
					ref.Namespace,
					ref.Name,
					owner,
				),
			)
		}

		current := statefulSetReplicas(sts)
		if resuming {
			if owner != session.ID {
				return domain.NewError(
					domain.ErrorConflict,
					"Victoria Logs resume",
					fmt.Sprintf(
						"StatefulSet %s/%s is not owned by session %s",
						ref.Namespace,
						ref.Name,
						session.ID,
					),
				)
			}

			if current != 0 && current != replicas {
				return domain.NewError(
					domain.ErrorConflict,
					"Victoria Logs resume",
					fmt.Sprintf(
						"StatefulSet %s/%s replicas changed to %d",
						ref.Namespace,
						ref.Name,
						current,
					),
				)
			}
		} else {
			if owner == session.ID && current == replicas {
				return nil
			}

			if owner == "" && current != *session.Spec.Workload().OriginalReplicas {
				return domain.NewError(
					domain.ErrorConflict,
					"Victoria Logs pause",
					fmt.Sprintf(
						"StatefulSet %s/%s replicas changed to %d",
						ref.Namespace,
						ref.Name,
						current,
					),
				)
			}
		}

		changed := current != replicas
		if changed {
			sts.Spec.Replicas = &replicas
		}

		if !resuming {
			if annotations == nil {
				annotations = map[string]string{}
			}

			if annotations[pauseSessionAnnotation] != session.ID {
				annotations[pauseSessionAnnotation] = session.ID
				changed = true
			}
		}

		if !changed {
			return nil
		}

		sts.SetAnnotations(annotations)
		_, err = m.typed.AppsV1().
			StatefulSets(ref.Namespace).
			Update(ctx, sts, metav1.UpdateOptions{})

		return err
	})
}

func (m *Manager) clearVictoriaLogsPauseOwner(ctx context.Context, session *domain.Session) error {
	ref := session.Spec.Workload().Controller

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sts, err := m.typed.AppsV1().
			StatefulSets(ref.Namespace).
			Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"Victoria Logs resume",
				"read StatefulSet",
				err,
			)
		}

		if sts.UID != ref.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"Victoria Logs resume",
				fmt.Sprintf("StatefulSet %s/%s UID changed", ref.Namespace, ref.Name),
			)
		}

		annotations := sts.GetAnnotations()
		if annotations[pauseSessionAnnotation] != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"Victoria Logs resume",
				fmt.Sprintf("StatefulSet %s/%s pause ownership changed", ref.Namespace, ref.Name),
			)
		}

		if session.Spec.Workload().OriginalReplicas == nil ||
			statefulSetReplicas(sts) != *session.Spec.Workload().OriginalReplicas {
			return domain.NewError(
				domain.ErrorConflict,
				"Victoria Logs resume",
				fmt.Sprintf(
					"StatefulSet %s/%s replicas changed while pause ownership was active",
					ref.Namespace,
					ref.Name,
				),
			)
		}

		delete(annotations, pauseSessionAnnotation)
		sts.SetAnnotations(annotations)
		_, err = m.typed.AppsV1().
			StatefulSets(ref.Namespace).
			Update(ctx, sts, metav1.UpdateOptions{})

		return err
	})
}

func (m *Manager) pauseVMCluster(ctx context.Context, session *domain.Session) error {
	vm := session.Spec.Workload().VMCluster
	if vm == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause VMCluster",
			"session lacks VMCluster state",
		)
	}

	workload := session.Spec.Workload()
	if workload.Ordinal == nil || workload.OriginalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause VMCluster",
			"session lacks StatefulSet replica state",
		)
	}

	if err := m.setVMClusterPaused(ctx, session); err != nil {
		return err
	}
	// Keep lower ordinals available while preventing the operator from
	// restoring the StatefulSet to its original replica count.
	if err := m.setVMClusterReplicaCount(
		ctx,
		session,
		*workload.Ordinal,
		vm.OriginalReplicas,
	); err != nil {
		if restoreErr := m.restoreVMClusterPause(ctx, session); restoreErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"pause VMCluster",
				fmt.Sprintf("set component replicas: %v; restore component pause state", err),
				restoreErr,
			)
		}

		return err
	}

	if err := m.patchStatefulSetReplicas(
		ctx,
		workload.Controller,
		*workload.Ordinal,
		*workload.OriginalReplicas,
	); err != nil {
		if restoreErr := m.restoreVMClusterPause(ctx, session); restoreErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"pause VMCluster",
				fmt.Sprintf("scale component StatefulSet: %v; restore component pause state", err),
				restoreErr,
			)
		}

		return workloadScaleError("pause VMCluster", "scale component StatefulSet", err)
	}

	for _, pod := range workload.AffectedPods {
		if err := m.waitForPodDeletion(ctx, pod, "pause VMCluster"); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) resumeVMCluster(ctx context.Context, session *domain.Session) error {
	vm := session.Spec.Workload().VMCluster
	if vm == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume VMCluster",
			"session lacks VMCluster state",
		)
	}

	workload := session.Spec.Workload()
	if workload.OriginalReplicas == nil || workload.Ordinal == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume VMCluster",
			"session lacks StatefulSet replica state",
		)
	}

	if err := m.setVMClusterReplicaCount(
		ctx,
		session,
		vm.OriginalReplicas,
		*workload.Ordinal,
	); err != nil {
		return err
	}

	if err := m.patchStatefulSetReplicas(
		ctx,
		workload.Controller,
		*workload.OriginalReplicas,
		*workload.Ordinal,
	); err != nil {
		return workloadScaleError("resume VMCluster", "restore component StatefulSet", err)
	}

	for _, ref := range workload.AffectedPods {
		if err := m.waitForResumedPod(
			ctx,
			session,
			ref,
			workload.Controller,
			"resume VMCluster",
		); err != nil {
			return err
		}
	}

	if err := m.restoreVMClusterPause(ctx, session); err != nil {
		return err
	}

	return m.waitForVMClusterOperational(ctx, session)
}

func (m *Manager) waitForVMClusterOperational(ctx context.Context, session *domain.Session) error {
	vm := session.Spec.Workload().VMCluster
	if vm == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"wait for VMCluster",
			"session lacks VMCluster state",
		)
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"wait for VMCluster",
			"dynamic client is required for convergence checks",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(vm.APIVersion, vmClusterResource)
	if err != nil {
		return err
	}

	resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)

	return m.waitFor(
		ctx,
		fmt.Sprintf("VMCluster %s/%s convergence", session.Spec.Workload().Pod.Namespace, vm.Name),
		func(waitCtx context.Context) (bool, error) {
			object, getErr := resource.Get(waitCtx, vm.Name, metav1.GetOptions{})
			if getErr != nil {
				return false, domain.WrapError(
					domain.ErrorKubernetes,
					"wait for VMCluster",
					"read VMCluster",
					getErr,
				)
			}

			if object.GetUID() != vm.UID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"wait for VMCluster",
					fmt.Sprintf(
						"VMCluster %s/%s UID changed",
						object.GetNamespace(),
						object.GetName(),
					),
				)
			}

			observedGeneration, found, nestedErr := unstructured.NestedInt64(
				object.Object,
				"status",
				"observedGeneration",
			)
			if nestedErr != nil {
				return false, domain.WrapError(
					domain.ErrorPrecondition,
					"wait for VMCluster",
					"read observed generation",
					nestedErr,
				)
			}

			if !found || observedGeneration < object.GetGeneration() {
				return false, nil
			}

			currentClusterPaused, clusterPausedFound, nestedErr := unstructured.NestedBool(
				object.Object,
				"spec",
				"paused",
			)
			if nestedErr != nil {
				return false, domain.WrapError(
					domain.ErrorPrecondition,
					"wait for VMCluster",
					"read top-level pause state",
					nestedErr,
				)
			}

			if vm.OriginalClusterPausedConfigured {
				if !clusterPausedFound || currentClusterPaused != vm.OriginalClusterPaused {
					return false, domain.NewError(
						domain.ErrorConflict,
						"wait for VMCluster",
						fmt.Sprintf(
							"VMCluster %s/%s top-level paused changed from expected %t to %t",
							object.GetNamespace(),
							object.GetName(),
							vm.OriginalClusterPaused,
							currentClusterPaused,
						),
					)
				}
			} else if clusterPausedFound && currentClusterPaused {
				return false, domain.NewError(
					domain.ErrorConflict,
					"wait for VMCluster",
					fmt.Sprintf(
						"VMCluster %s/%s was paused externally during migration",
						object.GetNamespace(),
						object.GetName(),
					),
				)
			}

			clusterStatus, _, nestedErr := unstructured.NestedString(
				object.Object,
				"status",
				"clusterStatus",
			)
			if nestedErr != nil {
				return false, domain.WrapError(
					domain.ErrorPrecondition,
					"wait for VMCluster",
					"read cluster status",
					nestedErr,
				)
			}

			updateStatus, _, nestedErr := unstructured.NestedString(
				object.Object,
				"status",
				"updateStatus",
			)
			if nestedErr != nil {
				return false, domain.WrapError(
					domain.ErrorPrecondition,
					"wait for VMCluster",
					"read update status",
					nestedErr,
				)
			}

			if vm.OriginalClusterPausedConfigured && vm.OriginalClusterPaused {
				// A top-level paused VMCluster intentionally remains outside the
				// operator's operational state machine. The observed generation still
				// fences us against an object replacement or a stale read.
				return true, nil
			}

			return strings.EqualFold(clusterStatus, "operational") &&
				strings.EqualFold(updateStatus, "operational"), nil
		},
	)
}

func (m *Manager) restoreVMClusterPause(ctx context.Context, session *domain.Session) error {
	vm := session.Spec.Workload().VMCluster
	if vm == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"restore VMCluster pause",
			"session lacks VMCluster state",
		)
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"restore VMCluster pause",
			"dynamic client is required for component pause control",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(vm.APIVersion, vmClusterResource)
	if err != nil {
		return err
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)

		object, getErr := resource.Get(ctx, vm.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"restore VMCluster pause",
				"read VMCluster",
				getErr,
			)
		}

		if object.GetUID() != vm.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"restore VMCluster pause",
				fmt.Sprintf("VMCluster %s/%s UID changed", object.GetNamespace(), object.GetName()),
			)
		}

		componentObject, ok, nestedErr := unstructured.NestedMap(
			object.Object,
			"spec",
			vm.Component,
		)
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"restore VMCluster pause",
				"read component pause state",
				nestedErr,
			)
		}

		if !ok {
			return domain.NewError(
				domain.ErrorPrecondition,
				"restore VMCluster pause",
				fmt.Sprintf("VMCluster component %s is absent", vm.Component),
			)
		}

		current, _, nestedErr := unstructured.NestedBool(componentObject, "paused")
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"restore VMCluster pause",
				"read component pause state",
				nestedErr,
			)
		}

		annotations := object.GetAnnotations()

		pauseOwner := annotations[pauseSessionAnnotation]

		currentReplicas, replicasFound, nestedErr := unstructured.NestedInt64(
			componentObject,
			"replicaCount",
		)
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"restore VMCluster pause",
				"read component replica count",
				nestedErr,
			)
		}

		restoreRequired, validateErr := validateVMClusterPauseRestoreState(
			session,
			vm,
			object,
			pauseOwner,
			current,
			currentReplicas,
			replicasFound,
		)
		if validateErr != nil || !restoreRequired {
			return validateErr
		}

		if current != vm.OriginalPaused {
			if err := unstructured.SetNestedField(
				componentObject,
				vm.OriginalPaused,
				"paused",
			); err != nil {
				return err
			}

			if err := unstructured.SetNestedField(
				object.Object,
				componentObject,
				"spec",
				vm.Component,
			); err != nil {
				return err
			}
		}

		if vm.OriginalReplicasConfigured && replicasFound &&
			session.Spec.Workload().Ordinal != nil &&
			currentReplicas == int64(*session.Spec.Workload().Ordinal) {
			if err := unstructured.SetNestedField(
				componentObject,
				int64(vm.OriginalReplicas),
				"replicaCount",
			); err != nil {
				return err
			}

			if err := unstructured.SetNestedField(
				object.Object,
				componentObject,
				"spec",
				vm.Component,
			); err != nil {
				return err
			}
		}

		delete(annotations, pauseSessionAnnotation)
		object.SetAnnotations(annotations)

		if _, updateErr := resource.Update(ctx, object, metav1.UpdateOptions{}); updateErr != nil {
			if apierrors.IsConflict(updateErr) {
				return updateErr
			}

			return domain.WrapError(
				domain.ErrorKubernetes,
				"restore VMCluster pause",
				"clear component pause owner",
				updateErr,
			)
		}

		return nil
	})
}

func validateVMClusterPauseRestoreState(
	session *domain.Session,
	vm *domain.VMClusterSpec,
	object *unstructured.Unstructured,
	pauseOwner string,
	current bool,
	currentReplicas int64,
	replicasFound bool,
) (bool, error) {
	if pauseOwner != "" && pauseOwner != session.ID {
		return false, domain.NewError(
			domain.ErrorConflict,
			"restore VMCluster pause",
			fmt.Sprintf(
				"VMCluster %s/%s pause is owned by session %s",
				object.GetNamespace(),
				object.GetName(),
				pauseOwner,
			),
		)
	}

	if pauseOwner == "" {
		if current != vm.OriginalPaused {
			return false, domain.NewError(
				domain.ErrorConflict,
				"restore VMCluster pause",
				fmt.Sprintf(
					"VMCluster component %s paused changed from expected %t to %t",
					vm.Component,
					vm.OriginalPaused,
					current,
				),
			)
		}

		if replicasFound != vm.OriginalReplicasConfigured ||
			(replicasFound && currentReplicas != int64(vm.OriginalReplicas)) {
			return false, domain.NewError(
				domain.ErrorConflict,
				"restore VMCluster pause",
				fmt.Sprintf(
					"VMCluster component %s replicaCount changed while pause ownership was absent",
					vm.Component,
				),
			)
		}

		return false, nil
	}

	if !current {
		return false, domain.NewError(
			domain.ErrorConflict,
			"restore VMCluster pause",
			fmt.Sprintf(
				"VMCluster component %s paused changed while session was active",
				vm.Component,
			),
		)
	}

	if !vm.OriginalReplicasConfigured && replicasFound {
		return false, domain.NewError(
			domain.ErrorConflict,
			"restore VMCluster pause",
			fmt.Sprintf(
				"VMCluster component %s replicaCount was added while session was active",
				vm.Component,
			),
		)
	}

	if !vm.OriginalReplicasConfigured {
		return true, nil
	}

	if !replicasFound {
		return false, domain.NewError(
			domain.ErrorConflict,
			"restore VMCluster pause",
			fmt.Sprintf(
				"VMCluster component %s replicaCount was removed while session was active",
				vm.Component,
			),
		)
	}

	if currentReplicas == int64(vm.OriginalReplicas) {
		return true, nil
	}

	workload := session.Spec.Workload()
	if workload.Ordinal == nil || currentReplicas != int64(*workload.Ordinal) {
		return false, domain.NewError(
			domain.ErrorConflict,
			"restore VMCluster pause",
			fmt.Sprintf(
				"VMCluster component %s replicaCount changed while session was active",
				vm.Component,
			),
		)
	}

	return true, nil
}

func (m *Manager) setVMClusterReplicaCount(
	ctx context.Context,
	session *domain.Session,
	replicas int32,
	allowedCurrent ...int32,
) error {
	vm := session.Spec.Workload().VMCluster
	if vm == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"set VMCluster replicas",
			"session lacks VMCluster state",
		)
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"set VMCluster replicas",
			"dynamic client is required for component replica control",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(vm.APIVersion, vmClusterResource)
	if err != nil {
		return err
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)

		object, getErr := resource.Get(ctx, vm.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"set VMCluster replicas",
				"read VMCluster",
				getErr,
			)
		}

		if object.GetUID() != vm.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"set VMCluster replicas",
				fmt.Sprintf("VMCluster %s/%s UID changed", object.GetNamespace(), object.GetName()),
			)
		}

		componentObject, ok, nestedErr := unstructured.NestedMap(
			object.Object,
			"spec",
			vm.Component,
		)
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"set VMCluster replicas",
				"read component replica count",
				nestedErr,
			)
		}

		if !ok {
			return domain.NewError(
				domain.ErrorPrecondition,
				"set VMCluster replicas",
				fmt.Sprintf("VMCluster component %s is absent", vm.Component),
			)
		}

		current, found, nestedErr := unstructured.NestedInt64(componentObject, "replicaCount")
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"set VMCluster replicas",
				"read component replica count",
				nestedErr,
			)
		}

		if found != vm.OriginalReplicasConfigured {
			return domain.NewError(
				domain.ErrorConflict,
				"set VMCluster replicas",
				fmt.Sprintf(
					"VMCluster component %s replicaCount representation changed",
					vm.Component,
				),
			)
		}

		if !found {
			return nil
		}

		annotations := object.GetAnnotations()
		if owner := annotations[pauseSessionAnnotation]; owner != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"set VMCluster replicas",
				fmt.Sprintf(
					"VMCluster %s/%s pause ownership changed",
					object.GetNamespace(),
					object.GetName(),
				),
			)
		}

		if current == int64(replicas) {
			return nil
		}

		allowed := false
		for _, candidate := range allowedCurrent {
			if current == int64(candidate) {
				allowed = true
				break
			}
		}

		if !allowed {
			return domain.NewError(
				domain.ErrorConflict,
				"set VMCluster replicas",
				fmt.Sprintf(
					"VMCluster component %s replicaCount changed to %d",
					vm.Component,
					current,
				),
			)
		}

		if err := unstructured.SetNestedField(
			componentObject,
			int64(replicas),
			"replicaCount",
		); err != nil {
			return err
		}

		if err := unstructured.SetNestedField(
			object.Object,
			componentObject,
			"spec",
			vm.Component,
		); err != nil {
			return err
		}

		if _, updateErr := resource.Update(ctx, object, metav1.UpdateOptions{}); updateErr != nil {
			if apierrors.IsConflict(updateErr) {
				return updateErr
			}

			return domain.WrapError(
				domain.ErrorKubernetes,
				"set VMCluster replicas",
				"update component replica count",
				updateErr,
			)
		}

		return nil
	})
}

func (m *Manager) setVMClusterPaused(
	ctx context.Context,
	session *domain.Session,
) error {
	vm := session.Spec.Workload().VMCluster

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"VMCluster pause",
			"dynamic client is required for component pause control",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(vm.APIVersion, vmClusterResource)
	if err != nil {
		return err
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)

		object, getErr := resource.Get(ctx, vm.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"VMCluster pause",
				"read VMCluster",
				getErr,
			)
		}

		if object.GetUID() != vm.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"VMCluster pause",
				fmt.Sprintf("VMCluster %s/%s UID changed", object.GetNamespace(), object.GetName()),
			)
		}

		componentObject, ok, nestedErr := unstructured.NestedMap(
			object.Object,
			"spec",
			vm.Component,
		)
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"VMCluster pause",
				"read component pause state",
				nestedErr,
			)
		}

		if !ok {
			return domain.NewError(
				domain.ErrorPrecondition,
				"VMCluster pause",
				fmt.Sprintf("VMCluster component %s is absent", vm.Component),
			)
		}

		current, _, nestedErr := unstructured.NestedBool(componentObject, "paused")
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"VMCluster pause",
				"read component pause state",
				nestedErr,
			)
		}

		annotations := object.GetAnnotations()

		pauseOwner := annotations[pauseSessionAnnotation]
		if pauseOwner != "" && pauseOwner != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"VMCluster pause",
				fmt.Sprintf(
					"VMCluster %s/%s pause is owned by session %s",
					object.GetNamespace(),
					object.GetName(),
					pauseOwner,
				),
			)
		}

		if pauseOwner == "" && current != vm.OriginalPaused {
			return domain.NewError(
				domain.ErrorConflict,
				"VMCluster pause",
				fmt.Sprintf(
					"VMCluster component %s paused changed from expected %t to %t",
					vm.Component,
					vm.OriginalPaused,
					current,
				),
			)
		}

		if pauseOwner == "" {
			currentReplicas, replicasFound, replicasErr := unstructured.NestedInt64(
				componentObject,
				"replicaCount",
			)
			if replicasErr != nil {
				return domain.WrapError(
					domain.ErrorPrecondition,
					"VMCluster pause",
					"read component replica count",
					replicasErr,
				)
			}

			if replicasFound != vm.OriginalReplicasConfigured ||
				(replicasFound && currentReplicas != int64(vm.OriginalReplicas)) {
				return domain.NewError(
					domain.ErrorConflict,
					"VMCluster pause",
					fmt.Sprintf(
						"VMCluster component %s replicaCount changed after discovery",
						vm.Component,
					),
				)
			}
		}

		if pauseOwner == session.ID && current {
			return nil
		}

		if pauseOwner == session.ID && !current {
			return domain.NewError(
				domain.ErrorConflict,
				"VMCluster pause",
				fmt.Sprintf(
					"VMCluster component %s paused changed while session was active",
					vm.Component,
				),
			)
		}

		if err := unstructured.SetNestedField(componentObject, true, "paused"); err != nil {
			return err
		}

		if err := unstructured.SetNestedField(
			object.Object,
			componentObject,
			"spec",
			vm.Component,
		); err != nil {
			return err
		}

		if annotations == nil {
			annotations = map[string]string{}
		}

		annotations[pauseSessionAnnotation] = session.ID

		object.SetAnnotations(annotations)

		_, updateErr := resource.Update(ctx, object, metav1.UpdateOptions{})
		if apierrors.IsConflict(updateErr) {
			return updateErr
		}

		if updateErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"VMCluster pause",
				"update component paused state",
				updateErr,
			)
		}

		return nil
	})
}

func (m *Manager) pauseGrafana(ctx context.Context, session *domain.Session) error {
	grafana := session.Spec.Workload().Grafana
	if grafana == nil || session.Spec.Workload().OriginalReplicas == nil {
		return domain.NewError(domain.ErrorInternal, "pause Grafana", "session lacks Grafana state")
	}

	if err := m.setGrafanaPaused(ctx, session); err != nil {
		return err
	}

	if err := m.patchDeploymentReplicas(
		ctx,
		session.Spec.Workload().Controller,
		0,
		*session.Spec.Workload().OriginalReplicas,
	); err != nil {
		if restoreErr := m.restoreGrafanaPause(ctx, session); restoreErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"pause Grafana",
				fmt.Sprintf("scale Deployment: %v; restore Grafana suspend state", err),
				restoreErr,
			)
		}

		return workloadScaleError("pause Grafana", "scale Deployment", err)
	}

	for _, ref := range session.Spec.Workload().AffectedPods {
		if err := m.waitForPodDeletion(ctx, ref, "pause Grafana"); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) resumeGrafana(ctx context.Context, session *domain.Session) error {
	grafana := session.Spec.Workload().Grafana
	if grafana == nil || session.Spec.Workload().OriginalReplicas == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume Grafana",
			"session lacks Grafana state",
		)
	}

	if err := m.patchDeploymentReplicas(
		ctx,
		session.Spec.Workload().Controller,
		*session.Spec.Workload().OriginalReplicas,
		0,
	); err != nil {
		return workloadScaleError("resume Grafana", "restore Deployment replicas", err)
	}

	if err := m.restoreGrafanaPause(ctx, session); err != nil {
		return err
	}

	var ready *corev1.Pod
	if err := m.waitFor(
		ctx,
		fmt.Sprintf(
			"Grafana Deployment %s/%s readiness",
			session.Spec.Workload().Controller.Namespace,
			session.Spec.Workload().Controller.Name,
		),
		func(waitCtx context.Context) (bool, error) {
			deployment, err := m.typed.AppsV1().
				Deployments(session.Spec.Workload().Controller.Namespace).
				Get(waitCtx, session.Spec.Workload().Controller.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}

			if expectedUID := session.Spec.Workload().Controller.UID; deployment.UID != expectedUID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"resume Grafana",
					fmt.Sprintf(
						"Deployment %s/%s UID changed",
						deployment.Namespace,
						deployment.Name,
					),
				)
			}

			selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
			if err != nil {
				return false, err
			}

			pods, err := m.typed.CoreV1().
				Pods(deployment.Namespace).
				List(waitCtx, metav1.ListOptions{LabelSelector: selector.String()})
			if err != nil {
				return false, err
			}

			for index := range pods.Items {
				owned, ownerErr := m.podControlledByDeployment(
					waitCtx,
					&pods.Items[index],
					deployment,
				)
				if ownerErr != nil {
					return false, ownerErr
				}

				if owned && podReady(&pods.Items[index]) {
					ready = &pods.Items[index]
					return true, nil
				}
			}

			return false, nil
		},
	); err != nil {
		return err
	}

	if ready != nil {
		workload := session.Spec.WorkloadPtr()
		previous := workload.Pod
		refreshResumedPodReference(workload, previous, ready)
		// Grafana discovery records one representative Deployment Pod. A new
		// ReplicaSet can change its generated name, so refresh that single
		// affected reference even when the name no longer matches.
		if len(workload.AffectedPods) == 1 {
			workload.AffectedPods[0] = podReference(ready)
		}
	}

	return nil
}

func (m *Manager) podControlledByDeployment(
	ctx context.Context,
	pod *corev1.Pod,
	deployment *appsv1.Deployment,
) (bool, error) {
	owner := controllerOwner(pod.OwnerReferences)
	if owner == nil || owner.APIVersion != domain.AppsAPIVersion ||
		owner.Kind != domain.KindReplicaSet ||
		owner.Name == "" {
		return false, nil
	}

	replicaSet, err := m.typed.AppsV1().
		ReplicaSets(pod.Namespace).
		Get(ctx, owner.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	if owner.UID == "" || replicaSet.UID == "" || replicaSet.UID != owner.UID {
		return false, nil
	}

	return sameControllerOwner(controllerOwner(replicaSet.OwnerReferences), &metav1.OwnerReference{
		APIVersion: domain.AppsAPIVersion,
		Kind:       domain.KindDeployment,
		Name:       deployment.Name,
		UID:        deployment.UID,
	}), nil
}

func (m *Manager) restoreGrafanaPause(ctx context.Context, session *domain.Session) error {
	grafana := session.Spec.Workload().Grafana
	if grafana == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"restore Grafana pause",
			"session lacks Grafana state",
		)
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"restore Grafana pause",
			"dynamic client is required for deployment pause control",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(grafana.APIVersion, grafanaResource)
	if err != nil {
		return err
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)

		object, getErr := resource.Get(ctx, grafana.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"restore Grafana pause",
				"read Grafana",
				getErr,
			)
		}

		if object.GetUID() != grafana.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"restore Grafana pause",
				fmt.Sprintf("Grafana %s/%s UID changed", object.GetNamespace(), object.GetName()),
			)
		}

		current, _, nestedErr := unstructured.NestedBool(object.Object, "spec", "suspend")
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"restore Grafana suspend",
				"read reconciliation suspend state",
				nestedErr,
			)
		}

		annotations := object.GetAnnotations()

		pauseOwner := annotations[pauseSessionAnnotation]
		if pauseOwner != "" && pauseOwner != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"restore Grafana suspend",
				fmt.Sprintf(
					"Grafana %s/%s suspend is owned by session %s",
					object.GetNamespace(),
					object.GetName(),
					pauseOwner,
				),
			)
		}

		if pauseOwner == "" {
			if current != grafana.OriginalSuspend {
				return domain.NewError(
					domain.ErrorConflict,
					"restore Grafana suspend",
					fmt.Sprintf(
						"Grafana suspend changed from expected %t to %t",
						grafana.OriginalSuspend,
						current,
					),
				)
			}

			return nil
		}

		if !current {
			return domain.NewError(
				domain.ErrorConflict,
				"restore Grafana suspend",
				"Grafana suspend state changed while session was active",
			)
		}

		if current != grafana.OriginalSuspend {
			if grafana.OriginalSuspendConfigured {
				if err := unstructured.SetNestedField(
					object.Object,
					grafana.OriginalSuspend,
					"spec",
					"suspend",
				); err != nil {
					return err
				}
			} else {
				unstructured.RemoveNestedField(object.Object, "spec", "suspend")
			}
		}

		delete(annotations, pauseSessionAnnotation)
		object.SetAnnotations(annotations)

		if _, updateErr := resource.Update(ctx, object, metav1.UpdateOptions{}); updateErr != nil {
			if apierrors.IsConflict(updateErr) {
				return updateErr
			}

			return domain.WrapError(
				domain.ErrorKubernetes,
				"restore Grafana suspend",
				"clear reconciliation suspend owner",
				updateErr,
			)
		}

		return nil
	})
}

func (m *Manager) setGrafanaPaused(
	ctx context.Context,
	session *domain.Session,
) error {
	grafana := session.Spec.Workload().Grafana

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"Grafana suspend",
			"dynamic client is required for reconciliation suspend control",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(grafana.APIVersion, grafanaResource)
	if err != nil {
		return err
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)

		object, getErr := resource.Get(ctx, grafana.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"Grafana suspend",
				"read Grafana",
				getErr,
			)
		}

		if object.GetUID() != grafana.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"Grafana suspend",
				fmt.Sprintf("Grafana %s/%s UID changed", object.GetNamespace(), object.GetName()),
			)
		}

		current, _, nestedErr := unstructured.NestedBool(object.Object, "spec", "suspend")
		if nestedErr != nil {
			return domain.WrapError(
				domain.ErrorPrecondition,
				"Grafana suspend",
				"read reconciliation suspend state",
				nestedErr,
			)
		}

		annotations := object.GetAnnotations()

		pauseOwner := annotations[pauseSessionAnnotation]
		if pauseOwner != "" && pauseOwner != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"Grafana suspend",
				fmt.Sprintf(
					"Grafana %s/%s suspend is owned by session %s",
					object.GetNamespace(),
					object.GetName(),
					pauseOwner,
				),
			)
		}

		if pauseOwner == "" && current != grafana.OriginalSuspend {
			return domain.NewError(
				domain.ErrorConflict,
				"Grafana suspend",
				fmt.Sprintf(
					"Grafana suspend changed from expected %t to %t",
					grafana.OriginalSuspend,
					current,
				),
			)
		}

		if pauseOwner == session.ID && current {
			return nil
		}

		if pauseOwner == session.ID && !current {
			return domain.NewError(
				domain.ErrorConflict,
				"Grafana suspend",
				"Grafana suspend state changed while session was active",
			)
		}

		if err := unstructured.SetNestedField(
			object.Object,
			true,
			"spec",
			"suspend",
		); err != nil {
			return err
		}

		if annotations == nil {
			annotations = map[string]string{}
		}

		annotations[pauseSessionAnnotation] = session.ID

		object.SetAnnotations(annotations)

		if _, updateErr := resource.Update(ctx, object, metav1.UpdateOptions{}); updateErr != nil {
			if apierrors.IsConflict(updateErr) {
				return updateErr
			}

			return domain.WrapError(
				domain.ErrorKubernetes,
				"Grafana suspend",
				"update reconciliation suspend state",
				updateErr,
			)
		}

		return nil
	})
}

func workloadScaleError(operation, message string, err error) error {
	if domain.CategoryOf(err) == domain.ErrorConflict {
		return err
	}
	return domain.WrapError(domain.ErrorKubernetes, operation, message, err)
}

func (m *Manager) patchDeploymentReplicas(
	ctx context.Context,
	ref domain.ObjectReference,
	replicas int32,
	allowedCurrent ...int32,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deployment, err := m.typed.AppsV1().
			Deployments(ref.Namespace).
			Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if deployment.UID != ref.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"scale Deployment",
				fmt.Sprintf("Deployment %s/%s UID changed", ref.Namespace, ref.Name),
			)
		}

		current := int32(1)
		if deployment.Spec.Replicas != nil {
			current = *deployment.Spec.Replicas
		}

		if current == replicas {
			return nil
		}

		allowed := slices.Contains(allowedCurrent, current)

		if !allowed {
			return domain.NewError(
				domain.ErrorConflict,
				"scale Deployment",
				fmt.Sprintf(
					"Deployment %s/%s replicas changed to %d",
					ref.Namespace,
					ref.Name,
					current,
				),
			)
		}

		deployment.Spec.Replicas = &replicas
		_, err = m.typed.AppsV1().
			Deployments(ref.Namespace).
			Update(ctx, deployment, metav1.UpdateOptions{})

		return err
	})
}

func (m *Manager) pauseStandalone(ctx context.Context, session *domain.Session) error {
	ref := session.Spec.Workload().Pod

	pod, err := m.typed.CoreV1().Pods(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "pause standalone Pod", "read Pod", err)
	}

	if pod.UID != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"pause standalone Pod",
			fmt.Sprintf("Pod %s/%s UID changed", ref.Namespace, ref.Name),
		)
	}

	options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID}}
	if err := m.typed.CoreV1().
		Pods(ref.Namespace).
		Delete(ctx, ref.Name, options); err != nil &&
		!apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "pause standalone Pod", "delete Pod", err)
	}

	return m.waitFor(
		ctx,
		fmt.Sprintf("Pod %s/%s deletion", ref.Namespace, ref.Name),
		func(waitCtx context.Context) (bool, error) {
			current, getErr := m.typed.CoreV1().
				Pods(ref.Namespace).
				Get(waitCtx, ref.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				return true, nil
			}

			if getErr == nil && current.UID != pod.UID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"pause standalone Pod",
					fmt.Sprintf(
						"Pod %s/%s was replaced while waiting for deletion",
						ref.Namespace,
						ref.Name,
					),
				)
			}

			return false, getErr
		},
	)
}

func (m *Manager) resumeStandalone(ctx context.Context, session *domain.Session) error {
	workload := session.Spec.Workload()
	existing, err := m.typed.CoreV1().
		Pods(workload.Pod.Namespace).
		Get(ctx, workload.Pod.Name, metav1.GetOptions{})

	var expectedUID types.UID
	if err == nil {
		if existing.Annotations[kube.SessionKey] != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"resume standalone Pod",
				fmt.Sprintf(
					"Pod %s/%s was recreated outside this session",
					existing.Namespace,
					existing.Name,
				),
			)
		}

		expectedUID = existing.UID
		if podReady(existing) {
			session.Spec.WorkloadPtr().Pod = podReference(existing)
			return nil
		}
	} else if !apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "resume standalone Pod", "read Pod", err)
	}

	var pod corev1.Pod
	if err := json.Unmarshal(workload.OriginalObject, &pod); err != nil {
		return domain.WrapError(
			domain.ErrorInternal,
			"resume standalone Pod",
			"decode saved Pod",
			err,
		)
	}

	pod.ResourceVersion = ""
	pod.UID = ""
	pod.GenerateName = ""
	pod.Generation = 0
	pod.CreationTimestamp = metav1.Time{}
	pod.DeletionTimestamp = nil
	pod.DeletionGracePeriodSeconds = nil
	pod.ManagedFields = nil
	pod.OwnerReferences = nil
	pod.Finalizers = nil
	pod.Status = corev1.PodStatus{}

	pod.Spec.NodeName = ""
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}

	pod.Annotations[kube.SessionKey] = session.ID
	options := session.Spec.WorkflowOptions()

	resumeNode := options.TargetNode
	if session.Status.Phase == domain.PhaseRollingBack ||
		session.Status.Phase == domain.PhaseAborting {
		resumeNode = options.SourceNode
	}

	if resumeNode != "" {
		node, getErr := m.typed.CoreV1().Nodes().Get(ctx, resumeNode, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"resume standalone Pod",
				"read resume node",
				getErr,
			)
		}

		hostname := node.Labels[corev1.LabelHostname]
		if hostname == "" {
			return domain.NewError(
				domain.ErrorPrecondition,
				"resume standalone Pod",
				fmt.Sprintf("node %s lacks kubernetes.io/hostname", resumeNode),
			)
		}

		if pod.Spec.NodeSelector == nil {
			pod.Spec.NodeSelector = map[string]string{}
		}

		pod.Spec.NodeSelector[corev1.LabelHostname] = hostname
	}

	created, err := m.typed.CoreV1().Pods(pod.Namespace).Create(ctx, &pod, metav1.CreateOptions{})
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"resume standalone Pod",
				"create Pod",
				err,
			)
		}
		// The initial Get and Create are a TOCTOU window. Revalidate ownership
		// after AlreadyExists so an unrelated actor cannot be adopted.
		existing, getErr := m.typed.CoreV1().
			Pods(pod.Namespace).
			Get(ctx, pod.Name, metav1.GetOptions{})
		if getErr != nil {
			return domain.WrapError(
				domain.ErrorKubernetes,
				"resume standalone Pod",
				"read concurrently created Pod",
				getErr,
			)
		}

		if existing.Annotations[kube.SessionKey] != session.ID {
			return domain.NewError(
				domain.ErrorConflict,
				"resume standalone Pod",
				fmt.Sprintf(
					"Pod %s/%s was created outside this session",
					existing.Namespace,
					existing.Name,
				),
			)
		}

		created = existing
	}

	if created == nil || created.Name == "" || created.UID == "" {
		return domain.NewError(
			domain.ErrorKubernetes,
			"resume standalone Pod",
			fmt.Sprintf("create Pod %s/%s returned an empty object", pod.Namespace, pod.Name),
		)
	}

	expectedUID = created.UID

	var ready *corev1.Pod
	if err := m.waitFor(
		ctx,
		fmt.Sprintf("Pod %s/%s readiness", pod.Namespace, pod.Name),
		func(waitCtx context.Context) (bool, error) {
			current, getErr := m.typed.CoreV1().
				Pods(pod.Namespace).
				Get(waitCtx, pod.Name, metav1.GetOptions{})
			if getErr != nil {
				return false, getErr
			}

			if current.Annotations[kube.SessionKey] != session.ID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"resume standalone Pod",
					fmt.Sprintf(
						"Pod %s/%s ownership changed while waiting for readiness",
						current.Namespace,
						current.Name,
					),
				)
			}

			if current.UID != expectedUID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"resume standalone Pod",
					fmt.Sprintf(
						"Pod %s/%s was replaced while waiting for readiness",
						current.Namespace,
						current.Name,
					),
				)
			}

			if podReady(current) {
				ready = current
				return true, nil
			}

			return false, nil
		},
	); err != nil {
		return err
	}

	session.Spec.WorkloadPtr().Pod = podReference(ready)

	return nil
}

func (m *Manager) pauseKubeBlocks(ctx context.Context, session *domain.Session) error {
	kb := session.Spec.Workload().KubeBlocks
	if kb == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause KubeBlocks",
			"session lacks KubeBlocks state",
		)
	}

	pod, err := m.typed.CoreV1().
		Pods(session.Spec.Workload().Pod.Namespace).
		Get(ctx, kb.Instance, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"pause KubeBlocks",
			"read instance Pod",
			err,
		)
	}

	if err == nil {
		if pod.UID != session.Spec.Workload().Pod.UID {
			return domain.NewError(
				domain.ErrorConflict,
				"pause KubeBlocks",
				fmt.Sprintf("Pod %s/%s UID changed", pod.Namespace, pod.Name),
			)
		}

		if err := validatePodController(
			pod,
			session.Spec.Workload().Controller,
			"pause KubeBlocks",
		); err != nil {
			return err
		}
	}

	if session.Status.Phase == domain.PhasePausing && err == nil && isLeaderRole(podRole(pod)) &&
		kb.SwitchoverCandidate != "" {
		switch kb.SwitchoverStrategy {
		case domain.KubeBlocksSwitchoverOpsRequest:
			spec := kubeBlocksSwitchoverSpec(
				kb.OpsAPIVersion,
				kb.Cluster,
				kb.Component,
				kb.Instance,
				kb.SwitchoverCandidate,
			)
			if err := m.createAndWaitOps(ctx, session, "switchover", spec); err != nil {
				return err
			}
		case domain.KubeBlocksSwitchoverMongoDBNative:
			if err := m.runMongoDBNativeSwitchover(ctx, session); err != nil {
				return err
			}
		default:
			return domain.NewError(
				domain.ErrorPrecondition,
				"pause KubeBlocks",
				fmt.Sprintf("unsupported persisted switchover strategy %q", kb.SwitchoverStrategy),
			)
		}

		if kb.SwitchoverStrategy != domain.KubeBlocksSwitchoverMongoDBNative {
			current, getErr := m.typed.CoreV1().
				Pods(session.Spec.Workload().Pod.Namespace).
				Get(ctx, kb.Instance, metav1.GetOptions{})
			if getErr != nil {
				return domain.WrapError(
					domain.ErrorKubernetes,
					"pause KubeBlocks",
					"verify switchover role",
					getErr,
				)
			}

			if err := validatePodController(
				current,
				session.Spec.Workload().Controller,
				"pause KubeBlocks",
			); err != nil {
				return err
			}

			if isLeaderRole(podRole(current)) {
				return domain.NewError(
					domain.ErrorPrecondition,
					"pause KubeBlocks",
					fmt.Sprintf(
						"instance %s retained role %s after switchover",
						kb.Instance,
						podRole(current),
					),
				)
			}
		}
	}

	if err := m.setKubeBlocksPaused(ctx, session, true); err != nil {
		return err
	}

	if session.Spec.Workload().Controller.Kind == domain.KindInstanceSet {
		if err := m.deleteKubeBlocksInstancePod(ctx, session); err != nil {
			return err
		}
		return m.VerifyPaused(ctx, session)
	}

	if err := m.waitForPodDeletion(
		ctx,
		session.Spec.Workload().Pod,
		"pause KubeBlocks",
	); err != nil {
		return err
	}

	return m.VerifyPaused(ctx, session)
}

func (m *Manager) runMongoDBNativeSwitchover(ctx context.Context, session *domain.Session) error {
	kb := session.Spec.Workload().KubeBlocks
	if kb == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause KubeBlocks",
			"session lacks KubeBlocks state",
		)
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
		return domain.WrapError(
			domain.ErrorKubernetes,
			"pause KubeBlocks",
			"read MongoDB switchover source Pod",
			err,
		)
	}

	if selected.UID != session.Spec.Workload().Pod.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"pause KubeBlocks",
			fmt.Sprintf("Pod %s/%s UID changed", selected.Namespace, selected.Name),
		)
	}

	if err := validatePodController(
		selected,
		session.Spec.Workload().Controller,
		"pause KubeBlocks",
	); err != nil {
		return err
	}

	headlessService := fmt.Sprintf("%s-%s-headless", kb.Cluster, kb.Component)
	leaderFQDN := fmt.Sprintf("%s.%s", kb.Instance, headlessService)

	candidateFQDN := fmt.Sprintf("%s.%s", kb.SwitchoverCandidate, headlessService)
	if m.logger != nil {
		m.logger.Info(
			"starting KubeBlocks MongoDB native switchover",
			"namespace",
			namespace,
			"cluster",
			kb.Cluster,
			"workload_component",
			kb.Component,
			"instance",
			kb.Instance,
			"candidate",
			kb.SwitchoverCandidate,
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
				kubeBlocksMongoDBNativeSwitchoverCommand(
					namespace,
					kb.Cluster,
					kb.Component,
					kb.Instance,
					kb.SwitchoverCandidate,
				),
			),
			executionErr,
		)
	}

	return m.waitFor(
		ctx,
		fmt.Sprintf(
			"KubeBlocks MongoDB switchover from %s to %s",
			kb.Instance,
			kb.SwitchoverCandidate,
		),
		func(waitCtx context.Context) (bool, error) {
			leader, leaderErr := m.typed.CoreV1().
				Pods(namespace).
				Get(waitCtx, kb.Instance, metav1.GetOptions{})
			if leaderErr != nil {
				return false, leaderErr
			}

			if err := validatePodController(
				leader,
				session.Spec.Workload().Controller,
				"pause KubeBlocks",
			); err != nil {
				return false, err
			}

			candidate, candidateErr := m.typed.CoreV1().
				Pods(namespace).
				Get(waitCtx, kb.SwitchoverCandidate, metav1.GetOptions{})
			if candidateErr != nil {
				return false, candidateErr
			}

			if err := validatePodController(
				candidate,
				session.Spec.Workload().Controller,
				"pause KubeBlocks",
			); err != nil {
				return false, err
			}

			return !isLeaderRole(podRole(leader)) && isLeaderRole(podRole(candidate)), nil
		},
	)
}

func (m *Manager) resumeKubeBlocks(ctx context.Context, session *domain.Session) error {
	kb := session.Spec.Workload().KubeBlocks
	if kb == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"resume KubeBlocks",
			"session lacks KubeBlocks state",
		)
	}

	if session.Spec.Workload().Controller.Kind == domain.KindInstanceSet && kb.OriginalPaused {
		return domain.NewError(
			domain.ErrorPrecondition,
			"resume KubeBlocks",
			"an initially paused InstanceSet cannot safely recreate the migrated Pod; set spec.paused=false and verify the Pod is Ready before recovery",
		)
	}

	if err := m.setKubeBlocksPaused(ctx, session, false); err != nil {
		return err
	}

	workload := session.Spec.Workload()

	return m.waitForResumedPod(ctx, session, workload.Pod, workload.Controller, "resume KubeBlocks")
}

func (m *Manager) setKubeBlocksPaused(
	ctx context.Context,
	session *domain.Session,
	paused bool,
) error {
	if session.Spec.Workload().Controller.Kind == domain.KindInstanceSet {
		return m.setKubeBlocksInstanceSetPaused(ctx, session, paused)
	}

	kb := session.Spec.Workload().KubeBlocks
	if kb == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"KubeBlocks pause",
			"session lacks KubeBlocks state",
		)
	}

	if kb.ClusterUID == "" || kb.OriginalStops == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"KubeBlocks pause",
			"session lacks Cluster identity or original component stop state",
		)
	}

	gvr, err := kube.ParseGroupVersionResource(kubeBlocksClusterAPIVersion, clusterResource)
	if err != nil {
		return err
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"KubeBlocks pause",
			"dynamic client is required for Cluster pause",
		)
	}

	if session.ID == "" {
		return domain.NewError(
			domain.ErrorInternal,
			"KubeBlocks pause",
			"session ID is required for Cluster pause ownership",
		)
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return m.updateKubeBlocksCluster(ctx, session, kb, paused, gvr)
	})
}

func (m *Manager) setKubeBlocksInstanceSetPaused(
	ctx context.Context,
	session *domain.Session,
	paused bool,
) error {
	workload := session.Spec.Workload()

	kb := workload.KubeBlocks
	if kb == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"InstanceSet pause",
			"session lacks KubeBlocks state",
		)
	}

	if m.dynamic == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"InstanceSet pause",
			"dynamic client is required for InstanceSet reconciliation control",
		)
	}

	if session.ID == "" {
		return domain.NewError(
			domain.ErrorInternal,
			"InstanceSet pause",
			"session ID is required for InstanceSet pause ownership",
		)
	}

	ref := workload.Controller

	gvr, err := kube.ParseGroupVersionResource(ref.APIVersion, instanceSetResource)
	if err != nil {
		return err
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return m.updateKubeBlocksInstanceSet(ctx, session, kb, ref, paused, gvr)
	})
}

func (m *Manager) updateKubeBlocksCluster(
	ctx context.Context,
	session *domain.Session,
	kb *domain.KubeBlocksSpec,
	paused bool,
	gvr schema.GroupVersionResource,
) error {
	resource := m.dynamic.Resource(gvr).Namespace(session.Spec.Workload().Pod.Namespace)

	cluster, err := resource.Get(ctx, kb.Cluster, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "KubeBlocks pause", "read Cluster", err)
	}

	if cluster.GetUID() != kb.ClusterUID {
		return domain.NewError(
			domain.ErrorConflict,
			"KubeBlocks pause",
			fmt.Sprintf("Cluster %s/%s UID changed", cluster.GetNamespace(), cluster.GetName()),
		)
	}

	components, ok, nestedErr := unstructured.NestedSlice(cluster.Object, "spec", "componentSpecs")
	if nestedErr != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"KubeBlocks pause",
			"read componentSpecs",
			nestedErr,
		)
	}

	if !ok || len(components) == 0 {
		return domain.NewError(
			domain.ErrorPrecondition,
			"KubeBlocks pause",
			"Cluster has no componentSpecs",
		)
	}

	annotations := cluster.GetAnnotations()

	pauseOwner := annotations[pauseSessionAnnotation]
	if pauseOwner != "" && pauseOwner != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"KubeBlocks pause",
			fmt.Sprintf(
				"Cluster %s/%s pause is owned by session %s",
				cluster.GetNamespace(),
				cluster.GetName(),
				pauseOwner,
			),
		)
	}

	changed, err := updateKubeBlocksComponent(components, kb, session.ID, paused, pauseOwner)
	if err != nil {
		return err
	}

	if paused && pauseOwner == "" {
		if annotations == nil {
			annotations = map[string]string{}
		}

		annotations[pauseSessionAnnotation] = session.ID
		cluster.SetAnnotations(annotations)

		changed = true
	}

	if !paused && pauseOwner == session.ID {
		delete(annotations, pauseSessionAnnotation)
		cluster.SetAnnotations(annotations)

		changed = true
	}

	if !changed {
		return nil
	}

	if err := unstructured.SetNestedField(
		cluster.Object,
		components,
		"spec",
		"componentSpecs",
	); err != nil {
		return err
	}

	_, err = resource.Update(ctx, cluster, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return err
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"KubeBlocks pause",
			"update Cluster component stop state",
			err,
		)
	}

	return nil
}

func updateKubeBlocksComponent(
	components []any,
	kb *domain.KubeBlocksSpec,
	sessionID string,
	paused bool,
	pauseOwner string,
) (bool, error) {
	for index := range components {
		component, ok := components[index].(map[string]any)
		if !ok {
			return false, domain.NewError(
				domain.ErrorPrecondition,
				"KubeBlocks pause",
				fmt.Sprintf("componentSpecs[%d] is malformed", index),
			)
		}

		name, found, err := unstructured.NestedString(component, "name")
		if err != nil || !found || name == "" {
			return false, domain.NewError(
				domain.ErrorPrecondition,
				"KubeBlocks pause",
				fmt.Sprintf("componentSpecs[%d] has no name", index),
			)
		}

		if name != kb.Component {
			continue
		}

		current, _, err := unstructured.NestedBool(component, "stop")
		if err != nil {
			return false, domain.WrapError(
				domain.ErrorPrecondition,
				"KubeBlocks pause",
				fmt.Sprintf("read component %s stop state", name),
				err,
			)
		}

		original, known := kb.OriginalStops[name]
		if !known {
			return false, domain.NewError(
				domain.ErrorConflict,
				"KubeBlocks pause",
				fmt.Sprintf("Cluster component %s lacks original stop state", name),
			)
		}

		expected := original
		if pauseOwner == sessionID {
			expected = true
		}

		want := true
		if !paused {
			want = original
		}

		if current != expected {
			return false, domain.NewError(
				domain.ErrorConflict,
				"KubeBlocks pause",
				fmt.Sprintf(
					"Cluster component %s stop changed from expected %t to %t",
					name,
					expected,
					current,
				),
			)
		}

		changed := current != want
		if changed {
			if err := unstructured.SetNestedField(component, want, "stop"); err != nil {
				return false, err
			}
		}

		components[index] = component

		return changed, nil
	}

	return false, domain.NewError(
		domain.ErrorConflict,
		"KubeBlocks pause",
		fmt.Sprintf("Cluster component %s was removed after discovery", kb.Component),
	)
}

func (m *Manager) updateKubeBlocksInstanceSet(
	ctx context.Context,
	session *domain.Session,
	kb *domain.KubeBlocksSpec,
	ref domain.ObjectReference,
	paused bool,
	gvr schema.GroupVersionResource,
) error {
	resource := m.dynamic.Resource(gvr).Namespace(ref.Namespace)

	object, err := resource.Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"InstanceSet pause",
			"read InstanceSet",
			err,
		)
	}

	if object.GetUID() != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"InstanceSet pause",
			fmt.Sprintf("InstanceSet %s/%s UID changed", ref.Namespace, ref.Name),
		)
	}

	current, found, err := unstructured.NestedBool(object.Object, "spec", "paused")
	if err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"InstanceSet pause",
			"read InstanceSet paused state",
			err,
		)
	}

	if !found {
		current = false
	}

	annotations := object.GetAnnotations()

	pauseOwner := annotations[pauseSessionAnnotation]
	if pauseOwner != "" && pauseOwner != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"InstanceSet pause",
			fmt.Sprintf(
				"InstanceSet %s/%s pause is owned by session %s",
				ref.Namespace,
				ref.Name,
				pauseOwner,
			),
		)
	}

	if err := validateInstanceSetPauseState(
		ref,
		kb,
		paused,
		current,
		found,
		pauseOwner,
	); err != nil {
		return err
	}

	want := paused
	if !paused {
		want = kb.OriginalPaused
	}

	changed := current != want || (!paused && found != kb.OriginalPausedConfigured)
	if changed {
		if !paused && !kb.OriginalPausedConfigured {
			unstructured.RemoveNestedField(object.Object, "spec", "paused")
		} else if err := unstructured.SetNestedField(object.Object, want, "spec", "paused"); err != nil {
			return err
		}
	}

	if paused {
		if annotations == nil {
			annotations = map[string]string{}
		}

		if annotations[pauseSessionAnnotation] != session.ID {
			annotations[pauseSessionAnnotation] = session.ID
			changed = true
		}
	} else if annotations[pauseSessionAnnotation] == session.ID {
		delete(annotations, pauseSessionAnnotation)

		changed = true
	}

	if !changed {
		return nil
	}

	object.SetAnnotations(annotations)

	updated, err := resource.Update(ctx, object, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return err
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"InstanceSet pause",
			"update InstanceSet paused state",
			err,
		)
	}

	actual, configured, err := unstructured.NestedBool(updated.Object, "spec", "paused")
	if err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"InstanceSet pause",
			"verify updated InstanceSet paused state",
			err,
		)
	}

	if paused && (!configured || !actual) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"InstanceSet pause",
			fmt.Sprintf(
				"InstanceSet %s/%s API did not preserve spec.paused",
				ref.Namespace,
				ref.Name,
			),
		)
	}

	return nil
}

func validateInstanceSetPauseState(
	ref domain.ObjectReference,
	kb *domain.KubeBlocksSpec,
	paused, current, found bool,
	pauseOwner string,
) error {
	if pauseOwner == "" {
		if current != kb.OriginalPaused {
			return domain.NewError(
				domain.ErrorConflict,
				"InstanceSet pause",
				fmt.Sprintf(
					"InstanceSet %s/%s paused changed from expected %t to %t",
					ref.Namespace,
					ref.Name,
					kb.OriginalPaused,
					current,
				),
			)
		}

		return nil
	}

	if paused && current {
		return nil
	}

	if !paused && !current {
		return domain.NewError(
			domain.ErrorConflict,
			"InstanceSet resume",
			fmt.Sprintf(
				"InstanceSet %s/%s paused state changed while session was active",
				ref.Namespace,
				ref.Name,
			),
		)
	}

	if paused && !current {
		return domain.NewError(
			domain.ErrorConflict,
			"InstanceSet pause",
			fmt.Sprintf(
				"InstanceSet %s/%s paused state changed while session was active",
				ref.Namespace,
				ref.Name,
			),
		)
	}

	_ = found

	return nil
}

func (m *Manager) verifyKubeBlocksInstanceSetPaused(
	ctx context.Context,
	session *domain.Session,
) error {
	ref := session.Spec.Workload().Controller

	gvr, err := kube.ParseGroupVersionResource(ref.APIVersion, instanceSetResource)
	if err != nil {
		return err
	}

	object, err := m.dynamic.Resource(gvr).
		Namespace(ref.Namespace).
		Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "verify paused", "read InstanceSet", err)
	}

	if object.GetUID() != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf("InstanceSet %s/%s UID changed", ref.Namespace, ref.Name),
		)
	}

	if object.GetAnnotations()[pauseSessionAnnotation] != session.ID {
		return domain.NewError(
			domain.ErrorConflict,
			"verify paused",
			fmt.Sprintf("InstanceSet %s/%s pause ownership changed", ref.Namespace, ref.Name),
		)
	}

	paused, found, nestedErr := unstructured.NestedBool(object.Object, "spec", "paused")
	if nestedErr != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"verify paused",
			"read InstanceSet paused state",
			nestedErr,
		)
	}

	if !found || !paused {
		return domain.NewError(
			domain.ErrorPrecondition,
			"verify paused",
			fmt.Sprintf("InstanceSet %s/%s reconciliation is not paused", ref.Namespace, ref.Name),
		)
	}

	return nil
}

func (m *Manager) deleteKubeBlocksInstancePod(ctx context.Context, session *domain.Session) error {
	ref := session.Spec.Workload().Pod

	pod, err := m.typed.CoreV1().Pods(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"pause KubeBlocks",
			"read instance Pod",
			err,
		)
	}

	if pod.UID != ref.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"pause KubeBlocks",
			fmt.Sprintf("Pod %s/%s UID changed", ref.Namespace, ref.Name),
		)
	}

	options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID}}
	if err := m.typed.CoreV1().
		Pods(ref.Namespace).
		Delete(ctx, ref.Name, options); err != nil &&
		!apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"pause KubeBlocks",
			"delete instance Pod",
			err,
		)
	}

	return m.waitFor(
		ctx,
		fmt.Sprintf("KubeBlocks Pod %s/%s deletion", ref.Namespace, ref.Name),
		func(waitCtx context.Context) (bool, error) {
			current, getErr := m.typed.CoreV1().
				Pods(ref.Namespace).
				Get(waitCtx, ref.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				return true, nil
			}

			if getErr == nil && current.UID != pod.UID {
				return false, domain.NewError(
					domain.ErrorConflict,
					"pause KubeBlocks",
					fmt.Sprintf(
						"Pod %s/%s was replaced while waiting for deletion",
						ref.Namespace,
						ref.Name,
					),
				)
			}

			return false, getErr
		},
	)
}
