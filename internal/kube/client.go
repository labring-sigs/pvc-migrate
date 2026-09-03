package kube

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type Clients struct {
	Kubernetes kubernetes.Interface
	Dynamic    dynamic.Interface
	Discovery  discovery.DiscoveryInterface
	RESTConfig *rest.Config
	// Runtime is the typed controller-runtime client for pvc-migrate API
	// objects. Dynamic remains available for third-party CRDs whose schemas are
	// intentionally discovered at runtime by existing workflows.
	Runtime crclient.Client
}

func NewClients(kubeconfigPath, kubeContext string) (*Clients, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		rules.ExplicitPath = kubeconfigPath
	}

	overrides := &clientcmd.ConfigOverrides{CurrentContext: kubeContext}

	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).
		ClientConfig()
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorValidation,
			"kubernetes config",
			fmt.Sprintf("load kubeconfig: %v", err),
			err,
		)
	}

	config.UserAgent = "pvc-migrate/dev"
	config.QPS = 30
	config.Burst = 60

	typed, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"kubernetes client",
			"create typed client",
			err,
		)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"kubernetes client",
			"create dynamic client",
			err,
		)
	}

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"kubernetes client",
			"create discovery client",
			err,
		)
	}

	apiScheme := k8sruntime.NewScheme()
	if err := v1alpha1.AddToScheme(apiScheme); err != nil {
		return nil, domain.WrapError(
			domain.ErrorInternal,
			"kubernetes client",
			"register pvc-migrate API scheme",
			err,
		)
	}

	runtimeClient, err := crclient.New(config, crclient.Options{Scheme: apiScheme})
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"kubernetes client",
			"create controller-runtime client",
			err,
		)
	}

	return &Clients{
		Kubernetes: typed,
		Dynamic:    dynamicClient,
		Discovery:  discoveryClient,
		RESTConfig: config,
		Runtime:    runtimeClient,
	}, nil
}

func EnsureNamespace(
	ctx context.Context,
	client kubernetes.Interface,
	name, sessionID string,
	dryRun bool,
) error {
	if name == "" {
		return domain.NewError(domain.ErrorValidation, "ensure namespace", "namespace is empty")
	}

	_, err := client.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return nil
	}

	if !apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"ensure namespace",
			"read namespace "+name,
			err,
		)
	}

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: name,
		Labels: map[string]string{
			ManagedByLabel: ManagedByValue,
		},
	}}
	if sessionID != "" {
		namespace.Labels[SessionKey] = sessionID
	}

	options := metav1.CreateOptions{}
	if dryRun {
		options.DryRun = []string{metav1.DryRunAll}
	}

	if _, err := client.CoreV1().
		Namespaces().
		Create(ctx, namespace, options); err != nil &&
		!apierrors.IsAlreadyExists(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"ensure namespace",
			"create namespace "+name,
			err,
		)
	}

	return nil
}

func HasAPIResource(
	discoveryClient discovery.DiscoveryInterface,
	groupVersion, resource string,
) bool {
	if discoveryClient == nil {
		return false
	}

	list, err := discoveryClient.ServerResourcesForGroupVersion(groupVersion)
	if err != nil {
		return false
	}

	for _, candidate := range list.APIResources {
		if candidate.Name == resource {
			return true
		}
	}

	return false
}

// HasAPIResources verifies that a complete API surface is served. Controller
// mode uses this to reject partially installed workflow CRDs before commands
// start creating resources that the active controller cannot watch.
func HasAPIResources(
	discoveryClient discovery.DiscoveryInterface,
	groupVersion string,
	resources ...string,
) bool {
	if discoveryClient == nil || len(resources) == 0 {
		return false
	}

	list, err := discoveryClient.ServerResourcesForGroupVersion(groupVersion)
	if err != nil {
		return false
	}

	served := make(map[string]struct{}, len(list.APIResources))
	for _, candidate := range list.APIResources {
		served[candidate.Name] = struct{}{}
	}

	for _, resource := range resources {
		if _, ok := served[resource]; !ok {
			return false
		}
	}

	return true
}

// AvailableControllerWorkflowKinds returns the workflow CRD Kinds served by
// the target cluster. Discovery is best-effort: an unavailable API group
// yields an empty set and explicit controller mode reports that requirement.
func AvailableControllerWorkflowKinds(
	discoveryClient discovery.DiscoveryInterface,
) []domain.ControllerKind {
	if discoveryClient == nil {
		return nil
	}

	list, err := discoveryClient.ServerResourcesForGroupVersion(domain.SessionAPIVersion)
	if err != nil {
		return nil
	}

	served := make(map[string]struct{}, len(list.APIResources))
	for _, resource := range list.APIResources {
		served[resource.Name] = struct{}{}
	}

	available := make([]domain.ControllerKind, 0, len(domain.ControllerWorkflows())*2)
	for _, workflow := range domain.ControllerWorkflows() {
		if workflow.Resource != "" {
			if workflowResourceWithStatusServed(served, workflow.Resource) {
				available = append(available, workflow.Kind)
			}
		}

		if workflow.ClusterResource != "" {
			if workflowResourceWithStatusServed(served, workflow.ClusterResource) {
				available = append(available, workflow.ClusterKind)
			}
		}
	}

	return available
}

func workflowResourceWithStatusServed(served map[string]struct{}, resource string) bool {
	if resource == "" {
		return false
	}

	_, parentServed := served[resource]
	_, statusServed := served[resource+"/status"]

	return parentServed && statusServed
}

// BackupRepositoryAvailable reports whether the namespaced repository API is
// served. The controller can resolve user-owned locations only when this
// resource is installed.
func BackupRepositoryAvailable(discoveryClient discovery.DiscoveryInterface) bool {
	return HasAPIResource(
		discoveryClient,
		domain.SessionAPIVersion,
		domain.BackupRepositoryResource,
	)
}

func WaitFor(
	ctx context.Context,
	interval time.Duration,
	description string,
	condition func(context.Context) (bool, error),
) error {
	if interval <= 0 {
		return domain.NewError(domain.ErrorValidation, "wait", "poll interval must be positive")
	}

	if condition == nil {
		return domain.NewError(domain.ErrorValidation, "wait", "condition is required")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return waitContextError(err, description)
		}

		ready, err := condition(ctx)
		if err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return waitContextError(contextErr, description)
			}
			return err
		}

		if ready {
			return nil
		}

		select {
		case <-ctx.Done():
			return waitContextError(ctx.Err(), description)
		case <-ticker.C:
		}
	}
}

func waitContextError(err error, description string) error {
	message := "timed out waiting for " + description
	if errors.Is(err, context.Canceled) {
		message = "canceled while waiting for " + description
	}

	return domain.WrapError(domain.ErrorTimeout, "wait", message, err)
}

func ParseGroupVersionResource(apiVersion, resource string) (schema.GroupVersionResource, error) {
	groupVersion, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return schema.GroupVersionResource{}, domain.WrapError(
			domain.ErrorValidation,
			"api resource",
			"parse apiVersion",
			err,
		)
	}

	return groupVersion.WithResource(resource), nil
}
