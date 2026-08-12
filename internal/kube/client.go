package kube

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Clients struct {
	Kubernetes kubernetes.Interface
	Dynamic    dynamic.Interface
	Discovery  discovery.DiscoveryInterface
	RESTConfig *rest.Config
}

func NewClients(kubeconfigPath, kubeContext string) (*Clients, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		rules.ExplicitPath = kubeconfigPath
	}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: kubeContext}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	if err != nil {
		return nil, domain.WrapError(domain.ErrorValidation, "kubernetes config", fmt.Sprintf("load kubeconfig: %v", err), err)
	}
	config.UserAgent = "pvc-migrate/dev"
	config.QPS = 30
	config.Burst = 60
	typed, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, domain.WrapError(domain.ErrorKubernetes, "kubernetes client", "create typed client", err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, domain.WrapError(domain.ErrorKubernetes, "kubernetes client", "create dynamic client", err)
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return nil, domain.WrapError(domain.ErrorKubernetes, "kubernetes client", "create discovery client", err)
	}
	return &Clients{Kubernetes: typed, Dynamic: dynamicClient, Discovery: discoveryClient, RESTConfig: config}, nil
}

func EnsureNamespace(ctx context.Context, client kubernetes.Interface, name, sessionID string, dryRun bool) error {
	if name == "" {
		return domain.NewError(domain.ErrorValidation, "ensure namespace", "namespace is empty")
	}
	_, err := client.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "ensure namespace", fmt.Sprintf("read namespace %s", name), err)
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
	if _, err := client.CoreV1().Namespaces().Create(ctx, namespace, options); err != nil && !apierrors.IsAlreadyExists(err) {
		return domain.WrapError(domain.ErrorKubernetes, "ensure namespace", fmt.Sprintf("create namespace %s", name), err)
	}
	return nil
}

func HasAPIResource(discoveryClient discovery.DiscoveryInterface, groupVersion, resource string) bool {
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

func WaitFor(ctx context.Context, interval time.Duration, description string, condition func(context.Context) (bool, error)) error {
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
	message := fmt.Sprintf("timed out waiting for %s", description)
	if errors.Is(err, context.Canceled) {
		message = fmt.Sprintf("canceled while waiting for %s", description)
	}
	return domain.WrapError(domain.ErrorTimeout, "wait", message, err)
}

func ParseGroupVersionResource(apiVersion, resource string) (schema.GroupVersionResource, error) {
	groupVersion, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return schema.GroupVersionResource{}, domain.WrapError(domain.ErrorValidation, "api resource", "parse apiVersion", err)
	}
	return groupVersion.WithResource(resource), nil
}
