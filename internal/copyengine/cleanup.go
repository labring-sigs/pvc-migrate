package copyengine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	"helm.sh/helm/v4/pkg/action"
	helmkube "helm.sh/helm/v4/pkg/kube"
	"helm.sh/helm/v4/pkg/storage/driver"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
)

// Cleanup removes the chart releases for one persisted copy attempt, including
// pending installations left behind by an interrupted controller process.
func (*PVMigrate) Cleanup(ctx context.Context, request CleanupRequest) error {
	type target struct{ config, context, namespace string }

	targets := []target{{request.KubeconfigPath, request.Context, request.Source.Namespace}}

	destination := target{
		request.DestinationKubeconfigPath,
		request.DestinationContext,
		request.DestinationNamespace,
	}
	if destination.config == "" {
		destination.config = request.KubeconfigPath
		if destination.context == "" {
			destination.context = request.Context
		}
	}

	if destination != targets[0] {
		targets = append(targets, destination)
	}

	for _, target := range targets {
		err := UninstallNamedRelease(
			ctx,
			target.config,
			target.context,
			target.namespace,
			copyReleaseNames(request)...,
		)
		if err != nil {
			return fmt.Errorf(
				"clean up interrupted copy in namespace %s: %w",
				target.namespace,
				err,
			)
		}
	}

	return nil
}

// UninstallNamedRelease removes helm releases by exact name in one namespace.
// A release that does not exist is success: the caller converges toward
// "the tool is gone" and a missing release is that state. The uninstall waits
// for the release resources to be deleted (upstream's own cleanup strategy),
// so a nil return means the tool Pods are actually gone, not merely marked
// for deletion.
func UninstallNamedRelease(
	ctx context.Context,
	kubeconfigPath, kubeContext, namespace string,
	releaseNames ...string,
) error {
	flags := genericclioptions.NewConfigFlags(false)
	flags.KubeConfig = &kubeconfigPath
	flags.Context = &kubeContext
	flags.Namespace = &namespace
	flags.WrapConfigFn = func(config *rest.Config) *rest.Config {
		// Helm uninstall has no context argument. Bound in-flight requests and
		// prevent further API calls after the execution fence is canceled.
		config.Timeout = 10 * time.Second
		config.Wrap(transport.ContextCanceller(ctx, context.Canceled))

		return config
	}

	config := new(action.Configuration)
	if err := config.Init(flags, namespace, os.Getenv("HELM_DRIVER")); err != nil {
		return fmt.Errorf("initialize release cleanup: %w", err)
	}

	return uninstallReleases(ctx, config, releaseNames...)
}

// uninstallReleases is the exact-name uninstall loop shared by every cleanup
// entry point, injectable with a prebuilt action configuration for tests.
func uninstallReleases(
	ctx context.Context,
	config *action.Configuration,
	releaseNames ...string,
) error {
	for _, name := range releaseNames {
		if err := ctx.Err(); err != nil {
			return err
		}

		uninstall := action.NewUninstall(config)
		uninstall.WaitStrategy = helmkube.LegacyStrategy
		uninstall.DeletionPropagation = "foreground"

		uninstall.Timeout = 30 * time.Second
		if _, err := uninstall.Run(name); err != nil && !errors.Is(err, driver.ErrReleaseNotFound) {
			return fmt.Errorf("uninstall release %s: %w", name, err)
		}
	}

	return nil
}

func copyReleaseNames(request CleanupRequest) []string {
	var names []string

	strategies := request.Strategies
	if len(strategies) == 0 || slices.Contains(strategies, "auto") {
		strategies = []string{"mount", "clusterip", "nodeport", "loadbalancer", "local"}
	}

	for _, strategy := range strategies {
		prefix := "pv-migrate-" + OperationID(request.AttemptIdentity) + "-" + strategy
		switch strategy {
		case "local", "nodeport", "loadbalancer":
			names = append(names, prefix+"-src", prefix+"-dest")
		case "mount", "clusterip":
			names = append(names, prefix)
		}
	}

	return names
}
