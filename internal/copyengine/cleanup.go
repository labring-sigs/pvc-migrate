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
func (*PVMigrate) Cleanup(ctx context.Context, request Request) error {
	type target struct{ config, context, namespace string }

	targets := []target{{request.KubeconfigPath, request.Context, request.Source.Namespace}}

	destination := target{
		request.DestinationKubeconfigPath,
		request.DestinationContext,
		request.Destination.Namespace,
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
		flags := genericclioptions.NewConfigFlags(false)
		flags.KubeConfig = &target.config
		flags.Context = &target.context
		flags.Namespace = &target.namespace
		flags.WrapConfigFn = func(config *rest.Config) *rest.Config {
			// Helm uninstall has no context argument. Bound in-flight requests and
			// prevent further API calls after the execution fence is canceled.
			config.Timeout = 10 * time.Second
			config.Wrap(transport.ContextCanceller(ctx, context.Canceled))

			return config
		}

		config := new(action.Configuration)
		if err := config.Init(flags, target.namespace, os.Getenv("HELM_DRIVER")); err != nil {
			return fmt.Errorf("initialize interrupted copy cleanup: %w", err)
		}

		if err := cleanupReleases(ctx, config, request); err != nil {
			return fmt.Errorf(
				"clean up interrupted copy in namespace %s: %w",
				target.namespace,
				err,
			)
		}
	}

	return nil
}

func cleanupReleases(ctx context.Context, config *action.Configuration, request Request) error {
	for _, name := range copyReleaseNames(request) {
		if err := ctx.Err(); err != nil {
			return err
		}

		uninstall := action.NewUninstall(config)
		uninstall.DisableHooks = true
		uninstall.WaitStrategy = helmkube.HookOnlyStrategy
		uninstall.DeletionPropagation = "foreground"

		uninstall.Timeout = 30 * time.Second
		if _, err := uninstall.Run(name); err != nil && !errors.Is(err, driver.ErrReleaseNotFound) {
			return fmt.Errorf("uninstall release %s: %w", name, err)
		}
	}

	return nil
}

func copyReleaseNames(request Request) []string {
	var names []string

	strategies := request.Strategies
	if len(strategies) == 0 || slices.Contains(strategies, "auto") {
		strategies = []string{"mount", "clusterip", "nodeport", "loadbalancer", "local"}
	}

	for _, strategy := range strategies {
		prefix := "pv-migrate-" + OperationID(request) + "-" + strategy
		switch strategy {
		case "local", "nodeport", "loadbalancer":
			names = append(names, prefix+"-src", prefix+"-dest")
		case "mount", "clusterip":
			names = append(names, prefix)
		}
	}

	return names
}
