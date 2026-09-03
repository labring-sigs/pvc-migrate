package kube

import (
	"os"
	"slices"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ZeroResourceRequirements prevents LimitRange defaults from assigning a
// compute or requested ephemeral-storage footprint to short-lived migration
// tools. A zero ephemeral-storage limit would make every tool immediately
// evictable because its writable layer and logs consume local storage, so that
// limit is deliberately omitted.
// PVC storage remains represented by the PVC requests.storage field.
func ZeroResourceRequirements() corev1.ResourceRequirements {
	zero := resource.MustParse("0")

	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:              zero.DeepCopy(),
			corev1.ResourceMemory:           zero.DeepCopy(),
			corev1.ResourceEphemeralStorage: zero.DeepCopy(),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    zero.DeepCopy(),
			corev1.ResourceMemory: zero.DeepCopy(),
		},
	}
}

// ZeroResourceHelmValues applies the same zero resource policy to every
// component emitted by the embedded pv-migrate Helm chart.
func ZeroResourceHelmValues() []string {
	components := []string{"rsync", "sshd", "rclone"}
	resources := []string{
		"requests.cpu",
		"requests.memory",
		"requests.ephemeral-storage",
		"limits.cpu",
		"limits.memory",
	}

	values := make([]string, 0, len(components)*len(resources))
	for _, component := range components {
		for _, resourceName := range resources {
			values = append(values, component+".resources."+resourceName+"=0")
		}
	}

	return values
}

// PVMigrateResourceEstimate describes the peak namespaced objects emitted by
// one embedded pv-migrate chart run. Multi-volume workflows serialize chart
// runs and clean each release before starting the next volume.
func PVMigrateResourceEstimate(
	strategies []string,
	sameNamespace bool,
	destinationSide bool,
) domain.ResourceEstimate {
	mount := slices.Contains(strategies, domain.StrategyMount)
	clusterIP := slices.Contains(strategies, domain.StrategyClusterIP)
	nodePort := slices.Contains(strategies, domain.StrategyNodePort)
	loadBalancer := slices.Contains(strategies, domain.StrategyLoadBalancer)
	local := slices.Contains(strategies, domain.StrategyLocal)
	twoReleaseRemote := nodePort || loadBalancer
	remote := clusterIP || twoReleaseRemote

	var estimate domain.ResourceEstimate
	switch {
	case sameNamespace:
		estimate = pvmigrateSameNamespaceEstimate(
			mount,
			remote,
			twoReleaseRemote,
			local,
			nodePort,
			loadBalancer,
		)
	case destinationSide:
		estimate = pvmigrateDestinationEstimate(remote, local)
	default:
		estimate = pvmigrateSourceEstimate(
			remote,
			twoReleaseRemote,
			local,
			nodePort,
			loadBalancer,
		)
	}

	estimate.Endpoints = estimate.Services
	estimate.EndpointSlices = estimate.Services
	estimate.NotTerminatingPods = estimate.Pods

	return estimate
}

func pvmigrateSameNamespaceEstimate(
	mount bool,
	remote bool,
	twoReleaseRemote bool,
	local bool,
	nodePort bool,
	loadBalancer bool,
) domain.ResourceEstimate {
	estimate := domain.ResourceEstimate{}
	if mount || remote || local {
		estimate.Pods = 1
		estimate.ServiceAccounts = 1
	}

	if remote || local {
		estimate.Pods = 2
		estimate.Secrets = 2
	}

	if mount || remote {
		estimate.Jobs = 1
	}

	if remote {
		estimate.Deployments = 1
		estimate.ReplicaSets = 1
		estimate.Services = 1
	}

	if local {
		estimate.Deployments = 2
		estimate.ReplicaSets = 2
		estimate.Services = 2
	}

	if nodePort || loadBalancer {
		estimate.ServiceNodePorts = 1
	}

	if loadBalancer {
		estimate.ServiceLoadBalancers = 1
	}

	releases := 0
	if mount || remote || local {
		releases = 1
	}

	if twoReleaseRemote || local {
		releases = 2
	}

	AddHelmReleaseObjectEstimate(&estimate, releases)

	return estimate
}

func pvmigrateDestinationEstimate(remote, local bool) domain.ResourceEstimate {
	estimate := domain.ResourceEstimate{}
	if remote || local {
		estimate.Pods = 1
		estimate.ServiceAccounts = 1
		estimate.Secrets = 1
		AddHelmReleaseObjectEstimate(&estimate, 1)
	}

	if remote {
		estimate.Jobs = 1
	}

	if local {
		estimate.Deployments = 1
		estimate.ReplicaSets = 1
		estimate.Services = 1
	}

	return estimate
}

func pvmigrateSourceEstimate(
	remote, twoReleaseRemote, local, nodePort, loadBalancer bool,
) domain.ResourceEstimate {
	estimate := domain.ResourceEstimate{}
	if remote || local {
		estimate.Pods = 1
		estimate.Deployments = 1
		estimate.ReplicaSets = 1
		estimate.Services = 1
		estimate.ServiceAccounts = 1
		estimate.Secrets = 1
	}

	if twoReleaseRemote || local {
		AddHelmReleaseObjectEstimate(&estimate, 1)
	}

	if nodePort || loadBalancer {
		estimate.ServiceNodePorts = 1
	}

	if loadBalancer {
		estimate.ServiceLoadBalancers = 1
	}

	return estimate
}

// AddHelmReleaseObjectEstimate adds the namespaced storage objects used by the
// configured Helm release driver.
func AddHelmReleaseObjectEstimate(estimate *domain.ResourceEstimate, releases int) {
	if estimate == nil || releases <= 0 {
		return
	}

	switch strings.ToLower(strings.TrimSpace(os.Getenv("HELM_DRIVER"))) {
	case "configmap", "configmaps":
		estimate.ConfigMaps += releases
	case "memory", "sql":
	default:
		estimate.Secrets += releases
	}
}
