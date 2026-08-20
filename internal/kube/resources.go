package kube

import (
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
