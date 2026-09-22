package planner

import (
	"fmt"
	"sort"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
)

// collectPVCConsumers shares only inventory filtering. Each operation owns
// its consumer policy and chooses whether terminating Pods still block it.
func collectPVCConsumers(
	plan checkRecorder,
	pvc *corev1.PersistentVolumeClaim,
	pods []corev1.Pod,
	listErr error,
	usesPVC func(*corev1.Pod, string) bool,
) ([]*corev1.Pod, bool) {
	if listErr != nil {
		plan.AddCheck(failed(domain.CheckNamePVCConsumers,
			fmt.Sprintf("list Pods for PVC %s/%s: %v", pvc.Namespace, pvc.Name, listErr)))
		return nil, false
	}

	consumers := make([]*corev1.Pod, 0)
	for i := range pods {
		if usesPVC(&pods[i], pvc.Name) {
			consumers = append(consumers, &pods[i])
		}
	}

	return consumers, true
}

func consumerNames(consumers []*corev1.Pod) string {
	names := make([]string, 0, len(consumers))
	for _, pod := range consumers {
		names = append(names, pod.Name)
	}

	sort.Strings(names)

	return strings.Join(names, ",")
}

func checkOfflinePVC(plan checkRecorder, pvc *corev1.PersistentVolumeClaim) {
	plan.AddCheck(passed(domain.CheckNamePVCConsumers,
		fmt.Sprintf("PVC %s/%s is offline", pvc.Namespace, pvc.Name)))
}

func checkCopyConsumers(
	plan checkRecorder,
	pvc *corev1.PersistentVolumeClaim,
	online bool,
	consumers []*corev1.Pod,
) {
	if len(consumers) == 0 {
		checkOfflinePVC(plan, pvc)
		return
	}

	if !online {
		plan.AddCheck(failed(
			domain.CheckNamePVCConsumers,
			fmt.Sprintf(
				"offline copy requires PVC %s/%s to have zero active Pod consumers; found %s; use --online for a finite warm copy",
				pvc.Namespace,
				pvc.Name,
				consumerNames(consumers),
			),
		))

		return
	}

	if kube.HasAccessMode(pvc.Spec.AccessModes, corev1.ReadWriteOncePod) {
		plan.AddCheck(failed(domain.CheckNamePVCConsumers,
			fmt.Sprintf("active RWOP PVC %s/%s cannot be warm-copied", pvc.Namespace, pvc.Name)))
		return
	}

	if kube.HasAccessMode(pvc.Spec.AccessModes, corev1.ReadWriteOnce) {
		unscheduled := make([]*corev1.Pod, 0)
		for _, pod := range consumers {
			if pod.Spec.NodeName == "" {
				unscheduled = append(unscheduled, pod)
			}
		}

		if len(unscheduled) > 0 {
			plan.AddCheck(failed(
				domain.CheckNameSourceNode,
				fmt.Sprintf(
					"RWO PVC %s/%s has unscheduled active consumer(s) %s; wait for every consumer to receive a node before online copy",
					pvc.Namespace,
					pvc.Name,
					consumerNames(unscheduled),
				),
			))

			return
		}
	}

	plan.AddCheck(warned(domain.CheckNamePVCConsumers,
		fmt.Sprintf("PVC %s/%s is active on Pod(s) %s; warm copy has file-level consistency",
			pvc.Namespace, pvc.Name, consumerNames(consumers))))
}

func checkReservationConsumers(
	plan checkRecorder,
	pvc *corev1.PersistentVolumeClaim,
	consumers []*corev1.Pod,
) {
	if len(consumers) == 0 {
		checkOfflinePVC(plan, pvc)
		return
	}

	plan.AddCheck(warned(
		domain.CheckNamePVCConsumers,
		fmt.Sprintf(
			"PVC %s/%s is active on Pod(s) %s; reservation keeps the source PVC mounted and provisions destination storage",
			pvc.Namespace,
			pvc.Name,
			consumerNames(consumers),
		),
	))
}

func checkPodMigrationConsumers(
	plan checkRecorder,
	pvc *corev1.PersistentVolumeClaim,
	sourcePod *corev1.Pod,
	workloadKind v1alpha1.WorkloadKind,
	affectedPods []v1alpha1.LocalResourceReference,
	consumers []*corev1.Pod,
) {
	if sourcePod == nil {
		return // Source discovery reports the missing selected Pod.
	}

	migrationUnit := workloadPodUIDs(affectedPods, sourcePod)

	others := migrationUnitExternalConsumers(workloadKind, migrationUnit, consumers)
	if len(others) > 0 {
		plan.AddCheck(failed(
			domain.CheckNamePVCConsumers,
			fmt.Sprintf(
				"PVC %s/%s is shared with Pod(s): %s; migrate-pod coordinates one selected workload only, so stop these external consumers or use offline migrate after quiescing every consumer",
				pvc.Namespace,
				pvc.Name,
				strings.Join(others, ","),
			),
		))

		return
	}

	plan.AddCheck(passed(domain.CheckNamePVCConsumers,
		fmt.Sprintf("PVC %s/%s belongs to the selected migration unit", pvc.Namespace, pvc.Name)))
}

func checkIdentityConsumers(
	plan checkRecorder,
	pvc *corev1.PersistentVolumeClaim,
	consumers []*corev1.Pod,
) {
	if len(consumers) == 0 {
		checkOfflinePVC(plan, pvc)
		return
	}

	plan.AddCheck(failed(domain.CheckNamePVCConsumers,
		fmt.Sprintf("PVC identity changes require PVC %s/%s to have no Pod references; found %s",
			pvc.Namespace, pvc.Name, consumerNames(consumers))))
}
