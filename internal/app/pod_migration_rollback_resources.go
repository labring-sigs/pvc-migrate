package app

import (
	"context"
	"fmt"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func podRollbackNeedsPause(
	phase, resumeFrom v1alpha1.WorkflowPhase,
	history []v1alpha1.WorkflowHistoryEntry,
) bool {
	if phase == domain.PhaseFailed {
		phase = resumeFrom
	}

	if phase == domain.PhaseRollingBack {
		phase = phaseBefore(history, domain.PhaseRollingBack)
	}

	return phase == domain.PhaseResuming || phase == domain.PhaseCompleted
}

func refreshRollbackPodReferences(
	adapter v1alpha1.WorkloadKind,
	pod v1alpha1.ObjectReference,
	affected []v1alpha1.ObjectReference,
	current []v1alpha1.ObjectReference,
) (v1alpha1.ObjectReference, []v1alpha1.ObjectReference) {
	affected = slices.Clone(affected)
	if len(current) == 0 {
		return pod, affected
	}

	switch adapter {
	case v1alpha1.WorkloadDeployment, v1alpha1.WorkloadGrafana:
		affected = slices.Clone(current)
		pod = current[0]
	default:
		byName := make(map[string]v1alpha1.ObjectReference, len(current))
		for _, ref := range current {
			byName[ref.Namespace+"/"+ref.Name] = ref
		}

		if ref, ok := byName[pod.Namespace+"/"+pod.Name]; ok {
			pod = ref
		}

		if len(affected) == 0 {
			affected = slices.Clone(current)
		} else {
			seen := make(map[string]struct{}, len(affected))
			for index, ref := range affected {
				key := ref.Namespace + "/" + ref.Name
				seen[key] = struct{}{}

				if updated, ok := byName[key]; ok {
					affected[index] = updated
				}
			}

			for _, ref := range current {
				key := ref.Namespace + "/" + ref.Name

				if _, ok := seen[key]; !ok {
					affected = append(affected, ref)
				}
			}
		}
	}

	return pod, affected
}

// Discover current workload identities and reject consumers outside its pause
// scope before any workload or storage mutation.
func (m *podMigrationResources) rollbackWorkloadCheckpoint(
	ctx context.Context,
	owner, namespace string,
	workload v1alpha1.WorkloadSpec,
	volumes []v1alpha1.VolumeSpec,
) (*v1alpha1.PodMigrationWorkloadStatus, error) {
	if m.workloads == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			"rollback pod migration",
			"workload controller is required",
		)
	}

	current, err := m.workloads.CurrentRollbackPods(ctx, owner, namespace, workload)
	if err != nil {
		return nil, err
	}

	allowed := make(map[string]types.UID)
	pod := qualifiedResourceReference(*workload.Pod, namespace)

	affected := make([]v1alpha1.ObjectReference, len(workload.AffectedPods))
	for i, ref := range workload.AffectedPods {
		affected[i] = qualifiedResourceReference(ref, namespace)
	}

	for _, ref := range append(append([]v1alpha1.ObjectReference{pod}, affected...), current...) {
		if ref.Namespace != namespace || ref.Name == "" || ref.UID == "" {
			return nil, domain.NewError(
				domain.ErrorConflict,
				"rollback pod migration",
				"current workload Pod identity is outside the source namespace or incomplete",
			)
		}

		allowed[ref.Name] = ref.UID
	}

	pods, err := m.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"rollback pod migration",
			"list workload consumers",
			err,
		)
	}

	for _, volume := range volumes {
		for _, consumer := range pods.Items {
			uid, controlled := allowed[consumer.Name]
			if kube.PodPreventsSafePVCDeletion(&consumer, volume.SourcePVC.Name) &&
				(!controlled || uid != consumer.UID) {
				return nil, domain.NewError(
					domain.ErrorPrecondition,
					"rollback pod migration",
					fmt.Sprintf(
						"PVC %s/%s is referenced by Pod %s outside the recorded workload pause scope",
						namespace,
						volume.SourcePVC.Name,
						consumer.Name,
					),
				)
			}
		}
	}

	updated, refs := refreshRollbackPodReferences(workload.Adapter, pod, affected, current)
	if updated == pod && slices.Equal(refs, affected) {
		return nil, nil
	}

	checkpoint := &v1alpha1.PodMigrationWorkloadStatus{Pod: localResourceReference(updated)}
	if refs != nil {
		checkpoint.AffectedPods = make([]v1alpha1.LocalResourceReference, len(refs))
		for i, ref := range refs {
			checkpoint.AffectedPods[i] = *localResourceReference(ref)
		}
	}

	return checkpoint, nil
}
