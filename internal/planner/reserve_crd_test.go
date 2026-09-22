package planner

import (
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestReservationPodSelectionDoesNotRequireWorkloadAdapter(t *testing.T) {
	controller := true
	objects := append(plannerObjects("2Gi"), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "writer", Namespace: "app", UID: "writer-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "example.io/v1", Kind: "CustomDatabase", Name: "database",
				UID: "database-uid", Controller: &controller,
			}},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-b",
			// The source Pod is only a volume selector. Its placement and
			// ServiceAccount are irrelevant to destination PVC reservation.
			NodeSelector: map[string]string{"application-only": "true"},
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data",
					},
				},
			}},
		},
	})
	// A Pod selects the PVC set for Copy/Reservation; it does not grant
	// exclusive ownership of those PVCs to the selected workload.
	sibling := podWithPVC("independent-writer")
	sibling.Spec.NodeName = "node-b"
	objects = append(objects, sibling)
	client := plannerClient(objects...)

	reservation := &v1alpha1.ClusterReservation{
		ObjectMeta: metav1.ObjectMeta{Name: "reserve-pod"},
		Spec: v1alpha1.ClusterReservationSpec{
			SourceNamespace: "app", DestinationNamespace: "app", SessionNamespace: "app",
			ReservationSpec: v1alpha1.ReservationSpec{
				Pod: &v1alpha1.LocalResourceReference{Name: "writer", UID: "writer-uid"},
			},
		},
	}

	plan, err := New(client, nil).PlanReserve(t.Context(), reservation, "example/tool:v1")
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready {
		t.Fatalf("reservation failed: %+v", plan.Checks)
	}

	if plan.Workload.Adapter != v1alpha1.WorkloadNone {
		t.Fatalf("reservation acquired workload controls: %+v", plan.Workload)
	}

	volumes := reservation.Status.Plan.Volumes
	if len(volumes) != 1 || volumes[0].SourcePVC.UID != "pvc-uid" {
		t.Fatalf("incorrect selected PVCs: %+v", volumes)
	}

	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" &&
			action.GetResource().Resource != "selfsubjectaccessreviews" {
			t.Fatalf("reservation planning mutated cluster: %v", action)
		}
	}

	copyObject := &v1alpha1.ClusterCopy{
		ObjectMeta: metav1.ObjectMeta{Name: "copy-pod"},
		Spec: v1alpha1.ClusterCopySpec{
			SourceNamespace: "app", DestinationNamespace: "app", SessionNamespace: "app",
			CopySpec: v1alpha1.CopySpec{
				Pod:    &v1alpha1.LocalResourceReference{Name: "writer", UID: "writer-uid"},
				Online: true,
			},
		},
	}

	copyPlan, err := New(client, nil).PlanCopy(t.Context(), copyObject, "example/tool:v1")
	if err != nil {
		t.Fatal(err)
	}

	if !copyPlan.Ready || copyPlan.Workload.Adapter != v1alpha1.WorkloadNone {
		t.Fatalf("copy acquired workload migration requirements: %+v", copyPlan.Checks)
	}

	for _, volume := range copyObject.Status.Plan.Volumes {
		if volume.ConcurrentConsumers != 0 {
			t.Fatalf("copy acquired workload activation state: %+v", volume)
		}
	}

	sibling.Spec.NodeName = "node-a"
	if _, err := client.CoreV1().
		Pods("app").
		Update(t.Context(), sibling, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	copyPlan, err = New(client, nil).PlanCopy(t.Context(), &v1alpha1.ClusterCopy{
		ObjectMeta: metav1.ObjectMeta{Name: "copy-pod"},
		Spec: v1alpha1.ClusterCopySpec{
			SourceNamespace: "app", DestinationNamespace: "app", SessionNamespace: "app",
			CopySpec: v1alpha1.CopySpec{
				Pod: &v1alpha1.LocalResourceReference{
					Name: "writer",
					UID:  "writer-uid",
				},
				Online: true,
			},
		},
	}, "example/tool:v1")
	if err != nil {
		t.Fatal(err)
	}

	if !hasFailedCheck(copyPlan.Checks, "source-node") {
		t.Fatalf("Pod selection bypassed online copy node constraints: %+v", copyPlan.Checks)
	}
}

func TestReservationPlannerPreservesInputAndChecksIdentity(t *testing.T) {
	object := &v1alpha1.ClusterReservation{
		ObjectMeta: metav1.ObjectMeta{Name: "reserve"},
		Spec: v1alpha1.ClusterReservationSpec{
			SourceNamespace: "app", DestinationNamespace: "app", SessionNamespace: "app",
			ReservationSpec: v1alpha1.ReservationSpec{
				Volumes: []v1alpha1.VolumeRequest{
					{SourcePVC: v1alpha1.LocalResourceReference{Name: "data", UID: "replaced"}},
				},
				TransferOptions: v1alpha1.TransferOptions{Strategies: []string{"mount"}},
			},
		},
	}
	before := object.DeepCopy()

	_, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).PlanReserve(t.Context(), object, "example/tool:v1")
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("reservation accepted changed source identity: %v", err)
	}

	if !reflect.DeepEqual(object, before) {
		t.Fatal("planning mutated the submitted CRD")
	}
}

func TestReservationPlannerRejectsExistingExecutionBeforeDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*v1alpha1.ClusterReservation)
	}{
		{"existing plan", func(object *v1alpha1.ClusterReservation) { object.Status.Plan = &v1alpha1.ClusterReservationPlan{} }},
		{"execution checkpoint", func(object *v1alpha1.ClusterReservation) {
			object.Status.Volumes = []v1alpha1.ClusterReservationVolumeStatus{{SourcePVCName: "data"}}
		}},
		{"reserving", func(object *v1alpha1.ClusterReservation) { object.Status.Phase = domain.PhaseReserving }},
		{"failed execution", func(object *v1alpha1.ClusterReservation) {
			object.Status.Phase = domain.PhaseFailed
			object.Status.ResumeFrom = domain.PhaseReserving
		}},
		{"deleting", func(object *v1alpha1.ClusterReservation) {
			now := metav1.Now()
			object.DeletionTimestamp = &now
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			object := &v1alpha1.ClusterReservation{
				ObjectMeta: metav1.ObjectMeta{Name: "reservation"},
			}
			tc.mutate(object)
			before := object.DeepCopy()

			_, err := New(nil, nil).PlanReserve(t.Context(), object, "")
			if domain.CategoryOf(err) != domain.ErrorPrecondition {
				t.Fatalf("error = %v", err)
			}

			if !reflect.DeepEqual(object, before) {
				t.Fatal("rejected planning changed the execution checkpoint")
			}
		})
	}
}
