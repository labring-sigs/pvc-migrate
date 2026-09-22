package planner

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWorkflowPlansMinimalVolumeIntent(t *testing.T) {
	for _, kind := range []domain.ControllerKind{domain.ControllerKindCopy, domain.ControllerKindMigration, domain.ControllerKindReservation} {
		t.Run(string(kind), func(t *testing.T) {
			volumes := []v1alpha1.VolumeRequest{
				{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
			}
			metadata := metav1.ObjectMeta{Name: "minimal", Namespace: "app"}

			client := plannerClient(plannerObjects("2Gi")...)
			p := New(client, nil).ForController()

			var (
				resolved []v1alpha1.VolumeSpec
				image    string
				err      error
			)
			switch kind {
			case domain.ControllerKindCopy:
				object := &v1alpha1.Copy{
					ObjectMeta: metadata,
					Spec:       v1alpha1.CopySpec{Volumes: volumes},
				}

				_, err = p.PlanNamespacedCopy(t.Context(), object, "example/tool:v1")
				if object.Status.Plan != nil {
					resolved, image = object.Status.Plan.Volumes, object.Status.Plan.ToolImage
				}
			case domain.ControllerKindMigration:
				object := &v1alpha1.Migration{
					ObjectMeta: metadata,
					Spec:       v1alpha1.MigrationSpec{Volumes: volumes},
				}

				_, err = p.PlanNamespacedMigration(t.Context(), object, "example/tool:v1")
				if object.Status.Plan != nil {
					resolved, image = object.Status.Plan.Volumes, object.Status.Plan.ToolImage
				}
			case domain.ControllerKindReservation:
				object := &v1alpha1.Reservation{
					ObjectMeta: metadata,
					Spec:       v1alpha1.ReservationSpec{Volumes: volumes},
				}

				_, err = p.PlanReservation(t.Context(), object, "example/tool:v1")
				if object.Status.Plan != nil {
					resolved, image = object.Status.Plan.Volumes, object.Status.Plan.ToolImage
				}
			}

			if err != nil {
				t.Fatal(err)
			}

			if len(resolved) != 1 || resolved[0].SourcePVC.UID != "pvc-uid" ||
				resolved[0].SourcePV.UID != "pv-uid" || resolved[0].Capacity != "2Gi" || image != "example/tool:v1" {
				t.Fatalf("incomplete resolved plan: %+v image=%s", resolved, image)
			}

			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("planning mutated cluster: %v", action)
				}
			}
		})
	}
}

func TestWorkflowHonorsIdentityConstraints(t *testing.T) {
	for _, test := range []struct {
		name      string
		pvc, pv   v1alpha1.LocalResourceReference
		wantError bool
	}{
		{name: "matching", pvc: v1alpha1.LocalResourceReference{Name: "data", UID: "pvc-uid"}, pv: v1alpha1.LocalResourceReference{Name: "pv-source", UID: "pv-uid"}},
		{name: "wrong pvc uid", pvc: v1alpha1.LocalResourceReference{Name: "data", UID: "replaced"}, wantError: true},
		{name: "wrong pv", pvc: v1alpha1.LocalResourceReference{Name: "data"}, pv: v1alpha1.LocalResourceReference{Name: "different"}, wantError: true},
		{name: "stale resource version", pvc: v1alpha1.LocalResourceReference{Name: "data", ResourceVersion: "1"}, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			volume := v1alpha1.VolumeRequest{SourcePVC: test.pvc}
			if test.pv.Name != "" {
				volume.SourcePV = &test.pv
			}

			object := &v1alpha1.Copy{
				ObjectMeta: metav1.ObjectMeta{Name: "identity", Namespace: "app"},
				Spec:       v1alpha1.CopySpec{Volumes: []v1alpha1.VolumeRequest{volume}},
			}

			_, err := New(
				plannerClient(plannerObjects("2Gi")...),
				nil,
			).ForController().PlanNamespacedCopy(t.Context(), object, "example/tool:v1")
			if test.wantError {
				if domain.CategoryOf(err) != domain.ErrorConflict {
					t.Fatalf("expected identity conflict, got %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWorkflowPodSelectionAllowsPartialOverrides(t *testing.T) {
	objects := plannerObjectsWithTwoPVCs(t)
	objects = append(
		objects,
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "writer", Namespace: "app", UID: "pod-uid"},
			Spec: corev1.PodSpec{NodeName: "node-b", Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "data",
						},
					},
				},
				{
					Name: "logs",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "logs",
						},
					},
				},
			}},
		},
	)

	object := &v1alpha1.Copy{
		ObjectMeta: metav1.ObjectMeta{Name: "partial", Namespace: "app"},
		Spec: v1alpha1.CopySpec{
			Pod:    &v1alpha1.LocalResourceReference{Name: "writer", UID: "pod-uid"},
			Online: true,
			Volumes: []v1alpha1.VolumeRequest{
				{
					SourcePVC:      v1alpha1.LocalResourceReference{Name: "logs"},
					Capacity:       "3Gi",
					DestinationPVC: &v1alpha1.LocalResourceReference{Name: "logs-copy"},
					TransferScope: &v1alpha1.TransferScope{
						SourcePath:      "logs",
						DestinationPath: "saved",
					},
				},
			},
		},
	}

	report, err := New(
		plannerClient(objects...),
		nil,
	).ForController().PlanNamespacedCopy(t.Context(), object, "example/tool:v1")
	if err != nil || object.Status.Plan == nil {
		t.Fatalf("plan=%+v err=%v", report, err)
	}

	spec := object.Status.Plan

	if len(spec.Volumes) != 2 {
		t.Fatalf("selected %d volumes", len(spec.Volumes))
	}

	for _, v := range spec.Volumes {
		if v.SourcePVC.Name == "logs" {
			if v.Capacity != "3Gi" || v.DestinationPVC.Name != "logs-copy" ||
				v.TransferScope.SourcePath != "logs" {
				t.Fatalf("override lost: %+v", v)
			}
		} else if v.Capacity != "2Gi" {
			t.Fatalf("default capacity lost: %+v", v)
		}
	}
}

func TestControllerSubmissionSinglePathDefaultsToVolumeRoot(t *testing.T) {
	object := &v1alpha1.ClusterCopy{
		ObjectMeta: metav1.ObjectMeta{Name: "request"}, Spec: v1alpha1.ClusterCopySpec{
			SourceNamespace: "app", DestinationNamespace: "app", SessionNamespace: "app",
			CopySpec: v1alpha1.CopySpec{Volumes: []v1alpha1.VolumeRequest{
				{
					SourcePVC: v1alpha1.LocalResourceReference{Name: "data"},
					TransferScope: &v1alpha1.TransferScope{
						SourcePath:      "logs",
						DestinationPath: ".",
					},
				},
			}},
		},
	}

	report, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).ForController().PlanCopy(t.Context(), object, "example/tool:v1")
	if err != nil || object.Status.Plan == nil {
		t.Fatalf("plan=%+v err=%v", report, err)
	}

	if scope := object.Status.Plan.Volumes[0].TransferScope; scope == nil ||
		scope.SourcePath != "logs" ||
		scope.DestinationPath != domain.VolumeRootPath {
		t.Fatalf("unexpected scope: %+v", scope)
	}
}
