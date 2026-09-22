package planner

import (
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestBackupPlanningUsesOnlyItsCRDRequestAndPlan(t *testing.T) {
	client := plannerClient(plannerObjects("2Gi")...)
	planner := New(client, nil)
	object := &v1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "backup", Namespace: "app"},
		Spec: v1alpha1.BackupSpec{
			SourcePVC:     v1alpha1.LocalResourceReference{Name: "data"},
			RepositoryRef: v1alpha1.LocalObjectReference{Name: "archive"},
			Name:          "daily", Path: "db", Online: true, OpenEBSLVMEnableShared: true,
		},
	}

	before := object.DeepCopy()
	if err := planner.PlanBackup(t.Context(), object, "example/tool:v1"); err != nil {
		t.Fatal(err)
	}

	plan := object.Status.Plan
	if !reflect.DeepEqual(object.Spec, before.Spec) || plan == nil ||
		plan.SourcePVC.UID != "pvc-uid" || plan.SourcePV.UID != "pv-uid" ||
		plan.Path != "db" || plan.Name != "daily" || plan.RepositoryRef.Name != "archive" ||
		!plan.Online || !plan.OpenEBSLVMEnableShared || plan.ToolImage != "example/tool:v1" {
		t.Fatalf("incomplete or changed backup contract: %+v", object)
	}

	planned := object.DeepCopy()
	if err := planner.PlanBackup(
		t.Context(),
		object,
		"example/tool:v2",
	); err == nil ||
		!reflect.DeepEqual(object, planned) {
		t.Fatal("replanning replaced a resolved backup identity")
	}

	for _, test := range []struct {
		name   string
		change func(*v1alpha1.Backup)
	}{
		{"wrong PVC UID", func(o *v1alpha1.Backup) { o.Spec.SourcePVC.UID = "replacement" }},
		{"wrong PV UID", func(o *v1alpha1.Backup) {
			o.Spec.SourcePV = &v1alpha1.LocalResourceReference{Name: "pv-source", UID: "replacement"}
		}},
		{"execution started", func(o *v1alpha1.Backup) { o.Status.Phase = domain.PhaseWarmCopying }},
		{"shared mount checkpoint", func(o *v1alpha1.Backup) {
			o.Status.OpenEBSLVMSharedMounts = []v1alpha1.SharedMountStatus{{}}
		}},
		{"deleting", func(o *v1alpha1.Backup) { now := metav1.Now(); o.DeletionTimestamp = &now }},
		{"missing repository", func(o *v1alpha1.Backup) { o.Spec.RepositoryRef.Name = "" }},
		{"offline shared mount", func(o *v1alpha1.Backup) { o.Spec.Online = false }},
	} {
		t.Run(test.name, func(t *testing.T) {
			object := before.DeepCopy()
			test.change(object)

			previous := object.DeepCopy()
			if err := planner.PlanBackup(
				t.Context(),
				object,
				"example/tool:v1",
			); err == nil ||
				!reflect.DeepEqual(object, previous) {
				t.Fatal("invalid backup request changed the object or was accepted")
			}
		})
	}

	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("planning mutated a cluster resource: %+v", action)
		}
	}
}

func TestRestorePlanningPinsExistingDestinationAndHonorsConstraints(t *testing.T) {
	for _, test := range []struct {
		name             string
		existing, create bool
		uid              string
		fail             bool
	}{
		{"existing", true, false, "", false},
		{"existing create", true, true, "pvc-uid", false},
		{"wrong uid", true, true, "replaced", true},
		{"create missing", false, true, "", false},
		{"pinned missing", false, true, "pvc-uid", true},
		{"missing", false, false, "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			object := &v1alpha1.Restore{
				ObjectMeta: metav1.ObjectMeta{Name: "restore", Namespace: "app"},
				Spec: v1alpha1.RestoreSpec{
					DestinationPVC: v1alpha1.LocalResourceReference{
						Name: "data",
						UID:  types.UID(test.uid),
					},
					RepositoryRef: v1alpha1.LocalObjectReference{Name: "repository"},
					Name:          "archive",
					CreatePVC:     test.create,
				},
			}
			before := object.DeepCopy()

			client := plannerClient()
			if test.existing {
				client = plannerClient(plannerObjects("2Gi")...)
			}

			planner := New(client, nil)

			err := planner.PlanRestore(t.Context(), object, "example/tool:v1")
			if (err != nil) != test.fail || !reflect.DeepEqual(object.Spec, before.Spec) {
				t.Fatalf(
					"unexpected result or mutated spec: error=%v want failure=%v",
					err,
					test.fail,
				)
			}

			if err != nil {
				if !reflect.DeepEqual(object, before) {
					t.Fatal("failed planning changed status")
				}
				return
			}

			plan := object.Status.Plan
			if plan == nil || (test.existing && plan.DestinationPVC.UID != "pvc-uid") {
				t.Fatal("destination identity not pinned")
			}

			if test.create && plan.DestinationAccessMode != string(corev1.ReadWriteOnce) {
				t.Fatal("creation access mode not resolved")
			}

			planned := object.DeepCopy()
			if err := planner.PlanRestore(
				t.Context(),
				object,
				"example/tool:v2",
			); err == nil ||
				!reflect.DeepEqual(object, planned) {
				t.Fatal("replanning replaced a restore checkpoint")
			}
		})
	}
}
