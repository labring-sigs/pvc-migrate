package planner

import (
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/controller"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMigrationPlannerOwnsConcreteOutput(t *testing.T) {
	object := &v1alpha1.ClusterMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "migration"},
		Spec: v1alpha1.ClusterMigrationSpec{
			SourceNamespace: "app", TemporaryNamespace: "system", SessionNamespace: "system",
			MigrationSpec: v1alpha1.MigrationSpec{
				TransferOptions: v1alpha1.TransferOptions{
					TargetNode: "node-b", Strategies: []string{"auto"},
					UnusedStoragePolicy: "Delete", SourcePath: "data",
				},
				Volumes: []v1alpha1.VolumeRequest{
					{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
				},
			},
		},
	}
	before := object.Spec.DeepCopy()
	report, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).PlanOfflineMigration(t.Context(), object, "")

	if err != nil || report == nil || !report.Ready || object.Status.Plan == nil {
		t.Fatalf("plan=%+v err=%v", report, err)
	}

	plan := object.Status.Plan

	if !reflect.DeepEqual(object.Spec, *before) || plan.SourceNamespace != "app" ||
		plan.TemporaryNamespace != "system" || plan.DestinationNamespace != "app" ||
		plan.UnusedStoragePolicy != "Delete" ||
		!hasPassedCheck(report.Checks, "strategy-selection") {
		t.Fatalf("spec=%+v plan=%+v checks=%+v", object.Spec, plan, report.Checks)
	}

	assertConcreteVolumeIdentity(t, plan.Volumes)

	report.Volumes[0].TransferScope.SourcePath = "changed"
	cloned := object.DeepCopy()
	cloned.Status.Plan.Volumes[0].AccessModes[0] = corev1.ReadOnlyMany
	cloned.Status.Plan.Strategies[0] = "changed"

	if plan.Volumes[0].TransferScope.SourcePath != "data" ||
		plan.Volumes[0].AccessModes[0] != corev1.ReadWriteOnce || plan.Strategies[0] == "changed" {
		t.Fatal("report or deep copy aliases the CRD plan")
	}
}

func TestCopyPlannerRejectsReplanningAfterSpecChanges(t *testing.T) {
	object := &v1alpha1.ClusterCopy{
		ObjectMeta: metav1.ObjectMeta{Name: "copy"},
		Spec: v1alpha1.ClusterCopySpec{
			SourceNamespace: "app", DestinationNamespace: "system", SessionNamespace: "system",
			CopySpec: v1alpha1.CopySpec{
				TransferOptions: v1alpha1.TransferOptions{TargetNode: "node-b"},
				Volumes: []v1alpha1.VolumeRequest{
					{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
				},
			},
		},
	}
	p := New(plannerClient(plannerObjects("2Gi")...), nil)
	report, err := p.PlanCopy(t.Context(), object, "")

	if err != nil || report == nil || !report.Ready || object.Status.Plan == nil {
		t.Fatalf("plan=%+v err=%v", report, err)
	}

	assertConcreteVolumeIdentity(t, object.Status.Plan.Volumes)
	before := object.Status.DeepCopy()
	object.Spec.Volumes[0].SourcePVC.UID = "replaced"

	if _, err := p.PlanCopy(
		t.Context(),
		object,
		"",
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("replanning error=%v", err)
	}

	if !reflect.DeepEqual(object.Status, *before) {
		t.Fatal("replanning replaced the saved plan")
	}

	object.Spec.Volumes[0].SourcePVC.UID = ""
	object.Spec.TargetNode = "missing"
	report, err = p.PlanCopy(t.Context(), object, "")

	if domain.CategoryOf(err) != domain.ErrorPrecondition || report != nil ||
		!reflect.DeepEqual(object.Status, *before) {
		t.Fatalf("failed planning changed saved status: report=%+v err=%v", report, err)
	}
}

func TestReservationPlannerWritesConcretePlan(t *testing.T) {
	object := &v1alpha1.ClusterReservation{
		ObjectMeta: metav1.ObjectMeta{Name: "reservation"},
		Spec: v1alpha1.ClusterReservationSpec{
			SourceNamespace: "app", DestinationNamespace: "system",
			ReservationSpec: v1alpha1.ReservationSpec{
				TransferOptions: v1alpha1.TransferOptions{
					TargetNode:          "node-b",
					UnusedStoragePolicy: "Keep",
				},
				Volumes: []v1alpha1.VolumeRequest{
					{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
				},
			},
		},
	}
	before := object.Spec.DeepCopy()
	report, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).PlanReserve(t.Context(), object, "")

	if err != nil || report == nil || !report.Ready || object.Status.Plan == nil {
		t.Fatalf("plan=%+v err=%v", report, err)
	}

	plan := object.Status.Plan

	if !reflect.DeepEqual(object.Spec, *before) || plan.SessionNamespace != "app" ||
		plan.DestinationNamespace != "system" || plan.UnusedStoragePolicy != "Keep" {
		t.Fatalf("spec=%+v plan=%+v", object.Spec, plan)
	}

	assertConcreteVolumeIdentity(t, plan.Volumes)
}

func TestPodMigrationPlannerPreservesZeroPrecopyInPlan(t *testing.T) {
	p, object := podMigrationPlanFixture(t)
	before := object.Spec.DeepCopy()
	report, err := p.PlanPodMigration(t.Context(), object, "")

	if err != nil || report == nil || !report.Ready || object.Status.Plan == nil {
		t.Fatalf("plan=%+v err=%v", report, err)
	}

	plan := object.Status.Plan

	if !reflect.DeepEqual(object.Spec, *before) || plan.PrecopyPasses != 0 ||
		plan.Workload.Pod == nil || plan.Workload.Pod.UID != "writer-uid" {
		t.Fatalf("spec=%+v plan=%+v", object.Spec, plan)
	}

	assertConcreteVolumeIdentity(t, plan.Volumes)

	report.Workload.Pod.UID = "changed-report"
	if plan.Workload.Pod.UID != "writer-uid" {
		t.Fatal("report workload aliases the persisted CRD plan")
	}
}

func TestTransferZonePolicyOwnedByPodMigration(t *testing.T) {
	p, object := podMigrationPlanFixture(t)

	report, err := p.PlanPodMigration(t.Context(), object, "")
	if err != nil || !report.Ready || object.Status.Plan == nil {
		t.Fatalf("initial plan=%+v err=%v", report, err)
	}

	previous := object.Status.DeepCopy()

	node, err := p.client.CoreV1().Nodes().Get(t.Context(), "node-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	node.Labels[corev1.LabelTopologyZone] = "zone-a"
	if _, err := p.client.CoreV1().
		Nodes().
		Update(t.Context(), node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	candidate := &v1alpha1.ClusterPodMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "cross-zone"},
		Spec:       *object.Spec.DeepCopy(),
	}

	report, err = p.PlanPodMigration(t.Context(), candidate, "")
	if err != nil || report.Ready || !hasFailedCheck(report.Checks, "availability-zone") ||
		candidate.Status.Plan != nil || !reflect.DeepEqual(previous, &object.Status) {
		t.Fatalf("cross-zone plan=%+v status=%+v err=%v", report, object.Status, err)
	}

	if err := p.client.CoreV1().
		Pods("app").
		Delete(t.Context(), "writer", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	migration := &v1alpha1.ClusterMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "offline-migration"},
		Spec: v1alpha1.ClusterMigrationSpec{
			SourceNamespace: "app", TemporaryNamespace: "system", SessionNamespace: "system",
			MigrationSpec: v1alpha1.MigrationSpec{
				TransferOptions: v1alpha1.TransferOptions{
					SourceNode: "node-a",
					TargetNode: "node-b",
				},
				Volumes: []v1alpha1.VolumeRequest{
					{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
				},
			},
		},
	}

	report, err = p.PlanOfflineMigration(t.Context(), migration, "")
	if err != nil || !report.Ready || migration.Status.Plan == nil ||
		hasFailedCheck(
			report.Checks,
			"availability-zone",
		) || hasWarningCheck(
		report.Checks, "availability-zone",
	) {
		t.Fatalf("offline cross-zone plan=%+v err=%v", report, err)
	}
}

func TestPodConsumerCountsRemainAlignedWhenSourceResolutionFails(t *testing.T) {
	p, object := podMigrationPlanFixture(t)

	pod, err := p.client.CoreV1().Pods("app").Get(t.Context(), "writer", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: "missing",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: "aaa-missing",
			},
		},
	})
	if _, err := p.client.CoreV1().
		Pods("app").
		Update(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	report, err := p.PlanPodMigration(t.Context(), object, "")
	if err != nil {
		t.Fatal(err)
	}

	if report.Ready || object.Status.Plan != nil || len(report.Volumes) != 1 ||
		report.Volumes[0].SourcePVC.Name != "data" || report.Volumes[0].ConcurrentConsumers != 1 {
		t.Fatalf("ready=%t volumes=%+v checks=%+v", report.Ready, report.Volumes, report.Checks)
	}
}

func podMigrationPlanFixture(t *testing.T) (*Planner, *v1alpha1.ClusterPodMigration) {
	t.Helper()

	objects := plannerObjects("2Gi")
	node := testutil.MustType[*corev1.Node](t, objects[2]).DeepCopy()
	node.Name = "node-a"
	node.Labels[corev1.LabelHostname] = "node-a"
	pod := podWithPVC("writer")
	pod.Status = corev1.PodStatus{
		Phase:      corev1.PodRunning,
		Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
	}
	objects = append(objects, node, pod, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "app"},
	})
	client := plannerClient(objects...)
	object := &v1alpha1.ClusterPodMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-migration"},
		Spec: v1alpha1.ClusterPodMigrationSpec{
			SourceNamespace: "app", TemporaryNamespace: "system", SessionNamespace: "system",
			PodMigrationSpec: v1alpha1.PodMigrationSpec{
				Pod:             v1alpha1.LocalResourceReference{Name: "writer"},
				TransferOptions: v1alpha1.TransferOptions{TargetNode: "node-b"},
				PrecopyPasses:   0,
			},
		},
	}

	return New(client, controller.NewManager(client, nil, nil)), object
}

func assertConcreteVolumeIdentity(t *testing.T, volumes []v1alpha1.VolumeSpec) {
	t.Helper()

	if len(volumes) != 1 || volumes[0].SourcePVC.UID != "pvc-uid" ||
		volumes[0].SourcePVC.ResourceVersion != "10" || volumes[0].SourcePV.UID != "pv-uid" ||
		volumes[0].SourcePV.ResourceVersion != "20" || volumes[0].Capacity != "2Gi" {
		t.Fatalf("resolved volumes=%+v", volumes)
	}
}
