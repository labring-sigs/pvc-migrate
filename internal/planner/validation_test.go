package planner

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestCheckPVCReferencesModelsOfflineWarmCopyRWOPAndSharedUnit(t *testing.T) {
	rwo := corev1.ReadWriteOnce
	rwop := corev1.ReadWriteOncePod
	tests := []struct {
		name      string
		operation domain.Operation
		mode      corev1.PersistentVolumeAccessMode
		pods      []runtime.Object
		sourcePod *corev1.Pod
		ready     bool
		severity  domain.CheckSeverity
		message   string
	}{
		{name: "offline", operation: domain.OperationCopy, mode: rwo, ready: true, severity: domain.SeverityInfo, message: "is offline"},
		{name: "active RWO warns", operation: domain.OperationCopy, mode: rwo, pods: []runtime.Object{podWithPVC("app", "consumer", "data")}, ready: true, severity: domain.SeverityWarning, message: "warm copy has file-level consistency"},
		{name: "active RWOP fails", operation: domain.OperationCopy, mode: rwop, pods: []runtime.Object{podWithPVC("app", "consumer", "data")}, severity: domain.SeverityError, message: "cannot be warm-copied"},
		{name: "active RWOP reserve warns accurately", operation: domain.OperationReserve, mode: rwop, pods: []runtime.Object{podWithPVC("app", "consumer", "data")}, ready: true, severity: domain.SeverityWarning, message: "reservation keeps the source PVC mounted"},
		{
			name:      "selected unit has another consumer",
			operation: domain.OperationCopy,
			mode:      rwo,
			pods:      []runtime.Object{podWithPVC("app", "selected", "data"), podWithPVC("app", "other", "data")},
			sourcePod: podWithPVC("app", "selected", "data"),
			severity:  domain.SeverityError,
			message:   "shared with Pod(s): other",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"}, Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{tt.mode}}}
			plan := &domain.MigrationPlan{Ready: true}
			New(kubernetesfake.NewClientset(tt.pods...), nil).checkPVCReferences(context.Background(), plan, pvc, tt.sourcePod, tt.operation, true)
			if plan.Ready != tt.ready || len(plan.Checks) != 1 || plan.Checks[0].Severity != tt.severity || !strings.Contains(plan.Checks[0].Message, tt.message) {
				t.Fatalf("plan ready=%t checks=%#v", plan.Ready, plan.Checks)
			}
		})
	}
}

func TestCheckPVCReferencesReportsListErrors(t *testing.T) {
	client := kubernetesfake.NewClientset()
	client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("list timeout")
	})
	plan := &domain.MigrationPlan{Ready: true}
	New(client, nil).checkPVCReferences(context.Background(), plan, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"}}, nil, domain.OperationMigrate, false)
	if plan.Ready || len(plan.Checks) != 1 || !strings.Contains(plan.Checks[0].Message, "list timeout") {
		t.Fatalf("checks=%#v", plan.Checks)
	}
}

func TestPlanRejectsUnschedulableTopologyAndBlockVolumes(t *testing.T) {
	objects := plannerObjects("2Gi")
	for _, object := range objects {
		switch value := object.(type) {
		case *corev1.Node:
			value.Spec.Unschedulable = true
			value.Labels[corev1.LabelTopologyZone] = "zone-a"
		case *corev1.PersistentVolumeClaim:
			mode := corev1.PersistentVolumeBlock
			value.Spec.VolumeMode = &mode
		}
	}
	plan, err := New(plannerClient(objects...), nil).Plan(context.Background(), Options{
		SessionID: "migration", SourceNamespace: "app", StagingNamespace: "system", SessionNamespace: "system", TemporaryNamespace: "system",
		SourcePVCs: []string{"data"}, TargetNode: "node-b", DestinationClass: "fast",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, checkName := range []string{"target-node", "storage-topology", "volume-mode"} {
		if !hasFailedCheck(plan, checkName) {
			t.Fatalf("failed check %q missing: %#v", checkName, plan.Checks)
		}
	}
}

func TestCheckCSINodeTreatsMissingAndUnregisteredDriversAsWarnings(t *testing.T) {
	sc := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "fast"}, Provisioner: "example.csi.io"}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}}
	tests := []struct {
		name    string
		objects []runtime.Object
		message string
	}{
		{name: "CSINode absent", message: "has no CSINode object"},
		{name: "driver absent", objects: []runtime.Object{&storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}, Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{Name: "other.csi.io"}}}}}, message: "is absent from CSINode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := &domain.MigrationPlan{Ready: true}
			New(kubernetesfake.NewClientset(tt.objects...), nil).checkCSINode(context.Background(), plan, sc, node)
			if !plan.Ready || len(plan.Checks) != 1 || plan.Checks[0].Severity != domain.SeverityWarning || !strings.Contains(plan.Checks[0].Message, tt.message) {
				t.Fatalf("checks=%#v", plan.Checks)
			}
		})
	}
}

func TestCheckCSINodeFailsOnEmptyObject(t *testing.T) {
	client := kubernetesfake.NewClientset()
	client.PrependReactor("get", "csinodes", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})
	plan := &domain.MigrationPlan{Ready: true}
	New(client, nil).checkCSINode(context.Background(), plan,
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "fast"}, Provisioner: "example.csi.io"},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}})
	if plan.Ready || len(plan.Checks) != 1 || plan.Checks[0].Severity != domain.SeverityError || !strings.Contains(plan.Checks[0].Message, "returned an empty object") {
		t.Fatalf("plan ready=%t checks=%#v", plan.Ready, plan.Checks)
	}
}

func TestPodPVCNamesAreUniqueAndSorted(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Volumes: []corev1.Volume{
		{Name: "z", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "z-data"}}},
		{Name: "a", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "a-data"}}},
		{Name: "duplicate", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "z-data"}}},
		{Name: "empty", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{}}},
	}}}
	names := podPVCNames(pod)
	if len(names) != 2 || names[0] != "a-data" || names[1] != "z-data" {
		t.Fatalf("PVC names=%v", names)
	}
}

func podWithPVC(namespace, name, claim string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: corev1.PodSpec{NodeName: "node-a", Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
		}}}},
	}
}
