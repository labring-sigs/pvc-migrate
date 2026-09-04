package planner

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/controller"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func plannerClient(objects ...runtime.Object) *kubernetesfake.Clientset {
	for _, object := range objects {
		quota, ok := object.(*corev1.ResourceQuota)
		if !ok {
			continue
		}

		quota.Status.Hard = quota.Spec.Hard.DeepCopy()
		if quota.Status.Used == nil {
			quota.Status.Used = corev1.ResourceList{}
		}

		for name := range quota.Status.Hard {
			if _, exists := quota.Status.Used[name]; !exists {
				quota.Status.Used[name] = resource.MustParse("0")
			}
		}
	}

	client := kubernetesfake.NewClientset(objects...)
	client.PrependReactor(
		"create",
		"selfsubjectaccessreviews",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			review, err := testutil.ActionObject[*authorizationv1.SelfSubjectAccessReview](action)
			if err != nil {
				return true, nil, err
			}

			review = review.DeepCopy()
			review.Status.Allowed = true

			return true, review, nil
		},
	)

	return client
}

func TestDestinationPVCNameTrimsTruncatedDNSBoundaries(t *testing.T) {
	name := destinationPVCNameFor(
		planOptions{SessionID: "pod-full-v2-20260807"},
		nil,
		"pod-data-a",
		0,
	)
	if name != "pod-data-a-migrated-pod-full-v2" {
		t.Fatalf("name=%q", name)
	}

	if problems := validation.IsDNS1123Subdomain(name); len(problems) != 0 {
		t.Fatalf("name=%q is invalid: %v", name, problems)
	}

	long := destinationPVCNameFor(
		planOptions{SessionID: "migration-20260807"},
		nil,
		strings.Repeat("a", 250),
		0,
	)
	if problems := validation.IsDNS1123Subdomain(
		long,
	); len(problems) != 0 ||
		strings.HasSuffix(long, "-") {
		t.Fatalf("long name=%q problems=%v", long, problems)
	}
}

func TestPlanLogsLongRunningChecksBeforeTheyRun(t *testing.T) {
	var logs bytes.Buffer

	planner := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).WithLogger(slog.New(slog.NewTextHandler(&logs, nil)))

	plan, err := planner.plan(context.Background(), planOptions{
		SessionID:          "migration",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready {
		t.Fatalf("checks=%#v", plan.Checks)
	}

	output := logs.String()
	for _, event := range []string{
		"migration planning started",
		"loading migration cluster inventory",
		"loading CSI storage capacity",
		"validating migration cluster policies",
		"checking migration RBAC permissions",
	} {
		if !strings.Contains(output, event) {
			t.Fatalf("logs missing %q: %s", event, output)
		}
	}
}

func TestPlanOfflineMigrationBypassesWorkloadDiscoveryAndAllowsKubeBlocksCapacityChange(
	t *testing.T,
) {
	objects := plannerObjects("2Gi")
	for _, object := range objects {
		pvc, ok := object.(*corev1.PersistentVolumeClaim)
		if !ok {
			continue
		}

		pvc.Labels = map[string]string{kube.ManagedByLabel: "kubeblocks"}
	}

	plan, err := New(plannerClient(objects...), nil).PlanOfflineMigration(
		context.Background(),
		OfflineMigrationOptions{
			SessionID:             "offline-migration",
			SourceNamespace:       "app",
			TemporaryNamespace:    "system",
			DestinationNamespace:  "app",
			SessionNamespace:      "system",
			StagingNamespace:      "system",
			SourcePVCs:            []string{"data"},
			DestinationCapacities: []string{"3Gi"},
			TargetNode:            "node-b",
			DestinationClass:      "fast",
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready {
		t.Fatalf("offline checks=%#v", plan.Checks)
	}

	if plan.SessionSpec.Type != domain.SessionTypeMigrate || plan.SessionSpec.Migrate == nil ||
		plan.SessionSpec.MigratePod != nil ||
		plan.SessionSpec.Workload().Adapter != domain.WorkloadNone {
		t.Fatalf("offline session spec=%#v", plan.SessionSpec)
	}

	if got := plan.SessionSpec.Volumes[0].Capacity; got != "3Gi" {
		t.Fatalf("offline KubeBlocks-labeled capacity=%s want=3Gi", got)
	}

	if hasFailedCheck(plan, "destination-capacity") {
		t.Fatalf("offline capacity was restricted by KubeBlocks ownership: %#v", plan.Checks)
	}
}

func TestPlanRejectsUnsupportedStrategiesAndDestinationCount(t *testing.T) {
	client := plannerClient(plannerObjects("2Gi")...)

	plan, err := New(client, nil).plan(context.Background(), planOptions{
		SessionID:          "migration",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		DestinationPVCs:    []string{"data-new", "extra"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
		Strategies:         []string{"mount", "unsupported"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "strategy") || !hasFailedCheck(plan, "destination-pvc") {
		t.Fatalf("plan should reject invalid arguments: %#v", plan.Checks)
	}
}

func TestPlanRejectsKnownLocalProvisionerAccessModeMismatch(t *testing.T) {
	objects := plannerObjects("2Gi")
	storageClass := testutil.MustType[*storagev1.StorageClass](t, objects[3])
	storageClass.Provisioner = kube.OpenEBSLocalPVProvisioner
	pvc := testutil.MustType[*corev1.PersistentVolumeClaim](t, objects[5])
	pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}

	plan, err := New(plannerClient(objects...), nil).PlanReserve(
		context.Background(),
		ReserveOptions{
			SessionID:          "reserve-access-mode",
			SourceNamespace:    "app",
			TemporaryNamespace: "system",
			SessionNamespace:   "system",
			StagingNamespace:   "system",
			SourcePVCs:         []string{"data"},
			TargetNode:         "node-b",
			DestinationClass:   "fast",
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "destination-access-modes") ||
		!hasFailedCheckContaining(plan, "destination-access-modes", "ReadWriteMany") {
		t.Fatalf("local provisioner access-mode mismatch was accepted: %#v", plan.Checks)
	}

	if hasFailedCheck(plan, "target-node") {
		t.Fatalf("access-mode failure added unrelated target-node guidance: %#v", plan.Checks)
	}
}

func TestPlanRejectsNegativePrecopyPasses(t *testing.T) {
	plan, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).plan(context.Background(), planOptions{
		SessionID:          "migration",
		Operation:          domain.OperationMigrate,
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs: []string{
			"data",
		},
		TargetNode:       "node-b",
		DestinationClass: "fast",
		PrecopyPasses:    -1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "precopy-passes") {
		t.Fatalf("negative precopy passes were accepted: %#v", plan.Checks)
	}
}

func TestPlanChecksOnlyRequiredOpenEBSLVMRBACForWarmCopy(t *testing.T) {
	for _, test := range []struct {
		name           string
		enableShared   bool
		shared         bool
		activeConsumer bool
		deniedVerb     string
		wantDenied     bool
	}{
		{name: "inspect", activeConsumer: true, deniedVerb: "list", wantDenied: true},
		{name: "enable shared for active unshared volume", enableShared: true, activeConsumer: true, deniedVerb: "patch", wantDenied: true},
		{name: "enable shared for already shared volume", enableShared: true, shared: true, activeConsumer: true, deniedVerb: "patch"},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects := plannerObjects("2Gi")
			objects = append(objects, &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{
					corev1.LabelHostname: "node-a", corev1.LabelTopologyZone: "zone-b",
				}},
				Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
					Type: corev1.NodeReady, Status: corev1.ConditionTrue,
				}}},
			})
			storageClass := testutil.MustType[*storagev1.StorageClass](t, objects[3])
			storageClass.Provisioner = "local.csi.openebs.io"

			testutil.MustType[*corev1.PersistentVolume](t, objects[6]).Spec.CSI = &corev1.CSIPersistentVolumeSource{
				Driver:       kube.OpenEBSLVMCSIDriver,
				VolumeHandle: "pv-source",
			}
			if test.activeConsumer {
				objects = append(objects, podWithPVC("database-0"))
			}

			client := kubernetesfake.NewClientset(objects...)
			client.PrependReactor(
				"create",
				"selfsubjectaccessreviews",
				func(action clienttesting.Action) (bool, runtime.Object, error) {
					review := testutil.MustActionObject[*authorizationv1.SelfSubjectAccessReview](
						t,
						action,
					).DeepCopy()
					attributes := review.Spec.ResourceAttributes
					review.Status.Allowed = attributes.Group != "local.openebs.io" ||
						attributes.Resource != "lvmvolumes" ||
						attributes.Verb != test.deniedVerb

					return true, review, nil
				},
			)

			plan, err := New(
				client,
				controller.NewManager(client, nil, nil),
			).WithOpenEBSLVMSharedVolumeManager(plannerOpenEBSLVMSharedVolumeManager{shared: test.shared}).
				plan(context.Background(), planOptions{
					SessionID:              "migration",
					Operation:              domain.OperationMigratePod,
					SourceNamespace:        "app",
					TemporaryNamespace:     "system",
					StagingNamespace:       "system",
					SessionNamespace:       "system",
					PodName:                "database-0",
					TargetNode:             "node-b",
					DestinationClass:       "fast",
					PrecopyPasses:          1,
					OpenEBSLVMEnableShared: test.enableShared,
				})
			if err != nil {
				t.Fatal(err)
			}

			if test.wantDenied {
				if plan.Ready || !hasFailedCheckContaining(plan, "rbac", test.deniedVerb) ||
					!hasFailedCheckContaining(plan, "rbac", "lvmvolumes") {
					t.Fatalf("RBAC checks=%#v", plan.Checks)
				}

				return
			}

			if hasFailedCheck(plan, "rbac") {
				t.Fatalf("RBAC checks=%#v", plan.Checks)
			}
		})
	}
}

func TestPlanPreservesExplicitPVCMappingAndRejectsDuplicateDestinations(t *testing.T) {
	objects := plannerObjects("2Gi")
	dataPVC := testutil.MustType[*corev1.PersistentVolumeClaim](t, objects[5])
	dataPV := testutil.MustType[*corev1.PersistentVolume](t, objects[6])
	logsPVC := dataPVC.DeepCopy()
	logsPVC.Name = "logs"
	logsPVC.UID = types.UID("logs-pvc-uid")
	logsPVC.Spec.VolumeName = "pv-logs"
	logsPV := dataPV.DeepCopy()
	logsPV.Name = "pv-logs"
	logsPV.UID = types.UID("logs-pv-uid")
	logsPV.Spec.ClaimRef = &corev1.ObjectReference{Namespace: "app", Name: "logs", UID: logsPVC.UID}
	objects = append(objects, logsPVC, logsPV)

	base := planOptions{
		SessionID:          "migration",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"logs", "data"},
		DestinationPVCs:    []string{"logs=logs-new", "data=data-new"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
	}

	plan, err := New(plannerClient(objects...), nil).plan(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready || len(plan.Volumes) != 2 {
		t.Fatalf("plan checks=%#v volumes=%#v", plan.Checks, plan.Volumes)
	}

	if plan.Volumes[0].SourcePVC.Name != "logs" ||
		plan.Volumes[0].DestinationPVC.Name != "logs-new" ||
		plan.Volumes[1].SourcePVC.Name != "data" ||
		plan.Volumes[1].DestinationPVC.Name != "data-new" {
		t.Fatalf("explicit mapping was reordered: %#v", plan.Volumes)
	}

	base.DestinationPVCs = []string{"logs=shared", "data=shared"}

	duplicate, err := New(plannerClient(objects...), nil).plan(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}

	if duplicate.Ready || !hasFailedCheck(duplicate, "destination-pvc") {
		t.Fatalf("duplicate destination plan checks=%#v", duplicate.Checks)
	}
}

func TestCopySupportsOfflineAndOnlineModesAcrossNamespaces(t *testing.T) {
	objects := plannerObjects("2Gi")
	objects = append(objects,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "archive"}},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "node-a",
				Labels: map[string]string{corev1.LabelHostname: "node-a"},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "writer"},
			Spec: corev1.PodSpec{
				NodeName: "node-a",
				Volumes: []corev1.Volume{
					{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "data",
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "default"}},
	)
	base := planOptions{
		Operation:            domain.OperationCopy,
		SessionID:            "copy",
		SourceNamespace:      "app",
		TemporaryNamespace:   "archive",
		DestinationNamespace: "archive",
		StagingNamespace:     "archive",
		SessionNamespace:     "system",
		SourcePVCs:           []string{"data"},
		TargetNode:           "node-b",
		DestinationClass:     "fast",
	}

	offline, err := New(plannerClient(objects...), nil).plan(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}

	if offline.Ready || !hasFailedCheck(offline, "pvc-consumers") {
		t.Fatalf("offline copy checks=%#v", offline.Checks)
	}

	base.Online = true

	online, err := New(plannerClient(objects...), nil).plan(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}

	if !online.Ready {
		t.Fatalf("online copy checks=%#v", online.Checks)
	}

	if online.SessionSpec.WorkflowOptions().SourceNode != "node-a" ||
		!online.SessionSpec.Online() ||
		online.Volumes[0].DestinationPVC.Name != "data" {
		t.Fatalf("online session=%#v volume=%#v", online.SessionSpec, online.Volumes[0])
	}

	withPod := base
	withPod.SessionID = "copy-pod"
	withPod.SourcePVCs = nil
	withPod.PodName = "writer"
	withPod.SourceNode = ""

	selectedPod, err := New(plannerClient(objects...), nil).plan(context.Background(), withPod)
	if err != nil {
		t.Fatal(err)
	}

	if !selectedPod.Ready || selectedPod.Workload.Adapter != domain.WorkloadNone ||
		selectedPod.SessionSpec.WorkflowOptions().SourceNode != "node-a" {
		t.Fatalf("pod copy plan=%#v", selectedPod)
	}
}

func TestPlanUsesPVCProtectionBoundaryForTerminalConsumers(t *testing.T) {
	for _, test := range []struct {
		name      string
		operation domain.Operation
		nodeName  string
		wantReady bool
	}{
		{name: "migration blocks scheduled terminal Pod", operation: domain.OperationMigrate, nodeName: "node-a", wantReady: false},
		{name: "migration ignores unscheduled terminal Pod", operation: domain.OperationMigrate, wantReady: true},
		{name: "offline copy ignores scheduled terminal Pod", operation: domain.OperationCopy, nodeName: "node-a", wantReady: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects := append(plannerObjects("2Gi"), &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "terminal"},
				Spec: corev1.PodSpec{NodeName: test.nodeName, Volumes: []corev1.Volume{
					{
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "data",
							},
						},
					},
				}},
				Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
			})

			plan, err := New(plannerClient(objects...), nil).plan(context.Background(), planOptions{
				Operation:          test.operation,
				SessionID:          "terminal-consumer",
				SourceNamespace:    "app",
				TemporaryNamespace: "system",
				StagingNamespace:   "system",
				SessionNamespace:   "system",
				SourcePVCs:         []string{"data"},
				TargetNode:         "node-b",
				DestinationClass:   "fast",
			})
			if err != nil {
				t.Fatal(err)
			}

			if plan.Ready != test.wantReady {
				t.Fatalf("ready=%t checks=%#v", plan.Ready, plan.Checks)
			}
		})
	}
}

func TestOnlineCopyRejectsUnscheduledRWOConsumer(t *testing.T) {
	objects := append(plannerObjects("2Gi"), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "pending-writer"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{
			{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data",
					},
				},
			},
		}},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	})

	plan, err := New(plannerClient(objects...), nil).plan(context.Background(), planOptions{
		Operation:            domain.OperationCopy,
		SessionID:            "copy-pending",
		SourceNamespace:      "app",
		TemporaryNamespace:   "system",
		DestinationNamespace: "system",
		StagingNamespace:     "system",
		SessionNamespace:     "system",
		SourcePVCs:           []string{"data"},
		TargetNode:           "node-b",
		DestinationClass:     "fast",
		Online:               true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "source-node") {
		t.Fatalf("plan checks=%#v", plan.Checks)
	}
}

func TestPlanPodReadsTheSourcePodOnce(t *testing.T) {
	objects := plannerObjects("2Gi")
	objects = append(objects,
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "default"}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "writer"},
			Spec: corev1.PodSpec{
				NodeName: "node-b",
				Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data",
					},
				}}},
			},
		},
	)
	client := plannerClient(objects...)

	var podGets atomic.Int32
	client.PrependReactor(
		"get",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			if getAction, ok := action.(clienttesting.GetAction); ok &&
				getAction.GetName() == "writer" {
				podGets.Add(1)
			}

			return false, nil, nil
		},
	)

	_, err := New(client, nil).plan(context.Background(), planOptions{
		Operation:            domain.OperationCopy,
		SessionID:            "copy-pod",
		SourceNamespace:      "app",
		TemporaryNamespace:   "system",
		DestinationNamespace: "system",
		StagingNamespace:     "system",
		SessionNamespace:     "system",
		PodName:              "writer",
		TargetNode:           "node-b",
		DestinationClass:     "fast",
		Online:               true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if podGets.Load() != 1 {
		t.Fatalf("source Pod GETs = %d, want 1", podGets.Load())
	}
}

func TestPlanPodMigrationDerivesMultiplePVCsAsOneUnit(t *testing.T) {
	objects := plannerObjectsWithTwoPVCs(t)
	objects = append(objects,
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-a",
				Labels: map[string]string{
					corev1.LabelHostname:     "node-a",
					corev1.LabelTopologyZone: "zone-b",
				},
			},
			Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
				Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			}}},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "app",
				Name:      "application",
				UID:       types.UID("application-uid"),
			},
			Spec: corev1.PodSpec{
				NodeName: "node-a",
				Volumes: []corev1.Volume{
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
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{{
					Type: corev1.PodReady, Status: corev1.ConditionTrue,
				}},
			},
		},
	)

	client := plannerClient(objects...)

	plan, err := New(client, controller.NewManager(client, nil, nil)).PlanPodMigration(
		context.Background(),
		PodMigrationOptions{
			SessionID:          "multi-pvc-pod",
			SourceNamespace:    "app",
			TemporaryNamespace: "system",
			StagingNamespace:   "system",
			SessionNamespace:   "system",
			PodName:            "application",
			TargetNode:         "node-b",
			DestinationClass:   "fast",
			PrecopyPasses:      1,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready || len(plan.Volumes) != 2 || len(plan.SessionSpec.Volumes) != 2 {
		t.Fatalf(
			"multi-PVC Pod plan ready=%t checks=%#v volumes=%#v",
			plan.Ready,
			plan.Checks,
			plan.Volumes,
		)
	}

	got := map[string]bool{}
	for _, volume := range plan.Volumes {
		got[volume.SourcePVC.Name] = true
	}

	if !got["data"] || !got["logs"] {
		t.Fatalf("Pod PVC set=%v, want data and logs", got)
	}
}

func TestPlanPodMigrationRejectsExternalPVCConsumer(t *testing.T) {
	objects := plannerObjectsWithTwoPVCs(t)
	objects = append(objects,
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-a",
				Labels: map[string]string{
					corev1.LabelHostname:     "node-a",
					corev1.LabelTopologyZone: "zone-b",
				},
			},
			Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
				Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			}}},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "app",
				Name:      "application",
				UID:       types.UID("application-uid"),
			},
			Spec: corev1.PodSpec{
				NodeName: "node-a",
				Volumes: []corev1.Volume{
					{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "data",
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{{
					Type: corev1.PodReady, Status: corev1.ConditionTrue,
				}},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "app",
				Name:      "other-workload",
				UID:       types.UID("other-uid"),
			},
			Spec: corev1.PodSpec{
				NodeName: "node-a",
				Volumes: []corev1.Volume{
					{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "data",
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
	)

	client := plannerClient(objects...)

	plan, err := New(client, controller.NewManager(client, nil, nil)).PlanPodMigration(
		context.Background(),
		PodMigrationOptions{
			SessionID:          "external-consumer",
			SourceNamespace:    "app",
			TemporaryNamespace: "system",
			StagingNamespace:   "system",
			SessionNamespace:   "system",
			PodName:            "application",
			TargetNode:         "node-b",
			DestinationClass:   "fast",
			PrecopyPasses:      0,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheckContaining(plan, "pvc-consumers", "other-workload") ||
		!hasFailedCheckContaining(
			plan,
			"pvc-consumers",
			"migrate-pod coordinates one selected workload only",
		) {
		t.Fatalf("external consumer plan ready=%t checks=%#v", plan.Ready, plan.Checks)
	}
}

func TestSelectedMigrationUnitAllowsMultipleConsumersInSameWorkload(t *testing.T) {
	selected := podWithPVC("web-0")
	peer := podWithPVC("web-1")
	workload := domain.WorkloadSpec{
		Adapter: domain.WorkloadDeployment,
		Pod: domain.ObjectReference{
			APIVersion: "v1",
			Kind:       domain.KindPod,
			Namespace:  selected.Namespace,
			Name:       selected.Name,
			UID:        selected.UID,
		},
		AffectedPods: []domain.ObjectReference{
			{
				APIVersion: "v1",
				Kind:       domain.KindPod,
				Namespace:  selected.Namespace,
				Name:       selected.Name,
				UID:        selected.UID,
			},
			{
				APIVersion: "v1",
				Kind:       domain.KindPod,
				Namespace:  peer.Namespace,
				Name:       peer.Name,
				UID:        peer.UID,
			},
		},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		},
	}
	plan := &domain.MigrationPlan{Ready: true}
	consumers := []*corev1.Pod{selected, peer}

	checkSelectedMigrationUnitConsumers(
		plan,
		pvc,
		selected,
		workload,
		domain.OperationMigratePod,
		false,
		consumers,
	)

	if !plan.Ready || hasFailedCheck(plan, "pvc-consumers") {
		t.Fatalf("same-workload consumers should be allowed: checks=%#v", plan.Checks)
	}

	if got := migrationUnitConsumerCount(workload, selected, consumers); got != 2 {
		t.Fatalf("same-workload consumer count=%d, want 2", got)
	}
}

func TestOfflineMigrationPlansMultiplePVCsInOneSession(t *testing.T) {
	objects := plannerObjectsWithTwoPVCs(t)

	plan, err := New(plannerClient(objects...), nil).PlanOfflineMigration(
		context.Background(),
		OfflineMigrationOptions{
			SessionID:             "multi-pvc-offline",
			SourceNamespace:       "app",
			TemporaryNamespace:    "system",
			DestinationNamespace:  "app",
			StagingNamespace:      "system",
			SessionNamespace:      "system",
			SourcePVCs:            []string{"data", "logs"},
			DestinationCapacities: []string{"data=2Gi", "logs=2Gi"},
			TargetNode:            "node-b",
			DestinationClass:      "fast",
			Strategies:            []string{"clusterip"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready || len(plan.Volumes) != 2 || len(plan.SessionSpec.Volumes) != 2 {
		t.Fatalf(
			"offline multi-PVC plan ready=%t checks=%#v volumes=%#v",
			plan.Ready,
			plan.Checks,
			plan.Volumes,
		)
	}
}

func TestPlanReportsMissingSelectedPodWithFlagGuidance(t *testing.T) {
	plan, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).plan(context.Background(), planOptions{
		Operation:            domain.OperationCopy,
		SessionID:            "copy-pod",
		SourceNamespace:      "app",
		TemporaryNamespace:   "system",
		DestinationNamespace: "system",
		StagingNamespace:     "system",
		SessionNamespace:     "system",
		PodName:              "missing",
		TargetNode:           "node-b",
		DestinationClass:     "fast",
		Online:               true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !hasFailedCheckContaining(
		plan,
		"source-pod",
		"source Pod app/missing does not exist; verify --namespace and --pod",
	) {
		t.Fatalf("source Pod guidance missing: %#v", plan.Checks)
	}
}

func TestPlanHandlesEmptyInventoryObjects(t *testing.T) {
	tests := []struct {
		name     string
		resource string
		object   string
		check    domain.CheckName
	}{
		{name: "PVC", resource: "persistentvolumeclaims", object: "data", check: "source-pvc"},
		{name: "PV", resource: "persistentvolumes", object: "pv-source", check: "source-pv"},
		{name: "target node", resource: "nodes", object: "node-b", check: "target-node"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := plannerClient(plannerObjects("2Gi")...)
			client.PrependReactor(
				"get",
				test.resource,
				func(action clienttesting.Action) (bool, runtime.Object, error) {
					getAction, ok := action.(clienttesting.GetAction)
					if ok && getAction.GetName() == test.object {
						return true, nil, nil
					}

					return false, nil, nil
				},
			)

			plan, err := New(client, nil).plan(context.Background(), planOptions{
				SessionID:            "empty-object",
				SourceNamespace:      "app",
				TemporaryNamespace:   "system",
				DestinationNamespace: "system",
				StagingNamespace:     "system",
				SessionNamespace:     "system",
				SourcePVCs:           []string{"data"},
				TargetNode:           "node-b",
				DestinationClass:     "fast",
			})
			if err != nil {
				t.Fatal(err)
			}

			if plan.Ready || !hasFailedCheck(plan, test.check) {
				t.Fatalf("plan checks=%#v", plan.Checks)
			}
		})
	}
}

func TestMigrateWithoutPodRejectsActiveConsumers(t *testing.T) {
	objects := append(plannerObjects("2Gi"),
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "writer"},
			Spec: corev1.PodSpec{
				NodeName: "node-b",
				Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data",
					},
				}}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
	)

	plan, err := New(plannerClient(objects...), nil).PlanOfflineMigration(
		context.Background(),
		OfflineMigrationOptions{
			SessionID:          "migration",
			SourceNamespace:    "app",
			TemporaryNamespace: "system",
			StagingNamespace:   "system",
			SessionNamespace:   "system",
			SourcePVCs:         []string{"data"},
			TargetNode:         "node-b",
			DestinationClass:   "fast",
			Strategies:         []string{"mount"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "pvc-consumers") {
		t.Fatalf("active PVC-only migration should fail: %#v", plan.Checks)
	}

	if hasWarningCheck(plan, "pvc-consumers") {
		t.Fatalf(
			"PVC-only migration should not suggest warm copy after preflight failure: %#v",
			plan.Checks,
		)
	}

	for _, check := range plan.Checks {
		if check.Name == "controller-adapter" && !check.Passed &&
			!strings.Contains(check.Message, "--pod") {
			t.Fatalf(
				"consumer failure should explain the actionable workload selection: %q",
				check.Message,
			)
		}
	}

	offline := objects[:len(objects)-1]

	plan, err = New(plannerClient(offline...), nil).plan(context.Background(), planOptions{
		Operation:          domain.OperationMigrate,
		SessionID:          "offline-migration",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
		Strategies:         []string{"clusterip"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready {
		t.Fatalf("offline PVC-only migration should remain supported: %#v", plan.Checks)
	}
}

func TestPlanFiltersImpossibleMountStrategyAcrossNamespaces(t *testing.T) {
	objects := plannerObjects("2Gi")
	objects = append(objects, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "archive"}})

	plan, err := New(plannerClient(objects...), nil).plan(context.Background(), planOptions{
		Operation:            domain.OperationCopy,
		SessionID:            "cross-namespace-copy",
		SourceNamespace:      "app",
		TemporaryNamespace:   "archive",
		DestinationNamespace: "archive",
		StagingNamespace:     "archive",
		SessionNamespace:     "system",
		SourcePVCs:           []string{"data"},
		TargetNode:           "node-b",
		Strategies:           []string{"mount", "clusterip"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready {
		t.Fatalf("fallback strategy should keep plan ready: %#v", plan.Checks)
	}

	if !hasWarningCheck(plan, "strategy") ||
		strings.Contains(
			strings.Join(plan.SessionSpec.WorkflowOptions().Strategies, ","),
			"mount",
		) {
		t.Fatalf(
			"plan should warn and remove mount: checks=%#v strategies=%v",
			plan.Checks,
			plan.SessionSpec.WorkflowOptions().Strategies,
		)
	}

	plan, err = New(plannerClient(objects...), nil).plan(context.Background(), planOptions{
		Operation:            domain.OperationCopy,
		SessionID:            "cross-namespace-mount-only",
		SourceNamespace:      "app",
		TemporaryNamespace:   "archive",
		DestinationNamespace: "archive",
		StagingNamespace:     "archive",
		SessionNamespace:     "system",
		SourcePVCs:           []string{"data"},
		TargetNode:           "node-b",
		Strategies:           []string{"mount"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasWarningCheck(plan, "strategy") {
		t.Fatalf("mount-only cross-namespace plan should fail: %#v", plan.Checks)
	}
}

func TestPlanFiltersMountWhenSourcePVExcludesTargetNode(t *testing.T) {
	objects := plannerObjects("2Gi")
	for _, object := range objects {
		if pv, ok := object.(*corev1.PersistentVolume); ok {
			pv.Spec.NodeAffinity = &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      corev1.LabelHostname,
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"node-a"},
						},
					}},
				},
			}}
		}
	}

	plan, err := New(plannerClient(objects...), nil).plan(context.Background(), planOptions{
		Operation:          domain.OperationCopy,
		SessionID:          "topology-copy",
		SourceNamespace:    "app",
		StagingNamespace:   "app",
		SessionNamespace:   "system",
		TemporaryNamespace: "system",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
		Strategies:         []string{"auto"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready || !hasWarningCheck(plan, "strategy") {
		t.Fatalf("fallback strategy should keep plan ready: %#v", plan.Checks)
	}

	if got := strings.Join(plan.SessionSpec.WorkflowOptions().Strategies, ","); got != "clusterip" {
		t.Fatalf("strategies=%q", got)
	}

	if got := strings.Join(
		plan.Strategies,
		",",
	); got != "clusterip" ||
		!hasPassedCheck(plan, "strategy-selection") {
		t.Fatalf("visible strategies=%q checks=%#v", got, plan.Checks)
	}

	plan, err = New(plannerClient(objects...), nil).plan(context.Background(), planOptions{
		Operation:          domain.OperationCopy,
		SessionID:          "topology-mount-only",
		SourceNamespace:    "app",
		StagingNamespace:   "app",
		SessionNamespace:   "system",
		TemporaryNamespace: "system",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
		Strategies:         []string{"mount"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "strategy") {
		t.Fatalf("mount-only topology conflict should fail: %#v", plan.Checks)
	}
}

func TestPlanAllowsStandalonePodHostnameSelectorToMove(t *testing.T) {
	objects := append(plannerObjects("2Gi"),
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "node-a",
				Labels: map[string]string{corev1.LabelHostname: "node-a"},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "default"}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "app",
				Name:      "application",
				UID:       types.UID("application-uid"),
			},
			Spec: corev1.PodSpec{
				NodeName:     "node-a",
				NodeSelector: map[string]string{corev1.LabelHostname: "node-a", "disk": "fast"},
				Containers:   []corev1.Container{{Name: "app", Image: "busybox"}},
				Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data",
					},
				}}},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		},
	)
	for _, object := range objects {
		if node, ok := object.(*corev1.Node); ok && node.Name == "node-b" {
			node.Labels["disk"] = "fast"
		}
	}

	client := plannerClient(objects...)

	plan, err := New(
		client,
		controller.NewManager(client, nil, nil),
	).plan(context.Background(), planOptions{
		Operation:          domain.OperationMigratePod,
		SessionID:          "repeat-migration",
		SourceNamespace:    "app",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		TemporaryNamespace: "system",
		PodName:            "application",
		TargetNode:         "node-b",
		DestinationClass:   "fast",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready || !hasWarningCheck(plan, "pod-scheduling") {
		t.Fatalf("repeat standalone migration checks=%#v", plan.Checks)
	}
}

func TestMigratePodRequiresForceForSameNodeAndStorageClass(t *testing.T) {
	wffc := storagev1.VolumeBindingWaitForFirstConsumer
	objects := append(plannerObjects("2Gi"),
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "default"}},
		&storagev1.StorageClass{
			ObjectMeta:        metav1.ObjectMeta{Name: "slow"},
			Provisioner:       "example.csi.io",
			VolumeBindingMode: &wffc,
			AllowedTopologies: []corev1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{
						{Key: corev1.LabelTopologyZone, Values: []string{"zone-b"}},
					},
				},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "app",
				Name:      "application",
				UID:       types.UID("application-uid"),
			},
			Spec: corev1.PodSpec{
				NodeName:   "node-b",
				Containers: []corev1.Container{{Name: "app", Image: "busybox"}},
				Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data",
					},
				}}},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		},
	)
	client := plannerClient(objects...)
	base := planOptions{
		Operation:          domain.OperationMigratePod,
		SessionID:          "same-destination",
		SourceNamespace:    "app",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		TemporaryNamespace: "system",
		PodName:            "application",
		TargetNode:         "node-b",
	}

	plan, err := New(
		client,
		controller.NewManager(client, nil, nil),
	).plan(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheckContaining(plan, "migration-needed", "--force-reprovision") {
		t.Fatalf("same destination plan checks=%#v", plan.Checks)
	}

	base.ForceReprovision = true

	forced, err := New(
		client,
		controller.NewManager(client, nil, nil),
	).plan(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}

	if !forced.Ready || !hasWarningCheck(forced, "force-reprovision") {
		t.Fatalf("forced reprovision plan ready=%t checks=%#v", forced.Ready, forced.Checks)
	}

	base.ForceReprovision = false
	base.DestinationClass = "slow"

	changedClass, err := New(
		client,
		controller.NewManager(client, nil, nil),
	).plan(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}

	if !changedClass.Ready {
		t.Fatalf(
			"changed StorageClass plan ready=%t checks=%#v",
			changedClass.Ready,
			changedClass.Checks,
		)
	}

	base.DestinationClass = ""
	base.TargetNode = domain.AutoValue

	automatic, err := New(
		client,
		controller.NewManager(client, nil, nil),
	).plan(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}

	if automatic.Ready || automatic.TargetNode != "node-b" ||
		!hasFailedCheckContaining(automatic, "migration-needed", "--force-reprovision") {
		t.Fatalf(
			"automatic same destination plan ready=%t target=%q checks=%#v",
			automatic.Ready,
			automatic.TargetNode,
			automatic.Checks,
		)
	}

	base.SourceNode = "node-missing"
	base.TargetNode = "node-missing"

	incorrectSource, err := New(
		client,
		controller.NewManager(client, nil, nil),
	).plan(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}

	if !hasFailedCheck(incorrectSource, "source-node") ||
		hasFailedCheck(incorrectSource, "migration-needed") {
		t.Fatalf("incorrect source node plan checks=%#v", incorrectSource.Checks)
	}
}

func TestAutoStrategiesChooseNamespaceCompatibleOrder(t *testing.T) {
	if got := autoStrategies("app", "app"); strings.Join(got, ",") != "mount,clusterip" {
		t.Fatalf("same-namespace auto strategies=%v", got)
	}

	if got := autoStrategies("app", "archive"); strings.Join(got, ",") != "clusterip,local" {
		t.Fatalf("cross-namespace auto strategies=%v", got)
	}
}

func TestResolveStrategiesNormalizesAutoAndClonesExplicitOrder(t *testing.T) {
	got := ResolveStrategies("app", "archive", []string{domain.StrategyAuto})
	if strings.Join(got, ",") != "clusterip,local" {
		t.Fatalf("cross-namespace resolved strategies=%v", got)
	}

	explicit := []string{domain.StrategyMount}

	got = ResolveStrategies("app", "app", explicit)
	if strings.Join(got, ",") != "mount" {
		t.Fatalf("explicit strategies=%v", got)
	}

	got[0] = domain.StrategyLocal

	if explicit[0] != domain.StrategyMount {
		t.Fatalf("explicit strategies were not cloned: %v", explicit)
	}
}

func TestPlanRejectsMixedAutoStrategyList(t *testing.T) {
	plan, err := New(
		plannerClient(plannerObjects("2Gi")...),
		nil,
	).plan(context.Background(), planOptions{
		Operation:          domain.OperationCopy,
		SessionID:          "mixed-auto",
		SourceNamespace:    "app",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		TemporaryNamespace: "system",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
		Strategies:         []string{"auto", "nodeport"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "strategy") ||
		!hasFailedCheckContaining(plan, "strategy", "cannot be combined") {
		t.Fatalf("mixed auto list should fail: %#v", plan.Checks)
	}
}

func plannerObjects(capacity string) []runtime.Object {
	wffc := storagev1.VolumeBindingWaitForFirstConsumer
	storageClass := "fast"
	mode := corev1.PersistentVolumeFilesystem
	pvcUID := types.UID("pvc-uid")

	return []runtime.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-b",
				Labels: map[string]string{
					corev1.LabelHostname:     "node-b",
					corev1.LabelTopologyZone: "zone-b",
				},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		},
		&storagev1.StorageClass{
			ObjectMeta:        metav1.ObjectMeta{Name: storageClass},
			Provisioner:       "example.csi.io",
			VolumeBindingMode: &wffc,
			AllowedTopologies: []corev1.TopologySelectorTerm{
				{
					MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{
						{Key: corev1.LabelTopologyZone, Values: []string{"zone-b"}},
					},
				},
			},
		},
		&storagev1.CSINode{
			ObjectMeta: metav1.ObjectMeta{Name: "node-b"},
			Spec: storagev1.CSINodeSpec{
				Drivers: []storagev1.CSINodeDriver{{Name: "example.csi.io", NodeID: "node-b"}},
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:       "app",
				Name:            "data",
				UID:             pvcUID,
				ResourceVersion: "10",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse(capacity),
					},
				},
				StorageClassName: &storageClass,
				VolumeMode:       &mode,
				VolumeName:       "pv-source",
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "pv-source",
				UID:             types.UID("pv-uid"),
				ResourceVersion: "20",
			},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(capacity),
				},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				ClaimRef: &corev1.ObjectReference{
					Namespace: "app",
					Name:      "data",
					UID:       pvcUID,
				},
				StorageClassName: storageClass,
			},
			Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
		},
	}
}

func plannerObjectsWithTwoPVCs(t *testing.T) []runtime.Object {
	t.Helper()

	objects := plannerObjects("2Gi")
	dataPVC := testutil.MustType[*corev1.PersistentVolumeClaim](t, objects[5])
	dataPV := testutil.MustType[*corev1.PersistentVolume](t, objects[6])

	logsPVC := dataPVC.DeepCopy()
	logsPVC.Name = "logs"
	logsPVC.UID = types.UID("logs-pvc-uid")
	logsPVC.Spec.VolumeName = "pv-logs"
	logsPV := dataPV.DeepCopy()
	logsPV.Name = "pv-logs"
	logsPV.UID = types.UID("logs-pv-uid")
	logsPV.Spec.ClaimRef = &corev1.ObjectReference{
		Namespace: "app",
		Name:      "logs",
		UID:       logsPVC.UID,
	}

	return append(objects,
		logsPVC,
		logsPV,
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "default"}},
	)
}

func TestPlanModelsTopologyQuotaAndSessionIdentity(t *testing.T) {
	client := plannerClient(plannerObjects("2Gi")...)

	plan, err := New(client, nil).plan(context.Background(), planOptions{
		SessionID:          "migration",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
		Strategies: []string{
			domain.StrategyNodePort,
			domain.StrategyLoadBalancer,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready {
		t.Fatalf("plan failed checks: %#v", plan.Checks)
	}

	if len(plan.Volumes) != 1 ||
		plan.Volumes[0].BindingMode != storagev1.VolumeBindingWaitForFirstConsumer {
		t.Fatalf("volumes: %#v", plan.Volumes)
	}

	if plan.TemporaryUsage.StorageRequests != "2Gi" ||
		plan.RollbackRetention.StorageRequests != "2Gi" {
		t.Fatalf(
			"storage estimates: temporary=%s rollback=%s",
			plan.TemporaryUsage.StorageRequests,
			plan.RollbackRetention.StorageRequests,
		)
	}

	if plan.TemporaryUsage.Secrets != 2 {
		t.Fatalf("temporary Secret estimate=%d, want 2", plan.TemporaryUsage.Secrets)
	}

	if plan.TemporaryUsage.Deployments != 0 || plan.TemporaryUsage.ReplicaSets != 0 {
		t.Fatalf(
			"temporary controller estimate deployments=%d replicaSets=%d, want 0/0",
			plan.TemporaryUsage.Deployments,
			plan.TemporaryUsage.ReplicaSets,
		)
	}

	if plan.TemporaryUsage.Endpoints != plan.TemporaryUsage.Services ||
		plan.TemporaryUsage.EndpointSlices != plan.TemporaryUsage.Services {
		t.Fatalf(
			"temporary endpoint estimates endpoints=%d endpointSlices=%d, want %d",
			plan.TemporaryUsage.Endpoints,
			plan.TemporaryUsage.EndpointSlices,
			plan.TemporaryUsage.Services,
		)
	}

	if plan.TemporaryUsage.ServiceNodePorts != plan.TemporaryUsage.Services ||
		plan.TemporaryUsage.ServiceLoadBalancers != plan.TemporaryUsage.Services {
		t.Fatalf(
			"temporary Service type estimates nodePorts=%d loadBalancers=%d, want %d",
			plan.TemporaryUsage.ServiceNodePorts,
			plan.TemporaryUsage.ServiceLoadBalancers,
			plan.TemporaryUsage.Services,
		)
	}

	if plan.SessionSpec.Volumes[0].SourcePVC.UID != types.UID("pvc-uid") ||
		plan.SessionSpec.Volumes[0].SourcePV.UID != types.UID("pv-uid") {
		t.Fatalf("session resource identities: %#v", plan.SessionSpec.Volumes[0])
	}
}

func TestPlanChecksApplicationPVCQuotaForOrchestratedActivation(t *testing.T) {
	objects := plannerObjects("2Gi")
	objects = append(objects, &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "application-storage"},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{corev1.ResourceRequestsStorage: resource.MustParse("2Gi")},
		},
		Status: corev1.ResourceQuotaStatus{
			Used: corev1.ResourceList{corev1.ResourceRequestsStorage: resource.MustParse("2Gi")},
		},
	})

	plan, err := New(plannerClient(objects...), nil).plan(context.Background(), planOptions{
		SessionID:            "migration",
		Operation:            domain.OperationMigrate,
		SourceNamespace:      "app",
		TemporaryNamespace:   "system",
		DestinationNamespace: "app",
		StagingNamespace:     "system",
		SessionNamespace:     "system",
		SourcePVCs: []string{
			"data",
		},
		TargetNode:            "node-b",
		DestinationClass:      "fast",
		DestinationCapacities: []string{"3Gi"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheckContaining(plan, "resource-quota", "activation PVC") {
		t.Fatalf("application PVC activation quota check missing: %#v", plan.Checks)
	}
}

func TestPlanAutoSelectsTopologyCompatibleTargetNode(t *testing.T) {
	objects := plannerObjects("2Gi")
	objects = append(objects,
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-a",
				Labels: map[string]string{
					corev1.LabelHostname:     "node-a",
					corev1.LabelTopologyZone: "zone-a",
				},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "control-plane",
				Labels: map[string]string{
					corev1.LabelHostname:     "control-plane",
					corev1.LabelTopologyZone: "zone-b",
				},
			},
			Spec: corev1.NodeSpec{
				Taints: []corev1.Taint{
					{
						Key:    "node-role.kubernetes.io/control-plane",
						Effect: corev1.TaintEffectNoSchedule,
					},
				},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		},
	)

	plan, err := New(plannerClient(objects...), nil).plan(context.Background(), planOptions{
		SessionID:          "auto-target",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		DestinationClass:   "fast",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready || plan.TargetNode != "node-b" {
		t.Fatalf(
			"auto target plan ready=%t target=%q checks=%#v",
			plan.Ready,
			plan.TargetNode,
			plan.Checks,
		)
	}

	if plan.SessionSpec.WorkflowOptions().TargetNode != "node-b" {
		t.Fatalf("session target=%q", plan.SessionSpec.WorkflowOptions().TargetNode)
	}

	if !hasPassedCheckContaining(
		plan,
		"target-node-selection",
		"auto selected target node node-b",
	) {
		t.Fatalf("selection check missing: %#v", plan.Checks)
	}
}

func TestPlanAutoTargetRejectsWhenNoTopologyCompatibleNodeExists(t *testing.T) {
	objects := plannerObjects("2Gi")
	for _, object := range objects {
		if node, ok := object.(*corev1.Node); ok {
			node.Labels[corev1.LabelTopologyZone] = "zone-a"
		}
	}

	plan, err := New(plannerClient(objects...), nil).plan(context.Background(), planOptions{
		SessionID:          "auto-target-fail",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		DestinationClass:   "fast",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "target-node") {
		t.Fatalf("expected target selection failure: ready=%t checks=%#v", plan.Ready, plan.Checks)
	}
}

func TestPlanChecksSourceAndSessionNamespaceToolQuotas(t *testing.T) {
	objects := plannerObjects("2Gi")
	objects = append(objects,
		&corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "source-tool-limit"},
			Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
				corev1.ResourceName("count/secrets"): resource.MustParse("0"),
			}},
		},
		&corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Namespace: "audit", Name: "session-limit"},
			Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
				corev1.ResourceName("count/configmaps"): resource.MustParse("0"),
			}},
		},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "audit"}},
	)

	plan, err := New(plannerClient(objects...), nil).plan(context.Background(), planOptions{
		SessionID:          "migration",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "audit",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready {
		t.Fatal("plan unexpectedly passed source/session tool quota checks")
	}

	var sourceFound, sessionFound bool
	for _, check := range plan.Checks {
		if check.Name != "resource-quota" || check.Passed {
			continue
		}

		if strings.Contains(check.Message, "app/source-tool-limit") {
			sourceFound = true
		}

		if strings.Contains(check.Message, "audit/session-limit") {
			sessionFound = true
		}
	}

	if !sourceFound || !sessionFound {
		t.Fatalf(
			"missing source/session quota failures: source=%t session=%t checks=%#v",
			sourceFound,
			sessionFound,
			plan.Checks,
		)
	}
}

func TestPlanRejectsUnavailableSourceNode(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*corev1.Node)
	}{
		{name: "not ready", mutate: func(node *corev1.Node) {
			node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
		}},
		{name: "unschedulable", mutate: func(node *corev1.Node) {
			node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
			node.Spec.Unschedulable = true
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects := plannerObjects("2Gi")
			objects = append(objects, &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "node-a",
					Labels: map[string]string{corev1.LabelHostname: "node-a"},
				},
			})
			test.mutate(testutil.MustType[*corev1.Node](t, objects[len(objects)-1]))

			plan, err := New(plannerClient(objects...), nil).plan(context.Background(), planOptions{
				SessionID:          "migration",
				SourceNamespace:    "app",
				TemporaryNamespace: "system",
				StagingNamespace:   "system",
				SessionNamespace:   "system",
				SourcePVCs: []string{
					"data",
				},
				SourceNode:       "node-a",
				TargetNode:       "node-b",
				DestinationClass: "fast",
			})
			if err != nil {
				t.Fatal(err)
			}

			if plan.Ready {
				t.Fatal("plan unexpectedly accepted an unavailable source node")
			}

			for _, check := range plan.Checks {
				if check.Name == "source-node" && strings.Contains(check.Message, "Ready") {
					return
				}
			}

			t.Fatalf("source-node readiness check missing: %#v", plan.Checks)
		})
	}
}

func TestPlanEvaluatesEveryQuotaAndLimitRangeWithQuantities(t *testing.T) {
	objects := plannerObjects("2Gi")
	objects = append(objects,
		&corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "storage"},
			Spec: corev1.ResourceQuotaSpec{
				Hard: corev1.ResourceList{
					corev1.ResourceRequestsStorage: resource.MustParse("10Gi"),
				},
			},
			Status: corev1.ResourceQuotaStatus{
				Used: corev1.ResourceList{
					corev1.ResourceRequestsStorage: resource.MustParse("9Gi"),
				},
			},
		},
		&corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "claims"},
			Spec: corev1.ResourceQuotaSpec{
				Hard: corev1.ResourceList{
					corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
				},
			},
			Status: corev1.ResourceQuotaStatus{
				Used: corev1.ResourceList{
					corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"),
				},
			},
		},
		&corev1.LimitRange{
			ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "pvc-size"},
			Spec: corev1.LimitRangeSpec{
				Limits: []corev1.LimitRangeItem{
					{
						Type: corev1.LimitTypePersistentVolumeClaim,
						Max: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1536Mi"),
						},
					},
				},
			},
		},
	)
	client := plannerClient(objects...)

	plan, err := New(client, nil).plan(context.Background(), planOptions{
		SessionID:          "migration",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready {
		t.Fatal("plan unexpectedly passed")
	}

	messages := make([]string, 0)
	for _, check := range plan.Checks {
		if !check.Passed {
			messages = append(messages, check.Message)
		}
	}

	joined := strings.Join(messages, " ")
	for _, expected := range []string{"storage", "claims", "pvc-size"} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("failed checks omit %q: %s", expected, joined)
		}
	}
}

func TestSchedulingIssuesCoverSelectorAffinityAndTaints(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-b",
			Labels: map[string]string{"disk": "ssd", "count": "3"},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{
				{Key: "dedicated", Value: "db", Effect: corev1.TaintEffectNoSchedule},
			},
		},
	}
	spec := corev1.PodSpec{
		NodeSelector: map[string]string{"disk": "hdd"},
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "count",
									Operator: corev1.NodeSelectorOpGt,
									Values:   []string{"4"},
								},
							},
						},
					},
				},
			},
		},
	}

	issues := schedulingIssues(spec, node)
	if len(issues) != 3 {
		t.Fatalf("issues=%v", issues)
	}
}

func hasWarningCheck(plan *domain.MigrationPlan, name domain.CheckName) bool {
	for _, check := range plan.Checks {
		if check.Name == name && check.Severity == domain.SeverityWarning && check.Passed {
			return true
		}
	}

	return false
}

func hasPassedCheck(plan *domain.MigrationPlan, name domain.CheckName) bool {
	for _, check := range plan.Checks {
		if check.Name == name && check.Passed {
			return true
		}
	}

	return false
}

func hasPassedCheckContaining(
	plan *domain.MigrationPlan,
	name domain.CheckName,
	message string,
) bool {
	for _, check := range plan.Checks {
		if check.Name == name && check.Passed && strings.Contains(check.Message, message) {
			return true
		}
	}

	return false
}

func hasFailedCheckContaining(
	plan *domain.MigrationPlan,
	name domain.CheckName,
	message string,
) bool {
	for _, check := range plan.Checks {
		if check.Name == name && !check.Passed && strings.Contains(check.Message, message) {
			return true
		}
	}

	return false
}
