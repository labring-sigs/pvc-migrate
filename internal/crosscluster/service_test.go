package crosscluster_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	. "github.com/labring-sigs/pvc-migrate/internal/crosscluster"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
)

type fakeCopier struct {
	requests []copyengine.Request
	failures int
}

func (f *fakeCopier) Copy(
	_ context.Context,
	request copyengine.Request,
	_ copyengine.ProgressFunc,
) error {
	f.requests = append(f.requests, request)
	if f.failures > 0 {
		f.failures--
		return errors.New("injected transfer failure")
	}

	return nil
}

func crossFixture() (*Service, Options, *fakeCopier) {
	source := fake.NewSimpleClientset(
		&corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: metav1.NamespaceSystem,
				UID:  types.UID("source-cluster"),
			},
		},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: "source-pvc"},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:  "pv-data",
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: "source-pv"},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")},
				ClaimRef: &corev1.ObjectReference{
					Namespace: "app",
					Name:      "data",
					UID:       "source-pvc",
				},
			},
		},
	)
	source.PrependReactor(
		"create",
		"leases",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			lease, err := testutil.ActionObject[*coordinationv1.Lease](action)
			if err != nil {
				return true, nil, err
			}

			lease.UID = types.UID("lease-" + lease.Name)

			return false, nil, nil
		},
	)
	source.PrependReactor(
		"create",
		"configmaps",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			configMap, err := testutil.ActionObject[*corev1.ConfigMap](action)
			if err != nil {
				return true, nil, err
			}

			configMap.UID = types.UID("configmap-" + configMap.Name)

			return false, nil, nil
		},
	)

	destination := fake.NewSimpleClientset(
		&corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: metav1.NamespaceSystem,
				UID:  types.UID("destination-cluster"),
			},
		},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "destination-node",
				Labels: map[string]string{corev1.LabelHostname: "destination-node"},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		},
		&storagev1.StorageClass{
			ObjectMeta:        metav1.ObjectMeta{Name: "fast", UID: "destination-sc"},
			VolumeBindingMode: new(storagev1.VolumeBindingImmediate),
		},
	)
	clientsSource := &kube.Clients{
		Kubernetes: source,
		RESTConfig: &rest.Config{Host: "https://source"},
	}
	clientsDestination := &kube.Clients{
		Kubernetes: destination,
		RESTConfig: &rest.Config{Host: "https://destination"},
	}
	copier := &fakeCopier{}
	service := NewService(
		clientsSource,
		clientsDestination,
		copier,
	).WithConnections("source.yaml", "source", "destination.yaml", "destination")
	options := Options{
		SessionID:               "cross-test",
		SessionNamespace:        "pvc-migrate-system",
		SourceNamespace:         "app",
		DestinationNamespace:    "app",
		SourcePVCs:              []string{"data"},
		DestinationPVCs:         []string{"data-copy"},
		DestinationStorageClass: "fast",
		ToolImage:               "example/tool:v1",
		Strategies:              []string{"local"},
		VerifyChecksum:          true,
	}

	return service, options, copier
}

func TestPlanAndCreateSessionKeepClustersSeparate(t *testing.T) {
	service, options, _ := crossFixture()

	plan, err := service.Plan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready {
		t.Fatalf("plan is not ready: %#v", plan.Checks)
	}

	session, err := service.CreateSession(context.Background(), options, plan)
	if err != nil {
		t.Fatal(err)
	}

	if session.Spec.SourceCluster.ID == session.Spec.DestinationCluster.ID ||
		session.Spec.Volumes[0].Source.PVC.ClusterID == session.Spec.Volumes[0].Destination.PVC.ClusterID {
		t.Fatalf("cluster refs collapsed: %#v", session.Spec)
	}

	if session.Spec.Volumes[0].Destination.StorageClass.UID != "destination-sc" {
		t.Fatalf(
			"storage class identity missing: %#v",
			session.Spec.Volumes[0].Destination.StorageClass,
		)
	}
}

func TestPlanChecksDestinationPVCQuota(t *testing.T) {
	service, options, _ := crossFixture()

	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: options.DestinationNamespace,
			Name:      "storage-limit",
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{corev1.ResourceRequestsStorage: resource.MustParse("1Gi")},
			Used: corev1.ResourceList{corev1.ResourceRequestsStorage: resource.MustParse("0")},
		},
	}
	if _, err := service.DestinationClientForTest().CoreV1().
		ResourceQuotas(options.DestinationNamespace).
		Create(context.Background(), quota, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	plan, err := service.Plan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasCrossClusterFailedCheck(plan, "destination-pvc-policy") {
		t.Fatalf("destination PVC quota was not enforced: %#v", plan.Checks)
	}
}

func TestPlanChecksSourceSessionQuota(t *testing.T) {
	service, options, _ := crossFixture()

	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: options.SessionNamespace, Name: "session-limit"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceName("count/leases.coordination.k8s.io"): resource.MustParse("0"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceName("count/leases.coordination.k8s.io"): resource.MustParse("0"),
			},
		},
	}
	if _, err := service.SourceClientForTest().CoreV1().ResourceQuotas(options.SessionNamespace).
		Create(context.Background(), quota, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	plan, err := service.Plan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasCrossClusterFailedCheck(plan, "source-session-resource-quota") {
		t.Fatalf("source session quota was not enforced: %#v", plan.Checks)
	}
}

func TestPlanChecksSourceToolQuota(t *testing.T) {
	service, options, _ := crossFixture()

	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: options.SourceNamespace, Name: "tool-limit"},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceName("count/deployments.apps"): resource.MustParse("0"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceName("count/deployments.apps"): resource.MustParse("0"),
			},
		},
	}
	if _, err := service.SourceClientForTest().CoreV1().ResourceQuotas(options.SourceNamespace).
		Create(context.Background(), quota, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	plan, err := service.Plan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasCrossClusterFailedCheck(plan, "source-tool-resource-quota") {
		t.Fatalf("source tool quota was not enforced: %#v", plan.Checks)
	}
}

func TestPlanAppliesCrossClusterToolPodQuotaToNotTerminatingScope(t *testing.T) {
	for _, test := range []struct {
		name      string
		scope     corev1.ResourceQuotaScope
		wantReady bool
	}{
		{
			name:      "Terminating excludes chart Pods",
			scope:     corev1.ResourceQuotaScopeTerminating,
			wantReady: true,
		},
		{
			name:  "NotTerminating includes chart Pods",
			scope: corev1.ResourceQuotaScopeNotTerminating,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, options, _ := crossFixture()

			quota := &corev1.ResourceQuota{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: options.SourceNamespace,
					Name:      "tool-pods",
				},
				Spec: corev1.ResourceQuotaSpec{
					Scopes: []corev1.ResourceQuotaScope{test.scope},
				},
				Status: corev1.ResourceQuotaStatus{
					Hard: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("0")},
					Used: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("0")},
				},
			}
			if _, err := service.SourceClientForTest().CoreV1().
				ResourceQuotas(options.SourceNamespace).
				Create(context.Background(), quota, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}

			plan, err := service.Plan(context.Background(), options)
			if err != nil {
				t.Fatal(err)
			}

			if plan.Ready != test.wantReady {
				t.Fatalf("ready=%t checks=%#v", plan.Ready, plan.Checks)
			}
		})
	}
}

func TestPlanAccountsForDestinationLimitRangeDefault(t *testing.T) {
	service, options, _ := crossFixture()
	limitRange := &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Namespace: options.DestinationNamespace, Name: "defaults"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypeContainer,
			Default: corev1.ResourceList{
				corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
			},
		}}},
	}

	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: options.DestinationNamespace,
			Name:      "ephemeral-limit",
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceLimitsEphemeralStorage: resource.MustParse("1Gi"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceLimitsEphemeralStorage: resource.MustParse("0"),
			},
		},
	}
	if _, err := service.DestinationClientForTest().CoreV1().
		LimitRanges(options.DestinationNamespace).
		Create(context.Background(), limitRange, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := service.DestinationClientForTest().CoreV1().
		ResourceQuotas(options.DestinationNamespace).
		Create(context.Background(), quota, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	plan, err := service.Plan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasCrossClusterFailedCheck(plan, "destination-tool-resource-quota") {
		t.Fatalf("LimitRange default was not included in destination quota: %#v", plan.Checks)
	}
}

func TestPlanDoesNotTurnLimitRangeDefaultRequestIntoLimit(t *testing.T) {
	service, options, _ := crossFixture()
	limitRange := &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Namespace: options.DestinationNamespace, Name: "requests"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{
			Type: corev1.LimitTypeContainer,
			DefaultRequest: corev1.ResourceList{
				corev1.ResourceEphemeralStorage: resource.MustParse("100Mi"),
			},
		}}},
	}

	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: options.DestinationNamespace,
			Name:      "ephemeral-limit",
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceLimitsEphemeralStorage: resource.MustParse("0"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceLimitsEphemeralStorage: resource.MustParse("0"),
			},
		},
	}
	if _, err := service.DestinationClientForTest().CoreV1().
		LimitRanges(options.DestinationNamespace).
		Create(context.Background(), limitRange, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := service.DestinationClientForTest().CoreV1().
		ResourceQuotas(options.DestinationNamespace).
		Create(context.Background(), quota, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	plan, err := service.Plan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready {
		t.Fatalf("defaultRequest became an ephemeral-storage limit: %#v", plan.Checks)
	}
}

func hasCrossClusterFailedCheck(plan *Plan, name string) bool {
	for _, check := range plan.Checks {
		if check.Name == name && !check.Passed {
			return true
		}
	}

	return false
}

func TestPlanMissingDestinationStorageClassReturnsFailedPlan(t *testing.T) {
	service, options, _ := crossFixture()
	options.DestinationStorageClass = "missing"

	plan, err := service.Plan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready {
		t.Fatalf("plan unexpectedly ready: %#v", plan.Checks)
	}
}

func TestPlanRejectsMismatchedSourcePVClaimRef(t *testing.T) {
	service, options, _ := crossFixture()

	pv, err := service.SourceClientForTest().CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pv.Spec.ClaimRef.UID = "different-pvc"
	if _, err := service.SourceClientForTest().CoreV1().
		PersistentVolumes().
		Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	plan, err := service.Plan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready {
		t.Fatalf("plan unexpectedly accepted a mismatched source PV claimRef: %#v", plan.Checks)
	}

	for _, check := range plan.Checks {
		if check.Name == "source-binding" && !check.Passed {
			return
		}
	}

	t.Fatalf("plan did not report a source-binding failure: %#v", plan.Checks)
}

func TestPlanRejectsReadOnlySourcePVC(t *testing.T) {
	service, options, _ := crossFixture()

	pvc, err := service.SourceClientForTest().CoreV1().
		PersistentVolumeClaims("app").
		Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany}
	if _, err := service.SourceClientForTest().CoreV1().
		PersistentVolumeClaims("app").
		Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	plan, err := service.Plan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready {
		t.Fatalf("plan unexpectedly accepted a read-only source PVC: %#v", plan.Checks)
	}

	for _, check := range plan.Checks {
		if check.Name == "access-mode" && !check.Passed {
			return
		}
	}

	t.Fatalf("plan did not report an access-mode failure: %#v", plan.Checks)
}

func TestPlanRejectsTransferPathsThatEscapePVC(t *testing.T) {
	service, options, _ := crossFixture()
	options.SourcePaths = []string{"../outside"}

	plan, err := service.Plan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready {
		t.Fatalf("plan unexpectedly accepted an escaping path: %#v", plan.Checks)
	}

	for _, check := range plan.Checks {
		if check.Name == "source-path" && !check.Passed {
			return
		}
	}

	t.Fatalf("plan did not report a source-path failure: %#v", plan.Checks)
}

func TestCopyUsesBothConnectionsAndPersistsTransferState(t *testing.T) {
	service, options, copier := crossFixture()

	plan, err := service.Plan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	session, err := service.CreateSession(context.Background(), options, plan)
	if err != nil {
		t.Fatal(err)
	}

	destination := service.DestinationClientForTest()
	destinationPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-copy",
			Namespace: "app",
			UID:       "destination-pvc",
			Labels:    map[string]string{ManagedByLabel: ManagedBy, SessionKey: session.ID},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       "pv-copy",
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: new("fast"),
			VolumeMode:       new(corev1.PersistentVolumeFilesystem),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	_, err = destination.CoreV1().
		PersistentVolumeClaims("app").
		Create(context.Background(), destinationPVC, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = destination.CoreV1().
		PersistentVolumes().
		Create(context.Background(), &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-copy", UID: "destination-pv"}, Spec: corev1.PersistentVolumeSpec{Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")}, ClaimRef: &corev1.ObjectReference{Namespace: "app", Name: "data-copy", UID: "destination-pvc"}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := service.Copy(context.Background(), session, 1, false); err != nil {
		t.Fatal(err)
	}

	if session.Status.Phase != PhaseCompleted ||
		session.Status.Volumes[0].Transfer.CompletedAt == nil {
		t.Fatalf("transfer state not completed: %#v", session.Status)
	}

	if len(copier.requests) != 1 || copier.requests[0].KubeconfigPath != "source.yaml" ||
		copier.requests[0].DestinationKubeconfigPath != "destination.yaml" {
		t.Fatalf("copy request did not preserve two connections: %#v", copier.requests)
	}

	for _, expected := range kube.ZeroResourceHelmValues() {
		if !slices.Contains(copier.requests[0].HelmStringValues, expected) {
			t.Fatalf(
				"copy request lacks zero resource value %q: %v",
				expected,
				copier.requests[0].HelmStringValues,
			)
		}
	}
}

func TestReservationConsumerUsesZeroToolResources(t *testing.T) {
	service, options, _ := crossFixture()
	options.TargetNode = "destination-node"

	plan, err := service.Plan(context.Background(), options)
	if err != nil || !plan.Ready {
		t.Fatalf("plan ready=%v err=%v checks=%#v", plan.Ready, err, plan.Checks)
	}

	session, err := service.CreateSession(context.Background(), options, plan)
	if err != nil {
		t.Fatal(err)
	}

	destination := service.DestinationClientForTest()

	destinationFake, ok := destination.(*fake.Clientset)
	if !ok {
		t.Fatalf("destination client type=%T", destination)
	}

	destinationFake.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod, err := testutil.ActionObject[*corev1.Pod](action)
			if err != nil {
				return true, nil, err
			}

			pod.UID = "reservation-pod"
			pod.Status.Phase = corev1.PodRunning
			pod.Status.Conditions = []corev1.PodCondition{{
				Type: corev1.PodScheduled, Status: corev1.ConditionTrue,
			}}

			return false, nil, nil
		},
	)

	if err := service.CreateReservationConsumerForTest(
		context.Background(),
		session,
		&session.Spec.Volumes[0],
	); err != nil {
		t.Fatal(err)
	}

	pods, err := destination.CoreV1().Pods(options.DestinationNamespace).List(
		context.Background(),
		metav1.ListOptions{},
	)
	if err != nil || len(pods.Items) != 1 {
		t.Fatalf("reservation Pods=%d err=%v", len(pods.Items), err)
	}

	resources := pods.Items[0].Spec.Containers[0].Resources

	zero := resource.MustParse("0")
	for _, name := range []corev1.ResourceName{
		corev1.ResourceCPU,
		corev1.ResourceMemory,
		corev1.ResourceEphemeralStorage,
	} {
		if got := resources.Requests[name]; got.Cmp(zero) != 0 {
			t.Fatalf("reservation request %s=%s, want 0", name, got.String())
		}
	}

	if _, exists := resources.Limits[corev1.ResourceEphemeralStorage]; exists {
		t.Fatal("reservation Pod sets an ephemeral-storage limit")
	}
}

func TestCopyResumeContinuesTransferAttemptCount(t *testing.T) {
	service, options, copier := crossFixture()

	plan, err := service.Plan(context.Background(), options)
	if err != nil || !plan.Ready {
		t.Fatalf("plan ready=%v err=%v", plan.Ready, err)
	}

	session, err := service.CreateSession(context.Background(), options, plan)
	if err != nil {
		t.Fatal(err)
	}

	destination := service.DestinationClientForTest()

	_, err = destination.CoreV1().
		PersistentVolumeClaims("app").
		Create(context.Background(), &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data-copy",
				Namespace: "app",
				UID:       "destination-pvc",
				Labels:    map[string]string{ManagedByLabel: ManagedBy, SessionKey: session.ID},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:       "pv-copy",
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: new("fast"),
				VolumeMode:       new(corev1.PersistentVolumeFilesystem),
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("2Gi"),
					},
				},
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = destination.CoreV1().
		PersistentVolumes().
		Create(context.Background(), &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-copy", UID: "destination-pv"},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")},
				ClaimRef: &corev1.ObjectReference{
					Namespace: "app",
					Name:      "data-copy",
					UID:       "destination-pvc",
				},
			},
		}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	copier.failures = 1

	if err := service.Copy(context.Background(), session, 1, false); err == nil {
		t.Fatal("first copy unexpectedly succeeded")
	}

	if session.Status.Volumes[0].Transfer.Attempts != 1 {
		t.Fatalf("first attempt count=%d", session.Status.Volumes[0].Transfer.Attempts)
	}

	if err := service.Copy(context.Background(), session, 1, false); err != nil {
		t.Fatal(err)
	}

	if session.Status.Volumes[0].Transfer.Attempts != 2 {
		t.Fatalf("resume reset attempt count to %d", session.Status.Volumes[0].Transfer.Attempts)
	}
}

func TestCopyResumeRejectsCompletedDestinationReplacement(t *testing.T) {
	service, options, _ := crossFixture()

	plan, err := service.Plan(context.Background(), options)
	if err != nil || !plan.Ready {
		t.Fatalf("plan ready=%v err=%v", plan.Ready, err)
	}

	session, err := service.CreateSession(context.Background(), options, plan)
	if err != nil {
		t.Fatal(err)
	}

	createBoundDestination(t, service, session)

	if err := service.Copy(context.Background(), session, 1, false); err != nil {
		t.Fatal(err)
	}

	if err := service.DestinationClientForTest().CoreV1().
		PersistentVolumeClaims("app").
		Delete(context.Background(), "data-copy", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	replacement := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-copy",
			Namespace: "app",
			UID:       "replacement-pvc",
			Labels:    map[string]string{ManagedByLabel: ManagedBy, SessionKey: session.ID},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       "pv-copy",
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: new("fast"),
			VolumeMode:       new(corev1.PersistentVolumeFilesystem),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	if _, err := service.DestinationClientForTest().CoreV1().
		PersistentVolumeClaims("app").
		Create(context.Background(), replacement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := service.Copy(context.Background(), session, 1, false); err == nil {
		t.Fatal("resume accepted a replacement for a completed destination PVC")
	}

	if session.Status.Phase != PhaseFailed || session.Status.Volumes[0].Transfer.LastError == "" {
		t.Fatalf(
			"destination replacement was not persisted as a failed session: %#v",
			session.Status,
		)
	}
}

func TestResolveMultiPVCValuesRequireExplicitMappings(t *testing.T) {
	sources := []string{"one", "two"}
	for name, resolve := range map[string]func([]string, []string) ([]string, error){
		"names":        ResolveNamesForTest,
		"sizes":        ResolveValuesForTest,
		"source paths": ResolvePathsForTest,
	} {
		if _, err := resolve([]string{"same"}, sources); err == nil {
			t.Fatalf("%s accepted an ambiguous positional value", name)
		}

		if _, err := resolve([]string{"one=value", "unknown=value"}, sources); err == nil {
			t.Fatalf("%s accepted an unknown source mapping", name)
		}

		if _, err := resolve([]string{"one=value", "one=other"}, sources); err == nil {
			t.Fatalf("%s accepted a duplicate source mapping", name)
		}
	}

	if _, err := ResolveNamesForTest([]string{"one=shared", "two=shared"}, sources); err == nil {
		t.Fatal("destination name resolver accepted two source PVCs mapped to one destination PVC")
	}
}

func TestResolveMultiPVCValuesAreAlignedByName(t *testing.T) {
	sources := []string{"one", "two"}

	got, err := ResolveValuesForTest([]string{"two=3Gi", "one=2Gi"}, sources)
	if err != nil || len(got) != 2 || got[0] != "2Gi" || got[1] != "3Gi" {
		t.Fatalf("capacity mapping alignment = %#v, err=%v", got, err)
	}
}

func TestResolveMultiPVCPathsRequireEveryExplicitMapping(t *testing.T) {
	if _, err := ResolvePathsForTest([]string{"one=data"}, []string{"one", "two"}); err == nil {
		t.Fatal("path resolver accepted a partially specified multi-PVC mapping")
	}
}

func TestReservationConsumerNamesDoNotCollideAfterDNSLengthLimit(t *testing.T) {
	first := ReservationConsumerNameForTest(
		"session",
		"a-very-long-persistent-volume-claim-name-with-a-shared-prefix-one",
	)

	second := ReservationConsumerNameForTest(
		"session",
		"a-very-long-persistent-volume-claim-name-with-a-shared-prefix-two",
	)
	if first == second {
		t.Fatalf("long PVC names collided: %q", first)
	}

	if len(first) > 63 || len(second) > 63 {
		t.Fatalf("reservation names exceed DNS label length: %d, %d", len(first), len(second))
	}
}

func TestSessionValidationRejectsUnapprovedShrinkAndUnsafePath(t *testing.T) {
	service, options, _ := crossFixture()

	plan, err := service.Plan(context.Background(), options)
	if err != nil || !plan.Ready {
		t.Fatalf("plan ready=%v err=%v", plan.Ready, err)
	}

	session, err := service.CreateSession(context.Background(), options, plan)
	if err != nil {
		t.Fatal(err)
	}

	session.Spec.Volumes[0].Destination.Capacity = "1Gi"
	if err := session.Validate(); err == nil {
		t.Fatal("session accepted shrink without explicit approval")
	}

	session.Spec.AllowVolumeShrink = true
	if err := session.Validate(); err == nil {
		t.Fatal("session accepted shrink without a source usage decision")
	}

	session.Spec.SkipSourceUsageCheck = true

	session.Spec.Volumes[0].Transfer.SourcePath = "../outside"
	if err := session.Validate(); err == nil {
		t.Fatal("session accepted a path outside the PVC root")
	}
}

func TestSessionValidationRejectsDuplicateDestinationPVCs(t *testing.T) {
	service, options, _ := crossFixture()

	plan, err := service.Plan(context.Background(), options)
	if err != nil || !plan.Ready {
		t.Fatalf("plan ready=%v err=%v", plan.Ready, err)
	}

	session, err := service.CreateSession(context.Background(), options, plan)
	if err != nil {
		t.Fatal(err)
	}

	duplicate := session.Spec.Volumes[0]
	duplicate.Source.PVC.Name = "other-data"
	duplicate.Source.PVC.UID = "other-source-pvc"
	duplicate.Source.PV.Name = "other-pv"
	duplicate.Source.PV.UID = "other-source-pv"
	session.Spec.Volumes = append(session.Spec.Volumes, duplicate)

	session.Status.Volumes = append(
		session.Status.Volumes,
		VolumeStatus{SourcePVCName: duplicate.Source.PVC.Name},
	)
	if err := session.Validate(); err == nil {
		t.Fatal("session accepted two source PVCs mapped to one destination PVC")
	}
}

func TestCopyRejectsSourcePVCReplacementAfterReservation(t *testing.T) {
	service, options, _ := crossFixture()

	plan, err := service.Plan(context.Background(), options)
	if err != nil || !plan.Ready {
		t.Fatalf("plan ready=%v err=%v", plan.Ready, err)
	}

	session, err := service.CreateSession(context.Background(), options, plan)
	if err != nil {
		t.Fatal(err)
	}

	destination := service.DestinationClientForTest()

	_, err = destination.CoreV1().
		PersistentVolumeClaims("app").
		Create(context.Background(), &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data-copy",
				Namespace: "app",
				UID:       "destination-pvc",
				Labels:    map[string]string{ManagedByLabel: ManagedBy, SessionKey: session.ID},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:       "pv-copy",
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: new("fast"),
				VolumeMode:       new(corev1.PersistentVolumeFilesystem),
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("2Gi"),
					},
				},
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = destination.CoreV1().
		PersistentVolumes().
		Create(context.Background(), &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-copy", UID: "destination-pv"},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")},
				ClaimRef: &corev1.ObjectReference{
					Namespace: "app",
					Name:      "data-copy",
					UID:       "destination-pvc",
				},
			},
		}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	sourcePVC, err := service.SourceClientForTest().CoreV1().
		PersistentVolumeClaims("app").
		Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	sourcePVC.UID = "replacement-pvc"
	if _, err := service.SourceClientForTest().CoreV1().
		PersistentVolumeClaims("app").
		Update(context.Background(), sourcePVC, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := service.Copy(context.Background(), session, 1, false); err == nil {
		t.Fatal("copy proceeded after source PVC replacement")
	}

	if session.Status.Phase != PhaseFailed || session.Status.Volumes[0].Transfer.LastError == "" {
		t.Fatalf("source replacement was not persisted as a failed session: %#v", session.Status)
	}
}

func TestGetRejectsSessionNamespaceMismatch(t *testing.T) {
	service, options, _ := crossFixture()

	plan, err := service.Plan(context.Background(), options)
	if err != nil || !plan.Ready {
		t.Fatalf("plan ready=%v err=%v", plan.Ready, err)
	}

	session, err := service.CreateSession(context.Background(), options, plan)
	if err != nil {
		t.Fatal(err)
	}

	session.Spec.SessionNamespace = "other"

	raw, err := json.Marshal(session)
	if err != nil {
		t.Fatal(err)
	}

	cm, err := service.SourceClientForTest().CoreV1().
		ConfigMaps(options.SessionNamespace).
		Get(context.Background(), "pvc-migrate-cross-cluster-"+session.ID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	cm.Data["session.json"] = string(raw)
	if _, err := service.SourceClientForTest().CoreV1().
		ConfigMaps(options.SessionNamespace).
		Update(context.Background(), cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := service.Get(
		context.Background(),
		options.SessionNamespace,
		session.ID,
	); err == nil {
		t.Fatal(
			"Get accepted a session whose persisted namespace differs from the ConfigMap namespace",
		)
	}
}

func TestCleanupDeletesOnlyOwnedDestinationPVCAndReleasedPV(t *testing.T) {
	service, options, _ := crossFixture()

	plan, err := service.Plan(context.Background(), options)
	if err != nil || !plan.Ready {
		t.Fatalf("plan ready=%v err=%v checks=%#v", plan.Ready, err, plan.Checks)
	}

	session, err := service.CreateSession(context.Background(), options, plan)
	if err != nil {
		t.Fatal(err)
	}

	session.Spec.Volumes[0].Destination.PVC.UID = types.UID("destination-pvc")
	session.Spec.Volumes[0].Destination.PV = ClusterResourceRef{
		ClusterID:  session.Spec.DestinationCluster.ID,
		APIVersion: "v1",
		Kind:       "PersistentVolume",
		Name:       "pv-copy",
		UID:        types.UID("destination-pv"),
	}
	session.Status.Volumes[0].Reservation.PVC = session.Spec.Volumes[0].Destination.PVC
	session.Status.Volumes[0].Reservation.PV = session.Spec.Volumes[0].Destination.PV

	session.Status.Phase = PhaseCompleted
	if err := service.SaveForTest(context.Background(), session, false); err != nil {
		t.Fatal(err)
	}

	destination := service.DestinationClientForTest()

	_, err = destination.CoreV1().
		PersistentVolumeClaims("app").
		Create(context.Background(), &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data-copy",
				Namespace: "app",
				UID:       "destination-pvc",
				Labels:    map[string]string{ManagedByLabel: ManagedBy, SessionKey: session.ID},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:       "pv-copy",
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: new("fast"),
				VolumeMode:       new(corev1.PersistentVolumeFilesystem),
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("2Gi"),
					},
				},
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = destination.CoreV1().
		PersistentVolumes().
		Create(context.Background(), &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-copy", UID: "destination-pv"},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")},
				ClaimRef: &corev1.ObjectReference{
					Namespace: "app",
					Name:      "data-copy",
					UID:       "destination-pvc",
				},
			},
			Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeReleased},
		}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := service.Cleanup(context.Background(), session, true, true); err != nil {
		t.Fatal(err)
	}

	if _, err := destination.CoreV1().
		PersistentVolumeClaims("app").
		Get(context.Background(), "data-copy", metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("destination PVC still exists: %v", err)
	}

	if _, err := destination.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-copy", metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("destination PV still exists: %v", err)
	}

	if _, err := service.Get(
		context.Background(),
		options.SessionNamespace,
		session.ID,
	); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("session still exists: %v", err)
	}
}

func TestCleanupCannotDeleteSessionWhileDestinationIsRecorded(t *testing.T) {
	service, options, _ := crossFixture()

	plan, err := service.Plan(context.Background(), options)
	if err != nil || !plan.Ready {
		t.Fatalf("plan ready=%v err=%v", plan.Ready, err)
	}

	session, err := service.CreateSession(context.Background(), options, plan)
	if err != nil {
		t.Fatal(err)
	}

	session.Spec.Volumes[0].Destination.PVC.UID = types.UID("destination-pvc")
	if err := service.SaveForTest(context.Background(), session, false); err != nil {
		t.Fatal(err)
	}

	if err := service.Cleanup(context.Background(), session, false, true); err == nil {
		t.Fatal("cleanup deleted a session while destination ownership was recorded")
	}
}

func TestCleanupCannotDeleteSessionWithUnrecordedDestinationPVC(t *testing.T) {
	service, options, _ := crossFixture()

	plan, err := service.Plan(context.Background(), options)
	if err != nil || !plan.Ready {
		t.Fatalf("plan ready=%v err=%v checks=%#v", plan.Ready, err, plan.Checks)
	}

	session, err := service.CreateSession(context.Background(), options, plan)
	if err != nil {
		t.Fatal(err)
	}

	_, err = service.DestinationClientForTest().CoreV1().
		PersistentVolumeClaims("app").
		Create(context.Background(), &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data-copy",
				Namespace: "app",
				UID:       "unrecorded-destination-pvc",
				Labels:    map[string]string{ManagedByLabel: ManagedBy, SessionKey: session.ID},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("2Gi"),
					},
				},
			},
		}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := service.Cleanup(context.Background(), session, false, true); err == nil {
		t.Fatal("cleanup deleted a session while an unrecorded destination PVC existed")
	}

	if _, err := service.Get(
		context.Background(),
		options.SessionNamespace,
		session.ID,
	); err != nil {
		t.Fatalf("cleanup removed the session despite the unrecorded destination PVC: %v", err)
	}
}

func TestCleanupRemovesReservationPodWhenPVCIsAlreadyGone(t *testing.T) {
	service, options, _ := crossFixture()

	plan, err := service.Plan(context.Background(), options)
	if err != nil || !plan.Ready {
		t.Fatalf("plan ready=%v err=%v", plan.Ready, err)
	}

	session, err := service.CreateSession(context.Background(), options, plan)
	if err != nil {
		t.Fatal(err)
	}

	reservationPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "reservation",
			Namespace: "app",
			UID:       "reservation-pod",
			Labels:    map[string]string{ManagedByLabel: ManagedBy, SessionKey: session.ID},
		},
	}
	if _, err := service.DestinationClientForTest().CoreV1().
		Pods("app").
		Create(context.Background(), reservationPod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	session.Status.Volumes[0].Reservation.ConsumerPod = ClusterResourceRef{
		ClusterID:  session.Spec.DestinationCluster.ID,
		APIVersion: "v1",
		Kind:       "Pod",
		Namespace:  "app",
		Name:       "reservation",
		UID:        reservationPod.UID,
	}
	if err := service.CleanupDestinationVolumeForTest(
		context.Background(),
		session,
		0,
	); err != nil {
		t.Fatal(err)
	}

	if _, err := service.DestinationClientForTest().CoreV1().
		Pods("app").
		Get(context.Background(), "reservation", metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("reservation Pod still exists: %v", err)
	}
}

func createBoundDestination(t *testing.T, service *Service, session *Session) {
	t.Helper()

	destination := service.DestinationClientForTest()

	_, err := destination.CoreV1().
		PersistentVolumeClaims("app").
		Create(context.Background(), &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data-copy",
				Namespace: "app",
				UID:       "destination-pvc",
				Labels:    map[string]string{ManagedByLabel: ManagedBy, SessionKey: session.ID},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:       "pv-copy",
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: new("fast"),
				VolumeMode:       new(corev1.PersistentVolumeFilesystem),
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("2Gi"),
					},
				},
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = destination.CoreV1().
		PersistentVolumes().
		Create(context.Background(), &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-copy", UID: "destination-pv"},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")},
				ClaimRef: &corev1.ObjectReference{
					Namespace: "app",
					Name:      "data-copy",
					UID:       "destination-pvc",
				},
			},
		}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
}

//go:fix inline

var _ = time.Second
