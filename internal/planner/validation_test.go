package planner

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

type plannerOpenEBSLVMSharedVolumeManager struct {
	shared bool
	err    error
}

func (m plannerOpenEBSLVMSharedVolumeManager) Shared(
	context.Context,
	domain.ObjectReference,
	domain.ObjectReference,
	string,
) (bool, error) {
	return m.shared, m.err
}

func (plannerOpenEBSLVMSharedVolumeManager) PrepareShared(
	context.Context,
	domain.ObjectReference,
) (kube.OpenEBSLVMSharedResult, error) {
	return kube.OpenEBSLVMSharedResult{}, nil
}

func (plannerOpenEBSLVMSharedVolumeManager) EnsureShared(
	context.Context,
	domain.ObjectReference,
	domain.ObjectReference,
) (kube.OpenEBSLVMSharedResult, error) {
	return kube.OpenEBSLVMSharedResult{}, nil
}

func (plannerOpenEBSLVMSharedVolumeManager) EnableShared(
	context.Context,
	string,
	domain.OpenEBSLVMSharedMount,
) error {
	return nil
}

func (plannerOpenEBSLVMSharedVolumeManager) ValidateRestoreShared(
	context.Context,
	string,
	domain.OpenEBSLVMSharedMount,
) error {
	return nil
}

func (plannerOpenEBSLVMSharedVolumeManager) RestoreShared(
	context.Context,
	string,
	domain.OpenEBSLVMSharedMount,
) error {
	return nil
}

func TestCheckPVCReferencesModelsOfflineWarmCopyRWOPAndSharedUnit(t *testing.T) {
	rwo := corev1.ReadWriteOnce
	rwop := corev1.ReadWriteOncePod

	tests := []struct {
		name      string
		operation domain.Operation
		mode      corev1.PersistentVolumeAccessMode
		pods      []*corev1.Pod
		sourcePod *corev1.Pod
		ready     bool
		severity  domain.CheckSeverity
		message   string
	}{
		{
			name:      "offline",
			operation: domain.OperationCopy,
			mode:      rwo,
			ready:     true,
			severity:  domain.SeverityInfo,
			message:   "is offline",
		},
		{
			name:      "active RWO warns",
			operation: domain.OperationCopy,
			mode:      rwo,
			pods:      []*corev1.Pod{podWithPVC("consumer")},
			ready:     true,
			severity:  domain.SeverityWarning,
			message:   "warm copy has file-level consistency",
		},
		{
			name:      "active RWOP fails",
			operation: domain.OperationCopy,
			mode:      rwop,
			pods:      []*corev1.Pod{podWithPVC("consumer")},
			severity:  domain.SeverityError,
			message:   "cannot be warm-copied",
		},
		{
			name:      "active RWOP reserve warns accurately",
			operation: domain.OperationReserve,
			mode:      rwop,
			pods:      []*corev1.Pod{podWithPVC("consumer")},
			ready:     true,
			severity:  domain.SeverityWarning,
			message:   "reservation keeps the source PVC mounted",
		},
		{
			name:      "selected unit has another consumer",
			operation: domain.OperationCopy,
			mode:      rwo,
			pods: []*corev1.Pod{
				podWithPVC("selected"),
				podWithPVC("other"),
			},
			sourcePod: podWithPVC("selected"),
			severity:  domain.SeverityError,
			message:   "shared with Pod(s): other",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{tt.mode},
				},
			}
			plan := &domain.MigrationPlan{Ready: true}

			pods := make([]corev1.Pod, len(tt.pods))
			for index, pod := range tt.pods {
				pods[index] = *pod
			}

			New(nil, nil).checkPVCReferencesFromPods(
				plan,
				pvc,
				tt.sourcePod,
				domain.WorkloadSpec{},
				tt.operation,
				true,
				pods,
				nil,
			)

			if plan.Ready != tt.ready || len(plan.Checks) != 1 ||
				plan.Checks[0].Severity != tt.severity ||
				!strings.Contains(plan.Checks[0].Message, tt.message) {
				t.Fatalf("plan ready=%t checks=%#v", plan.Ready, plan.Checks)
			}
		})
	}
}

func TestCheckWarmCopyMountCompatibility(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
	}

	consumer := podWithPVC("database-0")
	for _, test := range []struct {
		name         string
		operation    domain.Operation
		enableShared bool
		class        *storagev1.StorageClass
		consumers    []*corev1.Pod
		wantReady    bool
		wantLevel    domain.CheckSeverity
		wantText     string
		wantChecks   int
		wantInspect  bool
		wantPatch    bool
		lvmShared    bool
		lvmErr       error
		pvDriver     string
	}{
		{
			name:      "OpenEBS LVM without shared blocks warm copy",
			class:     &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "openebs-lvmpv"}, Provisioner: "local.csi.openebs.io", Parameters: map[string]string{"volgroup": "lvmvg"}},
			consumers: []*corev1.Pod{consumer}, wantText: "--precopy-passes 0", wantLevel: domain.SeverityError, wantChecks: 1, wantInspect: true,
		},
		{
			name:         "OpenEBS LVM explicit enable permits warm-copy probe",
			enableShared: true,
			class:        &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "openebs-lvmpv"}, Provisioner: "local.csi.openebs.io"},
			consumers:    []*corev1.Pod{consumer}, wantReady: true, wantText: "temporarily set it to yes", wantLevel: domain.SeverityInfo, wantChecks: 1, wantInspect: true, wantPatch: true,
		},
		{
			name:      "online copy uses copy-specific fallback",
			operation: domain.OperationCopy,
			class:     &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "openebs-lvmpv"}, Provisioner: "local.csi.openebs.io"},
			consumers: []*corev1.Pod{consumer}, wantText: "without --online", wantLevel: domain.SeverityError, wantChecks: 1, wantInspect: true,
		},
		{
			name:         "OpenEBS LVM current shared value supports runtime verification without patch",
			enableShared: true,
			class:        &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "openebs-lvmpv-shared"}, Provisioner: "local.csi.openebs.io", Parameters: map[string]string{"shared": "yes"}},
			consumers:    []*corev1.Pod{consumer}, lvmShared: true, wantReady: true, wantText: "currently has spec.shared=yes", wantLevel: domain.SeverityInfo, wantChecks: 1, wantInspect: true,
		},
		{
			name:      "StorageClass shared does not override current unshared LVMVolume",
			class:     &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "openebs-lvmpv-shared"}, Provisioner: "local.csi.openebs.io", Parameters: map[string]string{"shared": "yes"}},
			consumers: []*corev1.Pod{consumer}, wantText: "does not currently have spec.shared=yes", wantLevel: domain.SeverityError, wantChecks: 1, wantInspect: true,
		},
		{
			name:      "OpenEBS LVM current state read failure blocks warm copy",
			class:     &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "openebs-lvmpv-shared"}, Provisioner: "local.csi.openebs.io", Parameters: map[string]string{"shared": "yes"}},
			consumers: []*corev1.Pod{consumer}, lvmErr: errors.New("LVMVolume access denied"), wantText: "LVMVolume access denied", wantLevel: domain.SeverityError, wantChecks: 1, wantInspect: true,
		},
		{
			name:      "unknown CSI is probed at runtime",
			class:     &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "other"}, Provisioner: "storage.example.com"},
			consumers: []*corev1.Pod{consumer}, wantReady: true, wantText: "driver-specific", wantLevel: domain.SeverityWarning, wantChecks: 1,
		},
		{
			name:      "source PV driver identifies OpenEBS LVM after StorageClass changes",
			class:     &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "changed"}, Provisioner: "storage.example.com"},
			pvDriver:  kube.OpenEBSLVMCSIDriver,
			consumers: []*corev1.Pod{consumer}, wantText: "does not currently have spec.shared=yes", wantLevel: domain.SeverityError, wantChecks: 1, wantInspect: true,
		},
		{
			name:      "StorageClass provisioner does not override source PV driver",
			class:     &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "changed"}, Provisioner: kube.OpenEBSLVMCSIDriver},
			pvDriver:  "storage.example.com",
			consumers: []*corev1.Pod{consumer}, wantReady: true, wantText: "driver-specific", wantLevel: domain.SeverityWarning, wantChecks: 1,
		},
		{
			name: "OpenEBS Hostpath supports same-node Pod mounts",
			class: &storagev1.StorageClass{
				ObjectMeta:  metav1.ObjectMeta{Name: "openebs-hostpath", Annotations: map[string]string{"cas.openebs.io/config": "- name: StorageType\n  value: hostpath\n- name: BasePath\n  value: /var/openebs/local\n"}},
				Provisioner: "openebs.io/local",
			},
			consumers: []*corev1.Pod{consumer}, wantReady: true, wantText: "Local PV Hostpath", wantLevel: domain.SeverityInfo, wantChecks: 1,
		},
		{
			name:      "OpenEBS local device remains runtime-probed",
			class:     &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "openebs-device"}, Provisioner: "openebs.io/local", Parameters: map[string]string{"storageType": "device"}},
			consumers: []*corev1.Pod{consumer}, wantReady: true, wantText: "StorageType=device", wantLevel: domain.SeverityWarning, wantChecks: 1,
		},
		{
			name:         "offline OpenEBS source still requires runtime inspection",
			enableShared: true,
			class:        &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "openebs-lvmpv"}, Provisioner: "local.csi.openebs.io"},
			wantReady:    true, wantInspect: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := &domain.MigrationPlan{Ready: true}

			operation := test.operation
			if operation == "" {
				operation = domain.OperationMigrate
			}

			pvDriver := test.pvDriver
			if pvDriver == "" && test.class.Provisioner != "openebs.io/local" {
				pvDriver = test.class.Provisioner
			}

			pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-source"}}
			if pvDriver != "" {
				pv.Spec.CSI = &corev1.CSIPersistentVolumeSource{
					Driver:       pvDriver,
					VolumeHandle: "pv-source",
				}
			}

			planner := New(
				nil,
				nil,
			).WithOpenEBSLVMSharedVolumeManager(plannerOpenEBSLVMSharedVolumeManager{shared: test.lvmShared, err: test.lvmErr})

			inspect, patch := planner.checkWarmCopyMountCompatibility(
				context.Background(),
				plan,
				operation,
				test.enableShared,
				pvc,
				pv,
				test.class.Name,
				test.class,
				nil,
				test.consumers,
			)
			if inspect != test.wantInspect {
				t.Fatalf("inspect OpenEBS LVM=%t want=%t", inspect, test.wantInspect)
			}

			if patch != test.wantPatch {
				t.Fatalf("patch OpenEBS LVM=%t want=%t", patch, test.wantPatch)
			}

			if plan.Ready != test.wantReady || len(plan.Checks) != test.wantChecks {
				t.Fatalf("ready=%t checks=%#v", plan.Ready, plan.Checks)
			}

			if test.wantChecks > 0 &&
				(plan.Checks[0].Severity != test.wantLevel || !strings.Contains(plan.Checks[0].Message, test.wantText)) {
				t.Fatalf("check=%#v", plan.Checks[0])
			}
		})
	}
}

func TestWarmCopyRequestedUsesPrecopyPasses(t *testing.T) {
	if warmCopyRequested(Options{Operation: domain.OperationMigratePod, PrecopyPasses: 0}) {
		t.Fatal("offline migration requested warm copy")
	}

	if !warmCopyRequested(Options{Operation: domain.OperationMigratePod, PrecopyPasses: 1}) {
		t.Fatal("precopy migration did not request warm copy")
	}
}

func TestCheckWarmCopyMountCompatibilityUsesLVMSourcePVWithoutStorageClass(t *testing.T) {
	plan := &domain.MigrationPlan{Ready: true}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-source"},
		Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{
			CSI: &corev1.CSIPersistentVolumeSource{
				Driver:       kube.OpenEBSLVMCSIDriver,
				VolumeHandle: "pv-source",
			},
		}},
	}
	consumer := podWithPVC("database-0")
	planner := New(
		nil,
		nil,
	).WithOpenEBSLVMSharedVolumeManager(plannerOpenEBSLVMSharedVolumeManager{})

	inspect, patch := planner.checkWarmCopyMountCompatibility(
		context.Background(),
		plan,
		domain.OperationMigrate,
		false,
		pvc,
		pv,
		"deleted-class",
		nil,
		errors.New("not found"),
		[]*corev1.Pod{consumer},
	)
	if !inspect || patch || plan.Ready ||
		!hasFailedCheckContaining(plan, "warm-copy-mount", "LVMVolume") {
		t.Fatalf("inspect=%t patch=%t ready=%t checks=%#v", inspect, patch, plan.Ready, plan.Checks)
	}
}

func TestOpenEBSLocalStorageTypeParsesParametersAndConfig(t *testing.T) {
	for _, test := range []struct {
		name  string
		class *storagev1.StorageClass
		want  string
	}{
		{name: "parameter", class: &storagev1.StorageClass{Provisioner: "openebs.io/local", Parameters: map[string]string{"StorageType": "hostpath"}}, want: "hostpath"},
		{name: "annotation", class: &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"cas.openebs.io/config": "- name: StorageType\n  value: hostpath\n"}}, Provisioner: "openebs.io/local"}, want: "hostpath"},
		{name: "malformed annotation", class: &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"cas.openebs.io/config": "["}}, Provisioner: "openebs.io/local"}},
		{name: "different provisioner", class: &storagev1.StorageClass{Provisioner: "example.io", Parameters: map[string]string{"storageType": "hostpath"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := openEBSLocalStorageType(test.class); got != test.want {
				t.Fatalf("storage type=%q want=%q", got, test.want)
			}
		})
	}
}

func TestPlanReportsOpenEBSWarmCopyMountCheck(t *testing.T) {
	objects := plannerObjects("2Gi")
	storageClass := testutil.MustType[*storagev1.StorageClass](t, objects[3])
	storageClass.Provisioner = "local.csi.openebs.io"
	testutil.MustType[*corev1.PersistentVolume](t, objects[6]).Spec.CSI = &corev1.CSIPersistentVolumeSource{
		Driver:       kube.OpenEBSLVMCSIDriver,
		VolumeHandle: "pv-source",
	}
	consumer := podWithPVC("database-0")
	consumer.Status.Phase = corev1.PodRunning
	objects = append(objects, consumer)
	options := Options{
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
		PrecopyPasses:    1,
	}

	plan, err := New(plannerClient(objects...), nil).
		WithOpenEBSLVMSharedVolumeManager(plannerOpenEBSLVMSharedVolumeManager{}).
		Plan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if !hasFailedCheckContaining(plan, "warm-copy-mount", "--precopy-passes 0") {
		t.Fatalf("warm-copy mount check missing: %#v", plan.Checks)
	}

	if !hasFailedCheckContaining(plan, "warm-copy-mount", "--openebs-lvm-enable-shared") {
		t.Fatalf("OpenEBS LVM shared recovery missing: %#v", plan.Checks)
	}

	options.PrecopyPasses = 0

	offlinePlan, err := New(plannerClient(objects...), nil).
		WithOpenEBSLVMSharedVolumeManager(plannerOpenEBSLVMSharedVolumeManager{}).
		Plan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}

	if hasFailedCheck(offlinePlan, "warm-copy-mount") {
		t.Fatalf("offline plan includes warm-copy mount check: %#v", offlinePlan.Checks)
	}
}

func TestPlanRejectsSourcePVClaimRefDrift(t *testing.T) {
	objects := plannerObjects("2Gi")
	pv := testutil.MustType[*corev1.PersistentVolume](t, objects[6])
	pv.Spec.ClaimRef.Name = "other"

	plan, err := New(plannerClient(objects...), nil).Plan(context.Background(), Options{
		SessionID:          "binding-drift",
		Operation:          domain.OperationMigrate,
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
		PrecopyPasses:      0,
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "source-binding") {
		t.Fatalf("plan=%#v", plan)
	}
}

func TestCheckPVCReferencesReportsListErrors(t *testing.T) {
	plan := &domain.MigrationPlan{Ready: true}
	New(nil, nil).checkPVCReferencesFromPods(
		plan,
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
		},
		nil,
		domain.WorkloadSpec{},
		domain.OperationMigrate,
		false,
		nil,
		errors.New("list timeout"),
	)

	if plan.Ready || len(plan.Checks) != 1 ||
		!strings.Contains(plan.Checks[0].Message, "list timeout") {
		t.Fatalf("checks=%#v", plan.Checks)
	}
}

func TestCheckPVCReferencesTreatsDeploymentPodsAsOneMigrationUnit(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
	}
	selected := podWithPVC("web-1")
	sibling := podWithPVC("web-2")
	foreign := podWithPVC("other")
	workload := domain.WorkloadSpec{
		Adapter: domain.WorkloadDeployment,
		AffectedPods: []domain.ObjectReference{
			{Namespace: "app", Name: selected.Name, UID: selected.UID},
			{Namespace: "app", Name: sibling.Name, UID: sibling.UID},
		},
	}

	plan := &domain.MigrationPlan{Ready: true}
	New(nil, nil).checkPVCReferencesFromPods(
		plan,
		pvc,
		selected,
		workload,
		domain.OperationMigratePod,
		false,
		[]corev1.Pod{*selected, *sibling},
		nil,
	)

	if !plan.Ready || len(plan.Checks) != 1 || !plan.Checks[0].Passed {
		t.Fatalf("same-Deployment consumers plan=%#v", plan)
	}

	plan = &domain.MigrationPlan{Ready: true}
	New(nil, nil).checkPVCReferencesFromPods(
		plan,
		pvc,
		selected,
		workload,
		domain.OperationMigratePod,
		false,
		[]corev1.Pod{*selected, *sibling, *foreign},
		nil,
	)

	if plan.Ready || len(plan.Checks) != 1 ||
		!strings.Contains(plan.Checks[0].Message, foreign.Name) {
		t.Fatalf("foreign consumer plan=%#v", plan)
	}

	replacement := sibling.DeepCopy()
	replacement.UID = "replacement-uid"
	plan = &domain.MigrationPlan{Ready: true}
	New(nil, nil).checkPVCReferencesFromPods(
		plan,
		pvc,
		selected,
		workload,
		domain.OperationMigratePod,
		false,
		[]corev1.Pod{*selected, *replacement},
		nil,
	)

	if plan.Ready || len(plan.Checks) != 1 ||
		!strings.Contains(plan.Checks[0].Message, replacement.Name) {
		t.Fatalf("same-name replacement plan=%#v", plan)
	}
}

func TestPlanVolumeConsumersModelsConcurrentRWODestinationByVolume(t *testing.T) {
	selected := podWithPVC("database-0")
	sibling := podWithPVC("database-1")
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		},
	}
	workload := domain.WorkloadSpec{
		Adapter: domain.WorkloadStatefulSet,
		AffectedPods: []domain.ObjectReference{
			{Namespace: selected.Namespace, Name: selected.Name, UID: selected.UID},
			{Namespace: sibling.Namespace, Name: sibling.Name, UID: sibling.UID},
		},
	}

	for _, test := range []struct {
		name            string
		enableShared    bool
		accessModes     []corev1.PersistentVolumeAccessMode
		provisioner     string
		wantSharedCheck bool
		wantPatch       bool
	}{
		{
			name:            "RWO requires explicit authorization",
			accessModes:     []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			provisioner:     kube.OpenEBSLVMCSIDriver,
			wantSharedCheck: true,
		},
		{
			name:            "authorized RWO enables destination patch",
			enableShared:    true,
			accessModes:     []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			provisioner:     kube.OpenEBSLVMCSIDriver,
			wantSharedCheck: true,
			wantPatch:       true,
		},
		{
			name:        "RWX does not require OpenEBS shared",
			accessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			provisioner: kube.OpenEBSLVMCSIDriver,
		},
		{
			name:        "non OpenEBS target does not require OpenEBS shared",
			accessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			provisioner: "example.csi.io",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := &planState{
				options: Options{
					Operation:              domain.OperationMigratePod,
					OpenEBSLVMEnableShared: test.enableShared,
				},
				plan:      &domain.MigrationPlan{Ready: true},
				workload:  workload,
				sourcePod: selected,
				inventory: planInventory{namespacePods: []corev1.Pod{*selected, *sibling}},
				plannedVolumes: []domain.PlannedVolume{{
					AccessModes:    test.accessModes,
					CSIProvisioner: test.provisioner,
				}},
				volumeSpecs:        []domain.VolumeSpec{{AccessModes: test.accessModes}},
				storageClasses:     map[string]*storagev1.StorageClass{},
				storageClassErrors: map[string]error{},
				unmanagedConsumers: map[string]struct{}{},
				copyConsumerNodes:  map[string]struct{}{},
			}

			New(nil, nil).checkPlanVolumeConsumers(
				context.Background(),
				state,
				planVolumeInput{pvc: pvc},
			)

			if got := state.volumeSpecs[0].ConcurrentConsumers; got != 2 {
				t.Fatalf("session concurrent consumers=%d, want 2", got)
			}

			if got := state.plannedVolumes[0].ConcurrentConsumers; got != 2 {
				t.Fatalf("plan concurrent consumers=%d, want 2", got)
			}

			if got := hasFailedCheck(
				state.plan,
				"destination-shared-mount",
			); got != (test.wantSharedCheck && !test.enableShared) {
				t.Fatalf("failed shared check=%t checks=%#v", got, state.plan.Checks)
			}

			if got := hasPassedCheck(
				state.plan,
				"destination-shared-mount",
			); got != (test.wantSharedCheck && test.enableShared) {
				t.Fatalf("passed shared check=%t checks=%#v", got, state.plan.Checks)
			}

			if state.patchOpenEBSShared != test.wantPatch {
				t.Fatalf(
					"patch OpenEBS shared=%t, want %t",
					state.patchOpenEBSShared,
					test.wantPatch,
				)
			}
		})
	}
}

func TestCheckSharedRWOSchedulingRejectsHardCollocationConflicts(t *testing.T) {
	matchingSelector := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "database"}}

	for _, test := range []struct {
		name        string
		configure   func(*corev1.Pod)
		wantFailure bool
		wantMessage string
	}{
		{
			name: "required anti-affinity matching another consumer",
			configure: func(pod *corev1.Pod) {
				pod.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
						LabelSelector: matchingSelector,
						TopologyKey:   corev1.LabelHostname,
					}},
				}}
			},
			wantFailure: true,
			wantMessage: "required podAntiAffinity",
		},
		{
			name: "required anti-affinity selector does not match consumers",
			configure: func(pod *corev1.Pod) {
				pod.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "other"},
						},
						TopologyKey: corev1.LabelHostname,
					}},
				}}
			},
		},
		{
			name: "preferred anti-affinity permits co-location",
			configure: func(pod *corev1.Pod) {
				pod.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
					PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
						Weight: 100,
						PodAffinityTerm: corev1.PodAffinityTerm{
							LabelSelector: matchingSelector,
							TopologyKey:   corev1.LabelHostname,
						},
					}},
				}}
			},
		},
		{
			name: "strict hostname spread rejects excessive skew",
			configure: func(pod *corev1.Pod) {
				pod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
					MaxSkew:           1,
					TopologyKey:       corev1.LabelHostname,
					WhenUnsatisfiable: corev1.DoNotSchedule,
					LabelSelector:     matchingSelector,
				}}
			},
			wantFailure: true,
			wantMessage: "DoNotSchedule topologySpread",
		},
		{
			name: "strict hostname spread permits configured skew",
			configure: func(pod *corev1.Pod) {
				pod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
					MaxSkew:           2,
					TopologyKey:       corev1.LabelHostname,
					WhenUnsatisfiable: corev1.DoNotSchedule,
					LabelSelector:     matchingSelector,
				}}
			},
		},
		{
			name: "soft hostname spread permits co-location",
			configure: func(pod *corev1.Pod) {
				pod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
					MaxSkew:           1,
					TopologyKey:       corev1.LabelHostname,
					WhenUnsatisfiable: corev1.ScheduleAnyway,
					LabelSelector:     matchingSelector,
				}}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			selected := podWithPVC("database-a")
			sibling := podWithPVC("database-b")
			selected.Labels = map[string]string{"app": "database"}
			sibling.Labels = map[string]string{"app": "database"}

			test.configure(selected)

			target := sharedSchedulingNode("node-a")
			other := sharedSchedulingNode("node-b")
			state := &planState{
				options: Options{OpenEBSLVMEnableShared: true},
				plan:    &domain.MigrationPlan{Ready: true},
				workload: domain.WorkloadSpec{
					Adapter: domain.WorkloadDeployment,
					AffectedPods: []domain.ObjectReference{
						{Namespace: selected.Namespace, Name: selected.Name, UID: selected.UID},
						{Namespace: sibling.Namespace, Name: sibling.Name, UID: sibling.UID},
					},
				},
				sourcePod:  selected,
				targetNode: target,
				inventory: planInventory{
					namespacePods:   []corev1.Pod{*selected, *sibling},
					sourceNamespace: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}},
					nodes:           []corev1.Node{*target, *other},
				},
				plannedVolumes: []domain.PlannedVolume{{
					SourcePVC: domain.ObjectReference{Namespace: "app", Name: "data"},
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					CSIProvisioner:      kube.OpenEBSLVMCSIDriver,
					ConcurrentConsumers: 2,
				}},
			}

			New(nil, nil).checkSharedRWOScheduling(state)

			if got := hasFailedCheck(
				state.plan,
				"destination-shared-scheduling",
			); got != test.wantFailure {
				t.Fatalf(
					"failed scheduling check=%t, want %t; checks=%#v",
					got,
					test.wantFailure,
					state.plan.Checks,
				)
			}

			if !test.wantFailure && !hasPassedCheck(state.plan, "destination-shared-scheduling") {
				t.Fatalf("missing passed scheduling check: %#v", state.plan.Checks)
			}

			if test.wantMessage != "" &&
				!strings.Contains(state.plan.Checks[0].Message, test.wantMessage) {
				t.Fatalf(
					"message=%q, want substring %q",
					state.plan.Checks[0].Message,
					test.wantMessage,
				)
			}
		})
	}
}

func TestCheckSharedRWOSchedulingIgnoresUnrelatedVolumes(t *testing.T) {
	selected := podWithPVC("database-a")
	sibling := podWithPVC("database-b")
	selected.Labels = map[string]string{"app": "database"}
	sibling.Labels = map[string]string{"app": "database"}
	selected.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "database"}},
			TopologyKey:   corev1.LabelHostname,
		}},
	}}
	target := sharedSchedulingNode("node-a")

	for _, test := range []struct {
		name        string
		accessModes []corev1.PersistentVolumeAccessMode
		provisioner string
		consumers   int
	}{
		{name: "single consumer", accessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, provisioner: kube.OpenEBSLVMCSIDriver, consumers: 1},
		{name: "RWX", accessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}, provisioner: kube.OpenEBSLVMCSIDriver, consumers: 2},
		{name: "other CSI", accessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, provisioner: "example.csi.io", consumers: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := &planState{
				options:    Options{OpenEBSLVMEnableShared: true},
				plan:       &domain.MigrationPlan{Ready: true},
				sourcePod:  selected,
				targetNode: target,
				plannedVolumes: []domain.PlannedVolume{{
					SourcePVC:           domain.ObjectReference{Namespace: "app", Name: "data"},
					AccessModes:         test.accessModes,
					CSIProvisioner:      test.provisioner,
					ConcurrentConsumers: test.consumers,
				}},
				inventory: planInventory{namespacePods: []corev1.Pod{*selected, *sibling}},
			}

			New(nil, nil).checkSharedRWOScheduling(state)

			if len(state.plan.Checks) != 0 {
				t.Fatalf("unrelated volume checks=%#v", state.plan.Checks)
			}
		})
	}
}

func sharedSchedulingNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				corev1.LabelHostname: name,
			},
		},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: corev1.ConditionTrue,
		}}},
	}
}

func TestMigrationUnitConsumerCountUsesRecordedPodIdentity(t *testing.T) {
	selected := podWithPVC("web-1")
	sibling := podWithPVC("web-2")
	replacement := sibling.DeepCopy()
	replacement.UID = "replacement-uid"
	workload := domain.WorkloadSpec{
		Adapter: domain.WorkloadDeployment,
		AffectedPods: []domain.ObjectReference{
			{Namespace: selected.Namespace, Name: selected.Name, UID: selected.UID},
			{Namespace: sibling.Namespace, Name: sibling.Name, UID: sibling.UID},
		},
	}

	if got := migrationUnitConsumerCount(
		workload,
		selected,
		[]*corev1.Pod{selected, sibling},
	); got != 2 {
		t.Fatalf("matching consumer count=%d, want 2", got)
	}

	if got := migrationUnitConsumerCount(
		workload,
		selected,
		[]*corev1.Pod{selected, replacement},
	); got != 1 {
		t.Fatalf("replacement consumer count=%d, want 1", got)
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
		SessionID:          "migration",
		SourceNamespace:    "app",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		TemporaryNamespace: "system",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
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
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "fast"},
		Provisioner: "example.csi.io",
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}}

	tests := []struct {
		name    string
		csiNode *storagev1.CSINode
		message string
	}{
		{name: "CSINode absent", message: "has no CSINode object"},
		{
			name: "driver absent",
			csiNode: &storagev1.CSINode{
				ObjectMeta: metav1.ObjectMeta{Name: "node-b"},
				Spec: storagev1.CSINodeSpec{
					Drivers: []storagev1.CSINodeDriver{{Name: "other.csi.io"}},
				},
			},
			message: "is absent from CSINode",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := &domain.MigrationPlan{Ready: true}

			var err error
			if tt.csiNode == nil {
				err = apierrors.NewNotFound(
					schema.GroupResource{Group: "storage.k8s.io", Resource: "csinodes"},
					node.Name,
				)
			}

			New(nil, nil).checkCSINodeFromObject(plan, sc, node, tt.csiNode, err)

			if !plan.Ready || len(plan.Checks) != 1 ||
				plan.Checks[0].Severity != domain.SeverityWarning ||
				!strings.Contains(plan.Checks[0].Message, tt.message) {
				t.Fatalf("checks=%#v", plan.Checks)
			}
		})
	}
}

func TestCheckCSINodeFailsOnEmptyObject(t *testing.T) {
	plan := &domain.MigrationPlan{Ready: true}
	New(nil, nil).checkCSINodeFromObject(plan,
		&storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: "fast"},
			Provisioner: "example.csi.io",
		},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}},
		nil,
		nil,
	)

	if plan.Ready || len(plan.Checks) != 1 || plan.Checks[0].Severity != domain.SeverityError ||
		!strings.Contains(plan.Checks[0].Message, "returned an empty object") {
		t.Fatalf("plan ready=%t checks=%#v", plan.Ready, plan.Checks)
	}
}

func TestPodPVCNamesAreUniqueAndSorted(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Volumes: []corev1.Volume{
		{
			Name: "z",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: "z-data",
				},
			},
		},
		{
			Name: "a",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: "a-data",
				},
			},
		},
		{
			Name: "duplicate",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: "z-data",
				},
			},
		},
		{
			Name: "empty",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{},
			},
		},
	}}}

	names := podPVCNames(pod)
	if len(names) != 2 || names[0] != "a-data" || names[1] != "z-data" {
		t.Fatalf("PVC names=%v", names)
	}
}

func podWithPVC(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: name, UID: types.UID(name + "-uid")},
		Spec: corev1.PodSpec{
			NodeName: "node-a",
			Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
			}}},
		},
	}
}
