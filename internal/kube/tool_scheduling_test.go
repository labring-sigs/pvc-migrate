package kube_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	. "github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestToolComponentNodeHelmValuesMirrorHardTaints(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "storage-node"},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{
			{Key: "dedicated", Value: "storage", Effect: corev1.TaintEffectNoSchedule},
			{Key: "draining", Effect: corev1.TaintEffectNoExecute},
			{Key: "preference", Effect: corev1.TaintEffectPreferNoSchedule},
		}},
	}

	values, err := ToolComponentNodeHelmValues(ToolComponentRclone, node)
	if err != nil {
		t.Fatal(err)
	}

	for _, expected := range []string{
		"rclone.nodeName=storage-node",
		"rclone.tolerations[0].key=dedicated",
		"rclone.tolerations[0].operator=Equal",
		"rclone.tolerations[0].value=storage",
		"rclone.tolerations[1].key=draining",
		"rclone.tolerations[1].operator=Exists",
	} {
		if !slices.Contains(values, expected) {
			t.Fatalf("missing %q in %v", expected, values)
		}
	}

	for _, value := range values {
		if value == "rclone.tolerations[2].key=preference" {
			t.Fatalf("PreferNoSchedule taint emitted: %v", values)
		}
	}
}

func TestToolComponentNodeHelmValuesValidateInputs(t *testing.T) {
	if _, err := ToolComponentNodeHelmValues(
		ToolComponentShell,
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}},
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("component category=%s error=%v", domain.CategoryOf(err), err)
	}

	if _, err := ToolComponentNodeHelmValues(
		ToolComponentRclone,
		nil,
	); domain.CategoryOf(
		err,
	) != domain.ErrorKubernetes {
		t.Fatalf("node category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestToolComponentTolerationsDeduplicateAcrossNodes(t *testing.T) {
	taint := corev1.Taint{Key: "storage", Value: "true", Effect: corev1.TaintEffectNoSchedule}

	values := ToolComponentTolerationHelmValues(ToolComponentSSHD,
		&corev1.Node{Spec: corev1.NodeSpec{Taints: []corev1.Taint{taint}}},
		&corev1.Node{Spec: corev1.NodeSpec{Taints: []corev1.Taint{taint}}},
	)
	if got := len(values); got != 4 {
		t.Fatalf("values=%v", values)
	}
}

func TestToolServiceAccountHelmValues(t *testing.T) {
	values, err := ToolServiceAccountHelmValues("s3-workload.identity")
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Contains(values.Values, "rclone.serviceAccount.create=false") ||
		!slices.Contains(values.StringValues, "rclone.serviceAccount.name=s3-workload.identity") {
		t.Fatalf("service account values=%v", values)
	}

	_, err = ToolServiceAccountHelmValues("")
	if domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("empty service account category=%s error=%v", domain.CategoryOf(err), err)
	}

	if _, err := ToolServiceAccountHelmValues(
		"Invalid_Name",
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("invalid service account category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestTransferServiceAccountHelmValuesDisableChartCreation(t *testing.T) {
	values := TransferServiceAccountHelmValues()

	wantValues := []string{
		"sshd.serviceAccount.create=false",
		"rsync.serviceAccount.create=false",
	}

	wantStringValues := []string{
		"sshd.serviceAccount.name=" + TransferServiceAccountName,
		"rsync.serviceAccount.name=" + TransferServiceAccountName,
	}
	if !slices.Equal(values.Values, wantValues) ||
		!slices.Equal(values.StringValues, wantStringValues) {
		t.Fatalf(
			"transfer service account values=%v, want values=%v stringValues=%v",
			values,
			wantValues,
			wantStringValues,
		)
	}
}

func TestEnsureTransferServiceAccountCreatesNoTokenAccount(t *testing.T) {
	client := fake.NewClientset()
	if err := EnsureTransferServiceAccount(context.Background(), client, "tenant"); err != nil {
		t.Fatal(err)
	}

	account, err := client.CoreV1().ServiceAccounts("tenant").Get(
		context.Background(), TransferServiceAccountName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	if account.AutomountServiceAccountToken == nil || *account.AutomountServiceAccountToken {
		t.Fatalf(
			"automountServiceAccountToken=%v, want false",
			account.AutomountServiceAccountToken,
		)
	}

	if account.Labels[ManagedByLabel] != ManagedByValue {
		t.Fatalf("managed-by label=%q", account.Labels[ManagedByLabel])
	}
}

func TestEnsureTransferServiceAccountRejectsUnmanagedAccount(t *testing.T) {
	automount := false
	client := fake.NewClientset(&corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TransferServiceAccountName,
			Namespace: "tenant",
		},
		AutomountServiceAccountToken: &automount,
	})

	err := EnsureTransferServiceAccount(context.Background(), client, "tenant")
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("category=%s error=%v, want conflict", domain.CategoryOf(err), err)
	}
}

func TestEnsureTransferServiceAccountRepairsOwnedAutomount(t *testing.T) {
	client := fake.NewClientset()
	if err := EnsureTransferServiceAccount(context.Background(), client, "tenant"); err != nil {
		t.Fatal(err)
	}

	automount := true

	account, err := client.CoreV1().ServiceAccounts("tenant").Get(
		context.Background(), TransferServiceAccountName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	account.AutomountServiceAccountToken = &automount

	if _, err := client.CoreV1().ServiceAccounts("tenant").Update(
		context.Background(), account, metav1.UpdateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	if err := EnsureTransferServiceAccount(context.Background(), client, "tenant"); err != nil {
		t.Fatal(err)
	}

	account, err = client.CoreV1().ServiceAccounts("tenant").Get(
		context.Background(), TransferServiceAccountName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	if account.AutomountServiceAccountToken == nil || *account.AutomountServiceAccountToken {
		t.Fatalf(
			"automountServiceAccountToken=%v, want false",
			account.AutomountServiceAccountToken,
		)
	}
}

func TestEnsureTransferServiceAccountRetriesResourceVersionConflict(t *testing.T) {
	client := fake.NewClientset()
	if err := EnsureTransferServiceAccount(context.Background(), client, "tenant"); err != nil {
		t.Fatal(err)
	}

	automount := true

	account, err := client.CoreV1().ServiceAccounts("tenant").Get(
		context.Background(), TransferServiceAccountName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	account.AutomountServiceAccountToken = &automount

	if _, err := client.CoreV1().ServiceAccounts("tenant").Update(
		context.Background(), account, metav1.UpdateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	updates := 0
	client.PrependReactor(
		"update",
		"serviceaccounts",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			updates++
			if updates == 1 {
				return true, nil, apierrors.NewConflict(
					corev1.Resource("serviceaccounts"),
					TransferServiceAccountName,
					errors.New("concurrent update"),
				)
			}

			return false, nil, nil
		},
	)

	if err := EnsureTransferServiceAccount(context.Background(), client, "tenant"); err != nil {
		t.Fatal(err)
	}

	if updates < 2 {
		t.Fatalf("update attempts=%d, want retry after conflict", updates)
	}

	account, err = client.CoreV1().ServiceAccounts("tenant").Get(
		context.Background(), TransferServiceAccountName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	if account.AutomountServiceAccountToken == nil || *account.AutomountServiceAccountToken {
		t.Fatalf(
			"automountServiceAccountToken=%v, want false",
			account.AutomountServiceAccountToken,
		)
	}
}

func TestEnsureTransferServiceAccountValidatesNamespace(t *testing.T) {
	err := EnsureTransferServiceAccount(
		context.Background(),
		fake.NewClientset(),
		"bad namespace",
	)
	if domain.CategoryOf(err) != domain.ErrorValidation {
		t.Fatalf("category=%s error=%v, want validation", domain.CategoryOf(err), err)
	}
}
