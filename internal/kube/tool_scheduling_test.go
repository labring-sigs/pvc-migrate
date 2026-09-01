package kube_test

import (
	"slices"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	. "github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	if !slices.Contains(values, "rclone.serviceAccount.create=false") ||
		!slices.Contains(values, "rclone.serviceAccount.name=s3-workload.identity") {
		t.Fatalf("service account values=%v", values)
	}

	if values, err := ToolServiceAccountHelmValues(""); err != nil || values != nil {
		t.Fatalf("empty service account values=%v err=%v", values, err)
	}

	if _, err := ToolServiceAccountHelmValues(
		"Invalid_Name",
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("invalid service account category=%s error=%v", domain.CategoryOf(err), err)
	}
}
