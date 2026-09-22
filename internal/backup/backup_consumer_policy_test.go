package backup

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func accessModesPVC(modes ...corev1.PersistentVolumeAccessMode) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		Spec: corev1.PersistentVolumeClaimSpec{AccessModes: modes},
	}
}

// TestValidateBackupConsumersPolicy pins the consumer policy for backup
// sources: offline backup refuses any active consumer, online backup is
// refused only for ReadWriteOncePod volumes a second Pod cannot mount, and
// consumer-free sources always pass.
func TestValidateBackupConsumersPolicy(t *testing.T) {
	rwo := accessModesPVC(corev1.ReadWriteOnce)
	rwop := accessModesPVC(corev1.ReadWriteOncePod)
	rwx := accessModesPVC(corev1.ReadWriteMany)

	if err := validateBackupConsumers(rwo, []string{"app-0"}, false); err == nil ||
		!strings.Contains(err.Error(), "referenced by Pod(s) app-0") {
		t.Fatalf("offline error=%v", err)
	}

	if err := validateBackupConsumers(rwop, []string{"app-0"}, true); err == nil ||
		!strings.Contains(err.Error(), "ReadWriteOncePod") {
		t.Fatalf("online RWOP error=%v", err)
	}

	if err := validateBackupConsumers(rwo, []string{"app-0"}, true); err != nil {
		t.Fatalf("online RWO with consumers error=%v", err)
	}

	if err := validateBackupConsumers(rwx, []string{"app-0"}, true); err != nil {
		t.Fatalf("online RWX with consumers error=%v", err)
	}

	if err := validateBackupConsumers(rwop, nil, false); err != nil {
		t.Fatalf("consumer-free RWOP offline error=%v", err)
	}
}

// TestHasRWOPAndHasRWOModeClassification pins the access-mode classifiers
// behind the consumer policy: hasRWOP matches only ReadWriteOncePod, while
// hasRWO accepts both exclusive modes.
func TestHasRWOPAndHasRWOModeClassification(t *testing.T) {
	if !hasRWOP(accessModesPVC(corev1.ReadWriteOncePod)) {
		t.Fatal("RWOP volume must classify as RWOP")
	}

	if hasRWOP(accessModesPVC(corev1.ReadWriteOnce, corev1.ReadWriteMany)) {
		t.Fatal("non-RWOP volume must not classify as RWOP")
	}

	for _, modes := range [][]corev1.PersistentVolumeAccessMode{
		{corev1.ReadWriteOnce},
		{corev1.ReadWriteOncePod},
		{corev1.ReadWriteMany, corev1.ReadWriteOnce},
	} {
		if !hasRWO(accessModesPVC(modes...)) {
			t.Fatalf("modes %v must classify as exclusive-mount", modes)
		}
	}

	if hasRWO(accessModesPVC(corev1.ReadWriteMany, corev1.ReadOnlyMany)) {
		t.Fatal("shared-only volume must not classify as exclusive-mount")
	}
}
