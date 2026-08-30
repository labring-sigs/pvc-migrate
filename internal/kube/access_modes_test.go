package kube

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestValidateDestinationAccessModes(t *testing.T) {
	tests := []struct {
		name        string
		provisioner string
		modes       []corev1.PersistentVolumeAccessMode
		wantErr     bool
	}{
		{
			name:        "OpenEBS hostpath accepts RWO",
			provisioner: OpenEBSLocalPVProvisioner,
			modes:       []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		},
		{
			name:        "OpenEBS hostpath rejects RWOP",
			provisioner: OpenEBSLocalPVProvisioner,
			modes:       []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod},
			wantErr:     true,
		},
		{
			name:        "OpenEBS LVM accepts RWOP",
			provisioner: OpenEBSLVMCSIDriver,
			modes:       []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod},
		},
		{
			name:        "OpenEBS rejects RWX",
			provisioner: OpenEBSLocalPVProvisioner,
			modes:       []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			wantErr:     true,
		},
		{
			name:        "OpenEBS rejects ROX",
			provisioner: OpenEBSLVMCSIDriver,
			modes:       []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany},
			wantErr:     true,
		},
		{
			name:        "unknown provisioner is deferred",
			provisioner: "example.csi.io",
			modes:       []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateDestinationAccessModes(tt.provisioner, tt.modes); (err != nil) != tt.wantErr {
				t.Fatalf("ValidateDestinationAccessModes() error=%v wantErr=%t", err, tt.wantErr)
			}
		})
	}
}
