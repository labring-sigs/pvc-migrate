package kube

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// OpenEBSLocalPVProvisioner is the provisioner used by OpenEBS HostPath and
// device-backed LocalPV StorageClasses. This provisioner accepts only RWO PVCs.
const OpenEBSLocalPVProvisioner = "openebs.io/local"

// ValidateDestinationAccessModes checks capabilities that are known from the
// provisioner contract. Unknown CSI provisioners are left to their own
// admission and provisioning checks because Kubernetes has no access-mode
// capability field on StorageClass.
func ValidateDestinationAccessModes(
	provisioner string,
	modes []corev1.PersistentVolumeAccessMode,
) error {
	for _, mode := range modes {
		switch provisioner {
		case OpenEBSLocalPVProvisioner:
			if mode != corev1.ReadWriteOnce {
				return fmt.Errorf(
					"provisioner %s supports only ReadWriteOnce; requested %s",
					provisioner,
					mode,
				)
			}
		case OpenEBSLVMCSIDriver:
			if mode == corev1.ReadWriteOnce || mode == corev1.ReadWriteOncePod {
				continue
			}
			return fmt.Errorf(
				"provisioner %s supports only ReadWriteOnce and ReadWriteOncePod; requested %s",
				provisioner,
				mode,
			)
		default:
			return nil
		}
	}

	return nil
}
