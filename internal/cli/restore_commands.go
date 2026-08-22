package cli

import (
	"github.com/labring-sigs/pvc-migrate/internal/backup"
	"github.com/spf13/cobra"
)

type restoreBucketFlags struct {
	createPVC               bool
	destinationStorageClass string
	destinationAccessMode   string
	destinationCapacity     string
	targetNode              string
	allowMounted            bool
	deleteExtraneous        bool
}

func (r *rootState) newRestoreCommand() *cobra.Command {
	return r.newObjectTransferCommand(true, false)
}

func applyRestoreRequest(req *backup.Request, flags restoreBucketFlags) {
	req.CreatePVC = flags.createPVC
	req.DestinationStorageClass = flags.destinationStorageClass
	req.DestinationAccessMode = flags.destinationAccessMode
	req.DestinationCapacity = flags.destinationCapacity
	req.TargetNode = flags.targetNode
	req.AllowMounted = flags.allowMounted
	req.DeleteExtraneousFiles = flags.deleteExtraneous
}

func bindRestoreBucketFlags(command *cobra.Command, flags *restoreBucketFlags) {
	command.Flags().
		BoolVar(&flags.createPVC, "create-pvc", false, "Create the destination PVC when it does not exist")
	command.Flags().
		StringVar(&flags.destinationStorageClass, "destination-storage-class", "", "StorageClass for a destination PVC created by restore")
	command.Flags().
		StringVar(&flags.destinationAccessMode, "destination-access-mode", "", "Access mode for a destination PVC created by restore")
	command.Flags().
		StringVar(&flags.destinationCapacity, "destination-capacity", "", "Capacity for a destination PVC created by restore; defaults to the backup capacity")
	command.Flags().
		StringVar(&flags.targetNode, "target-node", "", "Node for restore tool scheduling and destination PVC binding")
	command.Flags().
		BoolVar(&flags.allowMounted, "allow-mounted", false, "Allow restore while the destination PVC has Pod consumers")
	command.Flags().
		BoolVar(&flags.deleteExtraneous, "delete-extraneous", false, "Delete destination files absent from the backup (destructive)")
}
