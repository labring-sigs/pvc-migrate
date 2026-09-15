package backup

import (
	"context"

	"k8s.io/client-go/kubernetes"
)

func preflightRestoreToolNode(
	ctx context.Context,
	client kubernetes.Interface,
	targetNode string,
	info *PVCInfo,
) (string, error) {
	consumerNode, err := rwoConsumerNode(info, "restore scheduling")
	if err != nil {
		return "", err
	}

	if consumerNode != "" && targetNode != "" {
		if _, err := selectRestoreToolNode(targetNode, consumerNode, ""); err != nil {
			return "", err
		}
	}

	if consumerNode != "" && targetNode == "" {
		return "", nil
	}

	return uniquePVToolNode(ctx, client, info.PV, "restore preflight")
}
