package cli

import (
	"context"

	"k8s.io/client-go/kubernetes"
)

type BucketFlagsForTest struct {
	AccessKey         string
	SecretKey         string
	Namespace         string
	AccessKeyKey      string
	SecretKeyKey      string
	CredentialsSecret string
	AccessKeyExplicit bool
	SecretKeyExplicit bool
}

func LoadS3CredentialsForTest(
	ctx context.Context,
	client kubernetes.Interface,
	input BucketFlagsForTest,
) (BucketFlagsForTest, error) {
	flags := &bucketFlags{
		accessKey: input.AccessKey, secretKey: input.SecretKey, namespace: input.Namespace,
		accessKeyKey: input.AccessKeyKey, secretKeyKey: input.SecretKeyKey,
		credentialsSecret: input.CredentialsSecret,
		accessKeyExplicit: input.AccessKeyExplicit, secretKeyExplicit: input.SecretKeyExplicit,
	}
	if err := loadS3Credentials(ctx, client, flags); err != nil {
		return BucketFlagsForTest{}, err
	}

	input.AccessKey, input.SecretKey = flags.accessKey, flags.secretKey

	return input, nil
}

func CrossClusterCleanupGuidanceForTest(input CrossClusterFlagsForTest, sessionID string) string {
	return crossClusterCopyCleanupCommand(&crossClusterCopyFlags{
		crossClusterConnectionFlags: crossClusterConnectionFlags{
			sourceKubeconfig:      input.SourceKubeconfig,
			sourceContext:         input.SourceContext,
			destinationKubeconfig: input.DestinationKubeconfig,
			destinationContext:    input.DestinationContext,
			sessionNamespace:      input.SessionNamespace,
		},
	}, sessionID)
}

type CrossClusterFlagsForTest struct {
	SourceKubeconfig      string
	SourceContext         string
	DestinationKubeconfig string
	DestinationContext    string
	SessionNamespace      string
}
