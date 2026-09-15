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
	flags := &s3CredentialFlags{
		accessKey: input.AccessKey, secretKey: input.SecretKey,
		accessKeyKey: input.AccessKeyKey, secretKeyKey: input.SecretKeyKey,
		secretName:        input.CredentialsSecret,
		accessKeyExplicit: input.AccessKeyExplicit, secretKeyExplicit: input.SecretKeyExplicit,
	}
	if err := loadS3Credentials(ctx, client, input.Namespace, flags); err != nil {
		return BucketFlagsForTest{}, err
	}

	input.AccessKey, input.SecretKey = flags.accessKey, flags.secretKey

	return input, nil
}

type CrossClusterFlagsForTest struct {
	SourceKubeconfig      string
	SourceContext         string
	DestinationKubeconfig string
	DestinationContext    string
	SessionNamespace      string
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
