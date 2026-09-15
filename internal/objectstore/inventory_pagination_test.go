package objectstore_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
)

type repeatingInventoryAPI struct {
	*fakeS3
	tokens []string
	calls  int
}

func (f *repeatingInventoryAPI) ListObjectsV2(
	context.Context,
	*s3.ListObjectsV2Input,
	...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	f.calls++
	if f.calls > len(f.tokens) {
		return nil, errors.New("listing continued after the token cycle")
	}

	return &s3.ListObjectsV2Output{
		IsTruncated:           aws.Bool(true),
		NextContinuationToken: aws.String(f.tokens[f.calls-1]),
	}, nil
}

func TestInventoryRejectsPaginationCycles(t *testing.T) {
	for _, tokens := range [][]string{{"a", "a"}, {"a", "b", "a"}} {
		api := &repeatingInventoryAPI{fakeS3: newFakeS3(), tokens: tokens}

		store, err := objectstore.NewWithClient(
			api,
			objectstore.Config{Bucket: "backups", Name: "daily"},
			objectstore.Credentials{},
		)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := store.Inventory(
			t.Context(),
		); err == nil ||
			!strings.Contains(err.Error(), "repeated a continuation token") {
			t.Fatalf("tokens = %v, error = %v", tokens, err)
		}

		if api.calls != len(tokens) {
			t.Fatalf("listing continued past the cycle: calls = %d", api.calls)
		}
	}
}

func TestInventoryCancellationPreventsFurtherReads(t *testing.T) {
	api := &repeatingInventoryAPI{fakeS3: newFakeS3(), tokens: []string{"a"}}

	store, err := objectstore.NewWithClient(
		api,
		objectstore.Config{Bucket: "backups", Name: "daily"},
		objectstore.Credentials{},
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := store.Inventory(ctx); !errors.Is(err, context.Canceled) || api.calls != 0 {
		t.Fatalf("canceled listing: calls = %d, error = %v", api.calls, err)
	}
}
