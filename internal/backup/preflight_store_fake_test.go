package backup

import (
	"bytes"
	"context"
	"io"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type preflightObjectStore struct {
	manifest  []byte
	puts      int
	onPut     func()
	getErr    error
	deleteErr error
}

func (*preflightObjectStore) ListObjectsV2(
	context.Context,
	*s3.ListObjectsV2Input,
	...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{}, nil
}

func (f *preflightObjectStore) HeadObject(
	context.Context,
	*s3.HeadObjectInput,
	...func(*s3.Options),
) (*s3.HeadObjectOutput, error) {
	return nil, preflightMissingObjectError{}
}

func (f *preflightObjectStore) GetObject(
	_ context.Context,
	_ *s3.GetObjectInput,
	_ ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}

	if len(f.manifest) == 0 {
		return nil, preflightMissingObjectError{}
	}

	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(f.manifest))}, nil
}

func (f *preflightObjectStore) PutObject(
	context.Context,
	*s3.PutObjectInput,
	...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
	f.puts++
	if f.onPut != nil {
		f.onPut()
	}

	return &s3.PutObjectOutput{ETag: aws.String("etag")}, nil
}

func (f *preflightObjectStore) DeleteObject(
	context.Context,
	*s3.DeleteObjectInput,
	...func(*s3.Options),
) (*s3.DeleteObjectOutput, error) {
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &s3.DeleteObjectOutput{}, nil
}

type preflightMissingObjectError struct{}

func (preflightMissingObjectError) Error() string                 { return "missing" }
func (preflightMissingObjectError) ErrorCode() string             { return "NoSuchKey" }
func (preflightMissingObjectError) ErrorMessage() string          { return "missing" }
func (preflightMissingObjectError) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

func containsString(values []string, value string) bool {
	return slices.Contains(values, value)
}
