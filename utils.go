package hefty

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// bucketExists checks whether a bucket exists in the current account.
// TODO: we need a better way to check if a bucket exists.  for example if a bucket exists in another account,
// we won't have access to it.  Needs research
func bucketExists(s3Client *s3.Client, bucketName string) (bool, error) {
	_, err := s3Client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch apiError.(type) {
			case *types.NotFound:
				return false, nil
			default:
				return false, fmt.Errorf("unable to check if bucket exits. %v", apiError)
			}
		}
	}
	return true, nil
}
