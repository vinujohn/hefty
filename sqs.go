package hefty

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type SqsClient struct {
	sqs.Client
	bucket string
}

func NewSqsClient(sqsClient *sqs.Client, s3Client *s3.Client, bucketName string) (*SqsClient, error) {
	// check if bucket exits, if not create it using the current s3 settings
	if ok, err := bucketExists(s3Client, bucketName); !ok {
		if err != nil {
			return nil, err
		}

		_, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
			CreateBucketConfiguration: &types.CreateBucketConfiguration{
				LocationConstraint: types.BucketLocationConstraint(s3Client.Options().Region),
			},
		})

		if err != nil {
			return nil, fmt.Errorf("unable to create bucket '%s'. %v", bucketName, err)
		}
	}

	return &SqsClient{
		Client: *sqsClient,
		bucket: bucketName,
	}, nil
}

func (client *SqsClient) SendHeftyMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return client.SendMessage(ctx, params, optFns...)
}

func (client *SqsClient) SendHeftyMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	return client.SendMessageBatch(ctx, params, optFns...)
}

func (client *SqsClient) ReceiveHeftyMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return client.ReceiveMessage(ctx, params, optFns...)
}
