package hefty

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type Client struct {
	sqs.Client
	bucket string
}

func NewSqsClient(sqsClient *sqs.Client, s3Client *s3.Client, bucketName string) (*Client, error) {
	return &Client{
		Client: *sqsClient,
		bucket: bucketName,
	}, nil
}

func (client *Client) SendHeftyMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return client.SendMessage(ctx, params, optFns...)
}

func (client *Client) SendHeftyMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	return client.SendMessageBatch(ctx, params, optFns...)
}

func (client *Client) ReceiveHeftyMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return client.ReceiveMessage(ctx, params, optFns...)
}
