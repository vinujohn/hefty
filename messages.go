package hefty

import (
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type referenceMsg struct {
	S3Region string `json:"s3_region"`
	S3Bucket string `json:"s3_bucket"`
	S3Key    string `json:"s3_key"`
}

type sqsLargeMsg struct {
	Body              *string                                   `json:"body"`
	MessageAttributes map[string]sqsTypes.MessageAttributeValue `json:"message_attributes"`
}
