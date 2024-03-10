package hefty

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snsTypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/google/uuid"
	"github.com/vinujohn/hefty/internal/messages"
	"github.com/vinujohn/hefty/internal/utils"
)

type SnsClientWrapper struct {
	sns.Client
	bucket     string
	s3Client   *s3.Client
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
}

// NewSnsClientWrapper will create a new Hefty SNS client wrapper using an existing AWS SNS client and AWS S3 client.
// This Hefty SNS client wrapper will save large messages greater than MaxSqsSnsMessageLengthBytes to AWS S3 in the
// bucket that is specified via `bucketName`. The S3 client should have the ability of reading and writing to this bucket.
// This function will also check if the bucket exists and is accessible.
func NewSnsClientWrapper(snsClient *sns.Client, s3Client *s3.Client, bucketName string) (*SnsClientWrapper, error) {
	// check if bucket exits
	if ok, err := utils.BucketExists(s3Client, bucketName); !ok {
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("bucket %s does not exist or is not accessible", bucketName)
	}

	return &SnsClientWrapper{
		Client:     *snsClient,
		bucket:     bucketName,
		s3Client:   s3Client,
		uploader:   s3manager.NewUploader(s3Client),
		downloader: s3manager.NewDownloader(s3Client),
	}, nil
}

// PublishHeftyMessage will calculate the messages size from `params` and determine if the MaxSqsSnsMessageLengthBytes is exceeded.
// If so, the message is saved in AWS S3 as a hefty message and a reference message is sent to AWS SNS instead.
// If not, the message is directly sent to AWS SNS.
//
// In the case of the reference message being sent, the message itself contains metadata about the hefty message saved in AWS S3
// including bucket name, S3 key, region, and md5 digests. Subscriptions to the AWS SNS topic used in this method should use
// 'Raw Message Delivery' as an option. This ensures that the hefty client can receive messages from these AWS SQS endpoints.
// Other endpoints like AWS Lambda can use the reference message directly and download the S3 message directly without using the
// hefty client.
//
// Note that this function's signature matches that of the AWS SNS SDK's Publish method.
func (client *SnsClientWrapper) PublishHeftyMessage(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
	// input validation; if invalid input let AWS SDK handle it
	if params == nil ||
		params.Message == nil ||
		len(*params.Message) == 0 {

		return client.Publish(ctx, params, optFns...)
	}

	// normalize message attributes
	msgAttributes := messages.MapFromSnsMessageAttributeValues(params.MessageAttributes)

	// calculate message size
	msgSize, err := messages.MessageSize(params.Message, msgAttributes)
	if err != nil {
		return nil, fmt.Errorf("unable to get size of message. %v", err)
	}

	// validate message size
	if msgSize <= MaxSqsSnsMessageLengthBytes {
		return client.Publish(ctx, params, optFns...)
	} else if msgSize > MaxHeftyMessageLengthBytes {
		return nil, fmt.Errorf("message size of %d bytes greater than allowed message size of %d bytes", msgSize, MaxHeftyMessageLengthBytes)
	}

	// create and serialize hefty message
	heftyMsg := messages.NewHeftyMessage(params.Message, msgAttributes, msgSize)
	serialized, bodyOffset, msgAttrOffset, err := heftyMsg.Serialize()
	if err != nil {
		return nil, fmt.Errorf("unable to serialize message. %v", err)
	}

	// create md5 digests
	msgBodyHash := messages.Md5Digest(serialized[bodyOffset:msgAttrOffset])
	msgAttrHash := ""
	if len(heftyMsg.MessageAttributes) > 0 {
		msgAttrHash = messages.Md5Digest(serialized[msgAttrOffset:])
	}

	// create reference message
	refMsg, err := newSnsReferenceMessage(params.TopicArn, client.bucket, client.Options().Region, msgBodyHash, msgAttrHash)
	if err != nil {
		return nil, fmt.Errorf("unable to create reference message from topicArn. %v", err)
	}

	// upload hefty message to s3
	_, err = client.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(client.bucket),
		Key:    aws.String(refMsg.S3Key),
		Body:   bytes.NewReader(serialized),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to upload hefty message to s3. %v", err)
	}

	// replace incoming message body with reference message
	jsonRefMsg, err := json.MarshalIndent(refMsg, "", "\t")
	if err != nil {
		return nil, fmt.Errorf("unable to marshal json message. %v", err)
	}
	params.Message = aws.String(string(jsonRefMsg))

	// overwrite message attributes (if any) with hefty message attributes
	params.MessageAttributes = make(map[string]snsTypes.MessageAttributeValue)
	params.MessageAttributes[HeftyClientVersionMessageAttributeKey] = snsTypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(HeftyClientVersion)}

	// replace overwritten values with original values
	defer func() {
		params.Message = heftyMsg.Body
		snsMsgAttr := messages.MapToSnsMessageAttributeValues(heftyMsg.MessageAttributes)
		params.MessageAttributes = snsMsgAttr
	}()

	out, err := client.Publish(ctx, params, optFns...)
	if err != nil {
		return out, err
	}

	return out, err
}

// Example topicArn: arn:aws:sns:us-west-2:765908583888:MyTopic
func newSnsReferenceMessage(topicArn *string, bucketName, region, msgBodyHash, msgAttrHash string) (*messages.ReferenceMsg, error) {
	const expectedTokenCount = 6

	if topicArn != nil {
		tokens := strings.Split(*topicArn, ":")
		if len(tokens) != expectedTokenCount {
			return nil, fmt.Errorf("expected %d tokens when splitting topicArn by ':' but received %d", expectedTokenCount, len(tokens))
		} else {
			return &messages.ReferenceMsg{
				S3Region:         region,
				S3Bucket:         bucketName,
				S3Key:            fmt.Sprintf("%s/%s", tokens[4], uuid.New().String()), // S3Key: topicArn/uuid
				Md5DigestMsgBody: msgBodyHash,
				Md5DigestMsgAttr: msgAttrHash,
			}, nil
		}
	}

	return nil, errors.New("topicArn is nil")
}
