package hefty

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/vinujohn/hefty/internal/messages"
	"github.com/vinujohn/hefty/internal/utils"
)

const (
	receiptHandlePrefix = "c976bb5ff9634b1ea7f69fd2390e3fef" // text used to differentiate a receipt handle belonging to a hefty message
)

type SqsClientWrapper struct {
	sqs.Client
	bucket     string
	s3Client   *s3.Client
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
}

// NewSqsClientWrapper will create a new Hefty SQS client wrapper using an existing AWS SQS client and AWS S3 client.
// This Hefty SQS client wrapper will save large messages greater than MaxSqsSnsMessageLengthBytes to AWS S3 in the
// bucket that is specified via `bucketName`. The S3 client should have the ability of reading and writing to this bucket.
// This function will also check if the bucket exists and is accessible.
func NewSqsClientWrapper(sqsClient *sqs.Client, s3Client *s3.Client, bucketName string) (*SqsClientWrapper, error) {
	// check if bucket exits
	if ok, err := utils.BucketExists(s3Client, bucketName); !ok {
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("bucket %s does not exist or is not accessible", bucketName)
	}

	return &SqsClientWrapper{
		Client:     *sqsClient,
		bucket:     bucketName,
		s3Client:   s3Client,
		uploader:   s3manager.NewUploader(s3Client),
		downloader: s3manager.NewDownloader(s3Client),
	}, nil
}

// SendHeftyMessage will calculate the messages size from `params` and determine if the MaxSqsSnsMessageLengthBytes is exceeded.
// If so, the message is saved in AWS S3 as a hefty message and a reference message is sent to AWS SQS instead.
// If not, the message is directly sent to AWS SNS.
//
// In the case of the reference message being sent, the message itself contains metadata about the hefty message saved in AWS S3
// including bucket name, S3 key, region, and md5 digests.
//
// Note that this function's signature matches that of the AWS SQS SDK's SendMessage function.
func (client *SqsClientWrapper) SendHeftyMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	// input validation; if invalid input let AWS SDK handle it
	if params == nil ||
		params.MessageBody == nil ||
		len(*params.MessageBody) == 0 {

		return client.SendMessage(ctx, params, optFns...)
	}

	// normalize message attributes
	msgAttributes := messages.MapFromSqsMessageAttributeValues(params.MessageAttributes)

	// calculate message size
	msgSize, err := messages.MessageSize(params.MessageBody, msgAttributes)
	if err != nil {
		return nil, fmt.Errorf("unable to get size of message. %v", err)
	}

	// validate message size
	if msgSize <= MaxAwsMessageLengthBytes {
		return client.SendMessage(ctx, params, optFns...)
	} else if msgSize > MaxHeftyMessageLengthBytes {
		return nil, fmt.Errorf("message size of %d bytes greater than allowed message size of %d bytes", msgSize, MaxHeftyMessageLengthBytes)
	}

	// create and serialize hefty message
	heftyMsg := messages.NewHeftyMessage(params.MessageBody, msgAttributes, msgSize)
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
	refMsg, err := newSqsReferenceMessage(params.QueueUrl, client.bucket, client.Options().Region, msgBodyHash, msgAttrHash)
	if err != nil {
		return nil, fmt.Errorf("unable to create reference message from queueUrl. %v", err)
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
	jsonRefMsg, err := refMsg.ToJson()
	if err != nil {
		return nil, fmt.Errorf("unable to marshal json message. %v", err)
	}
	params.MessageBody = aws.String(string(jsonRefMsg))

	// clear out all message attributes
	origMsgAttr := params.MessageAttributes
	params.MessageAttributes = nil

	// replace overwritten values with original values
	defer func() {
		params.MessageBody = heftyMsg.Body
		params.MessageAttributes = origMsgAttr
	}()

	// send reference message to sqs
	out, err := client.SendMessage(ctx, params, optFns...)
	if err != nil {
		return out, err
	}

	// overwrite md5 values
	out.MD5OfMessageBody = aws.String(msgBodyHash)
	out.MD5OfMessageAttributes = aws.String(msgAttrHash)

	return out, err
}

// SendHeftyMessageBatch is currently not supported and will use the underlying AWS SQS SDK's method `SendMessageBatch`
func (client *SqsClientWrapper) SendHeftyMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	return client.SendMessageBatch(ctx, params, optFns...)
}

// ReceiveHeftyMessage will determine if a message received is a reference to a hefty message residing in AWS S3.
// This method will then download the hefty message and then place its body and message attributes in the returned
// ReceiveMessageOutput. No modification of messages are made when the message has gone through AWS SQS. It is
// important to use this function when `SendHeftyMessage` is used so that hefty messages can be downloaded from S3.
//
// Note that this function's signature matches that of the AWS SQS SDK's ReceiveMessage function.
func (client *SqsClientWrapper) ReceiveHeftyMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	out, err := client.ReceiveMessage(ctx, params, optFns...)
	if err != nil || out == nil {
		return out, err
	}

	for i := range out.Messages {
		if !messages.IsReferenceMsg(*out.Messages[i].Body) {
			continue
		}

		// deserialize message body
		refMsg, err := messages.ToReferenceMsg(*out.Messages[i].Body)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshal reference message. %v", err)
		}

		// make call to s3 to get message
		buf := s3manager.NewWriteAtBuffer([]byte{})
		_, err = client.downloader.Download(ctx, buf, &s3.GetObjectInput{
			Bucket: &refMsg.S3Bucket,
			Key:    &refMsg.S3Key,
		})
		if err != nil {
			return nil, fmt.Errorf("unable to get message from s3. %v", err)
		}

		// decode message from s3
		heftyMsg, err := messages.DeserializeHeftyMessage(buf.Bytes())
		if err != nil {
			return nil, fmt.Errorf("unable to decode bytes into hefty message type. %v", err)
		}

		// replace message body and attributes with s3 message
		out.Messages[i].Body = heftyMsg.Body
		sqsAttributes := messages.MapToSqsMessageAttributeValues(heftyMsg.MessageAttributes)
		out.Messages[i].MessageAttributes = sqsAttributes

		// replace md5 hashes
		out.Messages[i].MD5OfBody = &refMsg.Md5DigestMsgBody
		out.Messages[i].MD5OfMessageAttributes = &refMsg.Md5DigestMsgAttr

		// modify receipt handle to contain s3 bucket and key info
		newReceiptHandle := fmt.Sprintf("%s|%s|%s|%s", receiptHandlePrefix, *out.Messages[i].ReceiptHandle, refMsg.S3Bucket, refMsg.S3Key)
		newReceiptHandle = base64.StdEncoding.EncodeToString([]byte(newReceiptHandle))
		out.Messages[i].ReceiptHandle = &newReceiptHandle
	}

	return out, nil
}

// DeleteHeftyMessage will delete a hefty message from AWS S3 and also the reference message from AWS SQS.
// It is important to use the `ReceiptHandle` from `ReceiveHeftyMessage` in this function as this is the only way to determine
// if a hefty message resides in AWS S3 or not.
//
// Note that this function's signature matches that of the AWS SQS SDK's DeleteMessage function.
func (client *SqsClientWrapper) DeleteHeftyMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	const expectedHeftyReceiptHandleTokenCount = 4

	if params.ReceiptHandle == nil {
		return client.DeleteMessage(ctx, params, optFns...)
	}

	// decode receipt handle
	decoded, err := base64.StdEncoding.DecodeString(*params.ReceiptHandle)
	if err != nil {
		return nil, fmt.Errorf("could not decode receipt handle. %v", err)
	}
	decodedStr := string(decoded)

	// check if decoded receipt handle is for a hefty message
	if !strings.HasPrefix(decodedStr, receiptHandlePrefix) {
		return client.DeleteMessage(ctx, params, optFns...)
	}

	// get tokens from receipt handle
	tokens := strings.Split(decodedStr, "|")
	if len(tokens) != expectedHeftyReceiptHandleTokenCount {
		return nil, fmt.Errorf("expected number of tokens (%d) not available in receipt handle", expectedHeftyReceiptHandleTokenCount)
	}

	// delete hefty message from s3
	receiptHandle, s3Bucket, s3Key := tokens[1], tokens[2], tokens[3]
	_, err = client.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s3Bucket,
		Key:    &s3Key,
	})
	if err != nil {
		return nil, fmt.Errorf("could not delete s3 object for hefty message. %v", err)
	}

	// replace receipt handle with real one to delete sqs message
	params.ReceiptHandle = &receiptHandle

	return client.DeleteMessage(ctx, params, optFns...)
}

// Example queueUrl: https://sqs.us-west-2.amazonaws.com/765908583888/MyTestQueue
func newSqsReferenceMessage(queueUrl *string, bucketName, region, msgBodyHash, msgAttrHash string) (*messages.ReferenceMsg, error) {
	const expectedTokenCount = 5

	if queueUrl != nil {
		tokens := strings.Split(*queueUrl, "/")
		if len(tokens) != expectedTokenCount {
			return nil, fmt.Errorf("expected %d tokens when splitting queueUrl by '/' but received %d", expectedTokenCount, len(tokens))
		} else {

			return messages.NewReferenceMsg(
				region,
				bucketName,
				fmt.Sprintf("%s/%s", tokens[4], uuid.New().String()), // S3Key: queueName/uuid
				msgBodyHash,
				msgAttrHash), nil
		}
	}

	return nil, errors.New("queueUrl is nil")
}
