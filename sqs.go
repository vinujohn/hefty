package hefty

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

const (
	MaxSqsMessageLengthBytes = 262_144
	ClientReservedBytes      = 30
	versionMessageKey        = "hefty-client-version"
)

type SqsClient struct {
	sqs.Client
	bucket   string
	s3Client *s3.Client
}

func NewSqsClient(sqsClient *sqs.Client, s3Client *s3.Client, bucketName string) (*SqsClient, error) {
	// check if bucket exits, if not create it using the current s3 settings
	if ok, err := bucketExists(s3Client, bucketName); !ok {
		if err != nil {
			return nil, err
		}

		_, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
			CreateBucketConfiguration: &s3Types.CreateBucketConfiguration{
				LocationConstraint: s3Types.BucketLocationConstraint(s3Client.Options().Region),
			},
		})

		if err != nil {
			return nil, fmt.Errorf("unable to create bucket '%s'. %v", bucketName, err)
		}
	}

	return &SqsClient{
		Client:   *sqsClient,
		bucket:   bucketName,
		s3Client: s3Client,
	}, nil
}

func (client *SqsClient) SendHeftyMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	// TODO: consider no message body but message attributes that are oversized
	if msgSize(params) > MaxSqsMessageLengthBytes {
		// create reference message
		refMsg, err := newSqsReferenceMessage(params.QueueUrl, client.bucket)
		if err != nil {
			return nil, fmt.Errorf("unable to create reference message from queueUrl. %v", err)
		}

		// upload large message to s3
		_, err = client.s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(client.bucket),
			Key:    aws.String(refMsg.S3Key),
			Body:   strings.NewReader(*params.MessageBody),
		})
		if err != nil {
			return nil, fmt.Errorf("unable to upload large message to s3. %v", err)
		}

		// replace incoming message body with reference message
		jsonBytes, err := json.MarshalIndent(refMsg, "", "\t")
		if err != nil {
			return nil, fmt.Errorf("unable to marshal json message. %v", err)
		}
		params.MessageBody = aws.String(string(jsonBytes))

		if params.MessageAttributes == nil {
			params.MessageAttributes = make(map[string]sqsTypes.MessageAttributeValue)
		}
		//TODO: get correct library version
		params.MessageAttributes[versionMessageKey] = sqsTypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("v0.1")}
	}

	//TODO: handle error by deleting s3 message
	return client.SendMessage(ctx, params, optFns...)
}

func (client *SqsClient) SendHeftyMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	return client.SendMessageBatch(ctx, params, optFns...)
}

func (client *SqsClient) ReceiveHeftyMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	out, err := client.ReceiveMessage(ctx, params, optFns...)
	if err != nil {
		return nil, err
	}
	for _, msg := range out.Messages {
		if _, ok := msg.MessageAttributes[versionMessageKey]; ok {
			// deserialize message body
			var refMsg *referenceMsg
			err = json.Unmarshal([]byte(*msg.Body), refMsg)
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshal reference message. %v", err)
			}

			// make call to s3 to get message
			s3Obj, err := client.s3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: &refMsg.S3Bucket,
				Key:    &refMsg.S3Key,
			})
			if err != nil {
				return nil, fmt.Errorf("unable to get message from s3. %v", err)
			}

			// replace message body with s3 message
			b, err := io.ReadAll(s3Obj.Body)
			if err != nil {
				return nil, fmt.Errorf("unable to read message from s3. %v", err)
			}
			msg.Body = aws.String(string(b))
		}
	}

	return out, nil
}

// https://sqs.us-west-2.amazonaws.com/765908583888/MyTestQueue
func newSqsReferenceMessage(queueUrl *string, bucketName string) (*referenceMsg, error) {
	if queueUrl != nil {
		tokens := strings.Split(*queueUrl, "/")
		if len(tokens) != 5 {
			return nil, fmt.Errorf("expected 5 tokens when splitting queueUrl by '/' but only received %d", len(tokens))
		} else {
			regionTokens := strings.Split(tokens[2], ".")
			return &referenceMsg{
				S3Region: regionTokens[1],
				S3Bucket: bucketName,
				S3Key:    fmt.Sprintf("%s/%s/%s/%s", tokens[3], regionTokens[1], tokens[4], uuid.New().String()),
			}, nil
		}
	}

	return nil, errors.New("queueUrl is nil")
}

// msgSize retrieves the size of the message being sent
func msgSize(params *sqs.SendMessageInput) int {
	var size int

	if params != nil {
		if params.MessageBody != nil {
			size += len(*params.MessageBody)
		}

		if params.MessageAttributes != nil {
			for k, v := range params.MessageAttributes {
				size += len(k)
				size += len(aws.ToString(v.DataType))
				size += len(aws.ToString(v.StringValue))
				size += len(v.BinaryValue)
			}
		}
	}
	return size
}
