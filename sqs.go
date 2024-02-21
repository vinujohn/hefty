package hefty

import (
	"bytes"
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
		sqsMsg := &sqsLargeMsg{
			Body:              params.MessageBody,
			MessageAttributes: params.MessageAttributes,
		}
		jsonSqsMsg, err := json.Marshal(sqsMsg)
		if err != nil {
			return nil, fmt.Errorf("unable to marshal sqs message as json. %v", err)
		}
		_, err = client.s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(client.bucket),
			Key:    aws.String(refMsg.S3Key),
			Body:   bytes.NewReader(jsonSqsMsg),
		})
		if err != nil {
			return nil, fmt.Errorf("unable to upload large message to s3. %v", err)
		}

		// replace incoming message body with reference message
		jsonRefMsg, err := json.MarshalIndent(refMsg, "", "\t")
		if err != nil {
			return nil, fmt.Errorf("unable to marshal json message. %v", err)
		}
		params.MessageBody = aws.String(string(jsonRefMsg))

		//TODO: get correct library version
		// overwrite message attributes (if any) with hefty message attributes
		params.MessageAttributes = make(map[string]sqsTypes.MessageAttributeValue)
		params.MessageAttributes[versionMessageKey] = sqsTypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("v0.1")}
	}
	//TODO: handle MD5 attributes
	//TODO: handle error by deleting s3 message
	return client.SendMessage(ctx, params, optFns...)
}

func (client *SqsClient) SendHeftyMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	return client.SendMessageBatch(ctx, params, optFns...)
}

func (client *SqsClient) ReceiveHeftyMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	// request hefty message attribute
	if params.MessageAttributeNames == nil {
		params.MessageAttributeNames = []string{versionMessageKey}
	} else {
		// TODO: what happens when user has 10 message attributes listed,
		params.MessageAttributeNames = append(params.MessageAttributeNames, versionMessageKey)
	}

	out, err := client.ReceiveMessage(ctx, params, optFns...)
	if err != nil || len(out.Messages) == 0 {
		return out, err
	}

	for i := range out.Messages {
		if _, ok := out.Messages[i].MessageAttributes[versionMessageKey]; ok {

			// deserialize message body
			var refMsg referenceMsg
			err = json.Unmarshal([]byte(*out.Messages[i].Body), &refMsg)
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
				return nil, fmt.Errorf("unable to read message body from s3. %v", err)
			}
			var sqsMsg sqsLargeMsg
			err = json.Unmarshal(b, &sqsMsg)
			if err != nil {
				return nil, fmt.Errorf("unable to deserialize s3 message body. %v", err)
			}
			out.Messages[i].Body = sqsMsg.Body
			out.Messages[i].MessageAttributes = sqsMsg.MessageAttributes
		}
	}

	// TODO: handle MD5 attributes
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
