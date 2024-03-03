package hefty

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

const (
	MaxSqsMessageLengthBytes        = 262_144
	MaxHeftyMessageLengthBytes      = 2_147_483_648
	heftyClientVersionMessageKey    = "hefty-client-version"
	receiptHandlePrefix             = "hefty-message"
	expectedReceiptHandleTokenCount = 4
)

type SqsClient struct {
	sqs.Client
	bucket   string
	s3Client *s3.Client
}

type largeMsg struct {
	Body              *string                                   `json:"body"`
	MessageAttributes map[string]sqsTypes.MessageAttributeValue `json:"message_attributes"`
}

func NewSqsClient(sqsClient *sqs.Client, s3Client *s3.Client, bucketName string) (*SqsClient, error) {
	// check if bucket exits
	if ok, err := bucketExists(s3Client, bucketName); !ok {
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("bucket %s does not exist or is not accessible", bucketName)
	}

	return &SqsClient{
		Client:   *sqsClient,
		bucket:   bucketName,
		s3Client: s3Client,
	}, nil
}

// TODO: make a size restriction of 2GB
func (client *SqsClient) SendHeftyMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	// input validation; if invalid input let AWS api handle it
	if params == nil ||
		params.MessageBody == nil ||
		len(*params.MessageBody) == 0 {

		return client.SendMessage(ctx, params, optFns...)
	}

	// check message size
	if size := msgSize(params); size <= MaxSqsMessageLengthBytes {
		return client.SendMessage(ctx, params, optFns...)
	} else if size > MaxHeftyMessageLengthBytes {
		return nil, fmt.Errorf("message size of %d bytes greater than allowed message size of %d bytes", size, MaxHeftyMessageLengthBytes)
	}

	// create large message
	s3LargeMsg := &largeMsg{
		Body:              params.MessageBody,
		MessageAttributes: params.MessageAttributes,
	}

	// serialize large message
	jsonLargeMsg, err := json.Marshal(s3LargeMsg)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal sqs message as json. %v", err)
	}

	// create reference message
	bodyHash, attributesHash := md5Hash(s3LargeMsg)
	refMsg, err := newSqsReferenceMessage(params.QueueUrl, client.bucket, bodyHash, attributesHash)
	if err != nil {
		return nil, fmt.Errorf("unable to create reference message from queueUrl. %v", err)
	}

	// upload large message to s3
	_, err = client.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(client.bucket),
		Key:    aws.String(refMsg.S3Key),
		Body:   bytes.NewReader(jsonLargeMsg),
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
	params.MessageAttributes[heftyClientVersionMessageKey] = sqsTypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("v0.1")}

	out, err := client.SendMessage(ctx, params, optFns...)
	if err != nil {
		return out, err
	}

	// overwrite md5 values
	out.MD5OfMessageBody = aws.String(bodyHash)
	out.MD5OfMessageAttributes = aws.String(attributesHash)

	return out, err
}

func (client *SqsClient) SendHeftyMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	return client.SendMessageBatch(ctx, params, optFns...)
}

func (client *SqsClient) ReceiveHeftyMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	// TODO: should we filter out message attributes from the original large message?
	// request hefty message attribute
	if params.MessageAttributeNames == nil {
		params.MessageAttributeNames = []string{heftyClientVersionMessageKey}
	} else {
		params.MessageAttributeNames = append(params.MessageAttributeNames, heftyClientVersionMessageKey)
	}

	out, err := client.ReceiveMessage(ctx, params, optFns...)
	if err != nil || len(out.Messages) == 0 {
		return out, err
	}

	for i := range out.Messages {
		if _, ok := out.Messages[i].MessageAttributes[heftyClientVersionMessageKey]; !ok {
			continue
		}

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

		// read large message
		body, err := io.ReadAll(s3Obj.Body)
		if err != nil {
			return nil, fmt.Errorf("unable to read message body from s3. %v", err)
		}

		// replace message body and attributes with s3 message
		var s3LargeMsg largeMsg
		err = json.Unmarshal(body, &s3LargeMsg)
		if err != nil {
			return nil, fmt.Errorf("unable to deserialize s3 message body. %v", err)
		}
		out.Messages[i].Body = s3LargeMsg.Body
		out.Messages[i].MessageAttributes = s3LargeMsg.MessageAttributes

		// replace md5 hashes
		out.Messages[i].MD5OfBody = &refMsg.SqsMd5HashBody
		out.Messages[i].MD5OfMessageAttributes = &refMsg.SqsMd5HashMsgAttr

		// modify receipt handle to contain s3 bucket and key info
		newReceiptHandle := fmt.Sprintf("%s|%s|%s|%s", receiptHandlePrefix, *out.Messages[i].ReceiptHandle, refMsg.S3Bucket, refMsg.S3Key)
		newReceiptHandle = base64.StdEncoding.EncodeToString([]byte(newReceiptHandle))
		out.Messages[i].ReceiptHandle = &newReceiptHandle
	}

	return out, nil
}

func (client *SqsClient) DeleteHeftyMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
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
	if len(tokens) != expectedReceiptHandleTokenCount {
		return nil, fmt.Errorf("expected number of tokens (%d) not available in receipt handle", expectedReceiptHandleTokenCount)
	}

	// delete hefty message from s3
	receiptHandle, s3Bucket, s3Key := tokens[1], tokens[2], tokens[3]
	_, err = client.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s3Bucket,
		Key:    &s3Key,
	})
	if err != nil {
		return nil, fmt.Errorf("could not delete s3 object for large message. %v", err)
	}

	// replace receipt handle with real one to delete sqs message
	params.ReceiptHandle = &receiptHandle

	return client.DeleteMessage(ctx, params, optFns...)
}

// https://sqs.us-west-2.amazonaws.com/765908583888/MyTestQueue
func newSqsReferenceMessage(queueUrl *string, bucketName, bodyHash, attributesHash string) (*referenceMsg, error) {
	if queueUrl != nil {
		tokens := strings.Split(*queueUrl, "/")
		if len(tokens) != 5 {
			return nil, fmt.Errorf("expected 5 tokens when splitting queueUrl by '/' but only received %d", len(tokens))
		} else {
			regionTokens := strings.Split(tokens[2], ".")
			return &referenceMsg{
				S3Region:          regionTokens[1],
				S3Bucket:          bucketName,
				S3Key:             fmt.Sprintf("%s/%s/%s/%s", tokens[3], regionTokens[1], tokens[4], uuid.New().String()),
				SqsMd5HashBody:    bodyHash,
				SqsMd5HashMsgAttr: attributesHash,
			}, nil
		}
	}

	return nil, errors.New("queueUrl is nil")
}

// msgSize retrieves the size of the message being sent
// current sqs size constraints are 256KB for both the body and message attributes
func msgSize(params *sqs.SendMessageInput) int {
	var size int

	size += len(*params.MessageBody)

	if params.MessageAttributes != nil {
		for k, v := range params.MessageAttributes {
			size += len(k)
			size += len(aws.ToString(v.DataType))
			size += len(aws.ToString(v.StringValue))
			size += len(v.BinaryValue)
		}
	}

	return size
}

// md5Hash gets the md5 hash for both the body and message attributes of a large message
func md5Hash(msg *largeMsg) (bodyHash string, attributesHash string) {
	type keyValue struct {
		key   string
		value sqsTypes.MessageAttributeValue
	}

	if msg.Body != nil {
		hash := md5.Sum([]byte(*msg.Body))
		bodyHash = hex.EncodeToString(hash[:])
	}

	if msg.MessageAttributes != nil {

		// sort slice of map keys and values
		msgAttributes := []keyValue{}
		for k, v := range msg.MessageAttributes {
			msgAttributes = append(msgAttributes, keyValue{
				key:   k,
				value: v,
			})
		}
		sort.Slice(msgAttributes, func(i, j int) bool {
			return msgAttributes[i].key < msgAttributes[j].key
		})

		buf := new(bytes.Buffer)
		for _, attr := range msgAttributes {
			binary.Write(buf, binary.BigEndian, int32(len(attr.key)))
			buf.Write([]byte(attr.key))
			if attr.value.DataType != nil {
				binary.Write(buf, binary.BigEndian, int32(len(*attr.value.DataType)))
				buf.Write([]byte(*attr.value.DataType))

				if attr.value.StringValue != nil {
					buf.Write([]byte{1})
					binary.Write(buf, binary.BigEndian, int32(len(*attr.value.StringValue)))
					buf.Write([]byte(*attr.value.StringValue))
				} else if len(attr.value.BinaryValue) > 0 {
					buf.Write([]byte{2})
					binary.Write(buf, binary.BigEndian, int32(len(attr.value.BinaryValue)))
					buf.Write(attr.value.BinaryValue)
				}
			}
		}

		hash := md5.Sum(buf.Bytes())
		attributesHash = hex.EncodeToString(hash[:])
	}

	return
}
