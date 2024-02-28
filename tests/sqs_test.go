package tests

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func Test(t *testing.T) {
	_, err := testHeftySqsClient.SendHeftyMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody: aws.String(testLargeMsg256KB),
		QueueUrl:    &testQueueUrl,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"test": {
				DataType:    aws.String("String"),
				StringValue: aws.String(testLargeMsg256KB),
			},
		},
	})
	if err != nil {
		t.Fatalf("could not send hefty message. %v", err)
	}

	i := 0
	for i < 3 {
		out, err := testHeftySqsClient.ReceiveHeftyMessage(context.TODO(), &sqs.ReceiveMessageInput{
			QueueUrl: &testQueueUrl,
		})
		if err != nil {
			t.Fatalf("could not receive hefty messages. %v", err)
		}

		for _, msg := range out.Messages {
			t.Logf("received message id: %s, sizeBodyBytes: %d receiptHandle: %s", *msg.MessageId, len(*msg.Body), *msg.ReceiptHandle)
			_, err = testHeftySqsClient.DeleteHeftyMessage(context.TODO(), &sqs.DeleteMessageInput{
				QueueUrl:      &testQueueUrl,
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err != nil {
				t.Fatalf("could not acknowledge hefty messages. %v", err)
			}

		}

		time.Sleep(time.Second)
		i++
	}
}

func TestPunyMessage(t *testing.T) {
	_, err := testHeftySqsClient.SendHeftyMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody: aws.String("test body"),
		QueueUrl:    &testQueueUrl,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"test1": {
				DataType:    aws.String("String"),
				StringValue: aws.String("testAttrValue1"),
			},
			"test2": {
				DataType:    aws.String("String"),
				StringValue: aws.String("testAttrValue1"),
			},
			"test3": {
				DataType:    aws.String("String"),
				StringValue: aws.String("testAttrValue1"),
			},
			"test4": {
				DataType:    aws.String("String"),
				StringValue: aws.String("testAttrValue1"),
			},
			"test5": {
				DataType:    aws.String("String"),
				StringValue: aws.String("testAttrValue1"),
			},
			"test6": {
				DataType:    aws.String("String"),
				StringValue: aws.String("testAttrValue1"),
			},
			"test7": {
				DataType:    aws.String("String"),
				StringValue: aws.String("testAttrValue1"),
			},
			"test8": {
				DataType:    aws.String("String"),
				StringValue: aws.String("testAttrValue1"),
			},
			"test9": {
				DataType:    aws.String("String"),
				StringValue: aws.String("testAttrValue1"),
			},
			"test10": {
				DataType:    aws.String("String"),
				StringValue: aws.String("testAttrValue1"),
			},
		},
	})
	if err != nil {
		t.Fatalf("could not send puny message. %v", err)
	}

	i := 0
	for i < 3 {
		out, err := testHeftySqsClient.ReceiveHeftyMessage(context.TODO(), &sqs.ReceiveMessageInput{
			QueueUrl: &testQueueUrl,
			MessageAttributeNames: []string{
				"test1", "test2", "test3", "test4",
				"test5", "test6", "test7", "test8",
				"test9", "test10",
			},
		})
		if err != nil {
			t.Fatalf("could not receive puny messages. %v", err)
		}

		for _, msg := range out.Messages {
			t.Logf("received message id: %s, sizeBodyBytes: %d message attributes: %v", *msg.MessageId, len(*msg.Body), msg.MessageAttributes)
			_, err = testHeftySqsClient.DeleteHeftyMessage(context.TODO(), &sqs.DeleteMessageInput{
				QueueUrl:      &testQueueUrl,
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err != nil {
				t.Fatalf("could not acknowledge puny messages. %v", err)
			}

		}

		time.Sleep(time.Second)
		i++
	}
}
