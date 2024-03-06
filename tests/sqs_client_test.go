package tests

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/vinujohn/hefty"
)

func TestHeftyMessageEndToEnd(t *testing.T) {
	msgText256KB := createMessageText(hefty.MaxSqsMessageLengthBytes)
	msgTextBody := createMessageText(hefty.MaxHeftyMessageLengthBytes -
		(hefty.MaxSqsMessageLengthBytes * 12) - (len("String") * 12) - (len("test1") * 9) - (len("test10") * 3))

	msgAttributes := map[string]types.MessageAttributeValue{
		"test1": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test2": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test3": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test4": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test5": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test6": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test7": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test8": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test9": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test10": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test11": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test12": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
	}

	t.Logf("%s: finished creating message", time.Now().String())
	_, err := testHeftySqsClient.SendHeftyMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody:       aws.String(msgTextBody),
		QueueUrl:          &testQueueUrl,
		MessageAttributes: msgAttributes,
	})
	if err != nil {
		t.Fatalf("could not send hefty message. %v", err)
	}
	t.Logf("%s: finished sending message", time.Now().String())

	i := 0
	for i < 3 {
		out, err := testHeftySqsClient.ReceiveHeftyMessage(context.TODO(), &sqs.ReceiveMessageInput{
			QueueUrl: &testQueueUrl,
		})
		if err != nil {
			t.Fatalf("could not receive hefty messages. %v", err)
		}

		for _, msg := range out.Messages {
			t.Logf("%s: received message", time.Now().String())

			// assert on message attributes
			assert.Len(t, msg.MessageAttributes, 12, "message attributes length is correct")
			for _, v := range msg.MessageAttributes {
				assert.Len(t, *v.StringValue, hefty.MaxSqsMessageLengthBytes, "message attribute value length is correct")
			}
			assert.Equal(t, msgAttributes, msg.MessageAttributes, "message attributes sent equal to message attributes received")

			// assert on message body
			assert.NotNil(t, msg.Body, "message body should not be nil")
			assert.Len(t, *msg.Body, len(msgTextBody), "message body length is as expected")
			assert.Equal(t, msgTextBody, *msg.Body, "message body sent is equal to message body received")

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

func TestPunyMessageEndToEnd(t *testing.T) {
	msgText10KB := createMessageText(10_000)
	msgTextBody := createMessageText(hefty.MaxSqsMessageLengthBytes -
		(len(msgText10KB) * 10) - (len("String") * 10) - (len("test0") * 10))
	msgAttributes := map[string]types.MessageAttributeValue{
		"test0": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText10KB),
		},
		"test1": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText10KB),
		},
		"test2": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText10KB),
		},
		"test3": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText10KB),
		},
		"test4": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText10KB),
		},
		"test5": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText10KB),
		},
		"test6": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText10KB),
		},
		"test7": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText10KB),
		},
		"test8": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText10KB),
		},
		"test9": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText10KB),
		},
	}
	_, err := testHeftySqsClient.SendHeftyMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody:       aws.String(msgTextBody),
		QueueUrl:          &testQueueUrl,
		MessageAttributes: msgAttributes,
	})
	if err != nil {
		t.Fatalf("could not send puny message. %v", err)
	}

	i := 0
	for i < 3 {
		out, err := testHeftySqsClient.ReceiveHeftyMessage(context.TODO(), &sqs.ReceiveMessageInput{
			QueueUrl: &testQueueUrl,
			MessageAttributeNames: []string{
				"test1", "test2", "test3",
			},
		})
		if err != nil {
			t.Fatalf("could not receive puny messages. %v", err)
		}

		for _, msg := range out.Messages {
			// assert on message attributes
			assert.Len(t, msg.MessageAttributes, 3, "message attributes length is correct")
			for _, v := range msg.MessageAttributes {
				assert.Len(t, *v.StringValue, len(msgText10KB), "message attribute value length is correct")
				assert.Equal(t, *v.StringValue, msgText10KB, "message attribute value sent equal to message attribute value received")
			}

			// assert on message body
			assert.NotNil(t, msg.Body, "message body should not be nil")
			assert.Len(t, *msg.Body, len(msgTextBody), "message body length is as expected")
			assert.Equal(t, msgTextBody, *msg.Body, "message body sent is equal to message body received")

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
