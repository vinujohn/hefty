package tests

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
)

func TestHeftyMessageEndToEnd(t *testing.T) {
	msgTextBody, msgAttributes := getMaxHeftyMsgBodyAndAttr()

	t.Logf("%s: finished creating message", time.Now().String())
	_, err := testHeftySqsClient.SendHeftyMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody:       msgTextBody,
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
			MessageAttributeNames: []string{
				"test1", "test2",
			},
		})
		if err != nil {
			t.Fatalf("could not receive hefty messages. %v", err)
		}

		for _, msg := range out.Messages {
			t.Logf("%s: received message", time.Now().String())

			// assert on message attributes
			assert.Len(t, msg.MessageAttributes, 11, "message attributes length is correct")
			assert.Equal(t, msgAttributes, msg.MessageAttributes, "message attributes sent equal to message attributes received")

			// assert on message body
			assert.NotNil(t, msg.Body, "message body should not be nil")
			assert.Len(t, *msg.Body, len(*msgTextBody), "message body length is as expected")
			assert.Equal(t, *msgTextBody, *msg.Body, "message body sent is equal to message body received")

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
	msgTextBody, msgAttributes := getMaxSqsMsgBodyAndAttr()

	_, err := testHeftySqsClient.SendHeftyMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody:       msgTextBody,
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
				"test01", "test02", // this is how we know this message went through sqs since hefty does not filter attributes
			},
		})
		if err != nil {
			t.Fatalf("could not receive puny messages. %v", err)
		}

		for _, msg := range out.Messages {
			// assert on message attributes
			assert.Len(t, msg.MessageAttributes, 2, "message attributes length is correct")
			assert.Equal(t, msgAttributes["test01"].BinaryValue, msg.MessageAttributes["test01"].BinaryValue, "message attribute binary value is correct")
			assert.Equal(t, msgAttributes["test02"].StringValue, msg.MessageAttributes["test02"].StringValue, "message attribute string value  is correct")

			// assert on message body
			assert.NotNil(t, msg.Body, "message body should not be nil")
			assert.Equal(t, *msgTextBody, *msg.Body, "message body sent is equal to message body received")

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
