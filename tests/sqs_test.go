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
