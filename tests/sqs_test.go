package tests

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func Test(t *testing.T) {
	_, err := testHeftySqsClient.SendHeftyMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody: aws.String("1"),
		QueueUrl:    &testQueueUrl,
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
			t.Logf("received message id: %s, sizeBytes: %d receiptHandle: %s", *msg.MessageId, len(*msg.Body), *msg.ReceiptHandle)
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
