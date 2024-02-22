package tests

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func Test(t *testing.T) {
	_, err := testHeftySqsClient.SendHeftyMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody: aws.String(testLargeMsg256KB + "1"),
		QueueUrl:    &testQueueUrl,
	})
	if err != nil {
		t.Fatalf("could not send hefty message. %v", err)
	}

	t.Fail()
}
