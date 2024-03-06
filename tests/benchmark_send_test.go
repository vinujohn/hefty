package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func BenchmarkSend(b *testing.B) {
	b.Cleanup(cleanup)

	setup()

	var err error
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		body, attr := getMessageBodyAndAttributesRandom()
		in := &sqs.SendMessageInput{
			QueueUrl:          &testQueueUrl,
			MessageBody:       body,
			MessageAttributes: attr,
		}
		fmt.Printf("body size:%d, num message attributes:%d\n", len(*body), len(attr))
		b.StartTimer()
		_, err = testHeftySqsClient.SendHeftyMessage(context.TODO(), in)
		if err != nil {
			panic(err)
		}
	}
}
