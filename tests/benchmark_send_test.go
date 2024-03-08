package tests

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/vinujohn/hefty"
	"github.com/vinujohn/hefty/internal/testutils"
)

func BenchmarkSend(b *testing.B) {
	b.Cleanup(cleanup)

	setup()

	var err error
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		body, attr := testutils.GetMsgBodyAndAttrsRandom()
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

var (
	testHeftySqsClient *hefty.SqsClientWrapper
	testS3Client       *s3.Client
	testBucket2        string
	testQueueUrl       string
)

func setup() {
	// create test clients
	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("couldn't load default aws configuration. %v", err)
	}
	sqsClient := sqs.NewFromConfig(sdkConfig)
	testS3Client = s3.NewFromConfig(sdkConfig)

	// create test bucket
	testBucket2 = "hefty-integration-tests"
	_, err = testS3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: &testBucket2,
		CreateBucketConfiguration: &s3Types.CreateBucketConfiguration{
			LocationConstraint: s3Types.BucketLocationConstraintUsWest2,
		},
	})
	if err != nil {
		log.Fatalf("could not create test bucket %s. %v", testBucket2, err)
	}

	// create hefty client
	testHeftySqsClient, err = hefty.NewSqsClientWrapper(sqsClient, testS3Client, testBucket2)
	if err != nil {
		log.Fatalf("could not create hefty client. %v", err)
	}

	// create test queue
	queueName := uuid.NewString()
	q, err := testHeftySqsClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
		QueueName: &queueName,
	})
	if err != nil {
		log.Fatalf("could not create queue %s. %v", queueName, err)
	}
	testQueueUrl = *q.QueueUrl
}

func cleanup() {
	// delete all remaining objects in test bucket
	var continueToken *string
	for {
		// list out objects to delete
		listObjects, err := testS3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket:            &testBucket2,
			ContinuationToken: continueToken,
		})
		if err != nil {
			log.Fatalf("could not list objects in bucket %s. %v", testBucket2, err)
		}

		// create list of object keys to delete
		itemsToDelete := []s3Types.ObjectIdentifier{}
		for _, obj := range listObjects.Contents {
			itemsToDelete = append(itemsToDelete, s3Types.ObjectIdentifier{
				Key: obj.Key,
			})
		}

		// delete objects
		if len(itemsToDelete) > 0 {
			_, err = testS3Client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
				Bucket: &testBucket2,
				Delete: &s3Types.Delete{
					Objects: itemsToDelete,
				},
			})
			if err != nil {
				log.Fatalf("could not delete objects in test bucket %s. %v", testBucket2, err)
			}
		}

		if !*listObjects.IsTruncated {
			break
		} else {
			continueToken = listObjects.ContinuationToken
		}
	}

	// delete test bucket
	_, err := testS3Client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: &testBucket2,
	})
	if err != nil {
		log.Printf("could not delete test bucket %s. %v", testBucket2, err)
	}

	// delete test queue
	_, err = testHeftySqsClient.DeleteQueue(context.TODO(), &sqs.DeleteQueueInput{
		QueueUrl: &testQueueUrl,
	})
	if err != nil {
		log.Printf("could not delete queue %s. %v", testQueueUrl, err)
	}
}
