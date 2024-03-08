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
	const bucket = "hefty-benchmark-tests"

	heftyClient, s3Client, queueUrl := setup(bucket)

	b.Cleanup(func() {
		cleanup(heftyClient, s3Client, queueUrl, bucket)
	})

	b.ResetTimer()

	var err error
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		body, attr := testutils.GetMsgBodyAndAttrsRandom()
		in := &sqs.SendMessageInput{
			QueueUrl:          &queueUrl,
			MessageBody:       body,
			MessageAttributes: attr,
		}
		fmt.Printf("body size:%d, num message attributes:%d\n", len(*body), len(attr))
		b.StartTimer()
		_, err = heftyClient.SendHeftyMessage(context.TODO(), in)
		if err != nil {
			panic(err)
		}
	}
}

func setup(bucket string) (heftyClient *hefty.SqsClientWrapper, s3Client *s3.Client, queueUrl string) {
	// create test clients
	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("couldn't load default aws configuration. %v", err)
	}
	sqsClient := sqs.NewFromConfig(sdkConfig)
	s3Client = s3.NewFromConfig(sdkConfig)

	// create test bucket

	_, err = s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: &bucket,
		CreateBucketConfiguration: &s3Types.CreateBucketConfiguration{
			LocationConstraint: s3Types.BucketLocationConstraintUsWest2,
		},
	})
	if err != nil {
		log.Fatalf("could not create test bucket %s. %v", bucket, err)
	}

	// create hefty client
	heftyClient, err = hefty.NewSqsClientWrapper(sqsClient, s3Client, bucket)
	if err != nil {
		log.Fatalf("could not create hefty client. %v", err)
	}

	// create test queue
	queueName := uuid.NewString()
	q, err := heftyClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
		QueueName: &queueName,
	})
	if err != nil {
		log.Fatalf("could not create queue %s. %v", queueName, err)
	}
	queueUrl = *q.QueueUrl

	return
}

func cleanup(heftyClient *hefty.SqsClientWrapper, s3Client *s3.Client, queueUrl, bucket string) {
	// delete all remaining objects in test bucket
	var continueToken *string
	for {
		// list out objects to delete
		listObjects, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket:            &bucket,
			ContinuationToken: continueToken,
		})
		if err != nil {
			log.Fatalf("could not list objects in bucket %s. %v", bucket, err)
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
			_, err = s3Client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
				Bucket: &bucket,
				Delete: &s3Types.Delete{
					Objects: itemsToDelete,
				},
			})
			if err != nil {
				log.Fatalf("could not delete objects in test bucket %s. %v", bucket, err)
			}
		}

		if !*listObjects.IsTruncated {
			break
		} else {
			continueToken = listObjects.ContinuationToken
		}
	}

	// delete test bucket
	_, err := s3Client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		log.Printf("could not delete test bucket %s. %v", bucket, err)
	}

	// delete test queue
	_, err = heftyClient.DeleteQueue(context.TODO(), &sqs.DeleteQueueInput{
		QueueUrl: &queueUrl,
	})
	if err != nil {
		log.Printf("could not delete queue %s. %v", queueUrl, err)
	}
}
