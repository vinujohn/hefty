package tests

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/vinujohn/hefty"
)

var (
	testHeftySqsClient *hefty.SqsClient
	testS3Client       *s3.Client
	testBucket         string
	testQueueUrl       string
)

func TestMain(m *testing.M) {
	// create test clients
	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("couldn't load default aws configuration. %v", err)
	}
	sqsClient := sqs.NewFromConfig(sdkConfig)
	testS3Client = s3.NewFromConfig(sdkConfig)

	// create test bucket
	testBucket = "hefty-integration-tests"
	_, err = testS3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: &testBucket,
		CreateBucketConfiguration: &s3Types.CreateBucketConfiguration{
			LocationConstraint: s3Types.BucketLocationConstraintUsWest2,
		},
	})
	if err != nil {
		log.Fatalf("could not create test bucket %s. %v", testBucket, err)
	}

	// create hefty client
	testHeftySqsClient, err = hefty.NewSqsClient(sqsClient, testS3Client, testBucket)
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

	exitCode := m.Run()

	// delete all remaining objects in test bucket
	var continueToken *string
	for {
		// list out objects to delete
		listObjects, err := testS3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket:            &testBucket,
			ContinuationToken: continueToken,
		})
		if err != nil {
			log.Fatalf("could not list objects in bucket %s. %v", testBucket, err)
		}

		// create list of object keys to delete
		itemsToDelete := []s3Types.ObjectIdentifier{}
		for _, obj := range listObjects.Contents {
			itemsToDelete = append(itemsToDelete, s3Types.ObjectIdentifier{
				Key: obj.Key,
			})
		}

		// delete objects
		_, err = testS3Client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
			Bucket: &testBucket,
			Delete: &s3Types.Delete{
				Objects: itemsToDelete,
			},
		})
		if err != nil {
			log.Fatalf("could not delete objects in test bucket %s. %v", testBucket, err)
		}

		if !*listObjects.IsTruncated {
			break
		} else {
			continueToken = listObjects.ContinuationToken
		}
	}

	// delete test bucket
	_, err = testS3Client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: &testBucket,
	})
	if err != nil {
		log.Printf("could not delete test bucket %s. %v", testBucket, err)
	}

	// delete test queue
	_, err = testHeftySqsClient.DeleteQueue(context.TODO(), &sqs.DeleteQueueInput{
		QueueUrl: &testQueueUrl,
	})
	if err != nil {
		log.Printf("could not delete queue %s. %v", queueName, err)
	}

	os.Exit(exitCode)
}
