package tests

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/vinujohn/hefty"
	"github.com/vinujohn/hefty/internal/testutils"
)

type testResult struct {
	QueueUrl      *string
	SendMsgResult *sqs.SendMessageOutput
	ReceivedMsg   *types.Message
}

var (
	heftyClient *hefty.SqsClientWrapper
	sqsClient   *sqs.Client
	s3Client    *s3.Client

	testBucket string // value shared between all ginkgo processes if running in parallel
	testQueues []*string
)

func TestHefty(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Hefty Tests Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	// get sdk config
	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	Expect(err).To(BeNil())

	// create s3 bucket
	client := s3.NewFromConfig(sdkConfig)
	bucket := uuid.NewString()
	_, err = client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: &bucket,
		CreateBucketConfiguration: &s3Types.CreateBucketConfiguration{
			LocationConstraint: s3Types.BucketLocationConstraintUsWest2,
		},
	})
	Expect(err).To(BeNil())

	return []byte(bucket)
}, func(bucket []byte) {
	testBucket = string(bucket)

	// get sdk config
	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	Expect(err).To(BeNil())

	// create clients to wrap
	sqsClient = sqs.NewFromConfig(sdkConfig)
	s3Client = s3.NewFromConfig(sdkConfig)

	// create hefty client
	heftyClient, err = hefty.NewSqsClientWrapper(sqsClient, s3Client, testBucket)
	Expect(err).To(BeNil())

})

var _ = SynchronizedAfterSuite(func() {
	// delete test queues
	for _, q := range testQueues {
		_, err := heftyClient.DeleteQueue(context.TODO(), &sqs.DeleteQueueInput{
			QueueUrl: q,
		})
		Expect(err).To(BeNil())
	}
}, func() {
	// delete all remaining objects in test bucket
	var continueToken *string
	for {
		// list out objects to delete
		listObjects, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket:            &testBucket,
			ContinuationToken: continueToken,
		})
		Expect(err).To(BeNil())

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
				Bucket: &testBucket,
				Delete: &s3Types.Delete{
					Objects: itemsToDelete,
				},
			})
			Expect(err).To(BeNil())
		}

		if !*listObjects.IsTruncated {
			break
		} else {
			continueToken = listObjects.ContinuationToken
		}
	}

	// delete test bucket
	_, err := s3Client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: &testBucket,
	})
	Expect(err).To(BeNil())
})

func sendSqsMsgViaHefty(msg *string, msgAttr map[string]types.MessageAttributeValue, msgAttrRequested ...string) <-chan testResult {
	queueName := uuid.NewString()
	q, err := heftyClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
		QueueName: &queueName,
	})
	if err != nil {
		panic(err)
	}
	testQueues = append(testQueues, q.QueueUrl)

	out, err := heftyClient.SendHeftyMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody:       msg,
		MessageAttributes: msgAttr,
		QueueUrl:          q.QueueUrl,
	})
	if err != nil {
		panic(err)
	}

	ret := make(chan testResult)
	go receiveSqsMsgViaHefty(testResult{
		QueueUrl:      q.QueueUrl,
		SendMsgResult: out,
	}, ret, msgAttrRequested...)

	return ret
}

func receiveSqsMsgViaHefty(result testResult, ret chan<- testResult, msgAttrRequested ...string) {
	out, err := heftyClient.ReceiveHeftyMessage(context.TODO(), &sqs.ReceiveMessageInput{
		QueueUrl:              result.QueueUrl,
		WaitTimeSeconds:       20,
		MessageAttributeNames: msgAttrRequested,
	})
	if err != nil {
		panic(err)
	}

	if len(out.Messages) > 0 {
		result.ReceivedMsg = &out.Messages[0]
	} else {
		panic("did not receive messages in 20 seconds")
	}

	ret <- result
}

var _ = Describe("Hefty SQS Client Wrapper", func() {

	Describe("When sending a message to AWS SQS with the Hefty client", func() {
		var msg *string
		var msgAttr map[string]types.MessageAttributeValue
		var res testResult

		Context("and the message is at but not over the Hefty message size limit", func() {
			BeforeEach(func() {
				msg, msgAttr = testutils.GetMaxHeftyMsgBodyAndAttr()
				res = <-sendSqsMsgViaHefty(msg, msgAttr, "test03", "test05")
			})

			It("message body and message attributes are the same as what was sent", func() {
				Expect(res.ReceivedMsg.Body).To(Equal(msg))
				Expect(res.ReceivedMsg.MessageAttributes).To(Equal(msgAttr))
			})

			It("and even though 2 attributes were requested, we get all of them.", func() {
				Expect(res.ReceivedMsg.MessageAttributes).To(BeNumerically(">", 2))
			})
		})

		Context("and the message is at but not over the AWS SQS size limit", func() {
			BeforeEach(func() {
				msg, msgAttr = testutils.GetMaxSqsMsgBodyAndAttr()
				res = <-sendSqsMsgViaHefty(msg, msgAttr, "test03", "test05")
			})

			It("message body and message attributes are the same as what was sent", func() {
				Expect(res.ReceivedMsg.Body).To(Equal(msg))
				Expect(res.ReceivedMsg.MessageAttributes).To(ConsistOf("test03", "test05"))
			})

			It("and since 2 attributes were requested, we get 2 of them", func() {
				Expect(res.ReceivedMsg.MessageAttributes).To(BeNumerically("==", 2))
			})
		})
	})

})
