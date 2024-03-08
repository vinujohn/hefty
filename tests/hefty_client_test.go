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

var _ = Describe("Hefty SQS Client Wrapper", func() {

	Describe("When sending a message to AWS SQS with the Hefty client", Ordered, func() {
		var msg *string
		var msgAttr map[string]types.MessageAttributeValue
		var res *sqs.ReceiveMessageOutput
		var queueUrl *string
		var requestedAttr []string

		BeforeAll(func() {
			// create queue
			queueName := uuid.NewString()
			q, err := heftyClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
				QueueName: &queueName,
			})
			Expect(err).To(BeNil())
			queueUrl = q.QueueUrl
			testQueues = append(testQueues, queueUrl) // for cleanup later
		})

		AfterAll(func() {
			_, err := heftyClient.DeleteHeftyMessage(context.TODO(), &sqs.DeleteMessageInput{
				QueueUrl:      queueUrl,
				ReceiptHandle: res.Messages[0].ReceiptHandle,
			})
			Expect(err).To(BeNil())
		})

		JustBeforeEach(OncePerOrdered, func() {
			_, err := heftyClient.SendHeftyMessage(context.TODO(), &sqs.SendMessageInput{
				MessageBody:       msg,
				MessageAttributes: msgAttr,
				QueueUrl:          queueUrl,
			})
			Expect(err).To(BeNil())
		})

		JustBeforeEach(OncePerOrdered, func() {
			var err error
			res, err = heftyClient.ReceiveHeftyMessage(context.TODO(), &sqs.ReceiveMessageInput{
				QueueUrl:              queueUrl,
				WaitTimeSeconds:       20,
				MessageAttributeNames: requestedAttr,
			})
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeNil())
			Expect(res.Messages).NotTo(BeEmpty())
		})

		Context("and the message is at, but not over the Hefty message size limit", Ordered, func() {
			BeforeAll(func() {
				// create message body and attributes
				msg, msgAttr = testutils.GetMaxHeftyMsgBodyAndAttr()
				requestedAttr = []string{"test03", "test05"}
			})

			It("and the message body is the same as what was sent", func() {
				Expect(res.Messages[0].Body).To(Equal(msg))
			})

			It("and even though 2 attributes were requested, we get all message attributes sent.", func() {
				Expect(res.Messages[0].MessageAttributes).To(Equal(msgAttr))
			})
		})

		Context("and the message is at but not over the AWS SQS size limit", Ordered, func() {
			BeforeAll(func() {
				// create message body and attributes
				msg, msgAttr = testutils.GetMaxSqsMsgBodyAndAttr()
				requestedAttr = []string{"test03", "test05"}
			})

			It("and the message body is the same as what was sent", func() {
				Expect(res.Messages[0].Body).To(Equal(msg))
			})

			It("and since 2 attributes were requested, we get 2 of them", func() {
				Expect(res.Messages[0].MessageAttributes).Should(HaveLen(2))
				Expect(res.Messages[0].MessageAttributes).Should(HaveKey(MatchRegexp("test0[3|5]$")))
			})
		})
	})

})
