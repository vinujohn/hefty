package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snsTypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/vinujohn/hefty"
	"github.com/vinujohn/hefty/internal/messages"
	"github.com/vinujohn/hefty/internal/testutils"
)

var (
	heftySqsClient *hefty.SqsClientWrapper
	heftySnsClient *hefty.SnsClientWrapper

	sqsClient *sqs.Client
	s3Client  *s3.Client
	snsClient *sns.Client

	testBucket string // value shared between all ginkgo processes if running in parallel
	testQueues []*string
	testTopics []*string
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
	snsClient = sns.NewFromConfig(sdkConfig)

	// create hefty clients
	heftySqsClient, err = hefty.NewSqsClientWrapper(sqsClient, s3Client, testBucket)
	Expect(err).To(BeNil())
	heftySnsClient, err = hefty.NewSnsClientWrapper(snsClient, s3Client, testBucket)
	Expect(err).To(BeNil())
})

var _ = SynchronizedAfterSuite(func() {
	// delete test queues
	for _, q := range testQueues {
		_, err := heftySqsClient.DeleteQueue(context.TODO(), &sqs.DeleteQueueInput{
			QueueUrl: q,
		})
		Expect(err).To(BeNil())
	}

	// delete test topics
	for _, t := range testTopics {
		_, err := heftySnsClient.DeleteTopic(context.TODO(), &sns.DeleteTopicInput{
			TopicArn: t,
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

var _ = Describe("Hefty Client Wrapper", func() {

	Describe("When sending a message to AWS SQS with the Hefty client", Ordered, func() {
		var msg *string
		var sqsMsgAttr map[string]sqsTypes.MessageAttributeValue
		var input *sqs.SendMessageInput
		var res *sqs.ReceiveMessageOutput
		var queueUrl *string
		var requestedAttr []string

		BeforeAll(func() {
			// create queue
			queueName := uuid.NewString()
			q, err := heftySqsClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
				QueueName: &queueName,
			})
			Expect(err).To(BeNil())
			queueUrl = q.QueueUrl
			testQueues = append(testQueues, queueUrl) // for cleanup later
		})

		AfterAll(func() {
			_, err := heftySqsClient.DeleteHeftyMessage(context.TODO(), &sqs.DeleteMessageInput{
				QueueUrl:      queueUrl,
				ReceiptHandle: res.Messages[0].ReceiptHandle,
			})
			Expect(err).To(BeNil())
		})

		JustBeforeEach(OncePerOrdered, func() {
			sqsAttributes := sqsMsgAttr
			input = &sqs.SendMessageInput{
				MessageBody:       msg,
				MessageAttributes: sqsAttributes,
				QueueUrl:          queueUrl,
			}
			_, err := heftySqsClient.SendHeftyMessage(context.TODO(), input)
			Expect(err).To(BeNil())
		})

		JustBeforeEach(OncePerOrdered, func() {
			var err error
			res, err = heftySqsClient.ReceiveHeftyMessage(context.TODO(), &sqs.ReceiveMessageInput{
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
				var msgAttr map[string]messages.MessageAttributeValue
				msg, msgAttr = testutils.GetMaxHeftyMsgBodyAndAttr()
				sqsMsgAttr = messages.MapToSqsMessageAttributeValues(msgAttr)
				requestedAttr = []string{"test03", "test05"}
			})

			It("and the message body and message attributes sent is not overwritten", func() {
				Expect(input.MessageBody).To(Equal(msg))
				Expect(input.MessageAttributes).To(Equal(sqsMsgAttr))
			})

			It("and the message body received is the same as what was sent", func() {
				Expect(res.Messages[0].Body).To(Equal(msg))
			})

			It("and even though 2 attributes were requested, we receive all message attributes sent.", func() {
				Expect(res.Messages[0].MessageAttributes).To(Equal(sqsMsgAttr))
			})
		})

		Context("and the message is at but not over the AWS SQS size limit", Ordered, func() {
			BeforeAll(func() {
				// create message body and attributes
				var msgAttr map[string]messages.MessageAttributeValue
				msg, msgAttr = testutils.GetMaxSqsMsgBodyAndAttr()
				sqsMsgAttr = messages.MapToSqsMessageAttributeValues(msgAttr)
				requestedAttr = []string{"test03", "test05"}
			})

			It("and the message body and message attributes sent is not overwritten", func() {
				Expect(input.MessageBody).To(Equal(msg))
				Expect(input.MessageAttributes).To(Equal(sqsMsgAttr))
			})

			It("and the message body received is the same as what was sent", func() {
				Expect(res.Messages[0].Body).To(Equal(msg))
			})

			It("and since 2 attributes were requested, we receive 2 of them", func() {
				Expect(res.Messages[0].MessageAttributes).Should(HaveLen(2))
				Expect(res.Messages[0].MessageAttributes).Should(HaveKey(MatchRegexp("test0[3|5]$")))
			})
		})
	})

	Describe("When sending a message to AWS SNS with the Hefty client", Ordered, func() {
		var msg *string
		var snsMsgAttr map[string]snsTypes.MessageAttributeValue
		var sqsMsgAttr map[string]sqsTypes.MessageAttributeValue
		var queueUrl *string
		var topicArn *string
		var input *sns.PublishInput
		var res *sqs.ReceiveMessageOutput
		var requestedAttr []string

		BeforeAll(func() {
			// create topic
			topicName := uuid.NewString()
			t, err := heftySnsClient.CreateTopic(context.TODO(), &sns.CreateTopicInput{
				Name: aws.String(topicName),
			})
			Expect(err).To(BeNil())
			topicArn = t.TopicArn
			testTopics = append(testTopics, topicArn)

			// create queue
			queueName := uuid.NewString()
			q, err := heftySqsClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
				QueueName: &queueName,
			})
			Expect(err).To(BeNil())
			queueUrl = q.QueueUrl
			testQueues = append(testQueues, queueUrl) // for cleanup later

			// get queue arn
			qAttr, err := heftySqsClient.GetQueueAttributes(context.TODO(), &sqs.GetQueueAttributesInput{
				QueueUrl: queueUrl,
				AttributeNames: []sqsTypes.QueueAttributeName{
					"QueueArn",
					"Policy",
				},
			})
			Expect(err).To(BeNil())
			qArn := qAttr.Attributes["QueueArn"]

			// subscribe queue to topic
			_, err = heftySnsClient.Subscribe(context.TODO(), &sns.SubscribeInput{
				Protocol: aws.String("sqs"),
				TopicArn: topicArn,
				Attributes: map[string]string{
					"RawMessageDelivery": "true",
				},
				Endpoint: aws.String(qArn),
			})
			Expect(err).To(BeNil())

			// add permission to queue so that sns can send messages to it
			_, err = heftySqsClient.SetQueueAttributes(context.TODO(), &sqs.SetQueueAttributesInput{
				QueueUrl: queueUrl,
				Attributes: map[string]string{
					"Policy": fmt.Sprintf(`
					{
						"Version": "2012-10-17",
						"Id": "snsAccessPolicy",
						"Statement": [
						  {
							"Effect": "Allow",
							"Principal": {
							  "Service": "sns.amazonaws.com"
							},
							"Action": "sqs:SendMessage",
							"Resource": "%s",
							"Condition": {
							  "ArnEquals": {
								"aws:SourceArn": "%s"
							  }
							}
						  }
						]
					}
					`, qArn, *topicArn),
				},
			})
			Expect(err).To(BeNil())
		})

		AfterAll(func() {
			_, err := heftySqsClient.DeleteHeftyMessage(context.TODO(), &sqs.DeleteMessageInput{
				QueueUrl:      queueUrl,
				ReceiptHandle: res.Messages[0].ReceiptHandle,
			})
			Expect(err).To(BeNil())
		})

		JustBeforeEach(OncePerOrdered, func() {
			input = &sns.PublishInput{
				Message:           msg,
				MessageAttributes: snsMsgAttr,
				TopicArn:          topicArn,
			}
			_, err := heftySnsClient.PublishHeftyMessage(context.TODO(), input)
			Expect(err).To(BeNil())
		})

		JustBeforeEach(OncePerOrdered, func() {
			var err error
			res, err = heftySqsClient.ReceiveHeftyMessage(context.TODO(), &sqs.ReceiveMessageInput{
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
				var msgAttr map[string]messages.MessageAttributeValue
				msg, msgAttr = testutils.GetMaxHeftyMsgBodyAndAttr()
				snsMsgAttr = messages.MapToSnsMessageAttributeValues(msgAttr)
				sqsMsgAttr = messages.MapToSqsMessageAttributeValues(msgAttr)
				requestedAttr = []string{"test03", "test05"}
			})

			It("and the message body and message attributes sent is not overwritten", func() {
				Expect(input.Message).To(Equal(msg))
				Expect(input.MessageAttributes).To(Equal(snsMsgAttr))
			})

			It("and the message body received is the same as what was sent", func() {
				Expect(res.Messages[0].Body).To(Equal(msg))
			})

			It("and even though 2 attributes were requested, we receive all message attributes sent.", func() {
				Expect(res.Messages[0].MessageAttributes).To(Equal(sqsMsgAttr))
			})
		})

		Context("and the message is at but not over the AWS SNS size limit", Ordered, func() {
			BeforeAll(func() {
				// create message body and attributes
				var msgAttr map[string]messages.MessageAttributeValue
				msg, msgAttr = testutils.GetMaxSnsMsgBodyAndAttr()
				snsMsgAttr = messages.MapToSnsMessageAttributeValues(msgAttr)
				requestedAttr = []string{"test03", "test05"}
			})

			It("and the message body and message attributes sent is not overwritten", func() {
				Expect(input.Message).To(Equal(msg))
				Expect(input.MessageAttributes).To(Equal(snsMsgAttr))
			})

			It("and the message body received is the same as what was sent", func() {
				Expect(res.Messages[0].Body).To(Equal(msg))
			})

			It("and since 2 attributes were requested, we receive 2 of them", func() {
				Expect(res.Messages[0].MessageAttributes).Should(HaveLen(2))
				Expect(res.Messages[0].MessageAttributes).Should(HaveKey(MatchRegexp("test0[3|5]$")))
			})
		})
	})
})
