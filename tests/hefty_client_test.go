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
	CreateSqsQueue := func() *string {
		GinkgoHelper()
		// create queue
		queueName := uuid.NewString()
		q, err := heftySqsClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
			QueueName: &queueName,
		})
		Expect(err).To(BeNil())
		testQueues = append(testQueues, q.QueueUrl) // for cleanup later

		return q.QueueUrl
	}

	SendSqsMessage := func(queueUrl string, msg string, attributes map[string]sqsTypes.MessageAttributeValue) *sqs.SendMessageInput {
		GinkgoHelper()
		input := &sqs.SendMessageInput{
			MessageBody:       &msg,
			MessageAttributes: attributes,
			QueueUrl:          &queueUrl,
		}
		_, err := heftySqsClient.SendHeftyMessage(context.TODO(), input)
		Expect(err).To(BeNil())

		return input
	}

	ReceiveSqsMessage := func(queueUrl string, requestedAttrNames []string) *sqs.ReceiveMessageOutput {
		GinkgoHelper()
		res, err := heftySqsClient.ReceiveHeftyMessage(context.TODO(), &sqs.ReceiveMessageInput{
			QueueUrl:              &queueUrl,
			WaitTimeSeconds:       20,
			MessageAttributeNames: requestedAttrNames,
		})
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		Expect(res.Messages).NotTo(BeEmpty())

		return res
	}

	DeleteHeftyMessage := func(queueUrl, receiptHandle string) {
		GinkgoHelper()
		_, err := heftySqsClient.DeleteHeftyMessage(context.TODO(), &sqs.DeleteMessageInput{
			QueueUrl:      &queueUrl,
			ReceiptHandle: &receiptHandle,
		})
		Expect(err).To(BeNil())
	}

	When("When sending a message to AWS SQS with the Hefty client wrapper", func() {
		var queueUrl *string
		var msg *string
		var input *sqs.SendMessageInput
		var sqsMsgAttr map[string]sqsTypes.MessageAttributeValue
		var requestedAttr []string
		var res *sqs.ReceiveMessageOutput

		JustBeforeEach(OncePerOrdered, func() {
			// send message to queue
			input = SendSqsMessage(*queueUrl, *msg, sqsMsgAttr)

			// receive message from queue
			res = ReceiveSqsMessage(*queueUrl, requestedAttr)

			// delete message from queue
			DeleteHeftyMessage(*queueUrl, *res.Messages[0].ReceiptHandle)
		})

		Context("and the message is at, but not over the Hefty message size limit", Ordered, func() {
			BeforeAll(func() {
				// create queue
				queueUrl = CreateSqsQueue()

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
				// create queue
				queueUrl = CreateSqsQueue()

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
				Expect(res.Messages[0].MessageAttributes).Should(Or(
					HaveKey("test03"),
					HaveKey(("test05")),
				))
			})

			Context("but the AlwaysSendToS3 option was set on the wrapper", Ordered, func() {
				BeforeAll(func() {
					// override client wrapper
					heftySqsClient, _ = hefty.NewSqsClientWrapper(sqsClient, s3Client, testBucket, hefty.AlwaysSendToS3())

					// send message to queue
					input = SendSqsMessage(*queueUrl, *msg, sqsMsgAttr)

					// receive message from queue
					res = ReceiveSqsMessage(*queueUrl, requestedAttr)

					// delete message from queue
					DeleteHeftyMessage(*queueUrl, *res.Messages[0].ReceiptHandle)
				})

				It("and even though 2 attributes were requested, we receive all of them", func() {
					Expect(res.Messages[0].MessageAttributes).To(Equal(sqsMsgAttr))
				})
			})
		})
	})

	When("When sending a message to AWS SNS with the Hefty client", func() {
		var msg *string
		var queueUrl *string
		var topicArn *string
		var snsMsgAttr map[string]snsTypes.MessageAttributeValue
		var sqsMsgAttr map[string]sqsTypes.MessageAttributeValue
		var requestedAttr []string
		var input *sns.PublishInput
		var res *sqs.ReceiveMessageOutput

		CreateSnsTopicAndSubscription := func(queueUrl *string) *string {
			GinkgoHelper()
			// create topic
			topicName := uuid.NewString()
			t, err := heftySnsClient.CreateTopic(context.TODO(), &sns.CreateTopicInput{
				Name: aws.String(topicName),
			})
			Expect(err).To(BeNil())
			testTopics = append(testTopics, t.TopicArn)

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
				TopicArn: t.TopicArn,
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
								`, qArn, *t.TopicArn),
				},
			})
			Expect(err).To(BeNil())

			return t.TopicArn
		}

		PublishSnsMessage := func(topicArn *string, msg *string, attributes map[string]snsTypes.MessageAttributeValue) *sns.PublishInput {
			input := &sns.PublishInput{
				Message:           msg,
				MessageAttributes: attributes,
				TopicArn:          topicArn,
			}
			_, err := heftySnsClient.PublishHeftyMessage(context.TODO(), input)
			Expect(err).To(BeNil())
			return input
		}

		JustBeforeEach(OncePerOrdered, func() {
			// publish message to topic
			input = PublishSnsMessage(topicArn, msg, snsMsgAttr)

			// receive message from queue
			res = ReceiveSqsMessage(*queueUrl, requestedAttr)

			// delete message from queue
			DeleteHeftyMessage(*queueUrl, *res.Messages[0].ReceiptHandle)
		})

		Context("and the message is at, but not over the Hefty message size limit", Ordered, func() {
			BeforeAll(func() {
				// create queue
				queueUrl = CreateSqsQueue()

				// create topic and subscription
				topicArn = CreateSnsTopicAndSubscription(queueUrl)

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
				// create queue
				queueUrl = CreateSqsQueue()
				// create topic and subscription
				topicArn = CreateSnsTopicAndSubscription(queueUrl)

				// create message body and attributes
				var msgAttr map[string]messages.MessageAttributeValue
				msg, msgAttr = testutils.GetMaxSnsMsgBodyAndAttr()
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

			It("and since 2 attributes were requested, we receive 2 of them", func() {
				Expect(res.Messages[0].MessageAttributes).Should(HaveLen(2))
				Expect(res.Messages[0].MessageAttributes).Should(Or(
					HaveKey("test03"),
					HaveKey(("test05")),
				))
			})
		})

		Context("but the AlwaysSendToS3 option was set on the wrapper", Ordered, func() {
			BeforeAll(func() {
				// override client wrapper
				heftySnsClient, _ = hefty.NewSnsClientWrapper(snsClient, s3Client, testBucket, hefty.AlwaysSendToS3())

				// publish message to topic
				input = PublishSnsMessage(topicArn, msg, snsMsgAttr)

				// receive message from queue
				res = ReceiveSqsMessage(*queueUrl, requestedAttr)

				// delete message from queue
				DeleteHeftyMessage(*queueUrl, *res.Messages[0].ReceiptHandle)
			})

			It("and even though 2 attributes were requested, we receive all of them", func() {
				Expect(res.Messages[0].MessageAttributes).To(Equal(sqsMsgAttr))
			})
		})
	})
})
