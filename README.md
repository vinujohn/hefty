# Hefty Client Library for Golang
This library provides a wrapper for the AWS SQS Golang SDK client, which allows you to send and receive messages from SQS for the purposes of sending messages larger than the stated AWS SQS size limit for messages. A wrapper for the AWS SNS SDK is also provided which allows integration between the two AWS services.

One of the limitations of AWS SQS is the size limit for messages at 256KB. This limit encompasses not only the message body but also the message attributes. This limit can discourage the use of AWS SQS for workflows that require large message sizes, like image and media processing.

A common solution to this problem is the idea of reference messaging, which is sometimes called the [claim-check pattern](https://learn.microsoft.com/en-us/azure/architecture/patterns/claim-check). This pattern stores a large message in a shared data store that has a secondary reference message (claim check) associated with it. The receiver receives the reference message and can get the large message from the shared data store.

![claim check](https://learn.microsoft.com/en-us/azure/architecture/patterns/_images/claim-check.png)

The Golang based library provides similar functionality to the extended client libraries provided by AWS for the purpose of sending large messages to SQS. Unfortunately, these clients are currently only for Python and Java at this time.

- [Amazon SQS Extended Client Library for Python](https://github.com/awslabs/amazon-sqs-python-extended-client-lib)
- [Amazon SQS Extended Client Library for Java](https://github.com/awslabs/amazon-sqs-java-extended-client-lib)

## Hefty SQS Client Wrapper
The Hefty SQS Client Wrapper uses AWS S3 as the data store for large messages. It calculates the message size for each message being sent to AWS SQS. Messages that come under the AWS SQS message size limit will be sent to AWS SQS directly, whereas messages over this limit will be sent to AWS S3, and a reference message will be sent to AWS SQS instead. Receivers of messages sent by the Hefty SQS Client Wrapper will get the original message sent by the sender. Both the sender and receiver are unaware of how messages are stored and sent while using the same underlying API provided by the AWS SQS SDK.

### Usage
```go
package main

import (
	"context"
	"log"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/vinujohn/hefty"
)

const (
	myBucket   = "my-bucket"
	myQueueUrl = "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue"
)

// generates a large message which will be stored in AWS S3
func getLargeMessage() string {
	builder := strings.Builder{}
	for i := 0; i < hefty.MaxSqsMessageLengthBytes+1; i++ {
		builder.WriteString("a")
	}
	return builder.String()
}

func main() {
	// Create AWS SQS and AWS S3 clients as you would usually do.
	// This is just one example of creating these clients using default config.
	sdkConfig, _ := config.LoadDefaultConfig(context.TODO())
	sqsClient := sqs.NewFromConfig(sdkConfig)
	s3Client := s3.NewFromConfig(sdkConfig)

	// Create a Hefty SQS Client Wrapper by passing in an AWS SQS client, AWS S3 client,
	// and a AWS S3 bucket used to save messages larger than 256KB.
	heftyClientWrapper, err := hefty.NewSqsClientWrapper(sqsClient, s3Client, myBucket)
	if err != nil {
		panic(err)
	}

	// Send a message to AWS SQS. Message sizes greater than 256KB will automatically
	// be stored in S3. Hefty client wrapper methods use the same input and return types as
	// the AWS SQS SDK method.
	largeMessageBody := getLargeMessage()
	_, err = heftyClientWrapper.SendHeftyMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody: &largeMessageBody,
		QueueUrl:    aws.String(myQueueUrl),
	})
	if err != nil {
		panic(err)
	}

	// Receive messages from AWS SQS. Messages larger than 256KB will automatically
	// be downloaded from AWS S3.
	out, err := heftyClientWrapper.ReceiveHeftyMessage(context.TODO(), &sqs.ReceiveMessageInput{
		QueueUrl:        aws.String(myQueueUrl),
		WaitTimeSeconds: 20,
	})
	if err != nil {
		panic(err)
	}

	for _, msg := range out.Messages {
		// perform some processing with "msg"
		// ...

		// make sure what was sent is what we received
		if !reflect.DeepEqual(largeMessageBody, *msg.Body) {
			panic("received different message than what was sent")
		}

		log.Println("received correct message")

		// Delete a message from AWS SQS. Messages larger than 256KB will automatically
		// be deleted from AWS S3.
		_, err = heftyClientWrapper.DeleteHeftyMessage(context.TODO(), &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(myQueueUrl),
			ReceiptHandle: msg.ReceiptHandle,
		})
		if err != nil {
			panic(err)
		}
	}
}
```
### API Design
Hefty has been designed to be as unobtrusive as possible, with little or no understanding needed to use it apart from understanding how AWS SQS works. Since it is a wrapper of the AWS SQS SDK, the Hefty API tries to mimic the exact apparent behavior of its AWS SQS SDK counterparts and even uses the same input types and return types. The following is a list of Hefty API methods and their AWS SQS SDK counterparts.
| Hefty SQS Client Wrapper | AWS SQS SDK     | Input   | Output   |
|----------------------|---------------------|--------|------- |
| SendHeftyMessage(...)   | SendMessage(...)    | context.Context, *sqs.SendMessageInput, ...func(*sqs.Options) | *sqs.SendMessageOutput, error |
| ReceiveHeftyMessage(...)| ReceiveMessage(...) | context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options) | *sqs.ReceiveMessageOutput, error |
| DeleteHeftyMessage(...) | DeleteMessage(...)  | context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options) | *sqs.DeleteMessageOutput, error|

### Important Considerations
#### Message Size Limit
The Hefty SQS Client Wrapper currently has a message size limit of **32MB** which is considerably greater than the AWS SQS message size limit of **256KB**. This includes the size of the message body and the sizes of the message attributes. The same criteria that AWS uses to calculate the [size of message attributes](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#message-attribute-components) is used by the Hefty SQS Client Wrapper as well.

#### MD5 Digest
Every message sent to AWS SQS has the MD5 digest calculated for both the message body and message attributes. However, when the Hefty SQS Client Wrapper stores a large message in AWS S3, the reference message sent to AWS SQS will naturally have different MD5 digests in the system. To account for this, the Hefty SQS Client Wrapper will calculate the MD5 digest of both the message body and message attributes for the original message and store that information with the reference message. This allows the receiver of the message to get the correct MD5 digests via the Hefty SQS Client Wrapper. The [MD5 digest calculation for the message attributes](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-attributes-md5-message-digest-calculation) used by the Hefty SQS Client Wrapper is the same as AWS.

#### Requesting Message Attributes
The AWS SQS SDK allows a user to request message attributes that he or she is interested in receiving. The capability is provided to request all attributes available in a message or a subset of attributes. The latter may provide some benefit when message attributes are numerous and many KBs. However, when using the Hefty SQS Client Wrapper and receiving a large message, all attributes will be returned that were originally sent. Theoretically, since AWS restricts the number of message attributes that can be sent to 10, if a large message is sent via the Hefty SQS Client Wrapper, an unlimited number of message attributes can be sent and received as long as the message size constraint of **32MB** is met.

#### Consistency With API Usage
It is important to be consistent when sending messages via the Hefty SQS Client Wrapper by using the corresponding Hefty API for receiving and deleting the same messages. Although it is possible to use the Hefty SQS Client Wrapper to send messages and then the AWS SQS SDK to receive and delete messages, undesirable behavior can occur. However, sending messages via the AWS SQS SDK and receiving and deleting messages via the Hefty SQS Client Wrapper should be OK.

#### Undeliverable Messages
There will always be cases with asynchronous messaging where messages cannot be processed and are undeliverable. It is important to use the capabilities that AWS SQS provides in these cases, such as dead letter queues, redrive policies, and message expiration. With the Hefty SQS Client Wrapper, the problem is compounded since there is a data store with these potentially undeliverable messages. If these stored messages are of a sensitive nature or are expensive to store, it is important to make sure they are secured properly with the right encryption and have the appropriate object lifecycles assigned to them.

## Hefty SNS Client Wrapper
The Hefty SNS Client Wrapper is similar to the Hefty SQS Client Wrapper and is provided to send large messages to AWS SNS so that they can be consumed by various endpoints. This includes AWS SQS, where there is an established pattern of sending a message to AWS SNS, which is in turn consumed by one or more AWS SQS queues. The same exact considerations listed for the Hefty SQS Client Wrapper apply to the Hefty SNS Client Wrapper as well, with some important additions listed later.

### Api Design
| Hefty SNS Client Wrapper | AWS SNS SDK     | Input   | Output   |
|----------------------|---------------------|--------|------- |
| PublishHeftyMessage(...)   | Publish(...)    | context.Context, *sns.PublishInput, ...func(*sns.Options) | *sns.PublishOutput, error |

### Important Considerations
#### Raw Message Delivery
When creating a subscription to an AWS SNS topic that will be used to publish large messages, it is important to enable the option `Raw Message Delivery`. This allows any message attributes sent with the AWS SNS message to be isolated separately from the message body when the message makes its way to AWS SQS. If this option is not enabled, the message attributes are sent along with the message body, and the Hefty SQS Client Wrapper `ReceiveMessage(...)` method has no way of determining if a message is in fact a large message stored in AWS S3.

#### Additional Endpoints
The Hefty SNS Client Wrapper has been exclusively tested with having AWS SQS as an endpoint. However, there are potentially additional endpoints that can be used such as AWS Lambda and HTTP/HTTPS endpoints. These endpoints could take the reference message and download the large message from AWS S3 themselves. A utility function `ReferenceMsg(...)` is provided to developers to take a message body string received by these endpoints, and convert it into a reference message. The following is a JSON representation of an example reference message.
```json
{
   "s3_region":           "us-west-2",
   "s3_bucket":           "my-bucket",
   "s3_key":              "foo/bar",
   "md5_digest_msg_body": "f6335cfd72eec3e93f84c1d0330c5f85",
   "md5_digest_msg_attr": "0d3b2bd785f7e1d17bf21d41d2e4939a"
}
```
