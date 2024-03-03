# Hefty Client Library for Golang
This library provides a wrapper for the AWS SQS Golang SDK client, which allows you to send and receive messages from SQS for the purposes of sending messages larger than the stated AWS SQS size limit for messages. A wrapper for the AWS SNS SDK will be forthcoming.

One of the limitations of AWS SQS is the size limit for messages at 256KB. This limit encompasses not only the message body but also the message attributes. This limit can discourage the use of AWS SQS for workflows that require large message sizes, like image and media processing.

A common solution to this problem is the idea of reference messaging, which is sometimes called the [claim-check pattern](https://learn.microsoft.com/en-us/azure/architecture/patterns/claim-check). This pattern stores a large message in a shared data store so that the sender can send a reference to that large message to a receiver. The receiver can then use the reference to get the large message.

![claim check](https://learn.microsoft.com/en-us/azure/architecture/patterns/_images/claim-check.png)

The Hefty message client provides similar functionality to the extended client libraries provided by AWS for the purpose of sending large messages to SQS. Unfortunately, these clients are currently only for Python and Java at this time.

- [Amazon SQS Extended Client Library for Python](https://github.com/awslabs/amazon-sqs-python-extended-client-lib)
- [Amazon SQS Extended Client Library for Java](https://github.com/awslabs/amazon-sqs-java-extended-client-lib)

# Usage

```go
package main

import (
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
    "github.com/vinujohn/hefty"
    ...
)

func main() {
    // Create AWS SQS and AWS S3 clients as you would usually do.
    // This is just one example.
    sdkConfig, _ := config.LoadDefaultConfig(context.TODO())
    sqsClient := sqs.NewFromConfig(sdkConfig)
    S3Client = s3.NewFromConfig(sdkConfig)

    // Create a Hefty message client by passing in an AWS SQS client, AWS S3 client,
    // and a AWS S3 bucket used to save messages larger than 256KB.
    myBucket := "myBucket"
    heftyClient, err = hefty.NewSqsClient(sqsClient, s3Client, myBucket)

    // Send a message to AWS SQS. Message sizes greater than 256KB will automatically 
    // be stored in S3. Hefty client methods use the same input and return types as 
    // the similar AWS SQS client method.
	largeMessageBody := "..."
    queueUrl := "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue"
    heftyClient.SendHeftyMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody: &largeMessageBody,
		QueueUrl:    &queueUrl,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"msgAttr1": {
				DataType:    aws.String("String"),
				StringValue: aws.String("MyString"),
			},
		},
	})


outer:
    for {
        // Receive messages from AWS SQS. Messages larger than 256KB will automatically
        // be downloaded from AWS S3.
        out, err := heftyClient.ReceiveHeftyMessage(context.TODO(), &sqs.ReceiveMessageInput{
			QueueUrl: &queueUrl,
		})

        for _, msg := range out.Messages {
            // perform some processing with "msg"
            
            // Delete a message from AWS SQS. Messages larger than 256KB will automatically
            // be deleted from AWS S3.
			heftyClient.DeleteHeftyMessage(context.TODO(), &sqs.DeleteMessageInput{
				QueueUrl:      &testQueueUrl,
				ReceiptHandle: msg.ReceiptHandle,
			})

            break outer
		}
    }
}
```