package hefty

import "github.com/vinujohn/hefty/internal/messages"

// ReferenceMsg determines if a message body is a reference message and returns a struct representing the reference message.
// This function is provided to developers who are using workflows where SNS/SQS messages are being sent to endpoints like
// AWS Lambda where it would be necessary to download the large message from S3 directly without using Hefty. Developers
// should also perform the necessary cleanup of S3 and SQS when using this workflow.
func ReferenceMsg(msgBody string) (*messages.ReferenceMsg, bool) {
	if !messages.IsReferenceMsg(msgBody) {
		return nil, false
	}

	ret, err := messages.ToReferenceMsg(msgBody)
	if err != nil {
		return nil, false
	}

	return ret, true
}
