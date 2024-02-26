package hefty

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
)

func TestMsgAttributesHash(t *testing.T) {
	msg := &largeMsg{
		Body: aws.String("test"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"test3": {
				DataType:    aws.String("Binary"),
				BinaryValue: []byte{1, 2, 3},
			},
			"test": {
				DataType:    aws.String("String"),
				StringValue: aws.String("test"),
			},
			"test2": {
				DataType:    aws.String("Number"),
				StringValue: aws.String("123"),
			},
		},
	}
	bHash, aHash := md5Hash(msg)
	assert.Equal(t, "098f6bcd4621d373cade4e832627b4f6", bHash)
	assert.Equal(t, "ae83a9fd2e99604a8073446145c4c523", aHash)
	t.Logf("MD5 Digest, Body:%s Attributes:%s", bHash, aHash)
}
