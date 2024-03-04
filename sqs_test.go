package hefty

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
)

func TestLargeMessageSerializeAndDeserialize(t *testing.T) {
	msg := &largeSqsMsg{
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

	size, _ := msg.Size()
	serialized, bodyOffset, msgAttrOffset, err := msg.Serialize(size)
	if err != nil {
		t.Fatalf("error when trying to serialize. %v", err)
	}
	assert.Len(t, serialized, size+lengthSize+(len(msg.MessageAttributes)*(numLengthSizesPerMsgAttr*lengthSize+transportTypeSize)))
	assert.Equal(t, "098f6bcd4621d373cade4e832627b4f6", md5Digest(serialized[bodyOffset:msgAttrOffset]))
	if len(msg.MessageAttributes) > 0 {
		assert.Equal(t, "ae83a9fd2e99604a8073446145c4c523", md5Digest(serialized[msgAttrOffset:]))
	}

	dMsg := &largeSqsMsg{}
	if err := dMsg.Deserialize(serialized); err != nil {
		t.Fatalf("error from deserialize. %v", err)
	}

	assert.Equal(t, msg, dMsg)
}
