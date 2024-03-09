package messages

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
)

func TestHeftyMessageSerializeAndDeserialize(t *testing.T) {
	msg := aws.String("test")
	attributes := map[string]MessageAttributeValue{
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
	}
	msgSize, _ := MessageSize(msg, attributes)
	heftyMsg := NewHeftyMessage(msg, attributes, msgSize)

	serialized, bodyOffset, msgAttrOffset, err := heftyMsg.Serialize()
	if err != nil {
		t.Fatalf("error when trying to serialize. %v", err)
	}
	assert.Len(t, serialized, heftyMsg.Size+lengthSize+(len(heftyMsg.MessageAttributes)*(numLengthSizesPerMsgAttr*lengthSize+transportTypeSize)))
	assert.Equal(t, "098f6bcd4621d373cade4e832627b4f6", Md5Digest(serialized[bodyOffset:msgAttrOffset]))
	if len(heftyMsg.MessageAttributes) > 0 {
		assert.Equal(t, "ae83a9fd2e99604a8073446145c4c523", Md5Digest(serialized[msgAttrOffset:]))
	}

	var dMsg *HeftyMessage
	if dMsg, err = DeserializeHeftyMessage(serialized); err != nil {
		t.Fatalf("error from deserialize. %v", err)
	}

	assert.Equal(t, heftyMsg, dMsg)
}
