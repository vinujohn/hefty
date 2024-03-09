package testutils

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/vinujohn/hefty"
	"github.com/vinujohn/hefty/internal/messages"
)

func GetMsgBodyAndAttrs(bodySize, numAttributes, attributeValueSize int) (*string, map[string]messages.MessageAttributeValue) {
	body := createMessageText(bodySize)

	var msgAttributes map[string]messages.MessageAttributeValue

	if numAttributes > 0 {
		numAttributes = min(numAttributes, 99)
		msgAttributeTypes := []string{"String", "Binary"}
		msgAttributes = make(map[string]messages.MessageAttributeValue)
		for i := 0; i < numAttributes; i++ {
			key := "test" + fmt.Sprintf("%02d", i)
			dataType := msgAttributeTypes[i%len(msgAttributeTypes)]
			msgAttrVal := messages.MessageAttributeValue{
				DataType: aws.String(dataType),
			}
			switch dataType {
			case "String":
				msgAttrVal.StringValue = aws.String(createMessageText(attributeValueSize))
				msgAttributes[key] = msgAttrVal
			case "Binary":
				msgAttrVal.BinaryValue = createMessageBytes(attributeValueSize)
				msgAttributes[key] = msgAttrVal
			default:
				panic("unexpected data type for message attributes")
			}
		}
	}

	return &body, msgAttributes
}

func GetMsgBodyAndAttrsRandom() (*string, map[string]messages.MessageAttributeValue) {
	minBodySize := 30
	minAttrValueSize := 10
	maxAttrValueSize := 50
	minNumAttr := 0
	maxNumAttr := 10

	random := func(min, max int) int {
		return rand.Intn(max-min+1) + min
	}

	return GetMsgBodyAndAttrs(random(minBodySize, hefty.MaxSqsMessageLengthBytes*1.5), random(minNumAttr, maxNumAttr), random(minAttrValueSize, maxAttrValueSize))
}

func GetMaxHeftyMsgBodyAndAttr() (*string, map[string]messages.MessageAttributeValue) {
	const numAttributes = 11 // more than the sqs limit of 10

	attrTotalSize := (hefty.MaxSqsMessageLengthBytes +
		len("String") + // covers both "String" and "Binary"
		len("test01")) * numAttributes

	bodySize := hefty.MaxHeftyMessageLengthBytes - attrTotalSize

	return GetMsgBodyAndAttrs(bodySize, numAttributes, hefty.MaxSqsMessageLengthBytes)
}

func GetMaxSqsMsgBodyAndAttr() (*string, map[string]messages.MessageAttributeValue) {
	const numAttributes = 10 // sqs limit
	const attrValueSizeBytes = 256

	attrTotalSize := (attrValueSizeBytes +
		len("String") + // covers both "String" and "Binary"
		len("test01")) * numAttributes

	bodySize := hefty.MaxSqsMessageLengthBytes - attrTotalSize

	return GetMsgBodyAndAttrs(bodySize, numAttributes, attrValueSizeBytes)
}

func GetMaxSnsMsgBodyAndAttr() (*string, map[string]messages.MessageAttributeValue) {
	const numAttributes = 10 // sns limit
	const attrValueSizeBytes = 256

	attrTotalSize := (attrValueSizeBytes +
		len("String") + // covers both "String" and "Binary"
		len("test01")) * numAttributes

	bodySize := hefty.MaxSqsMessageLengthBytes - attrTotalSize

	msgAttr := make(map[string]messages.MessageAttributeValue)
	msgBody, sqsMsgAttr := GetMsgBodyAndAttrs(bodySize, numAttributes, attrValueSizeBytes)
	for k, v := range sqsMsgAttr {
		val := messages.MessageAttributeValue{
			DataType:    v.DataType,
			BinaryValue: v.BinaryValue,
			StringValue: v.StringValue,
		}
		msgAttr[k] = val
	}

	return msgBody, msgAttr
}

func GetMaxHeftyMsg() *messages.HeftyMessage {
	body, attributes := GetMaxHeftyMsgBodyAndAttr()
	msgSize, _ := messages.MessageSize(body, attributes)
	msg := messages.NewHeftyMessage(body, attributes, msgSize)

	return msg
}

func createMessageText(numBytes int) string {
	builder := strings.Builder{}

	// printable characters
	min := 33
	max := 126

	for i := 0; i < numBytes; i++ {
		randNum := rand.Intn(max-min+1) + min
		builder.WriteByte(byte(randNum))
	}

	return builder.String()
}

func createMessageBytes(numBytes int) []byte {
	ret := make([]byte, numBytes)

	min := 0
	max := 255

	for i := 0; i < numBytes; i++ {
		randNum := rand.Intn(max-min+1) + min
		ret[i] = byte(randNum)
	}

	return ret
}
