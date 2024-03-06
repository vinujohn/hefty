package tests

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/vinujohn/hefty"
	"github.com/vinujohn/hefty/internal/messages"
)

func getMessageBodyAndAttributes(bodySize, numAttributes, attributeValueSize int) (*string, map[string]types.MessageAttributeValue) {
	body := createMessageText(bodySize)

	var msgAttributes map[string]types.MessageAttributeValue

	if numAttributes > 0 {
		numAttributes = min(numAttributes, 99)
		msgAttributeTypes := []string{"String", "Binary"}
		msgAttributes = make(map[string]types.MessageAttributeValue)
		for i := 0; i < numAttributes; i++ {
			key := "test" + fmt.Sprintf("%02d", i)
			dataType := msgAttributeTypes[i%len(msgAttributeTypes)]
			msgAttrVal := types.MessageAttributeValue{
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

func getMaxHeftyMsgBodyAndAttr() (*string, map[string]types.MessageAttributeValue) {
	const numAttributes = 11 // more than the sqs limit of 10

	attrTotalSize := (hefty.MaxSqsMessageLengthBytes +
		len("String") + // covers both "String" and "Binary"
		len("test01")) * numAttributes

	bodySize := hefty.MaxHeftyMessageLengthBytes - attrTotalSize

	return getMessageBodyAndAttributes(bodySize, numAttributes, hefty.MaxSqsMessageLengthBytes)
}

func getMaxSqsMsgBodyAndAttr() (*string, map[string]types.MessageAttributeValue) {
	const numAttributes = 10 // sqs limit
	const attrValueSizeBytes = 256

	attrTotalSize := (attrValueSizeBytes +
		len("String") + // covers both "String" and "Binary"
		len("test01")) * numAttributes

	bodySize := hefty.MaxSqsMessageLengthBytes - attrTotalSize

	return getMessageBodyAndAttributes(bodySize, numAttributes, attrValueSizeBytes)
}

func getMaxHeftyMessage() *messages.HeftySqsMsg {
	body, attributes := getMaxHeftyMsgBodyAndAttr()
	msg, _ := messages.NewHeftySqsMessage(body, attributes)

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
