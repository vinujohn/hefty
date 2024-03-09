package messages

import (
	snsTypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type MessageAttributeValue struct {
	DataType    *string
	StringValue *string
	BinaryValue []byte
}

func MapFromSqsMessageAttributeValues(m map[string]sqsTypes.MessageAttributeValue) map[string]MessageAttributeValue {
	if m == nil {
		return nil
	}

	ret := make(map[string]MessageAttributeValue)

	for k, v := range m {
		ret[k] = MessageAttributeValue{
			DataType:    v.DataType,
			StringValue: v.StringValue,
			BinaryValue: v.BinaryValue,
		}
	}

	return ret
}

func MapToSqsMessageAttributeValues(m map[string]MessageAttributeValue) map[string]sqsTypes.MessageAttributeValue {
	if m == nil {
		return nil
	}

	ret := make(map[string]sqsTypes.MessageAttributeValue)

	for k, v := range m {
		ret[k] = sqsTypes.MessageAttributeValue{
			DataType:    v.DataType,
			StringValue: v.StringValue,
			BinaryValue: v.BinaryValue,
		}
	}

	return ret
}

func MapFromSnsMessageAttributeValues(m map[string]snsTypes.MessageAttributeValue) map[string]MessageAttributeValue {
	if m == nil {
		return nil
	}

	ret := make(map[string]MessageAttributeValue)

	for k, v := range m {
		ret[k] = MessageAttributeValue{
			DataType:    v.DataType,
			StringValue: v.StringValue,
			BinaryValue: v.BinaryValue,
		}
	}

	return ret
}

func MapToSnsMessageAttributeValues(m map[string]MessageAttributeValue) map[string]snsTypes.MessageAttributeValue {
	if m == nil {
		return nil
	}

	ret := make(map[string]snsTypes.MessageAttributeValue)

	for k, v := range m {
		ret[k] = snsTypes.MessageAttributeValue{
			DataType:    v.DataType,
			StringValue: v.StringValue,
			BinaryValue: v.BinaryValue,
		}
	}

	return ret
}
