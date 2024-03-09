package messages

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func MessageSize(msg *string, msgAttr map[string]MessageAttributeValue) (int, error) {
	var size int
	size += len(*msg)

	for k, v := range msgAttr {
		dataType := aws.ToString(v.DataType)
		size += len(k)
		size += len(dataType)
		if strings.HasPrefix(dataType, "String") || strings.HasPrefix(dataType, "Number") {
			size += len(aws.ToString(v.StringValue))
		} else if strings.HasPrefix(dataType, "Binary") {
			size += len(v.BinaryValue)
		} else {
			return -1, fmt.Errorf("encountered unexpected data type for message attribute: %s", dataType)
		}
	}

	return size, nil
}
