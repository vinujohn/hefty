package messages

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
)

const (
	ErrUnexpectedDataType = "encountered unexpected data type for message attribute: %s"
)

func MessageSize(msg *string, msgAttr map[string]MessageAttributeValue) (int, error) {
	var size int
	if msg != nil {
		size += len(*msg)
	}

	for k, v := range msgAttr {
		dataType := aws.ToString(v.DataType)
		size += len(k)
		size += len(dataType)
		if strings.HasPrefix(dataType, "String") || strings.HasPrefix(dataType, "Number") {
			size += len(aws.ToString(v.StringValue))
		} else if strings.HasPrefix(dataType, "Binary") {
			size += len(v.BinaryValue)
		} else {
			return -1, fmt.Errorf(ErrUnexpectedDataType, dataType)
		}
	}

	return size, nil
}

func Md5Digest(buf []byte) string {
	hash := md5.Sum(buf)
	return hex.EncodeToString(hash[:])
}
