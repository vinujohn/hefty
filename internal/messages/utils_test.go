package messages

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
)

func TestMessageSize(t *testing.T) {
	var tests = []struct {
		desc      string
		inMsgBody *string
		//len(key)+len(datatype)+len(value)
		inMsgAttr map[string]MessageAttributeValue
		expSize   int
		expErr    error
	}{
		{
			desc:    "body_nil_attr_nil_out_0",
			expSize: 0,
			expErr:  nil,
		},
		{
			desc:      "body_10_attr_nil_out_10",
			inMsgBody: aws.String("0123456789"),
			expSize:   10,
			expErr:    nil,
		},
		{
			desc:      "body_10_attr_1_string_22_out_32",
			inMsgBody: aws.String("0123456789"),
			inMsgAttr: map[string]MessageAttributeValue{
				"Test01": {
					DataType:    aws.String("String.blah"),
					StringValue: aws.String("01234"),
				},
			},
			expSize: 32,
			expErr:  nil,
		},
		{
			desc:      "body_10_attr_1_binary_22_out_32",
			inMsgBody: aws.String("0123456789"),
			inMsgAttr: map[string]MessageAttributeValue{
				"Test01": {
					DataType:    aws.String("Binary.blah"),
					BinaryValue: []byte{0, 1, 2, 3, 4},
				},
			},
			expSize: 32,
			expErr:  nil,
		},
		{
			desc:      "body_10_attr_1_invalid_datatype_out_-1",
			inMsgBody: aws.String("0123456789"),
			inMsgAttr: map[string]MessageAttributeValue{
				"Test01": {
					DataType:    aws.String("blah"),
					StringValue: aws.String("01234"),
				},
			},
			expSize: -1,
			expErr:  fmt.Errorf(ErrUnexpectedDataType, "blah"),
		},
		{
			desc:      "body_10_attr_2_string_22_binary_22_out_54",
			inMsgBody: aws.String("0123456789"),
			inMsgAttr: map[string]MessageAttributeValue{
				"Test01": {
					DataType:    aws.String("String"),
					StringValue: aws.String("01234"),
				},
				"Test02": {
					DataType:    aws.String("Binary"),
					BinaryValue: []byte{0, 1, 2, 3, 4},
				},
			},
			expSize: 44,
		},
		{
			desc:      "body_10_attr_1_string_binary_22_out_28",
			inMsgBody: aws.String("0123456789"),
			inMsgAttr: map[string]MessageAttributeValue{
				"Test01": {
					DataType:    aws.String("String"),
					StringValue: aws.String("012345"),
					BinaryValue: []byte{0, 1, 2, 3, 4},
				},
			},
			expSize: 28,
		},
	}

	for _, tt := range tests {

		t.Run(tt.desc, func(t *testing.T) {
			size, err := MessageSize(tt.inMsgBody, tt.inMsgAttr)
			assert.Equal(t, tt.expSize, size, "expected size")
			assert.Equal(t, tt.expErr, err, "expected error")
		})
	}
}
