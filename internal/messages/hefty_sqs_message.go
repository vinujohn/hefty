package messages

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// HeftySqsMsg is an AWS SQS message that is over 256KB and needs to be stored in AWS S3
type HeftySqsMsg struct {
	Body              *string
	MessageAttributes map[string]sqsTypes.MessageAttributeValue
	Size              int // size of both the body and message attributes
}

const (
	lengthSize                    = 4
	transportTypeSize             = 1
	numLengthSizesPerMsgAttr      = 3
	stringTransportType      byte = 1
	binaryTransportType      byte = 2
)

func NewHeftySqsMessage(body *string, msgAttributes map[string]sqsTypes.MessageAttributeValue) (*HeftySqsMsg, error) {
	msg := &HeftySqsMsg{
		Body:              body,
		MessageAttributes: msgAttributes,
	}

	var err error
	msg.Size, err = msgSize(msg)
	if err != nil {
		return nil, fmt.Errorf("unable to calculate message size. %v", err)
	}

	return msg, nil
}

/*
|length|body|length|attribute name|length|attribute datatype|attribute transport type|length|attribute value|
|4Bytes|	|4Bytes|			  |4Bytes|					|1Byte					 |4Bytes|				|
|---once----|-----------------------------------------zero or more------------------------------------------|
*/
func (msg *HeftySqsMsg) Serialize() (serialized []byte, bodyOffset int, msgAttrOffset int, err error) {
	// create a buffer
	b := make([]byte, 0, msg.Size+lengthSize+(len(msg.MessageAttributes)*(numLengthSizesPerMsgAttr*lengthSize+transportTypeSize)))
	buf := bytes.NewBuffer(b)

	// write body
	err = writeNext(buf, msg.Body)
	if err != nil {
		err = fmt.Errorf("unable to write message body to buffer. %s", err)
		return
	}

	// calculate offsets
	bodyOffset = lengthSize
	msgAttrOffset = len(*msg.Body) + bodyOffset

	if msg.MessageAttributes != nil && len(msg.MessageAttributes) > 0 {
		type keyValue struct {
			key   string
			value sqsTypes.MessageAttributeValue
		}
		// sort slice of map keys and values as per aws requirements
		// for calculating md5 digest
		msgAttributes := []keyValue{}
		for k, v := range msg.MessageAttributes {
			msgAttributes = append(msgAttributes, keyValue{
				key:   k,
				value: v,
			})
		}
		sort.Slice(msgAttributes, func(i, j int) bool {
			return msgAttributes[i].key < msgAttributes[j].key
		})

		for _, attr := range msgAttributes {
			// write message attribute key
			err = writeNext(buf, attr.key)
			if err != nil {
				err = fmt.Errorf("unable to write message attribute key to buffer. %s", err)
				return
			}

			// write message attribute data type
			err = writeNext(buf, attr.value.DataType)
			if err != nil {
				err = fmt.Errorf("unable to write message attribute data type to buffer. %s", err)
				return
			}

			// write message attribute value
			if strings.HasPrefix(*attr.value.DataType, "String") || strings.HasPrefix(*attr.value.DataType, "Number") {
				err = writeNext(buf, stringTransportType)
				if err != nil {
					err = fmt.Errorf("unable to write message attribute transport type (string) to buffer. %s", err)
					return
				}
				err = writeNext(buf, attr.value.StringValue)
				if err != nil {
					err = fmt.Errorf("unable to write message attribute string value to buffer. %s", err)
					return
				}
			} else if strings.HasPrefix(*attr.value.DataType, "Binary") {
				err = writeNext(buf, binaryTransportType)
				if err != nil {
					err = fmt.Errorf("unable to write message attribute transport type (binary) to buffer. %s", err)
					return
				}
				err = writeNext(buf, attr.value.BinaryValue)
				if err != nil {
					err = fmt.Errorf("unable to write message attribute binary value to buffer. %s", err)
					return
				}
			} else {
				err = fmt.Errorf("unexpected message attribute data type %s", *attr.value.DataType)
				return
			}
		}
	}

	serialized = buf.Bytes()

	return
}

func writeNext(buf *bytes.Buffer, data any) error {
	var err error

	switch v := data.(type) {
	case *string:
		err = binary.Write(buf, binary.BigEndian, int32(len(*v)))
		if err != nil {
			return err
		}
		_, err = buf.WriteString(*v)
		if err != nil {
			return err
		}
	case []byte:
		err = binary.Write(buf, binary.BigEndian, int32(len(v)))
		if err != nil {
			return err
		}
		_, err = buf.Write(v)
		if err != nil {
			return err
		}
	case string:
		err = binary.Write(buf, binary.BigEndian, int32(len(v)))
		if err != nil {
			return err
		}
		_, err = buf.WriteString(v)
		if err != nil {
			return err
		}
	case byte:
		err = buf.WriteByte(v)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown type to serialize")
	}

	return nil
}

/*
|length|body|length|attribute name|length|attribute datatype|attribute transport type|length|attribute value|
|4Bytes|	|4Bytes|			  |4Bytes|					|1Byte					 |4Bytes|				|
|---once----|-----------------------------------------zero or more------------------------------------------|
*/
func DeserializeHeftySqsMsg(in []byte) (*HeftySqsMsg, error) {
	reader := bytes.NewReader(in)

	var data []byte
	var ok bool

	// read body
	var body string
	if data, ok = readNext(reader); ok {
		body = string(data)
	} else {
		return nil, fmt.Errorf("unable to read body during deserialization")
	}

	// create message attributes
	var msgAttr map[string]sqsTypes.MessageAttributeValue
	if reader.Len() > 0 {
		msgAttr = make(map[string]sqsTypes.MessageAttributeValue)
	}

	for reader.Len() > 0 {
		// read attribute name
		var attrName string
		if data, ok = readNext(reader); ok {
			attrName = string(data)
		} else {
			return nil, fmt.Errorf("unable to read attribute name during deserialization")
		}

		// read attribute data type
		var attrDataType string
		if data, ok = readNext(reader); ok {
			attrDataType = string(data)
		} else {
			return nil, fmt.Errorf("unable to read attribute name during deserialization")
		}

		// read attribute transport type
		attrTransportType, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("unable to read attribute transport type during deserialization. %v", err)
		}

		// read attribute value
		if data, ok = readNext(reader); !ok {
			return nil, fmt.Errorf("unable to read attribute value during deserialization")
		}

		// construct message attribute
		if attrTransportType == stringTransportType {
			strValue := string(data)
			msgAttr[attrName] = sqsTypes.MessageAttributeValue{
				DataType:    &attrDataType,
				StringValue: &strValue,
			}
		} else if attrTransportType == binaryTransportType {
			msgAttr[attrName] = sqsTypes.MessageAttributeValue{
				DataType:    &attrDataType,
				BinaryValue: data,
			}
		}
	}

	return NewHeftySqsMessage(&body, msgAttr)
}

func readNext(reader *bytes.Reader) ([]byte, bool) {
	length := int32(lengthSize)
	err := binary.Read(reader, binary.BigEndian, &length)
	if err != nil {
		return nil, false
	}

	data := make([]byte, length)
	read, err := reader.Read(data)
	if err != nil {
		return nil, false
	}

	return data, read == int(length)
}

func msgSize(msg *HeftySqsMsg) (int, error) {
	var size int
	size += len(*msg.Body)

	if msg.MessageAttributes != nil {
		for k, v := range msg.MessageAttributes {
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
	}

	return size, nil
}
