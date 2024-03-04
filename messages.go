package hefty

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type referenceMsg struct {
	S3Region          string `json:"s3_region"`
	S3Bucket          string `json:"s3_bucket"`
	S3Key             string `json:"s3_key"`
	SqsMd5HashBody    string `json:"sqs_md5_hash_body"`
	SqsMd5HashMsgAttr string `json:"sqs_md5_hash_msg_attr"`
}

type largeSqsMsg struct {
	Body              *string
	MessageAttributes map[string]sqsTypes.MessageAttributeValue
}

const (
	lengthSize               = 4
	transportTypeSize        = 1
	stringTransportType byte = 1
	binaryTransportType byte = 2
)

/*
|length|body|length|attribute name|length|attribute datatype|attribute transport type|length|attribute value|
|4Bytes|	|4Bytes|			  |4Bytes|					|1Byte					 |4Bytes|				|
|---once----|-----------------------------------------zero or more------------------------------------------|
*/
func (msg *largeSqsMsg) Serialize(msgSize int) (serialized []byte, bodyHash string, attributesHash string, err error) {
	b := make([]byte, 0, msgSize+lengthSize+(len(msg.MessageAttributes)*(3*lengthSize+transportTypeSize)))
	buf := bytes.NewBuffer(b)

	// write body
	err = writeNext(buf, msg.Body)
	if err != nil {
		err = fmt.Errorf("unable to write message body to buffer. %s", err)
		return
	}

	// calculate hash for body
	hash := md5.Sum(buf.Bytes()[lengthSize:])
	bodyHash = hex.EncodeToString(hash[:])

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

		hash := md5.Sum(buf.Bytes()[len(*msg.Body)+lengthSize:])
		attributesHash = hex.EncodeToString(hash[:])
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
func (msg *largeSqsMsg) Deserialize(in []byte) error {
	reader := bytes.NewReader(in)

	var data []byte
	var ok bool

	// read body
	if data, ok = readNext(reader); ok {
		body := string(data)
		msg.Body = &body
	} else {
		return fmt.Errorf("unable to read body during deserialization")
	}

	for reader.Len() > 0 {
		if msg.MessageAttributes == nil {
			msg.MessageAttributes = make(map[string]sqsTypes.MessageAttributeValue)
		}

		// read attribute name
		var attrName string
		if data, ok = readNext(reader); ok {
			attrName = string(data)
		} else {
			return fmt.Errorf("unable to read attribute name during deserialization")
		}

		// read attribute data type
		var attrDataType string
		if data, ok = readNext(reader); ok {
			attrDataType = string(data)
		} else {
			return fmt.Errorf("unable to read attribute name during deserialization")
		}

		// read attribute transport type
		attrTransportType, err := reader.ReadByte()
		if err != nil {
			return fmt.Errorf("unable to read attribute transport type during deserialization. %v", err)
		}

		// read attribute value
		if data, ok = readNext(reader); !ok {
			return fmt.Errorf("unable to read attribute value during deserialization")
		}

		// construct message attribute
		if attrTransportType == stringTransportType {
			attrValue := string(data)
			msg.MessageAttributes[attrName] = sqsTypes.MessageAttributeValue{
				DataType:    &attrDataType,
				StringValue: &attrValue,
			}
		} else if attrTransportType == binaryTransportType {
			msg.MessageAttributes[attrName] = sqsTypes.MessageAttributeValue{
				DataType:    &attrDataType,
				BinaryValue: data,
			}
		}
	}

	return nil
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
