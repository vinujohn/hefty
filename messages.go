package hefty

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"

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

/*
|length|body|length|attribute name|length|attribute datatype|attribute transport type|length|attribute value|
|4Bytes|	|4Bytes|			  |4Bytes|					|1Byte					 |4Bytes|				|
|---once----|-----------------------------------------zero or more------------------------------------------|
*/
func (msg *largeSqsMsg) Serialize(msgSize int) (serialized []byte, bodyHash string, attributesHash string) {
	//msgSize + 4 bytes for length of body + (number of attributes * 13 bytes for length and transport type)
	b := make([]byte, 0, msgSize+4+(len(msg.MessageAttributes)*13))
	buf := bytes.NewBuffer(b)
	binary.Write(buf, binary.BigEndian, int32(len(*msg.Body)))
	buf.WriteString(*msg.Body) //TODO: handle error

	// calculate hash for body
	hash := md5.Sum(buf.Bytes()[4:])
	bodyHash = hex.EncodeToString(hash[:])

	if msg.MessageAttributes != nil {
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
			binary.Write(buf, binary.BigEndian, int32(len(attr.key)))
			buf.Write([]byte(attr.key))
			if attr.value.DataType != nil {
				binary.Write(buf, binary.BigEndian, int32(len(*attr.value.DataType)))
				buf.Write([]byte(*attr.value.DataType))

				if attr.value.StringValue != nil {
					buf.Write([]byte{1})
					binary.Write(buf, binary.BigEndian, int32(len(*attr.value.StringValue)))
					buf.Write([]byte(*attr.value.StringValue))
				} else if len(attr.value.BinaryValue) > 0 {
					buf.Write([]byte{2})
					binary.Write(buf, binary.BigEndian, int32(len(attr.value.BinaryValue)))
					buf.Write(attr.value.BinaryValue)
				}
			}
		}

		hash := md5.Sum(buf.Bytes()[len(*msg.Body)+4:])
		attributesHash = hex.EncodeToString(hash[:])
	}

	serialized = buf.Bytes()

	return
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
		if attrTransportType == 1 {
			attrValue := string(data)
			msg.MessageAttributes[attrName] = sqsTypes.MessageAttributeValue{
				DataType:    &attrDataType,
				StringValue: &attrValue,
			}
		} else if attrTransportType == 2 {
			msg.MessageAttributes[attrName] = sqsTypes.MessageAttributeValue{
				DataType:    &attrDataType,
				BinaryValue: data,
			}
		}
	}

	return nil
}

func readNext(reader *bytes.Reader) ([]byte, bool) {
	length := int32(4)
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
