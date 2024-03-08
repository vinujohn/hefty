package messages

import (
	snsTypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
)

// HeftySnsMsg is an AWS SNS message that is over 256KB and needs to be stored in AWS S3
type HeftySnsMsg struct {
	Body              *string
	MessageAttributes map[string]snsTypes.MessageAttributeValue
	Size              int // size of both the body and message attributes
}

func NewHeftySnsMessage(body *string, msgAttributes map[string]snsTypes.MessageAttributeValue) (*HeftySnsMsg, error) {
	msg := &HeftySnsMsg{
		Body:              body,
		MessageAttributes: msgAttributes,
	}

	// TODO: implement msgsize for sns messages
	// var err error
	// msg.Size, err = msgSize(msg)
	// if err != nil {
	// 	return nil, fmt.Errorf("unable to calculate message size. %v", err)
	// }

	return msg, nil
}

func (msg *HeftySnsMsg) Serialize() (serialized []byte, bodyOffset int, msgAttrOffset int, err error) {
	return nil, 0, 0, nil
}
