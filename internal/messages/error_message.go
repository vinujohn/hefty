package messages

import (
	"encoding/json"
	"fmt"
	"strings"
)

const errorMsgIdentifierKey = "b58c8bae78504da3a2e32cceeb77d342"

var jsonErrorMsgPrefix string

func init() {
	jsonErrorMsgPrefix = fmt.Sprintf("{\n\t\"identifier\": \"%s\",", errorMsgIdentifierKey)
}

type ErrorMsg struct {
	Identifier   string        `json:"identifier"` // used to identify an error message from other types of messages
	Error        string        `json:"error"`
	ReferenceMsg *ReferenceMsg `json:"reference_msg"`
}

func NewErrorMsg(err error, refMsg *ReferenceMsg) *ErrorMsg {
	return &ErrorMsg{
		Identifier:   errorMsgIdentifierKey,
		Error:        err.Error(),
		ReferenceMsg: refMsg,
	}
}

func (msg *ErrorMsg) ToJson() ([]byte, error) {
	return json.MarshalIndent(msg, "", "\t")
}

func ToErrorMsg(msg string) (*ErrorMsg, error) {
	var errMsg ErrorMsg
	err := json.Unmarshal([]byte(msg), &errMsg)
	return &errMsg, err
}

func IsErrorMsg(msg string) bool {
	return strings.HasPrefix(msg, jsonErrorMsgPrefix)
}
