package messages

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrorMessageSerialization(t *testing.T) {
	expected := `{
	"identifier": "%s",
	"error": "testErrorVal",
	"reference_msg": {
		"identifier": "%s",
		"s3_region": "testS3RegionVal",
		"s3_bucket": "testS3BucketVal",
		"s3_key": "testS3KeyVal",
		"md5_digest_msg_body": "testMd5BodyVal",
		"md5_digest_msg_attr": "testMd5AttrVal"
	}
}`
	expected = fmt.Sprintf(expected, errorMsgIdentifierKey, referenceMsgIdentifierKey)
	testRefMsg := NewReferenceMsg("testS3RegionVal", "testS3BucketVal", "testS3KeyVal", "testMd5BodyVal", "testMd5AttrVal")
	testErrMsg := NewErrorMsg(errors.New("testErrorVal"), testRefMsg)

	// test ToJson
	j, err := testErrMsg.ToJson()
	assert.Nil(t, err, "error should be nil when calling ToJson")
	assert.Equal(t, expected, string(j))

	// test IsErrorMessage
	assert.True(t, IsErrorMsg(string(j)))
	assert.False(t, IsErrorMsg("foo"))

	// test ToErrorMsg
	errMsg2, err := ToErrorMsg(string(j))
	assert.Nil(t, err, "error should be nil when calling ToErrorMsg")
	assert.Equal(t, testErrMsg, errMsg2)
}
