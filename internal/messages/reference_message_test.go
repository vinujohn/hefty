package messages

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReferenceMessageSerialization(t *testing.T) {
	expected := `{
	"identifier": "%s",
	"s3_region": "testS3RegionVal",
	"s3_bucket": "testS3BucketVal",
	"s3_key": "testS3KeyVal",
	"md5_digest_msg_body": "testMd5BodyVal",
	"md5_digest_msg_attr": "testMd5AttrVal"
}`
	expected = fmt.Sprintf(expected, referenceMsgIdentifierKey)
	testRefMsg := NewReferenceMsg("testS3RegionVal", "testS3BucketVal", "testS3KeyVal", "testMd5BodyVal", "testMd5AttrVal")

	// test ToJson
	j, err := testRefMsg.ToJson()
	assert.Nil(t, err, "error should be nil when calling ToJson")
	assert.Equal(t, expected, string(j))

	// test IsReferenceMessage
	assert.True(t, IsReferenceMsg(string(j)))
	assert.False(t, IsReferenceMsg("foo"))

	// test ToReferenceMsg
	refMsg2, err := ToReferenceMsg(string(j))
	assert.Nil(t, err, "error should be nil when calling ToReferenceMsg")
	assert.Equal(t, testRefMsg, refMsg2)
}
