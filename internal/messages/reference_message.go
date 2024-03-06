package messages

// ReferenceMsg is what is sent to AWS SQS in place of hefty message stored in AWS S3.
type ReferenceMsg struct {
	S3Region          string `json:"s3_region"`
	S3Bucket          string `json:"s3_bucket"`
	S3Key             string `json:"s3_key"`
	SqsMd5HashBody    string `json:"sqs_md5_hash_body"`
	SqsMd5HashMsgAttr string `json:"sqs_md5_hash_msg_attr"`
}
