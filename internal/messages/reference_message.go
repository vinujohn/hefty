package messages

// ReferenceMsg is what is sent to AWS SQS or AWS SNS in place of hefty message stored in AWS S3.
type ReferenceMsg struct {
	S3Region         string `json:"s3_region"`
	S3Bucket         string `json:"s3_bucket"`
	S3Key            string `json:"s3_key"`
	Md5DigestMsgBody string `json:"md5_digest_msg_body"`
	Md5DigestMsgAttr string `json:"md5_digest_msg_attr"`
}
