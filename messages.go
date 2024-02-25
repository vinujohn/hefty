package hefty

type referenceMsg struct {
	S3Region  string `json:"s3_region"`
	S3Bucket  string `json:"s3_bucket"`
	S3Key     string `json:"s3_key"`
	S3Md5Hash string `json:"s3_md5_hash"`
}
