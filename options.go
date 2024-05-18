package hefty

type options struct {
	alwaysSendToS3 bool
}

type Option func(opts *options) error

// If selected, the message payload will always be sent to AWS S3 regardless of its size
func AlwaysSendToS3() Option {
	return func(opts *options) error {
		opts.alwaysSendToS3 = true
		return nil
	}
}
