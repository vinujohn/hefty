package hefty

const (
	MaxAwsMessageLengthBytes              = 262_144                // 256KB; used for both SQS and SNS
	MaxHeftyMessageLengthBytes            = 33_554_432             // 32MB
	HeftyClientVersionMessageAttributeKey = "hefty-client-version" // used by hefty when sending a reference message to AWS SQS/SNS
	HeftyClientVersion                    = "v0.2"
)
