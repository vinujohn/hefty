package hefty

const (
	MaxSqsSnsMessageLengthBytes           = 262_144                // 256KB
	MaxHeftyMessageLengthBytes            = 33_554_432             // 32MB
	HeftyClientVersionMessageAttributeKey = "hefty-client-version" // used by hefty when sending a reference message to AWS SQS/SNS
	HeftyClientVersion                    = "v0.2"
)
