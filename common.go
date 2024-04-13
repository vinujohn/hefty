package hefty

const (
	MaxAwsMessageLengthBytes   = 262_144    // 256KB; used for both SQS and SNS
	MaxHeftyMessageLengthBytes = 33_554_432 // 32MB
)
