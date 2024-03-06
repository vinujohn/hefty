package messages

import (
	"crypto/md5"
	"encoding/hex"
)

func Md5Digest(buf []byte) string {
	hash := md5.Sum(buf)
	return hex.EncodeToString(hash[:])
}
