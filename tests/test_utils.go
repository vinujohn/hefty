package tests

import (
	"math/rand"
	"strings"
)

func createMessageText(numBytes int) string {
	builder := strings.Builder{}

	// printable characters
	min := 33
	max := 126

	for i := 0; i < numBytes; i++ {
		randNum := rand.Intn(max-min+1) + min
		builder.WriteByte(byte(randNum))
	}

	return builder.String()
}
