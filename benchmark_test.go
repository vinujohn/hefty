package hefty

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"math/rand"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func getMsg() *largeSqsMsg {
	msgText256KB := createMessageText(MaxSqsMessageLengthBytes)
	msgTextBody := createMessageText(MaxHeftyMessageLengthBytes -
		(MaxSqsMessageLengthBytes * 12) - (len("String") * 12) - (len("test1") * 9) - (len("test10") * 3))

	msgAttributes := map[string]types.MessageAttributeValue{
		"test1": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test2": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test3": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test4": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test5": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test6": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test7": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test8": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test9": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test10": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test11": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
		"test12": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msgText256KB),
		},
	}

	msg, _ := newLargeSqsMessage(&msgTextBody, msgAttributes)
	return msg
}

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

func gobSerialize(msg *largeSqsMsg) ([]byte, error) {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(msg)

	return network.Bytes(), err
}

func gobDeserialize(b []byte) (*largeSqsMsg, error) {
	var network bytes.Buffer = *bytes.NewBuffer(b)
	dec := gob.NewDecoder(&network)
	var msg largeSqsMsg
	err := dec.Decode(&msg)
	return &msg, err
}

func BenchmarkSerialize(b *testing.B) {
	msg := getMsg()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, _, err := msg.Serialize()
		if err != nil {
			b.Fatalf("error encountered during benchmarking. %v", err)
		}
	}
}

func BenchmarkJsonSerialize(b *testing.B) {
	msg := getMsg()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(msg)
		if err != nil {
			b.Fatalf("error encountered during benchmarking. %v", err)
		}

	}
}

func BenchmarkGobSerialize(b *testing.B) {
	msg := getMsg()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := gobSerialize(msg)
		if err != nil {
			b.Fatalf("error encountered during benchmarking. %v", err)
		}
	}
}

func BenchmarkDeserialize(b *testing.B) {
	msg := getMsg()
	serial, _, _, err := msg.Serialize()
	if err != nil {
		b.Fatalf("error encountered during benchmarking. %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = deserializeLargeSqsMsg(serial)
		if err != nil {
			b.Fatalf("error encountered during benchmarking. %v", err)
		}
	}
}

func BenchmarkJsonDeserialize(b *testing.B) {
	msg := getMsg()
	j, err := json.Marshal(msg)
	if err != nil {
		b.Fatalf("error encountered during benchmarking. %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var msg largeSqsMsg
		err = json.Unmarshal(j, &msg)
		if err != nil {
			b.Fatalf("error encountered during benchmarking. %v", err)
		}
	}
}

func BenchmarkGobDeserialize(b *testing.B) {
	msg := getMsg()
	buf, err := gobSerialize(msg)
	if err != nil {
		b.Fatalf("error encountered during benchmarking. %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := gobDeserialize(buf)
		if err != nil {
			b.Fatalf("error encountered during benchmarking. %v", err)
		}
	}
}
