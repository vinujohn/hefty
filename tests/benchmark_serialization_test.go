package tests

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/vinujohn/hefty"
	"github.com/vinujohn/hefty/internal/messages"
)

func getMsg() *messages.LargeSqsMsg {
	msgText256KB := createMessageText(hefty.MaxSqsMessageLengthBytes)
	msgTextBody := createMessageText(hefty.MaxHeftyMessageLengthBytes -
		(hefty.MaxSqsMessageLengthBytes * 12) - (len("String") * 12) - (len("test1") * 9) - (len("test10") * 3))

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

	msg, _ := messages.NewLargeSqsMessage(&msgTextBody, msgAttributes)
	return msg
}

func gobSerialize(msg *messages.LargeSqsMsg) ([]byte, error) {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(msg)

	return network.Bytes(), err
}

func gobDeserialize(b []byte) (*messages.LargeSqsMsg, error) {
	var network bytes.Buffer = *bytes.NewBuffer(b)
	dec := gob.NewDecoder(&network)
	var msg messages.LargeSqsMsg
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
		_, err = messages.DeserializeLargeSqsMsg(serial)
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
		var msg messages.LargeSqsMsg
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
