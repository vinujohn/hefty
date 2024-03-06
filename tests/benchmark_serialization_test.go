package tests

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"testing"

	"github.com/vinujohn/hefty/internal/messages"
)

func gobSerialize(msg *messages.HeftySqsMsg) ([]byte, error) {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(msg)

	return network.Bytes(), err
}

func gobDeserialize(b []byte) (*messages.HeftySqsMsg, error) {
	var network bytes.Buffer = *bytes.NewBuffer(b)
	dec := gob.NewDecoder(&network)
	var msg messages.HeftySqsMsg
	err := dec.Decode(&msg)
	return &msg, err
}

func BenchmarkSerialize(b *testing.B) {
	msg := getMaxHeftyMessage()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, _, err := msg.Serialize()
		if err != nil {
			b.Fatalf("error encountered during benchmarking. %v", err)
		}
	}
}

func BenchmarkJsonSerialize(b *testing.B) {
	msg := getMaxHeftyMessage()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(msg)
		if err != nil {
			b.Fatalf("error encountered during benchmarking. %v", err)
		}

	}
}

func BenchmarkGobSerialize(b *testing.B) {
	msg := getMaxHeftyMessage()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := gobSerialize(msg)
		if err != nil {
			b.Fatalf("error encountered during benchmarking. %v", err)
		}
	}
}

func BenchmarkDeserialize(b *testing.B) {
	msg := getMaxHeftyMessage()
	serial, _, _, err := msg.Serialize()
	if err != nil {
		b.Fatalf("error encountered during benchmarking. %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = messages.DeserializeHeftySqsMsg(serial)
		if err != nil {
			b.Fatalf("error encountered during benchmarking. %v", err)
		}
	}
}

func BenchmarkJsonDeserialize(b *testing.B) {
	msg := getMaxHeftyMessage()
	j, err := json.Marshal(msg)
	if err != nil {
		b.Fatalf("error encountered during benchmarking. %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var msg messages.HeftySqsMsg
		err = json.Unmarshal(j, &msg)
		if err != nil {
			b.Fatalf("error encountered during benchmarking. %v", err)
		}
	}
}

func BenchmarkGobDeserialize(b *testing.B) {
	msg := getMaxHeftyMessage()
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
