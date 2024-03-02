package streams

import (
	"context"
	"strconv"
)

func StringDeserializer() Deserializer[string, string] {
	return &stringDeserializer{}
}

type stringDeserializer struct{}

func (s *stringDeserializer) Read(ctx context.Context, msg Message) (Record[string, string], error) {
	return Record[string, string]{
		Key: string(msg.Key),
		Val: string(msg.Val),
	}, nil
}

func StringSerializer() Serializer[string, string] {
	return &stringSerializer{}
}

type stringSerializer struct{}

func (s *stringSerializer) Write(ctx context.Context, r Record[string, string]) (Message, error) {
	return Message{
		Key: []byte(r.Key),
		Val: []byte(r.Val),
	}, nil
}

func IntSerializer() Serializer[string, int] {
	return &intSerializer{}
}

type intSerializer struct{}

func (s *intSerializer) Write(ctx context.Context, i Record[string, int]) (Message, error) {
	return Message{
		Key: []byte(i.Key),
		Val: []byte(strconv.Itoa(i.Val)),
	}, nil
}
