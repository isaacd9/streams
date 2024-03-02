package streams

import (
	"context"
	"strconv"
)

func StringUnmarshaler() Unmarshaler[string, string] {
	return &stringUnmarshaler{}
}

type stringUnmarshaler struct{}

func (s *stringUnmarshaler) Read(ctx context.Context, msg Message) (Record[string, string], error) {
	return Record[string, string]{
		Key: string(msg.Key),
		Val: string(msg.Val),
	}, nil
}

func StringMarshaler() Marshaler[string, string] {
	return &stringMarshaler{}
}

type stringMarshaler struct{}

func (s *stringMarshaler) Write(ctx context.Context, r Record[string, string]) (Message, error) {
	return Message{
		Key: []byte(r.Key),
		Val: []byte(r.Val),
	}, nil
}

type stringSerde struct {
	Marshaler[string, string]
	Unmarshaler[string, string]
}

func StringMarshalerUnmarshaler() MarshalerUnmarshaler[string, string] {
	return &stringSerde{
		StringMarshaler(),
		StringUnmarshaler(),
	}
}

func IntMarshaler() Marshaler[string, int] {
	return &intMarshaler{}
}

type intMarshaler struct{}

func (s *intMarshaler) Write(ctx context.Context, i Record[string, int]) (Message, error) {
	return Message{
		Key: []byte(i.Key),
		Val: []byte(strconv.Itoa(i.Val)),
	}, nil
}
