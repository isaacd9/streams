package streams

import (
	"fmt"
	"strconv"
)

func StringUnmarshaler() Unmarshaler[string, string] {
	return &stringUnmarshaler{}
}

type stringUnmarshaler struct{}

func (s *stringUnmarshaler) Unmarshal(msg Message, r *Record[string, string]) error {
	r.Key = string(msg.Key)
	r.Val = string(msg.Val)
	return nil
}

func StringMarshaler() Marshaler[string, string] {
	return &stringMarshaler{}
}

type stringMarshaler struct{}

func (s *stringMarshaler) Marshal(r Record[string, string]) (Message, error) {
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

func IntMarshaler() Marshaler[string, uint64] {
	return &intMarshaler{}
}

type intMarshaler struct{}

func (s *intMarshaler) Marshal(i Record[string, uint64]) (Message, error) {
	return Message{
		Key: []byte(i.Key),
		Val: []byte(strconv.Itoa(int(i.Val))),
	}, nil
}

func AnyMarshaler[K any, V any]() Marshaler[K, V] {
	return &anyMarshaler[K, V]{}
}

type anyMarshaler[K any, V any] struct{}

func (s *anyMarshaler[K, V]) Marshal(i Record[K, V]) (Message, error) {
	return Message{
		Key: []byte(fmt.Sprintf("%+v", i.Key)),
		Val: []byte(fmt.Sprintf("%+v", i.Val)),
	}, nil
}
