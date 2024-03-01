package streams

import (
	"context"
	"strconv"
)

func StringDeserializer[T string]() Deserializer[string] {
	return &stringDeserializer[string]{}
}

type stringDeserializer[T string] struct{}

func (s *stringDeserializer[T]) Read(ctx context.Context, msg []byte) (string, error) {
	return string(msg), nil
}

func StringSerializer[T string]() Serializer[string] {
	return &stringSerializer[string]{}
}

type stringSerializer[T string] struct{}

func (s *stringSerializer[T]) Write(ctx context.Context, i string) ([]byte, error) {
	return []byte(i), nil
}

func IntSerializer[T int]() Serializer[int] {
	return &intSerializer[int]{}
}

type intSerializer[T int] struct{}

func (s *intSerializer[T]) Write(ctx context.Context, i int) ([]byte, error) {
	return []byte(strconv.Itoa(i)), nil
}
