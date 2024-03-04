package streams

import (
	"context"
	"fmt"
)

type unmarshalReader[K, V any] struct {
	LaggedReader[[]byte, []byte]
	unmarshalKey   func([]byte) K
	unmarshalValue func([]byte) V
}

func (r *unmarshalReader[K, V]) Read(ctx context.Context) (Record[K, V], CommitFunc, error) {
	msg, done, err := r.LaggedReader.Read(ctx)
	if err != nil {
		return Record[K, V]{}, done, err
	}

	return Record[K, V]{
		Key:   r.unmarshalKey(msg.Key),
		Value: r.unmarshalValue(msg.Value),
		Time:  msg.Time,
	}, done, nil
}

func UnmarshalString(r LaggedReader[[]byte, []byte]) LaggedReader[string, string] {
	return &unmarshalReader[string, string]{
		LaggedReader: r,
		unmarshalKey: func(b []byte) string {
			return string(b)

		},
		unmarshalValue: func(b []byte) string {
			return string(b)
		},
	}
}

func MarshalAny[K, V any](r Reader[K, V]) Reader[[]byte, []byte] {
	return Map(r, func(r KeyValue[K, V]) KeyValue[[]byte, []byte] {
		return KeyValue[[]byte, []byte]{
			Key:   []byte(fmt.Sprintf("%v", r.Key)),
			Value: []byte(fmt.Sprintf("%v", r.Value)),
		}
	})
}
