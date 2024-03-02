package streams

import (
	"context"
)

type AggregatorReader[K comparable, In any, Out any] struct {
	r     Reader[K, In]
	agg   func(Record[K, In], Out) Out
	state State[K, Out]
}

func (a *AggregatorReader[K, In, Out]) Read(ctx context.Context) (Record[K, Out], error) {
	msg, err := a.r.Read(ctx)
	if err != nil {
		return Record[K, Out]{}, err
	}

	cur, err := a.state.Get(msg.Key)
	if err != nil {
		return Record[K, Out]{}, err
	}

	new := a.agg(msg, cur)
	a.state.Put(msg.Key, new)

	return Record[K, Out]{
		Key:   msg.Key,
		Value: new,
	}, nil
}

func Aggregate[K comparable, In any, Out any](reader Reader[K, In], state State[K, Out], agg func(Record[K, In], Out) Out) Reader[K, Out] {
	return &AggregatorReader[K, In, Out]{
		r:     reader,
		agg:   agg,
		state: state,
	}
}

func Count[K comparable, In any](r Reader[K, In], state State[K, uint64]) Reader[K, uint64] {
	return Aggregate(r, state, func(r Record[K, In], u uint64) uint64 {
		return u + 1
	})
}

func NewReducer[K comparable, V any](r Reader[K, V], state State[K, V], reducer func(k K, a, b V) V) Reader[K, V] {
	return Aggregate(r, state, func(r Record[K, V], v V) V {
		return reducer(r.Key, v, r.Value)
	})
}
