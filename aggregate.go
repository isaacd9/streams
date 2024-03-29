package streams

import (
	"context"
)

type TableReader[K comparable, V any] interface {
	Reader[K, V]
	Get(key K) (V, error)
}

type aggregatorReader[K comparable, In any, Out any] struct {
	r     Reader[K, In]
	agg   func(Record[K, In], Out) Out
	state State[K, Out]
}

func (a *aggregatorReader[K, In, Out]) Get(key K) (Out, error) {
	return a.state.Get(key)
}

func (a *aggregatorReader[K, In, Out]) Read(ctx context.Context) (Record[K, Out], CommitFunc, error) {
	msg, done, err := a.r.Read(ctx)
	if err != nil {
		return Record[K, Out]{}, done, err
	}

	cur, err := a.state.Get(msg.Key)
	if err != nil {
		return Record[K, Out]{}, done, err
	}

	new := a.agg(msg, cur)
	a.state.Put(msg.Key, new)

	return Record[K, Out]{
		Key:   msg.Key,
		Value: new,
		Time:  msg.Time,
	}, done, nil
}

func Aggregate[K comparable, In any, Out any](reader Reader[K, In], state State[K, Out], agg func(Record[K, In], Out) Out) TableReader[K, Out] {
	return &aggregatorReader[K, In, Out]{
		r:     reader,
		agg:   agg,
		state: state,
	}
}

func Count[K comparable, In any](r Reader[K, In], state State[K, uint64]) TableReader[K, uint64] {
	return Aggregate(r, state, func(r Record[K, In], u uint64) uint64 {
		return u + 1
	})
}

func Reduce[K comparable, V any](r Reader[K, V], state State[K, V], reducer func(a, b V) V) TableReader[K, V] {
	return Aggregate(r, state, func(r Record[K, V], v V) V {
		return reducer(v, r.Value)
	})
}
