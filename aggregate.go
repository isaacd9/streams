package streams

import "context"

type Aggregator[K comparable, VIn, VOut any] interface {
	Aggregate(context.Context, Record[K, VIn], func(Record[K, VOut]) error) error
}

type Aggregation[K comparable, In any, Out any] struct {
	agg   func(Record[K, In], Out) Out
	state State[K, Out]
}

func NewAggregation[K comparable, VIn any, VOut any](state State[K, VOut], agg func(Record[K, VIn], VOut) VOut) Aggregator[K, VIn, VOut] {
	return &Aggregation[K, VIn, VOut]{
		agg:   agg,
		state: state,
	}
}

func (a *Aggregation[K, VIn, VOut]) Aggregate(ctx context.Context, r Record[K, VIn], next func(o Record[K, VOut]) error) error {
	cur, err := a.state.Get(r.Key)
	if err != nil {
		return err
	}

	new := a.agg(r, cur)
	a.state.Put(r.Key, new)

	return next(Record[K, VOut]{
		Key: r.Key,
		Val: new,
	})
}

func NewCount[K comparable, In any](state State[K, uint64]) Aggregator[K, In, uint64] {
	return NewAggregation(state, func(r Record[K, In], u uint64) uint64 {
		return u + 1
	})
}

func NewReducer[K comparable, V any](state State[K, V], reducer func(k K, a, b V)) Aggregator[K, V, V] {
	return NewAggregation(state, func(r Record[K, V], v V) V {
		reducer(r.Key, v, r.Val)
		return v
	})
}
