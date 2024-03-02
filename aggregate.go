package streams

import "context"

// State is a very simple key-value store.
type State[K comparable, V any] interface {
	Get(k K) (V, error)
	Put(k K, v V) error
}

type MapState[K comparable, V any] struct {
	m map[K]V
}

func (m *MapState[K, V]) Get(k K) (V, error) {
	v := m.m[k]
	return v, nil
}

func (m *MapState[K, V]) Put(k K, v V) error {
	m.m[k] = v
	return nil
}

func NewMapState[K comparable, V any]() State[K, V] {
	return &MapState[K, V]{
		m: make(map[K]V),
	}
}

type Aggregator[K comparable, VIn, VOut any] interface {
	Aggregate(context.Context, Record[K, VIn], func(o Record[K, VOut]) error) error
}

type Aggregation[K comparable, In any, Out any] struct {
	agg func(Record[K, In], Out) Out

	// TODO: Replace this with an interface!
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
	return NewAggregation[K, In, uint64](state, func(r Record[K, In], u uint64) uint64 {
		return u + 1
	})
}
