package streams

import (
	"context"
)

type Reader[T any] interface {
	ReadMessage(ctx context.Context) (T, error)
}

type FilteredReader[T any] struct {
	inner Reader[T]
	fn    func(T) bool
}

func NewFilteredReader[T any](r Reader[T], fn func(T) bool) Reader[T] {
	return &FilteredReader[T]{
		inner: r,
		fn:    fn,
	}
}

func (m *FilteredReader[T]) ReadMessage(ctx context.Context) (T, error) {
	for {
		msg, err := m.inner.ReadMessage(ctx)
		if err != nil {
			var t T
			return t, err
		}
		if m.fn(msg) {
			return msg, nil
		}
	}
}

type MappedReeader[In any, Out any] struct {
	inner Reader[In]
	fn    func(In) Out
}

func NewMappedReader[In any, Out any](r Reader[In], fn func(In) Out) Reader[Out] {
	return &MappedReeader[In, Out]{
		inner: r,
		fn:    fn,
	}
}

func (m *MappedReeader[In, Out]) ReadMessage(ctx context.Context) (Out, error) {
	msg, err := m.inner.ReadMessage(ctx)
	if err != nil {
		var o Out
		return o, err
	}

	return m.fn(msg), err
}

type FlatMapReader[In any, Out any] struct {
	inner Reader[In]
	fn    func(In) []Out

	batch []Out
}

func NewFlatMapReader[In any, Out any](r Reader[In], fn func(In) []Out) Reader[Out] {
	return &FlatMapReader[In, Out]{
		inner: r,
		fn:    fn,
	}
}

func (m *FlatMapReader[In, Out]) ReadMessage(ctx context.Context) (Out, error) {
	if len(m.batch) == 0 {
		msg, err := m.inner.ReadMessage(ctx)
		if err != nil {
			var o Out
			return o, err
		}

		m.batch = m.fn(msg)
	}
	r := m.batch[0]
	m.batch = m.batch[1:]
	return r, nil
}

type GroupBy[In any, Key comparable] struct {
	inner Reader[In]
	fn    func(In) Key
}

func NewGroupBy[In any, Key comparable](r Reader[In], fn func(In) Key) *GroupBy[In, Key] {
	return &GroupBy[In, Key]{
		inner: r,
		fn:    fn,
	}
}

type KeyedReader[K any, T any] interface {
	ReadMessage(ctx context.Context) (K, T, error)
}

type Aggregation[K comparable, In any, Out any] struct {
	g    *GroupBy[In, K]
	init Out
	agg  func(K, In, Out) Out

	// TODO: Replace this with an interface!
	state map[K]Out
}

func NewAggregation[K comparable, In any, Out any](g *GroupBy[In, K], init Out, agg func(K, In, Out) Out) KeyedReader[K, Out] {
	return &Aggregation[K, In, Out]{
		g:     g,
		init:  init,
		agg:   agg,
		state: make(map[K]Out),
	}
}

func (a *Aggregation[K, In, Out]) ReadMessage(ctx context.Context) (K, Out, error) {
	msg, err := a.g.inner.ReadMessage(ctx)
	if err != nil {
		var (
			o Out
			k K
		)
		return k, o, err
	}

	msgKey := a.g.fn(msg)
	if _, ok := a.state[msgKey]; !ok {
		a.state[msgKey] = a.init
	}
	a.state[msgKey] = a.agg(msgKey, msg, a.state[msgKey])
	return msgKey, a.state[msgKey], nil
}

type Reducer[K comparable, V any] struct {
	g    *GroupBy[V, K]
	init V
	agg  func(V, V) V

	// TODO: Replace this with an interface!
	state map[K]V
}

func NewReducer[K comparable, V any](g *GroupBy[V, K], init V, reducer func(a, b V) V) KeyedReader[K, V] {
	var v V
	return &Reducer[K, V]{
		g:     g,
		init:  v,
		agg:   reducer,
		state: make(map[K]V),
	}
}

func (r *Reducer[K, V]) ReadMessage(ctx context.Context) (K, V, error) {
	msg, err := r.g.inner.ReadMessage(ctx)
	if err != nil {
		var (
			k K
			v V
		)
		return k, v, err
	}

	msgKey := r.g.fn(msg)
	if _, ok := r.state[msgKey]; !ok {
		r.state[msgKey] = r.init
	}
	r.state[msgKey] = r.agg(msg, r.state[msgKey])
	return msgKey, r.state[msgKey], nil
}

func NewCount[K comparable, In any](g *GroupBy[In, K]) KeyedReader[K, uint64] {
	return &Aggregation[K, In, uint64]{
		g:    g,
		init: 0,
		agg: func(k K, i In, u uint64) uint64 {
			return u + 1
		},
		state: make(map[K]uint64),
	}
}
