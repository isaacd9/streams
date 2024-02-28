package streams

import (
	"context"
	"time"
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

func NewReducer[K comparable, V any](g *GroupBy[V, K], init V, reducer func(a, b V) V) KeyedReader[K, V] {
	var v V
	return &Aggregation[K, V, V]{
		g:    g,
		init: v,
		agg: func(k K, v1, v2 V) V {
			return reducer(v1, v2)
		},
		state: make(map[K]V),
	}
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

type TimeWindowCfg struct {
	Size    time.Duration
	Advance time.Duration
}

type WindowKey[K comparable] struct {
	Start time.Time
	End   time.Time
	K     K
}

type TimeWindow[K comparable, V any] struct {
	g       *GroupBy[V, K]
	windows TimeWindowCfg
}

func NewTimeWindow[K comparable, V any](g *GroupBy[V, K], windows TimeWindowCfg) *TimeWindow[K, V] {
	return &TimeWindow[K, V]{
		g:       g,
		windows: windows,
	}
}

type WindowedAggregation[K comparable, In any, Out any] struct {
	w    *TimeWindow[K, In]
	init Out
	agg  func(K, In, Out) Out

	// TODO: Replace this with an interface!
	state map[WindowKey[K]]Out
}

func NewWindowedAggregation[K comparable, In any, Out any](w *TimeWindow[K, In], init Out, agg func(K, In, Out) Out) KeyedReader[WindowKey[K], Out] {
	return &WindowedAggregation[K, In, Out]{
		w:     w,
		init:  init,
		agg:   agg,
		state: make(map[WindowKey[K]]Out),
	}
}

func (a *WindowedAggregation[K, In, Out]) ReadMessage(ctx context.Context) (WindowKey[K], Out, error) {
	msg, err := a.w.g.inner.ReadMessage(ctx)
	if err != nil {
		var (
			o Out
		)
		return WindowKey[K]{}, o, err
	}

	windowStart := time.Now().Round(a.w.windows.Size)
	windowEnd := windowStart.Add(a.w.windows.Advance)
	msgKey := a.w.g.fn(msg)

	key := WindowKey[K]{
		Start: windowStart,
		End:   windowEnd,
		K:     msgKey,
	}

	if _, ok := a.state[key]; !ok {
		a.state[key] = a.init
	}
	a.state[key] = a.agg(msgKey, msg, a.state[key])
	return key, a.state[key], nil
}

func NewWindowedCount[K comparable, In any](w *TimeWindow[K, In]) KeyedReader[WindowKey[K], uint64] {
	return &WindowedAggregation[K, In, uint64]{
		w:    w,
		init: 0,
		agg: func(k K, i In, u uint64) uint64 {
			return u + 1
		},
		state: make(map[WindowKey[K]]uint64),
	}
}

func NewWindowedReducer[K comparable, V any](w *TimeWindow[K, V], reducer func(a, b V) V) KeyedReader[WindowKey[K], V] {
	var v V
	return &WindowedAggregation[K, V, V]{
		w:    w,
		init: v,
		agg: func(k K, v1, v2 V) V {
			return reducer(v1, v2)
		},
		state: make(map[WindowKey[K]]V),
	}
}
