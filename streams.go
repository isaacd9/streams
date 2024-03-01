package streams

import (
	"context"
)

type Processor[T any] interface {
	ProcessMessage(ctx context.Context, next func(T) error) error
}

type FilteredProcessor[T any] struct {
	inner Processor[T]
	fn    func(T) bool
}

func NewFilteredProcessor[T any](r Processor[T], fn func(T) bool) Processor[T] {
	return &FilteredProcessor[T]{
		inner: r,
		fn:    fn,
	}
}

func (m *FilteredProcessor[T]) ProcessMessage(ctx context.Context, next func(T) error) error {
	return m.inner.ProcessMessage(ctx, func(msg T) error {
		if m.fn(msg) {
			return next(msg)
		}
		return nil
	})
}

type MappedReeader[In any, Out any] struct {
	inner Processor[In]
	fn    func(In) Out
}

func NewMappedProcessor[In any, Out any](r Processor[In], fn func(In) Out) Processor[Out] {
	return &MappedReeader[In, Out]{
		inner: r,
		fn:    fn,
	}
}

func (m *MappedReeader[In, Out]) ProcessMessage(ctx context.Context, next func(msg Out) error) error {
	return m.inner.ProcessMessage(ctx, func(msg In) error {
		o := m.fn(msg)
		next(o)
		return nil
	})
}

type FlatMapProcessor[In any, Out any] struct {
	inner Processor[In]
	fn    func(In) []Out
}

func NewFlatMapProcessor[In any, Out any](r Processor[In], fn func(In) []Out) Processor[Out] {
	return &FlatMapProcessor[In, Out]{
		inner: r,
		fn:    fn,
	}
}

func (m *FlatMapProcessor[In, Out]) ProcessMessage(ctx context.Context, next func(msg Out) error) error {
	return m.inner.ProcessMessage(ctx, func(msg In) error {
		outs := m.fn(msg)
		for _, out := range outs {
			if err := next(out); err != nil {
				return err
			}
		}
		return nil
	})
}

type KeyedProcessor[K comparable, T any] interface {
	ProcessMessage(ctx context.Context) (K, T, error)
}

/*
type GroupBy[In any, Key comparable] struct {
	inner Processor[In]
	fn    func(In) Key
}

func NewGroupBy[In any, Key comparable](r Processor[In], fn func(In) Key) *GroupBy[In, Key] {
	return &GroupBy[In, Key]{
		inner: r,
		fn:    fn,
	}
}

func (g *GroupBy[In, Key]) ProcessMessage(ctx context.Context) (Key, In, error) {
	msg, err := g.inner.ProcessMessage(ctx)
	if err != nil {
		var (
			i In
			k Key
		)
		return k, i, err
	}

	return g.fn(msg), msg, nil
}

type Aggregation[K comparable, In any, Out any] struct {
	g    *GroupBy[In, K]
	init Out
	agg  func(K, In, Out) Out

	// TODO: Replace this with an interface!
	state map[K]Out
}

func NewAggregation[K comparable, In any, Out any](g *GroupBy[In, K], init Out, agg func(K, In, Out) Out) KeyedProcessor[K, Out] {
	return &Aggregation[K, In, Out]{
		g:     g,
		init:  init,
		agg:   agg,
		state: make(map[K]Out),
	}
}

func (a *Aggregation[K, In, Out]) ProcessMessage(ctx context.Context) (K, Out, error) {
	key, msg, err := a.g.ProcessMessage(ctx)
	if err != nil {
		var (
			k K
			o Out
		)
		return k, o, err
	}

	if _, ok := a.state[key]; !ok {
		a.state[key] = a.init
	}
	a.state[key] = a.agg(key, msg, a.state[key])
	return key, a.state[key], nil
}

func NewReducer[K comparable, V any](g *GroupBy[V, K], init V, reducer func(a, b V) V) KeyedProcessor[K, V] {
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

func NewCount[K comparable, In any](g *GroupBy[In, K]) KeyedProcessor[K, uint64] {
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
	inner   KeyedProcessor[K, V]
	windows TimeWindowCfg
}

func (w *TimeWindow[K, V]) ProcessMessage(ctx context.Context) (WindowKey[K], V, error) {
	msgKey, msg, err := w.inner.ProcessMessage(ctx)
	if err != nil {
		var (
			v V
		)
		return WindowKey[K]{}, v, err
	}

	windowStart := time.Now().Round(w.windows.Size)
	windowEnd := windowStart.Add(w.windows.Advance)

	key := WindowKey[K]{
		Start: windowStart,
		End:   windowEnd,
		K:     msgKey,
	}

	return key, msg, nil
}

func NewTimeWindow[K comparable, V any](inner KeyedProcessor[K, V], windows TimeWindowCfg) *TimeWindow[K, V] {
	return &TimeWindow[K, V]{
		inner:   inner,
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

func NewWindowedAggregation[K comparable, In any, Out any](w *TimeWindow[K, In], init Out, agg func(K, In, Out) Out) KeyedProcessor[WindowKey[K], Out] {
	return &WindowedAggregation[K, In, Out]{
		w:     w,
		init:  init,
		agg:   agg,
		state: make(map[WindowKey[K]]Out),
	}
}

func (a *WindowedAggregation[K, In, Out]) ProcessMessage(ctx context.Context) (WindowKey[K], Out, error) {
	key, msg, err := a.w.ProcessMessage(ctx)
	if err != nil {
		var o Out
		return WindowKey[K]{}, o, err
	}

	if _, ok := a.state[key]; !ok {
		a.state[key] = a.init
	}
	a.state[key] = a.agg(key.K, msg, a.state[key])
	return key, a.state[key], nil
}

func NewWindowedCount[K comparable, In any](w *TimeWindow[K, In]) KeyedProcessor[WindowKey[K], uint64] {
	return &WindowedAggregation[K, In, uint64]{
		w:    w,
		init: 0,
		agg: func(k K, i In, u uint64) uint64 {
			return u + 1
		},
		state: make(map[WindowKey[K]]uint64),
	}
}

func NewWindowedReducer[K comparable, V any](w *TimeWindow[K, V], reducer func(a, b V) V) KeyedProcessor[WindowKey[K], V] {
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

*/
