package streams

import (
	"context"
)

type Source interface {
	Read(ctx context.Context) ([]byte, error)
}

type Sink interface {
	Write(ctx context.Context, msg []byte) error
}

type Deserializer[T any] interface {
	Read(ctx context.Context, msg []byte) (T, error)
}

type Serializer[T any] interface {
	Write(ctx context.Context, t T) ([]byte, error)
}

type Executor struct {
}

func NewStream[T any](e *Executor, from Source, d Deserializer[T]) *Stream[T] {
	return &Stream[T]{
		source: from,
		d:      d,
		e:      e,
	}

}

type Stream[T any] struct {
	source Source
	d      Deserializer[T]
	sink   Sink
	s      Serializer[T]

	e *Executor
}

func (s *Stream[T]) To(ctx context.Context, serializer Serializer[T], sink Sink) {
	s.s = serializer
	s.sink = sink
}

func Through[In any, Out any](s *Stream[In], p Processor[In, Out]) *Stream[Out] {
	return &Stream[Out]{
		e: s.e,
	}
}

type Processor[In any, Out any] interface {
	ProcessMessage(ctx context.Context, msg In, next func(o Out) error) error
}

type FilteredProcessor[T any] struct {
	fn func(T) bool
}

func NewFilteredProcessor[T any](fn func(T) bool) Processor[T, T] {
	return &FilteredProcessor[T]{
		fn: fn,
	}
}

func (m *FilteredProcessor[T]) ProcessMessage(ctx context.Context, msg T, next func(T) error) error {
	if m.fn(msg) {
		return next(msg)
	}
	return nil
}

type MappedReeader[In any, Out any] struct {
	fn func(In) Out
}

func NewMappedProcessor[In any, Out any](fn func(In) Out) Processor[In, Out] {
	return &MappedReeader[In, Out]{
		fn: fn,
	}
}

func (m *MappedReeader[In, Out]) ProcessMessage(ctx context.Context, msg In, next func(o Out) error) error {
	o := m.fn(msg)
	return next(o)
}

type FlatMapProcessor[In any, Out any] struct {
	fn func(In) []Out
}

func NewFlatMapProcessor[In any, Out any](fn func(In) []Out) Processor[In, Out] {
	return &FlatMapProcessor[In, Out]{
		fn: fn,
	}
}

func (m *FlatMapProcessor[In, Out]) ProcessMessage(ctx context.Context, msg In, next func(msg Out) error) error {
	outs := m.fn(msg)
	for _, out := range outs {
		if err := next(out); err != nil {
			return err
		}
	}
	return nil
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
