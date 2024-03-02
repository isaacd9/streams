package streams

import (
	"context"
)

type Message struct {
	Key []byte
	Val []byte
}

type Source interface {
	Read(ctx context.Context) (Message, error)
}

type Sink interface {
	Write(ctx context.Context, msg Message) error
}

type Record[K, V any] struct {
	Key K
	Val V
}

type Pipe interface {
	Source
	Sink
}

type Deserializer[K, V any] interface {
	Read(ctx context.Context, msg Message) (Record[K, V], error)
}

type Serializer[K, V any] interface {
	Write(ctx context.Context, t Record[K, V]) (Message, error)
}

type SerDe[K, V any] interface {
	Deserializer[K, V]
	Serializer[K, V]
}

type Stream[K, V any] struct {
	e        *Executor
	node     topologyNode
	sinkNode *sinkNode[K, V]
}

func NewStream[K, V any](e *Executor, from Source, d Deserializer[K, V]) *Stream[K, V] {
	node := &sourceNode[K, V]{
		source: from,
		d:      d,
	}

	st := &Stream[K, V]{
		node: node,
		e:    e,
	}

	e.root = node
	e.last = node

	return st
}

func To[K, V any](s *Stream[K, V], serializer Serializer[K, V], sink Sink) {
	node := &sinkNode[K, V]{
		sink: sink,
		s:    serializer,
	}

	s.sinkNode = node

	s.e.last.setNext(node)
	s.e.last = node
}

func Process[KIn, VIn, KOut, VOut any](s *Stream[KIn, VIn], p Processor[KIn, VIn, KOut, VOut]) *Stream[KOut, VOut] {
	node := &processorNode[KIn, VIn, KOut, VOut]{
		p: p,
	}

	s.e.last.setNext(node)
	s.e.last = node

	return &Stream[KOut, VOut]{
		e:    s.e,
		node: node,
	}
}

func Through[K, V any](s *Stream[K, V], p Pipe, serde SerDe[K, V]) *Stream[K, V] {
	node := &pipedNode[K, V]{
		sink: &sinkNode[K, V]{
			sink: p,
			s:    serde,
		},
		source: &sourceNode[K, V]{
			source: p,
			d:      serde,
		},
	}

	// Todo: We should register the nodes that need to run concurrently and
	// run them in the executor
	go func() {
		node.run(context.Background())
	}()

	s.e.last.setNext(node)
	s.e.last = node

	return &Stream[K, V]{
		e:    s.e,
		node: node,
	}
}

/*
type GroupedStream[In any, Key comparable] struct {
	fn   func(In) Key
	node topologyNode
}

func GroupBy[In any, Key comparable](s *Stream[In], fn func(In) Key) *GroupedStream[In, Key] {
	node := &groupedNode[In, Key]{
		fn: fn,
	}

	s.e.last.setNext(node)
	s.e.last = node

	return &GroupedStream[In, Key]{
		fn:   fn,
		node: node,
	}
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
