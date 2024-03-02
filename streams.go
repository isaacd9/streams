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

type Unmarshaler[K, V any] interface {
	Unmarshal(Message, *Record[K, V]) error
}

type Marshaler[K, V any] interface {
	Marshal(Record[K, V]) (Message, error)
}

type MarshalerUnmarshaler[K, V any] interface {
	Unmarshaler[K, V]
	Marshaler[K, V]
}

type Stream[K, V any] struct {
	e        *Executor
	node     topologyNode
	sinkNode *sinkNode[K, V]
}

func NewStream[K, V any](e *Executor, from Source, d Unmarshaler[K, V]) *Stream[K, V] {
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

func To[K, V any](s *Stream[K, V], Marshaler Marshaler[K, V], sink Sink) {
	node := &sinkNode[K, V]{
		sink: sink,
		s:    Marshaler,
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

func Through[K, V any](s *Stream[K, V], p Pipe, serde MarshalerUnmarshaler[K, V]) *Stream[K, V] {
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

	s.e.last.setNext(node)
	s.e.last = node

	s.e.runnables = append(s.e.runnables, node)

	return &Stream[K, V]{
		e:    s.e,
		node: node,
	}
}

type KeyedStream[K comparable, V any] struct {
	e    *Executor
	node topologyNode
}

func ToStream[K comparable, V any](s *KeyedStream[K, V]) *Stream[K, V] {
	return &Stream[K, V]{
		e:    s.e,
		node: s.node,
	}
}

func Aggregate[K comparable, VIn any, VOut any](s *Stream[K, VIn], agg Aggregator[K, VIn, VOut]) *KeyedStream[K, VOut] {
	node := &aggregatorNode[K, VIn, VOut]{
		agg: agg,
	}

	ks := &KeyedStream[K, VOut]{
		e:    s.e,
		node: node,
	}

	s.e.last.setNext(node)
	s.e.last = node

	return ks
}

/*
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
