package streams

import (
	"context"
	"time"
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
	Key  K
	Val  V
	time time.Time
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

func Window[K comparable, V any](s *Stream[K, V], w Windower[K, V]) *Stream[WindowKey[K], V] {
	node := &windowNode[K, V]{
		w: w,
	}

	ks := &Stream[WindowKey[K], V]{
		e:    s.e,
		node: node,
	}

	s.e.last.setNext(node)
	s.e.last = node

	return ks
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
