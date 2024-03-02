package streams

import (
	"context"
	"errors"
	"fmt"
	"io"
)

type topologyNode interface {
	setNext(n topologyNode)
	do(ctx context.Context, a any) (e error)
}

type runnableNode interface {
	topologyNode
	run(ctx context.Context) error
}

type sourceNode[K any, V any] struct {
	source Source
	d      Unmarshaler[K, V]
	child  topologyNode
}

func (s *sourceNode[K, V]) setNext(n topologyNode) {
	s.child = n
}

func (s *sourceNode[K, V]) do(ctx context.Context, a any) error {
	for {
		msg, err := s.source.Read(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		var t Record[K, V]
		if err := s.d.Unmarshal(msg, &t); err != nil {
			return err
		}
		s.child.do(ctx, t)
	}
}

type sinkNode[K any, V any] struct {
	sink Sink
	s    Marshaler[K, V]
}

func (s *sinkNode[K, V]) setNext(n topologyNode) {
	panic("sink node cannot have a next node")
}

func (s *sinkNode[K, V]) do(ctx context.Context, a any) error {
	t, ok := a.(Record[K, V])
	if !ok {
		return fmt.Errorf("expected type %T, got %T", t, a)
	}

	msg, err := s.s.Marshal(t)
	if err != nil {
		return err
	}

	return s.sink.Write(ctx, msg)
}

type processorNode[KIn any, VIn any, KOut any, VOut any] struct {
	p   Processor[KIn, VIn, KOut, VOut]
	out topologyNode
}

func (s *processorNode[KIn, VIn, KOut, VOut]) setNext(n topologyNode) {
	s.out = n
}

func (s *processorNode[KIn, VIn, KOut, VOut]) do(ctx context.Context, a any) error {
	in, ok := a.(Record[KIn, VIn])
	if !ok {
		return fmt.Errorf("expected type %T, got %T", in, a)
	}

	return s.p.ProcessMessage(ctx, in, func(o Record[KOut, VOut]) error {
		return s.out.do(ctx, o)
	})
}

type pipedNode[K any, V any] struct {
	sink   topologyNode
	source topologyNode
}

func (s *pipedNode[K, V]) setNext(n topologyNode) {
	s.source.setNext(n)
}

func (s *pipedNode[K, V]) run(ctx context.Context) error {
	return s.source.do(ctx, nil)
}

func (s *pipedNode[K, V]) do(ctx context.Context, a any) error {
	in, ok := a.(Record[K, V])
	if !ok {
		return fmt.Errorf("expected type %T, got %T", in, a)
	}

	return s.sink.do(ctx, a)

}

type aggregatorNode[K comparable, VIn any, VOut any] struct {
	agg Aggregator[K, VIn, VOut]
	out topologyNode
}

func (s *aggregatorNode[K, VIn, VOut]) setNext(n topologyNode) {
	s.out = n
}

func (s *aggregatorNode[K, VIn, VOut]) do(ctx context.Context, a any) error {
	in, ok := a.(Record[K, VIn])
	if !ok {
		return fmt.Errorf("expected type %T, got %T", in, a)
	}

	return s.agg.Aggregate(ctx, in, func(o Record[K, VOut]) error {
		return s.out.do(ctx, o)
	})
}

type windowNode[K comparable, V any] struct {
	w   Windower[K, V]
	out topologyNode
}

func (s *windowNode[K, V]) setNext(n topologyNode) {
	s.out = n
}

func (s *windowNode[K, V]) do(ctx context.Context, a any) error {
	in, ok := a.(Record[K, V])
	if !ok {
		return fmt.Errorf("expected type %T, got %T", in, a)
	}

	return s.w.Window(ctx, in, func(o Record[WindowKey[K], V]) error {
		return s.out.do(ctx, o)
	})
}
