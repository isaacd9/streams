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

type sourceNode[T any] struct {
	source Source
	d      Deserializer[T]
	child  topologyNode
}

func (s *sourceNode[T]) setNext(n topologyNode) {
	s.child = n
}

func (s *sourceNode[T]) do(ctx context.Context, a any) error {
	for {
		msg, err := s.source.Read(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		t, err := s.d.Read(ctx, msg)
		if err != nil {
			return err
		}
		s.child.do(ctx, t)
	}
}

type sinkNode[T any] struct {
	sink Sink
	s    Serializer[T]
}

func (s *sinkNode[T]) setNext(n topologyNode) {
	panic("sink node cannot have a next node")
}

func (s *sinkNode[T]) do(ctx context.Context, a any) error {
	t, ok := a.(T)
	if !ok {
		return fmt.Errorf("expected type %T, got %T", t, a)
	}

	msg, err := s.s.Write(ctx, t)
	if err != nil {
		return err
	}

	return s.sink.Write(ctx, msg)
}

type processorNode[In any, Out any] struct {
	p   Processor[In, Out]
	out topologyNode
}

func (s *processorNode[In, Out]) setNext(n topologyNode) {
	s.out = n
}

func (s *processorNode[In, Out]) do(ctx context.Context, a any) error {
	in, ok := a.(In)
	if !ok {
		return fmt.Errorf("expected type %T, got %T", in, a)
	}

	return s.p.ProcessMessage(ctx, in, func(o Out) error {
		return s.out.do(ctx, o)
	})
}

type pipedNode[T any] struct {
	sink   topologyNode
	source topologyNode
}

func (s *pipedNode[T]) setNext(n topologyNode) {
	s.source = n
}

func (s *pipedNode[T]) do(ctx context.Context, a any) error {
	if err := s.sink.do(ctx, a); err != nil {
		return err
	}

	if err := s.source.do(ctx, a); err != nil {
		return err
	}

	return nil
}

/*
type groupedNode[In any, Key comparable] struct {
	fn  func(In) Key
	out topologyNode
}

func (s *groupedNode[In, Key]) setNext(n topologyNode) {
	s.out = n
}

func (s *groupedNode[In, Key]) do(ctx context.Context, a any) error {
	key := g.fn(msg)
	return s.do(ctx, key, msg)
}
*/
