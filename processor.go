package streams

import "context"

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

type MappedProcessor[In any, Out any] struct {
	fn func(In) Out
}

func NewMappedProcessor[In any, Out any](fn func(In) Out) Processor[In, Out] {
	return &MappedProcessor[In, Out]{
		fn: fn,
	}
}

func (m *MappedProcessor[In, Out]) ProcessMessage(ctx context.Context, msg In, next func(o Out) error) error {
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
