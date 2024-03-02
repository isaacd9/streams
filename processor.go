package streams

import "context"

type Processor[KIn, VIn, KOut, VOut any] interface {
	ProcessMessage(ctx context.Context, msg Record[KIn, VIn], next func(o Record[KOut, VOut]) error) error
}

type FilteredProcessor[K, V any] struct {
	fn func(Record[K, V]) bool
}

func NewFilteredProcessor[K, V any](fn func(Record[K, V]) bool) Processor[K, V, K, V] {
	return &FilteredProcessor[K, V]{
		fn: fn,
	}
}

func (m *FilteredProcessor[K, V]) ProcessMessage(ctx context.Context, msg Record[K, V], next func(Record[K, V]) error) error {
	if m.fn(msg) {
		return next(msg)
	}
	return nil
}

type MappedProcessor[KIn, VIn, KOut, VOut any] struct {
	fn func(Record[KIn, VIn]) Record[KOut, VOut]
}

func NewMappedProcessor[KIn, VIn, KOut, VOut any](fn func(Record[KIn, VIn]) Record[KOut, VOut]) Processor[KIn, VIn, KOut, VOut] {
	return &MappedProcessor[KIn, VIn, KOut, VOut]{
		fn: fn,
	}
}

func (m *MappedProcessor[KIn, VIn, KOut, VOut]) ProcessMessage(ctx context.Context, msg Record[KIn, VIn], next func(o Record[KOut, VOut]) error) error {
	return next(m.fn(msg))
}

type FlatMapProcessor[KIn, VIn, KOut, VOut any] struct {
	fn func(Record[KIn, VIn]) []Record[KOut, VOut]
}

func NewFlatMapProcessor[KIn, VIn, KOut, VOut any](fn func(Record[KIn, VIn]) []Record[KOut, VOut]) Processor[KIn, VIn, KOut, VOut] {
	return &FlatMapProcessor[KIn, VIn, KOut, VOut]{
		fn: fn,
	}
}

func (m *FlatMapProcessor[KIn, VIn, KOut, VOut]) ProcessMessage(ctx context.Context, msg Record[KIn, VIn], next func(msg Record[KOut, VOut]) error) error {
	outs := m.fn(msg)
	for _, out := range outs {
		if err := next(out); err != nil {
			return err
		}
	}
	return nil
}
