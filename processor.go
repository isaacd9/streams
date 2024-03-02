package streams

import "context"

type Processor[KIn, VIn, KOut, VOut any] interface {
	ProcessMessage(ctx context.Context, msg Record[KIn, VIn], next func(o Record[KOut, VOut]) error) error
}

type ProcessorFunc[KIn, VIn, KOut, VOut any] func(ctx context.Context, msg Record[KIn, VIn], next func(o Record[KOut, VOut]) error) error

func (p ProcessorFunc[KIn, VIn, KOut, VOut]) ProcessMessage(ctx context.Context, msg Record[KIn, VIn], next func(o Record[KOut, VOut]) error) error {
	return p(ctx, msg, next)
}

func Filter[K, V any](fn func(Record[K, V]) bool) Processor[K, V, K, V] {
	return ProcessorFunc[K, V, K, V](func(ctx context.Context, msg Record[K, V], next func(o Record[K, V]) error) error {
		if fn(msg) {
			return next(msg)
		}
		return nil
	})
}

func Map[KIn, VIn, KOut, VOut any](fn func(Record[KIn, VIn]) Record[KOut, VOut]) Processor[KIn, VIn, KOut, VOut] {
	return ProcessorFunc[KIn, VIn, KOut, VOut](func(ctx context.Context, msg Record[KIn, VIn], next func(o Record[KOut, VOut]) error) error {
		return next(fn(msg))
	})
}

func FlatMap[KIn, VIn, KOut, VOut any](fn func(Record[KIn, VIn]) []Record[KOut, VOut]) Processor[KIn, VIn, KOut, VOut] {
	return ProcessorFunc[KIn, VIn, KOut, VOut](func(ctx context.Context, msg Record[KIn, VIn], next func(o Record[KOut, VOut]) error) error {
		outs := fn(msg)
		for _, out := range outs {
			if err := next(out); err != nil {
				return err
			}
		}
		return nil
	})
}
