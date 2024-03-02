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

func FilterKeys[K, V any](fn func(K) bool) Processor[K, V, K, V] {
	return ProcessorFunc[K, V, K, V](func(ctx context.Context, msg Record[K, V], next func(o Record[K, V]) error) error {
		if fn(msg.Key) {
			return next(msg)
		}
		return nil
	})
}

func FilterValues[K, V any](fn func(V) bool) Processor[K, V, K, V] {
	return ProcessorFunc[K, V, K, V](func(ctx context.Context, msg Record[K, V], next func(o Record[K, V]) error) error {
		if fn(msg.Val) {
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

func MapValues[K, VIn, VOut any](fn func(VIn) VOut) Processor[K, VIn, K, VOut] {
	return ProcessorFunc[K, VIn, K, VOut](func(ctx context.Context, msg Record[K, VIn], next func(o Record[K, VOut]) error) error {
		return next(Record[K, VOut]{
			Key: msg.Key,
			Val: fn(msg.Val),
		})
	})
}

func MapKeys[KIn, KOut, V any](fn func(KIn) KOut) Processor[KIn, V, KOut, V] {
	return ProcessorFunc[KIn, V, KOut, V](func(ctx context.Context, msg Record[KIn, V], next func(o Record[KOut, V]) error) error {
		return next(Record[KOut, V]{
			Key: fn(msg.Key),
			Val: msg.Val,
		})
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

func FlatMapValues[K, VIn, VOut any](fn func(VIn) []VOut) Processor[K, VIn, K, VOut] {
	return ProcessorFunc[K, VIn, K, VOut](func(ctx context.Context, msg Record[K, VIn], next func(o Record[K, VOut]) error) error {
		outs := fn(msg.Val)
		for _, out := range outs {
			err := next(Record[K, VOut]{
				Key: msg.Key,
				Val: out,
			})

			if err != nil {
				return err
			}
		}
		return nil
	})
}

func FlatMapKeys[KIn, KOut, V any](fn func(KIn) []KOut) Processor[KIn, V, KOut, V] {
	return ProcessorFunc[KIn, V, KOut, V](func(ctx context.Context, msg Record[KIn, V], next func(o Record[KOut, V]) error) error {
		outs := fn(msg.Key)
		for _, out := range outs {
			err := next(Record[KOut, V]{
				Key: out,
				Val: msg.Val,
			})

			if err != nil {
				return err
			}
		}
		return nil
	})
}
