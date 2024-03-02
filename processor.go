package streams

import "context"

type FilterReader[K, V any] struct {
	r  Reader[K, V]
	fn func(Record[K, V]) bool
}

func (f *FilterReader[K, V]) Read(ctx context.Context) (Record[K, V], error) {
	for {
		msg, err := f.r.Read(ctx)
		if err != nil {
			return Record[K, V]{}, err
		}

		if f.fn(msg) {
			return msg, nil
		}
	}
}

func Filter[K, V any](r Reader[K, V], fn func(Record[K, V]) bool) Reader[K, V] {
	return &FilterReader[K, V]{
		r:  r,
		fn: fn,
	}
}

func FilterKeys[K, V any](r Reader[K, V], fn func(K) bool) Reader[K, V] {
	return &FilterReader[K, V]{
		r: r,
		fn: func(msg Record[K, V]) bool {
			return fn(msg.Key)
		},
	}
}

func FilterValues[K, V any](r Reader[K, V], fn func(V) bool) Reader[K, V] {
	return &FilterReader[K, V]{
		r: r,
		fn: func(msg Record[K, V]) bool {
			return fn(msg.Value)
		},
	}
}

type MapReader[KIn, VIn, KOut, VOut any] struct {
	r  Reader[KIn, VIn]
	fn func(Record[KIn, VIn]) Record[KOut, VOut]
}

func (m *MapReader[KIn, VIn, KOut, VOut]) Read(ctx context.Context) (Record[KOut, VOut], error) {
	msg, err := m.r.Read(ctx)
	if err != nil {
		return Record[KOut, VOut]{}, err
	}

	return m.fn(msg), nil
}

func Map[KIn, VIn, KOut, VOut any](r Reader[KIn, VIn], fn func(Record[KIn, VIn]) Record[KOut, VOut]) Reader[KOut, VOut] {
	return &MapReader[KIn, VIn, KOut, VOut]{
		r:  r,
		fn: fn,
	}
}

func MapValues[K, VIn, VOut any](r Reader[K, VIn], fn func(VIn) VOut) Reader[K, VOut] {
	return &MapReader[K, VIn, K, VOut]{
		r: r,
		fn: func(msg Record[K, VIn]) Record[K, VOut] {
			return Record[K, VOut]{
				Key:   msg.Key,
				Value: fn(msg.Value),
			}
		},
	}
}

func MapKeys[KIn, KOut, V any](r Reader[KIn, V], fn func(KIn) KOut) Reader[KOut, V] {
	return &MapReader[KIn, V, KOut, V]{
		r: r,
		fn: func(msg Record[KIn, V]) Record[KOut, V] {
			return Record[KOut, V]{
				Key:   fn(msg.Key),
				Value: msg.Value,
			}
		},
	}
}

type FlatMapReader[KIn, VIn, KOut, VOut any] struct {
	r     Reader[KIn, VIn]
	fn    func(Record[KIn, VIn]) []Record[KOut, VOut]
	batch []Record[KOut, VOut]
}

func (f *FlatMapReader[KIn, VIn, KOut, VOut]) Read(ctx context.Context) (Record[KOut, VOut], error) {
	if len(f.batch) == 0 {
		msg, err := f.r.Read(ctx)
		if err != nil {
			return Record[KOut, VOut]{}, err
		}
		f.batch = f.fn(msg)
	}

	out := f.batch[0]
	f.batch = f.batch[1:]
	return out, nil
}

func FlatMap[KIn, VIn, KOut, VOut any](r Reader[KIn, VIn], fn func(Record[KIn, VIn]) []Record[KOut, VOut]) Reader[KOut, VOut] {
	return &FlatMapReader[KIn, VIn, KOut, VOut]{
		r:  r,
		fn: fn,
	}
}

func FlatMapValues[K, VIn, VOut any](r Reader[K, VIn], fn func(VIn) []VOut) Reader[K, VOut] {
	return &FlatMapReader[K, VIn, K, VOut]{
		r: r,
		fn: func(msg Record[K, VIn]) []Record[K, VOut] {
			outs := fn(msg.Value)
			batch := make([]Record[K, VOut], len(outs))
			for i, out := range outs {
				batch[i] = Record[K, VOut]{
					Key:   msg.Key,
					Value: out,
				}
			}
			return batch
		},
	}
}

func FlatMapKeys[KIn, KOut, V any](r Reader[KIn, V], fn func(KIn) []KOut) Reader[KOut, V] {
	return &FlatMapReader[KIn, V, KOut, V]{
		r: r,
		fn: func(msg Record[KIn, V]) []Record[KOut, V] {
			outs := fn(msg.Key)
			batch := make([]Record[KOut, V], len(outs))
			for i, out := range outs {
				batch[i] = Record[KOut, V]{
					Key:   out,
					Value: msg.Value,
				}
			}
			return batch
		},
	}
}
