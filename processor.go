package streams

import "context"

type filterReader[K, V any] struct {
	r  Reader[K, V]
	fn func(Record[K, V]) bool
}

func (f *filterReader[K, V]) Read(ctx context.Context) (Record[K, V], error) {
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
	return &filterReader[K, V]{
		r:  r,
		fn: fn,
	}
}

func FilterKeys[K, V any](r Reader[K, V], fn func(K) bool) Reader[K, V] {
	return &filterReader[K, V]{
		r: r,
		fn: func(msg Record[K, V]) bool {
			return fn(msg.Key)
		},
	}
}

func FilterValues[K, V any](r Reader[K, V], fn func(V) bool) Reader[K, V] {
	return &filterReader[K, V]{
		r: r,
		fn: func(msg Record[K, V]) bool {
			return fn(msg.Value)
		},
	}
}

type mapReader[KIn, VIn, KOut, VOut any] struct {
	r  Reader[KIn, VIn]
	fn func(Record[KIn, VIn]) Record[KOut, VOut]
}

func (m *mapReader[KIn, VIn, KOut, VOut]) Read(ctx context.Context) (Record[KOut, VOut], error) {
	msg, err := m.r.Read(ctx)
	if err != nil {
		return Record[KOut, VOut]{}, err
	}

	return m.fn(msg), nil
}

type KeyValue[K any, V any] struct {
	Key   K
	Value V
}

func Map[KIn, VIn, KOut, VOut any](r Reader[KIn, VIn], fn func(kv KeyValue[KIn, VIn]) KeyValue[KOut, VOut]) Reader[KOut, VOut] {
	return &mapReader[KIn, VIn, KOut, VOut]{
		r: r,
		fn: func(msg Record[KIn, VIn]) Record[KOut, VOut] {
			kv := fn(KeyValue[KIn, VIn]{msg.Key, msg.Value})
			return Record[KOut, VOut]{
				Key:   kv.Key,
				Value: kv.Value,
				Time:  msg.Time,
			}
		},
	}
}

func MapValues[K, VIn, VOut any](r Reader[K, VIn], fn func(VIn) VOut) Reader[K, VOut] {
	return &mapReader[K, VIn, K, VOut]{
		r: r,
		fn: func(msg Record[K, VIn]) Record[K, VOut] {
			return Record[K, VOut]{
				Key:   msg.Key,
				Value: fn(msg.Value),
				Time:  msg.Time,
			}
		},
	}
}

func MapKeys[KIn, KOut, V any](r Reader[KIn, V], fn func(KIn) KOut) Reader[KOut, V] {
	return &mapReader[KIn, V, KOut, V]{
		r: r,
		fn: func(msg Record[KIn, V]) Record[KOut, V] {
			return Record[KOut, V]{
				Key:   fn(msg.Key),
				Value: msg.Value,
				Time:  msg.Time,
			}
		},
	}
}

type flatMapReader[KIn, VIn, KOut, VOut any] struct {
	r     Reader[KIn, VIn]
	fn    func(Record[KIn, VIn]) []Record[KOut, VOut]
	batch []Record[KOut, VOut]
}

func (f *flatMapReader[KIn, VIn, KOut, VOut]) Read(ctx context.Context) (Record[KOut, VOut], error) {
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

func FlatMap[KIn, VIn, KOut, VOut any](r Reader[KIn, VIn], fn func(KeyValue[KIn, VIn]) []KeyValue[KOut, VOut]) Reader[KOut, VOut] {
	return &flatMapReader[KIn, VIn, KOut, VOut]{
		r: r,
		fn: func(r Record[KIn, VIn]) []Record[KOut, VOut] {
			kv := KeyValue[KIn, VIn]{r.Key, r.Value}
			var batch []Record[KOut, VOut]
			for _, kv := range fn(kv) {
				batch = append(batch, Record[KOut, VOut]{
					Key:   kv.Key,
					Value: kv.Value,
					Time:  r.Time,
				})
			}
			return batch
		},
	}
}

func FlatMapValues[K, VIn, VOut any](r Reader[K, VIn], fn func(VIn) []VOut) Reader[K, VOut] {
	return &flatMapReader[K, VIn, K, VOut]{
		r: r,
		fn: func(msg Record[K, VIn]) []Record[K, VOut] {
			outs := fn(msg.Value)
			batch := make([]Record[K, VOut], len(outs))
			for i, out := range outs {
				batch[i] = Record[K, VOut]{
					Key:   msg.Key,
					Value: out,
					Time:  msg.Time,
				}
			}
			return batch
		},
	}
}

func FlatMapKeys[KIn, KOut, V any](r Reader[KIn, V], fn func(KIn) []KOut) Reader[KOut, V] {
	return &flatMapReader[KIn, V, KOut, V]{
		r: r,
		fn: func(msg Record[KIn, V]) []Record[KOut, V] {
			outs := fn(msg.Key)
			batch := make([]Record[KOut, V], len(outs))
			for i, out := range outs {
				batch[i] = Record[KOut, V]{
					Key:   out,
					Value: msg.Value,
					Time:  msg.Time,
				}
			}
			return batch
		},
	}
}
