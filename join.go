package streams

import "context"

type TableReader[K comparable, V any] interface {
	Reader[K, V]
	get(key K) (V, error)
}

type joinReader[K comparable, V, VJoin, VOut any] struct {
	r      Reader[K, V]
	t      TableReader[K, VJoin]
	joiner func(Record[K, V], VJoin) VOut
}

func (j *joinReader[K, V, VJoin, VOut]) Read(ctx context.Context) (Record[K, VOut], error) {
	msg, err := j.r.Read(ctx)
	if err != nil {
		return Record[K, VOut]{}, nil
	}

	join, err := j.t.get(msg.Key)
	if err != nil {
		return Record[K, VOut]{}, nil
	}

	out := j.joiner(msg, join)

	return Record[K, VOut]{
		Key:   msg.Key,
		Value: out,
		Time:  msg.Time,
	}, nil
}

func Join[K comparable, V, VJoin, VOut any](r Reader[K, V], t TableReader[K, VJoin], joiner func(Record[K, V], VJoin) VOut) Reader[K, VOut] {
	return &joinReader[K, V, VJoin, VOut]{
		r:      r,
		t:      t,
		joiner: joiner,
	}
}
