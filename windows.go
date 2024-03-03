package streams

import (
	"context"
	"time"
)

type WindowedReader[K comparable, V any] interface {
	Read(ctx context.Context) (Record[WindowKey[K], V], error)
}

type WindowKey[K comparable] struct {
	Start time.Time
	End   time.Time
	K     K
}

type windowReader[K comparable, V any] struct {
	windower func(ctx context.Context, r Record[K, V]) (Record[WindowKey[K], V], error)
	r        Reader[K, V]
}

func (w *windowReader[K, V]) Read(ctx context.Context) (Record[WindowKey[K], V], CommitFunc, error) {
	r, done, err := w.r.Read(ctx)
	if err != nil {
		return Record[WindowKey[K], V]{}, done, err
	}

	msg, err := w.windower(ctx, r)
	return msg, done, nil
}

type TimeWindows struct {
	Size    time.Duration
	Advance time.Duration
}

func window[K comparable, V any](r Record[K, V], t time.Time, cfg TimeWindows) Record[WindowKey[K], V] {
	windowStart := t.Truncate(cfg.Size)
	windowEnd := windowStart.Add(cfg.Advance)

	key := WindowKey[K]{
		Start: windowStart,
		End:   windowEnd,
		K:     r.Key,
	}

	return Record[WindowKey[K], V]{
		Key:   key,
		Value: r.Value,
		Time:  r.Time,
	}
}

func RealTimeWindow[K comparable, V any](reader Reader[K, V], cfg TimeWindows) Reader[WindowKey[K], V] {
	return &windowReader[K, V]{
		windower: func(ctx context.Context, r Record[K, V]) (Record[WindowKey[K], V], error) {
			return window[K, V](r, time.Now(), cfg), nil
		},
		r: reader,
	}
}

func RecordTimeWindow[K comparable, V any](reader Reader[K, V], cfg TimeWindows) Reader[WindowKey[K], V] {
	return &windowReader[K, V]{
		windower: func(ctx context.Context, r Record[K, V]) (Record[WindowKey[K], V], error) {
			return window[K, V](r, r.Time, cfg), nil
		},
		r: reader,
	}
}

type windowAggregatorReader[K comparable, In any, Out any] struct {
	r     Reader[WindowKey[K], In]
	agg   func(Record[WindowKey[K], In], Out) Out
	state WindowState[K, Out]
}

func (a *windowAggregatorReader[K, In, Out]) get(key WindowKey[K]) (Out, error) {
	return a.state.Get(key)
}

func (a *windowAggregatorReader[K, In, Out]) Read(ctx context.Context) (Record[WindowKey[K], Out], CommitFunc, error) {
	msg, done, err := a.r.Read(ctx)
	if err != nil {
		return Record[WindowKey[K], Out]{}, done, err
	}

	cur, err := a.state.Get(msg.Key)
	if err != nil {
		return Record[WindowKey[K], Out]{}, done, err
	}

	new := a.agg(msg, cur)
	a.state.Put(msg.Key, new)

	return Record[WindowKey[K], Out]{
		Key:   msg.Key,
		Value: new,
		Time:  msg.Time,
	}, done, nil
}

func WindowAggregate[K comparable, In any, Out any](reader Reader[WindowKey[K], In], state WindowState[K, Out], agg func(Record[WindowKey[K], In], Out) Out) TableReader[WindowKey[K], Out] {
	return &windowAggregatorReader[K, In, Out]{
		r:     reader,
		agg:   agg,
		state: state,
	}
}

func WindowCount[K comparable, In any](r Reader[WindowKey[K], In], state WindowState[K, uint64]) TableReader[WindowKey[K], uint64] {
	return WindowAggregate(r, state, func(r Record[WindowKey[K], In], u uint64) uint64 {
		return u + 1
	})
}

func WindowReduce[K comparable, V any](r Reader[WindowKey[K], V], state WindowState[K, V], reducer func(a, b V) V) TableReader[WindowKey[K], V] {
	return WindowAggregate(r, state, func(r Record[WindowKey[K], V], v V) V {
		return reducer(v, r.Value)
	})
}
