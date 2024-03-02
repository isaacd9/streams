package streams

import (
	"context"
	"time"
)

type WindowKey[K comparable] struct {
	Start time.Time
	End   time.Time
	K     K
}

type windowReader[K comparable, V any] struct {
	windower func(ctx context.Context, r Record[K, V]) (Record[WindowKey[K], V], error)
	r        Reader[K, V]
}

func (w *windowReader[K, V]) Read(ctx context.Context) (Record[WindowKey[K], V], error) {
	r, err := w.r.Read(ctx)
	if err != nil {
		return Record[WindowKey[K], V]{}, err
	}

	return w.windower(ctx, r)
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
