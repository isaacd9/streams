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

type Windower[K comparable, V any] interface {
	Window(context.Context, Record[K, V], func(Record[WindowKey[K], V]) error) error
}

type WindowFunc[K comparable, V any] func(context.Context, Record[K, V], func(Record[WindowKey[K], V]) error) error

func (w WindowFunc[K, V]) Window(ctx context.Context, r Record[K, V], next func(Record[WindowKey[K], V]) error) error {
	return w(ctx, r, next)
}

type TimeWindows struct {
	Size    time.Duration
	Advance time.Duration
}

func NewRealTimeWindow[K comparable, V any](cfg TimeWindows) Windower[K, V] {
	return WindowFunc[K, V](func(ctx context.Context, r Record[K, V], next func(Record[WindowKey[K], V]) error) error {
		windowStart := time.Now().Round(cfg.Size)
		windowEnd := windowStart.Add(cfg.Advance)

		key := WindowKey[K]{
			Start: windowStart,
			End:   windowEnd,
			K:     r.Key,
		}

		return next(Record[WindowKey[K], V]{
			Key:   key,
			Value: r.Value,
		})
	})
}
