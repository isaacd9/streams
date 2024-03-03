package streams

import (
	"context"
	"io"
	"time"

	"golang.org/x/sync/errgroup"
)

type TableJoinReader[K comparable, V, VJoin, VOut any] struct {
	Reader Reader[K, V]
	Table  TableReader[K, VJoin]
	Joiner func(Record[K, V], VJoin) VOut
}

func (j *TableJoinReader[K, V, VJoin, VOut]) Read(ctx context.Context) (Record[K, VOut], error) {
	msg, err := j.Reader.Read(ctx)
	if err != nil {
		return Record[K, VOut]{}, err
	}

	join, err := j.Table.get(msg.Key)
	if err != nil {
		return Record[K, VOut]{}, err
	}

	out := j.Joiner(msg, join)

	return Record[K, VOut]{
		Key:   msg.Key,
		Value: out,
		Time:  msg.Time,
	}, nil
}

type JoinWindows struct {
	Before time.Duration
	After  time.Duration
}

type StreamJoinReader[K comparable, V, VJoin, VOut any] struct {
	Left       Reader[K, V]
	Right      Reader[K, VJoin]
	LeftState  WindowState[K, V]
	RightState WindowState[K, VJoin]
	Joiner     func(Record[K, V], Record[K, VJoin]) VOut
	Cfg        JoinWindows

	batch []Record[K, VOut]
}

func min(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func max(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func readWindow[K comparable, V any](
	ctx context.Context,
	stream Reader[K, V],
	state WindowState[K, V],
	minTime, maxTime time.Time,
	cfg JoinWindows,
) error {
	for {
		msg, err := stream.Read(ctx)
		// log.Printf("read msg: %v", msg)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		// Skip records that are too old and catch up to the
		// current min
		if msg.Time.Before(minTime) {
			continue
		}

		state.Put(
			// Each record creates a new window
			WindowKey[K]{
				K:     msg.Key,
				Start: msg.Time,
				End:   msg.Time.Add(cfg.After),
			},
			msg.Value,
		)

		// If we've read all records within the window, we can
		// stop
		if msg.Time.After(maxTime) {
			return nil
		}
	}
}

func (j *StreamJoinReader[K, V, VJoin, VOut]) prepareBatch(ctx context.Context) error {
	left, err := j.Left.Read(ctx)
	if err != nil {
		return err
	}
	right, err := j.Left.Read(ctx)
	if err != nil {
		return err
	}

	minTime := min(left.Time.Add(-j.Cfg.Before), right.Time.Add(-j.Cfg.Before))
	maxTime := max(left.Time.Add(j.Cfg.After), right.Time.Add(j.Cfg.After))

	var g errgroup.Group

	// Read all records from the left and right streams that fall within the
	// window
	g.Go(func() error {
		return readWindow(
			ctx,
			j.Left,
			j.LeftState,
			minTime,
			maxTime,
			j.Cfg,
		)
	})

	g.Go(func() error {
		return readWindow(
			ctx,
			j.Right,
			j.RightState,
			minTime,
			maxTime,
			j.Cfg,
		)
	})

	if err := g.Wait(); err != nil {
		return err
	}

	j.LeftState.Every(minTime, maxTime, func(key WindowKey[K], value V) error {
		joinStart := key.Start.Add(-j.Cfg.Before)
		joinEnd := key.End.Add(j.Cfg.After)

		j.RightState.Each(key.K, joinStart, joinEnd, func(joinKey WindowKey[K], joinValue VJoin) error {
			out := j.Joiner(
				Record[K, V]{Key: key.K, Value: value},
				Record[K, VJoin]{Key: joinKey.K, Value: joinValue},
			)

			j.batch = append(j.batch, Record[K, VOut]{
				Key:   key.K,
				Value: out,
				Time:  key.Start,
			})
			return nil
		})
		return nil
	})

	// Join
	// out := j.Joiner(left, right)
	return nil
}

func (j *StreamJoinReader[K, V, VJoin, VOut]) Read(ctx context.Context) (Record[K, VOut], error) {
	if len(j.batch) == 0 {
		if err := j.prepareBatch(ctx); err != nil {
			return Record[K, VOut]{}, err
		}
	}

	// If we have records in the batch, return the first one
	msg := j.batch[0]
	j.batch = j.batch[1:]
	return msg, nil
}
