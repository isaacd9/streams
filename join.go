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

func (j *TableJoinReader[K, V, VJoin, VOut]) Read(ctx context.Context) (Record[K, VOut], CommitFunc, error) {
	msg, done, err := j.Reader.Read(ctx)
	if err != nil {
		return Record[K, VOut]{}, done, err
	}

	join, err := j.Table.Get(msg.Key)
	if err != nil {
		return Record[K, VOut]{}, done, err
	}

	out := j.Joiner(msg, join)

	return Record[K, VOut]{
		Key:   msg.Key,
		Value: out,
		Time:  msg.Time,
	}, done, nil
}

type JoinWindows struct {
	Before time.Duration
	After  time.Duration
}

// This is buggy and probably doesn't work
type StreamJoinReader[K comparable, V, VJoin, VOut any] struct {
	Left       Reader[K, V]
	Right      Reader[K, VJoin]
	LeftState  WindowState[K, V]
	RightState WindowState[K, VJoin]
	Joiner     func(Record[K, V], Record[K, VJoin]) VOut
	Cfg        JoinWindows

	batchNo        int
	batch          []Record[K, VOut]
	batchRemaining map[int]int
	batchCommits   map[int]CommitFunc
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
) (CommitFunc, error) {
	var dones []CommitFunc

	commit := func() error {
		for _, done := range dones {
			if err := done(); err != nil {
				return err
			}
		}
		return nil
	}

	for {
		msg, done, err := stream.Read(ctx)
		if err != nil {
			if err == io.EOF {
				return commit, nil
			}
			return commit, err
		}

		dones = append(dones, done)

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
			},
			msg.Value,
		)

		// If we've read all records within the window, we can
		// stop
		if msg.Time.After(maxTime) {
			return commit, nil
		}
	}
}

func (j *StreamJoinReader[K, V, VJoin, VOut]) prepareBatch(ctx context.Context) (CommitFunc, error) {
	var dones []CommitFunc

	commit := func() error {
		for _, done := range dones {
			if err := done(); err != nil {
				return err

			}
		}
		return nil
	}

	left, done, err := j.Left.Read(ctx)
	if err != nil {
		return commit, err
	}
	dones = append(dones, done)
	j.LeftState.Put(
		WindowKey[K]{
			K:     left.Key,
			Start: left.Time,
		},
		left.Value,
	)

	right, done, err := j.Right.Read(ctx)
	if err != nil {
		return commit, err
	}
	j.RightState.Put(
		WindowKey[K]{
			K:     right.Key,
			Start: right.Time,
		},
		right.Value,
	)

	dones = append(dones, done)

	minTime := min(left.Time.Add(-j.Cfg.Before), right.Time.Add(-j.Cfg.Before))
	maxTime := max(left.Time.Add(j.Cfg.After), right.Time.Add(j.Cfg.After))

	var g errgroup.Group

	// Read all records from the left and right streams that fall within the
	// window
	g.Go(func() error {
		done, err := readWindow(
			ctx,
			j.Left,
			j.LeftState,
			minTime,
			maxTime,
		)

		if err != nil {
			return err
		}
		dones = append(dones, done)
		return nil
	})

	g.Go(func() error {
		done, err := readWindow(
			ctx,
			j.Right,
			j.RightState,
			minTime,
			maxTime,
		)

		if err != nil {
			return err
		}
		dones = append(dones, done)
		return nil
	})

	if err := g.Wait(); err != nil {
		return commit, err
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

	return commit, nil
}

func (j *StreamJoinReader[K, V, VJoin, VOut]) Read(ctx context.Context) (Record[K, VOut], CommitFunc, error) {
	if len(j.batch) == 0 {
		done, err := j.prepareBatch(ctx)
		if err != nil {
			return Record[K, VOut]{}, done, err
		}

		if j.batchCommits == nil {
			j.batchCommits = make(map[int]CommitFunc)
		}
		j.batchCommits[j.batchNo] = done

		if j.batchRemaining == nil {
			j.batchRemaining = make(map[int]int)
		}
		j.batchRemaining[j.batchNo] = len(j.batch)
		j.batchNo++
	}

	// If we have records in the batch, return the first one
	msg := j.batch[0]
	j.batch = j.batch[1:]

	commit := func() error {
		batchNo := j.batchNo - 1
		j.batchRemaining[batchNo]--
		if j.batchRemaining[batchNo] == 0 {
			commit := j.batchCommits[batchNo]
			delete(j.batchRemaining, batchNo)
			return commit()
		}
		return nil
	}

	return msg, commit, nil
}
