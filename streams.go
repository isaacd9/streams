package streams

import (
	"context"
	"io"
	"time"
)

type Message = Record[[]byte, []byte]

type Record[K, V any] struct {
	Key   K
	Value V
	Time  time.Time
}

type CommitFunc func() error

type Reader[K, V any] interface {
	Read(context.Context) (Record[K, V], CommitFunc, error)
}

type Writer[K, V any] interface {
	Write(ctx context.Context, r Record[K, V]) error
}

type tableReader[K comparable, V any] struct {
	state State[K, V]
	r     Reader[K, V]
}

func (r *tableReader[K, V]) Get(k K) (V, error) {
	return r.state.Get(k)
}

func (r *tableReader[K, V]) Read(ctx context.Context) (Record[K, V], CommitFunc, error) {
	msg, done, err := r.r.Read(ctx)
	if err != nil {
		return msg, done, err
	}
	if err := r.state.Put(msg.Key, msg.Value); err != nil {
		return msg, done, err
	}
	return msg, done, nil
}

type LaggedReader[K, V any] interface {
	Reader[K, V]
	Lag(context.Context) (uint64, error)
}

// TOOD: Figure out fault tolerance. We need to be able to resume state from a
// stream or checkpoint and catch up to the current state. before allowing other
// readers to read from the state.
// This is kind of a bad implemenation of this because we resume up to the lag
// immediately. If the lag is large, we'll be holding up the stream for a long
// time before we return a reader.
// Maybe we should have a separate function that returns a reader, and then we
// can fill it async or something.
func PipeTable[K comparable, V any](r LaggedReader[K, V], state State[K, V]) (TableReader[K, V], error) {
	rdr := &tableReader[K, V]{
		r:     r,
		state: state,
	}

	for {
		lag, err := r.Lag(context.Background())
		if err != nil {
			return nil, err
		}

		// If there's no lag, we're caught up.
		if lag == 0 {
			break
		}

		msg, done, err := r.Read(context.Background())
		if err != nil {
			return nil, err
		}
		if err := state.Put(msg.Key, msg.Value); err != nil {
			return nil, err
		}
		done()
	}

	return rdr, nil
}

func Consume[K comparable, V any](r TableReader[K, V]) error {
	for {
		_, done, err := r.Read(context.Background())
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		done()
	}
}

func Pipe[K, V any](r Reader[K, V], w Writer[K, V]) (int, error) {
	var n int
	for {
		msg, done, err := r.Read(context.Background())
		if err != nil {
			if err == io.EOF {
				return n, nil
			}
			return n, err
		}

		if err := w.Write(context.Background(), msg); err != nil {
			return n, err
		}

		done()
		n += 1
	}
}
