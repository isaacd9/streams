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
	time  time.Time
}

type Reader[K, V any] interface {
	Read(ctx context.Context) (Record[K, V], error)
}

type Writer[K, V any] interface {
	Write(ctx context.Context, r Record[K, V]) error
}

func Pipe[K, V any](r Reader[K, V], w Writer[K, V]) (int, error) {
	var n int
	for {
		msg, err := r.Read(context.Background())
		if err != nil {
			if err == io.EOF {
				return n, nil
			}
			return n, err
		}

		if err := w.Write(context.Background(), msg); err != nil {
			return n, err
		}

		n += 1
	}
}
