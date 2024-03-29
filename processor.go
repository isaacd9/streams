package streams

import (
	"context"
)

type merge[K, V any] struct {
	Record[K, V]
	Commit CommitFunc
	Error  error
}

type mergedReader[K, V any] struct {
	readers []Reader[K, V]
	ch      chan merge[K, V]
}

func (m *mergedReader[K, V]) Read(ctx context.Context) (Record[K, V], CommitFunc, error) {
	merge := <-m.ch
	return merge.Record, merge.Commit, merge.Error
}

func (m *mergedReader[K, V]) merge() {
	for _, r := range m.readers {
		go func(r Reader[K, V]) {
			for {
				msg, done, err := r.Read(context.TODO())
				if err != nil {
					m.ch <- merge[K, V]{Error: err}
					return
				}
				m.ch <- merge[K, V]{Record: msg, Commit: done}
			}
		}(r)
	}
}

func Merge[K, V any](readers ...Reader[K, V]) Reader[K, V] {
	rdr := &mergedReader[K, V]{
		readers: readers,
		ch:      make(chan merge[K, V]),
	}

	rdr.merge()

	return rdr
}

type filterReader[K, V any] struct {
	r  Reader[K, V]
	fn func(Record[K, V]) bool
}

func (f *filterReader[K, V]) Read(ctx context.Context) (Record[K, V], CommitFunc, error) {
	for {
		msg, done, err := f.r.Read(ctx)
		if err != nil {
			return Record[K, V]{}, done, err
		}

		if f.fn(msg) {
			return msg, done, nil
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

func (m *mapReader[KIn, VIn, KOut, VOut]) Read(ctx context.Context) (Record[KOut, VOut], CommitFunc, error) {
	msg, done, err := m.r.Read(ctx)
	if err != nil {
		return Record[KOut, VOut]{}, done, err
	}

	return m.fn(msg), done, nil
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
	r       Reader[KIn, VIn]
	fn      func(Record[KIn, VIn]) []Record[KOut, VOut]
	batchNo uint64
	batch   []Record[KOut, VOut]
	// batchRemaining keeps track of how many records are left in the
	// current batch to commit
	batchRemaining map[uint64]int
	// batchCommit is the source reader's commit function for the current
	// batch
	batchCommits map[uint64]CommitFunc
}

func (f *flatMapReader[KIn, VIn, KOut, VOut]) Read(ctx context.Context) (Record[KOut, VOut], CommitFunc, error) {
	if len(f.batch) == 0 {
		msg, done, err := f.r.Read(ctx)
		if err != nil {
			return Record[KOut, VOut]{}, done, err
		}
		f.batch = f.fn(msg)

		if f.batchRemaining == nil {
			f.batchRemaining = make(map[uint64]int)
		}
		f.batchRemaining[f.batchNo] = len(f.batch)

		if f.batchCommits == nil {
			f.batchCommits = make(map[uint64]CommitFunc)
		}
		f.batchCommits[f.batchNo] = done

		f.batchNo++
	}

	out := f.batch[0]
	f.batch = f.batch[1:]

	commit := CommitFunc(func() error {
		// make it clear we're closed over the batchNo
		// we want to commit the _previous_ batch number as we
		// incremented it when we set the batch
		batchNo := f.batchNo - 1
		f.batchRemaining[batchNo]--
		if f.batchRemaining[batchNo] == 0 {
			commit := f.batchCommits[batchNo]
			delete(f.batchRemaining, batchNo)
			return commit()
		}
		return nil
	})
	return out, commit, nil
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
		batchRemaining: make(map[uint64]int),
		batchCommits:   make(map[uint64]CommitFunc),
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
		batchRemaining: make(map[uint64]int),
		batchCommits:   make(map[uint64]CommitFunc),
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
		batchRemaining: make(map[uint64]int),
		batchCommits:   make(map[uint64]CommitFunc),
	}
}
