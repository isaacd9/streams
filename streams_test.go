package streams

import (
	"context"
	"io"
	"testing"
)

type TestReader[T any] struct {
	st []T
}

func (m *TestReader[T]) ReadMessage(ctx context.Context) (T, error) {
	if len(m.st) == 0 {
		var t T
		return t, io.EOF
	}
	r := m.st[0]
	m.st = m.st[1:]
	return r, nil
}

func TestWordCount(t *testing.T) {
	r := &TestReader[string]{st: []string{
		"the quick BROWN",
		"fox JUMPS over",
		"the lazy lazy dog",
	}}
}

/*
func TestWindowedWordCount(t *testing.T) {
	r := &TestReader[string]{st: []string{
		"the quick BROWN",
		"fox JUMPS over",
		"the lazy lazy dog",
	}}
	rr := NewMappedReader[string, string](r, func(s string) string {
		return strings.ToLower(s)
	})
	fm := NewFlatMapReader[string, string](rr, func(s string) []string {
		return strings.Split(s, " ")
	})
	gb := NewGroupBy[string, string](fm, func(s string) string {
		return s
	})
	w := NewTimeWindow[string, string](gb, TimeWindowCfg{
		Size:    10 * time.Millisecond,
		Advance: 10 * time.Millisecond,
	})
	c := NewWindowedCount[string, string](w)

	for {
		k, m, err := c.ReadMessage(context.Background())
		if err != nil {
			return
		}
		log.Printf("%q=%d", k, m)
	}
}

func TestReducer(t *testing.T) {
	r := &TestReader[int]{st: []int{
		1, 2, 3,
		10, 20, 30,
	}}

	gb := NewGroupBy[int, string](r, func(s int) string {
		if s > 9 {
			return "big"
		} else {
			return "small"
		}
	})

	reducer := NewReducer[string, int](gb, 0, func(a, b int) int {
		return a + b
	})

	for {
		k, m, err := reducer.ReadMessage(context.Background())
		if err != nil {
			return
		}
		log.Printf("%q=%d", k, m)
	}
}

*/
