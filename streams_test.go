package streams

import (
	"context"
	"log"
	"strings"
	"testing"
)

type TestReader[T any] struct {
	st []T
}

func (m *TestReader[T]) ProcessMessage(ctx context.Context, next func(T) error) error {
	for _, v := range m.st {
		if err := next(v); err != nil {
			return err
		}
	}
	return nil
}

func TestWordCount(t *testing.T) {
	r := &TestReader[string]{st: []string{
		"the quick BROWN",
		"fox JUMPS over",
		"the lazy lazy dog",
	}}
	rr := NewMappedProcessor[string, string](r, func(s string) string {
		return strings.ToLower(s)
	})
	fm := NewFlatMapProcessor[string, string](rr, func(s string) []string {
		return strings.Split(s, " ")
	})

	fm.ProcessMessage(context.Background(), func(s string) error {
		log.Println(s)
		return nil
	})
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
