package streams

import (
	"context"
	"io"
	"log"
	"strings"
	"testing"
)

type TestReader struct {
	st []string
}

func (m *TestReader) Read(ctx context.Context) ([]byte, error) {
	if len(m.st) == 0 {
		return nil, io.EOF
	}
	r := m.st[0]
	m.st = m.st[1:]
	return []byte(r), nil
}

type TestWriter struct{}

func (m *TestWriter) Write(ctx context.Context, msg []byte) error {
	log.Printf("%s", string(msg))
	return nil
}

func TestWordCount(t *testing.T) {
	r := &TestReader{st: []string{
		"the quick BROWN",
		"fox JUMPS over",
		"the lazy lazy dog",
	}}
	rr := NewMappedProcessor[string, string](func(s string) string {
		return strings.ToLower(s)
	})
	fm := NewFlatMapProcessor[string, string](func(s string) []string {
		return strings.Split(s, " ")
	})
	_ = NewMappedProcessor[string, int](func(s string) int {
		return len(s)
	})

	var e Executor
	a := NewStream(&e, r, StringDeserializer())
	b := Through(a, rr)
	c := Through(b, fm)
	// intStream := Through(c, m)
	c.To(StringSerializer(), &TestWriter{})

	e.Execute(context.Background())
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
