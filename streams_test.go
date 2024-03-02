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

func (m *TestReader) Read(ctx context.Context) (Message, error) {
	if len(m.st) == 0 {
		return Message{}, io.EOF
	}
	r := m.st[0]
	m.st = m.st[1:]
	return Message{Val: []byte(r)}, nil
}

type TestWriter struct{}

func (m *TestWriter) Write(ctx context.Context, msg Message) error {
	log.Printf("k: %q, v: %q", msg.Key, msg.Val)
	return nil
}

func TestWordLen(t *testing.T) {
	r := &TestReader{st: []string{
		"the quick BROWN",
		"fox JUMPS over",
		"the lazy lazy dog",
	}}
	rr := NewMappedProcessor(func(s Record[string, string]) Record[string, string] {
		return Record[string, string]{
			Key: s.Key,
			Val: strings.ToLower(s.Val),
		}
	})
	fm := NewFlatMapProcessor(func(r Record[string, string]) []Record[string, string] {
		sp := strings.Split(r.Val, " ")
		var ret []Record[string, string]
		for _, s := range sp {
			ret = append(ret, Record[string, string]{
				Key: r.Key,
				Val: strings.ToLower(s),
			})
		}
		return ret
	})
	m := NewMappedProcessor(func(s Record[string, string]) Record[string, int] {
		return Record[string, int]{
			Key: s.Key,
			Val: len(s.Val),
		}
	})

	var e Executor
	a := NewStream(&e, r, StringDeserializer())
	b := Process(a, rr)
	c := Process(b, fm)
	d := Process(c, m)
	To(d, IntSerializer(), &TestWriter{})

	e.Execute(context.Background())
}

func TestWordCount(t *testing.T) {
	r := &TestReader{st: []string{
		"the quick BROWN",
		"fox JUMPS over",
		"the lazy lazy dog",
	}}
	rr := NewMappedProcessor(func(s Record[string, string]) Record[string, string] {
		return Record[string, string]{
			Key: s.Key,
			Val: strings.ToLower(s.Val),
		}
	})
	fm := NewFlatMapProcessor(func(r Record[string, string]) []Record[string, string] {
		sp := strings.Split(r.Val, " ")
		var ret []Record[string, string]
		for _, s := range sp {
			ret = append(ret, Record[string, string]{
				Key: r.Key,
				Val: strings.ToLower(s),
			})
		}
		return ret
	})

	var e Executor
	a := NewStream(&e, r, StringDeserializer())
	b := Process(a, rr)
	c := Process(b, fm)
	// e := GrouBy(c, g)
	d := Through(c, NewNoopPipe(), StringSerde())
	To(d, StringSerializer(), &TestWriter{})

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
