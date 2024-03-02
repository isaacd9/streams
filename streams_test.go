package streams

import (
	"context"
	"io"
	"log"
	"strconv"
	"strings"
	"testing"
	"time"
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
	rr := MapValues[string](func(in string) string {
		return strings.ToLower(in)
	})
	fm := FlatMapValues[string](func(s string) []string {
		return strings.Split(s, " ")
	})
	m := MapValues[string](func(s string) uint64 {
		return uint64(len(s))
	})

	var e Executor
	a := NewStream(&e, r, StringUnmarshaler())
	b := Process(a, rr)
	c := Process(b, fm)
	d := Process(c, m)
	To(d, IntMarshaler(), &TestWriter{})

	e.Execute(context.Background())
}

func TestWordCount(t *testing.T) {
	r := &TestReader{st: []string{
		"the quick BROWN",
		"fox JUMPS over",
		"the lazy lazy dog",
	}}
	fm := FlatMapValues[string](func(s string) []string {
		return strings.Split(s, " ")
	})
	makeKeys := Map[string, string, string, string](func(r Record[string, string]) Record[string, string] {
		return Record[string, string]{Key: strings.ToLower(r.Val)}
	})

	pipe := NewNoopPipe()

	ex := NewExecutor()

	counter := NewCount[string, string](NewMapState[string, uint64]())

	s := NewStream(ex, r, StringUnmarshaler())
	s = Process(s, fm)
	s = Process(s, makeKeys)
	s = Through(s, pipe, StringMarshalerUnmarshaler())
	a := Aggregate(s, counter)
	To(ToStream(a), IntMarshaler(), &TestWriter{})

	ex.Execute(context.Background())
}

func TestWindowedWordCount(t *testing.T) {
	r := &TestReader{st: []string{
		"the quick BROWN",
		"fox JUMPS over",
		"the lazy lazy dog",
	}}
	fm := FlatMapValues[string](func(s string) []string {
		return strings.Split(s, " ")
	})
	makeKeys := Map[string, string, string, string](func(r Record[string, string]) Record[string, string] {
		return Record[string, string]{Key: strings.ToLower(r.Val)}
	})

	pipe := NewNoopPipe()

	ex := NewExecutor()

	counter := NewCount[WindowKey[string], string](NewMapState[WindowKey[string], uint64]())

	s := NewStream(ex, r, StringUnmarshaler())
	s = Process(s, fm)
	s = Process(s, makeKeys)
	s = Through(s, pipe, StringMarshalerUnmarshaler())
	w := Window(s, NewRealTimeWindow[string, string](TimeWindows{
		Size:    1 * time.Minute,
		Advance: 1 * time.Minute,
	}))
	aa := Aggregate(w, counter)
	To(ToStream(aa), AnyMarshaler[WindowKey[string], uint64](), &TestWriter{})

	ex.Execute(context.Background())
}

func TestReducer(t *testing.T) {
	r := &TestReader{st: []string{
		"1, 2, 3",
		"10, 20, 30",
	}}

	ex := NewExecutor()
	s := NewStream(ex, r, StringUnmarshaler())
	s = Process(s, FlatMapValues[string](func(s string) []string {
		return strings.Split(s, ", ")
	}))
	ss := Process(s, MapValues[string](func(s string) int {
		i, err := strconv.Atoi(s)
		if err != nil {
			panic(err)
		}
		return i
	}))

	sss := Process(ss, Map[string, int, string, int](func(r Record[string, int]) Record[string, int] {
		var k string
		if r.Val < 10 {
			k = "small"
		} else {
			k = "big"
		}

		return Record[string, int]{Key: k, Val: r.Val}
	}))

	agg := Aggregate[string, int, int](sss, NewReducer(NewMapState[string, int](), func(k string, a, b int) int {
		return a + b
	}))

	To(ToStream(agg), AnyMarshaler[string, int](), &TestWriter{})
	ex.Execute(context.Background())
}
