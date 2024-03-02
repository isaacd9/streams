package streams

import (
	"context"
	"fmt"
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
	return Message{Value: []byte(r)}, nil
}

type TestWriter struct{}

func (m *TestWriter) Write(ctx context.Context, msg Message) error {
	log.Printf("k: %q, v: %q", msg.Key, msg.Value)
	return nil
}

func TestWordLen(t *testing.T) {
	r := &TestReader{st: []string{
		"the quick BROWN",
		"fox JUMPS over",
		"the lazy lazy dog",
	}}
	unmarshal := Map(r, func(r Message) Record[string, string] {
		return Record[string, string]{
			Key:   string(r.Key),
			Value: string(r.Value),
		}
	})
	lowercase := MapValues(unmarshal, func(in string) string {
		return strings.ToLower(in)
	})
	split := FlatMapValues(lowercase, func(s string) []string {
		return strings.Split(s, " ")
	})
	length := MapValues(split, func(s string) uint64 {
		return uint64(len(s))
	})
	marshal := Map(length, func(r Record[string, uint64]) Message {
		return Message{
			Key:   []byte(fmt.Sprintf("%v", r.Key)),
			Value: []byte(fmt.Sprintf("%v", r.Value)),
		}
	})

	Pipe(marshal, &TestWriter{})
}

func TestInProcessWordCount(t *testing.T) {
	r := &TestReader{st: []string{
		"the quick BROWN",
		"fox JUMPS over",
		"the lazy lazy dog",
	}}
	split := FlatMapValues(UnmarshalString(r), func(s string) []string {
		return strings.Split(s, " ")
	})
	rekey := Map(split, func(r Record[string, string]) Record[string, string] {
		return Record[string, string]{
			Key: strings.ToLower(r.Value),
		}
	})

	count := Count(rekey, NewMapState[string, uint64]())

	marshal := Map(count, func(r Record[string, uint64]) Message {
		return Message{
			Key:   []byte(fmt.Sprintf("%v", r.Key)),
			Value: []byte(fmt.Sprintf("%v", r.Value)),
		}
	})

	Pipe(marshal, &TestWriter{})
}

func TestThroughWordCount(t *testing.T) {
	intermediate := NewNoopPipe()
	go func() {
		um := Map(intermediate, func(r Message) Record[string, string] {
			return Record[string, string]{
				Key:   string(r.Key),
				Value: string(r.Value),
			}
		})
		count := Count(um, NewMapState[string, uint64]())
		Pipe(MarshalAny(count), &TestWriter{})
	}()

	r := &TestReader{st: []string{
		"the quick BROWN",
		"fox JUMPS over",
		"the lazy lazy dog",
	}}
	split := FlatMapValues(UnmarshalString(r), func(s string) []string {
		return strings.Split(s, " ")
	})
	rekey := Map(split, func(r Record[string, string]) Record[string, string] {
		return Record[string, string]{
			Key: strings.ToLower(r.Value),
		}
	})
	n, err := Pipe(MarshalAny(rekey), intermediate)
	if err != nil {
		t.Errorf("error: %v", err)
	}
	intermediate.Close()
	// log.Printf("processed %d records", n)

}

/*
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

	sss := Process(ss, Map(func(r Record[string, int]) Record[string, int] {
		var k string
		if r.Val < 10 {
			k = "small"
		} else {
			k = "big"
		}

		return Record[string, int]{
			Key: k,
			Value: r.Val,
		}
	}))

	agg := Aggregate(sss, NewReducer(NewMapState[string, int](), func(k string, a, b int) int {
		return a + b
	}))

	To(ToStream(agg), AnyMarshaler[string, int](), &TestWriter{})
	ex.Execute(context.Background())
}

*/
