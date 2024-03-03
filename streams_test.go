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
	return Message{Value: []byte(r)}, nil
}

type TestWriter struct{}

func (m *TestWriter) Write(ctx context.Context, msg Message) error {
	log.Printf("k: %q, v: %q", msg.Key, msg.Value)
	return nil
}

type NullWriter struct{}

func (m *NullWriter) Write(ctx context.Context, msg Message) error {
	return nil
}

func TestWordLen(t *testing.T) {
	r := &TestReader{st: []string{
		"the quick BROWN",
		"fox JUMPS over",
		"the lazy lazy dog",
	}}
	lowercase := MapValues(UnmarshalString(r), func(in string) string {
		return strings.ToLower(in)
	})
	split := FlatMapValues(lowercase, func(s string) []string {
		return strings.Split(s, " ")
	})
	length := MapValues(split, func(s string) uint64 {
		return uint64(len(s))
	})

	Pipe(MarshalAny(length), &TestWriter{})
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
	rekey := Map(split, func(r KeyValue[string, string]) KeyValue[string, string] {
		return KeyValue[string, string]{
			Key: strings.ToLower(r.Value),
		}
	})

	count := Count(rekey, NewMapState[string, uint64]())
	Pipe(MarshalAny(count), &TestWriter{})
}

func TestThroughWordCount(t *testing.T) {
	intermediate := NewNoopPipe()

	go func() {
		r := &TestReader{st: []string{
			"the quick BROWN",
			"fox JUMPS over",
			"the lazy lazy dog",
		}}
		split := FlatMapValues(UnmarshalString(r), func(s string) []string {
			return strings.Split(s, " ")
		})
		rekey := Map(split, func(r KeyValue[string, string]) KeyValue[string, string] {
			return KeyValue[string, string]{
				Key: strings.ToLower(r.Value),
			}
		})
		_, err := Pipe(MarshalAny(rekey), intermediate)
		if err != nil {
			t.Errorf("error: %v", err)
		}

		_ = intermediate.Close()
	}()

	state := NewMapState[string, uint64]()
	count := Count(UnmarshalString(intermediate), state)
	Pipe(MarshalAny(count), &TestWriter{})
	// log.Printf("processed %d records", n)

}

func TestWindowedWordCount(t *testing.T) {
	intermediate := NewNoopPipe()

	go func() {
		r := &TestReader{st: []string{
			"the quick BROWN",
			"fox JUMPS over",
			"the lazy lazy dog",
		}}
		split := FlatMapValues(UnmarshalString(r), func(s string) []string {
			return strings.Split(s, " ")
		})
		rekey := Map(split, func(r KeyValue[string, string]) KeyValue[string, string] {
			return KeyValue[string, string]{
				Key: strings.ToLower(r.Value),
			}
		})
		_, err := Pipe(MarshalAny(rekey), intermediate)
		if err != nil {
			t.Errorf("error: %v", err)
		}

		_ = intermediate.Close()
	}()

	windowState := NewMapWindowState[string, uint64]()

	windowed := RealTimeWindow(UnmarshalString(intermediate), TimeWindows{
		Size:    1 * time.Minute,
		Advance: 1 * time.Minute,
	})

	counted := WindowCount(windowed, windowState)
	Pipe(MarshalAny(counted), &TestWriter{})

	log.Printf("state: %v", windowState)
}

func TestReducer(t *testing.T) {
	r := &TestReader{st: []string{
		"1, 2, 3",
		"10, 20, 30",
	}}

	split := FlatMapValues(UnmarshalString(r), func(s string) []string {
		return strings.Split(s, ", ")
	})

	parse := MapValues(split, func(s string) int {
		n, err := strconv.Atoi(s)
		if err != nil {
			panic(err)
		}
		return n
	})

	repartition := Map(parse, func(r KeyValue[string, int]) KeyValue[string, int] {
		var k string
		if r.Value < 10 {
			k = "small"
		} else {
			k = "big"
		}
		return KeyValue[string, int]{
			Key:   k,
			Value: r.Value,
		}
	})

	state := NewMapState[string, int]()
	sum := Reduce(repartition, state, func(a int, b int) int {
		return a + b
	})

	Pipe(MarshalAny(sum), &TestWriter{})
	// log.Printf("state: %v", state)
}

func TestJoin(t *testing.T) {
	r := &TestReader{st: []string{
		"the quick BROWN",
		"fox JUMPS over",
		"the lazy lazy dog",
	}}

	state := NewMapState[string, int]()
	split := FlatMapValues(UnmarshalString(r), func(s string) []string {
		return strings.Split(s, " ")
	})
	rekey := Map(split, func(r KeyValue[string, string]) KeyValue[string, string] {
		return KeyValue[string, string]{
			Key: strings.ToLower(r.Value),
		}
	})
	ag := Aggregate(rekey, state, func(r Record[string, string], s int) int {
		return len(r.Key)
	})

	Pipe(MarshalAny(ag), &NullWriter{})

	r2 := &TestReader{st: []string{
		"the", "the", "the",
		"brown", "brown", "brown",
	}}

	rekeyr2 := Map(UnmarshalString(r2), func(r KeyValue[string, string]) KeyValue[string, string] {
		return KeyValue[string, string]{
			Key:   strings.ToLower(r.Value),
			Value: strings.ToLower(r.Value),
		}
	})
	joined := JoinTable(rekeyr2, ag, func(r Record[string, string], i int) string {
		return r.Value + ":" + strconv.Itoa(i)
	})
	Pipe(MarshalAny(joined), &TestWriter{})
}
