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

func (m *TestReader) Lag(ctx context.Context) (uint64, error) {
	return 0, nil
}

func (m *TestReader) Read(ctx context.Context) (Message, CommitFunc, error) {
	eofCommit := func() error {
		return nil
	}
	if len(m.st) == 0 {
		return Message{}, eofCommit, io.EOF
	}
	r := m.st[0]
	m.st = m.st[1:]

	commit := func() error {
		log.Printf("committing: %q", r)
		return nil
	}

	return Message{Value: []byte(r), Time: time.Now()}, commit, nil
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
			time.Sleep(150 * time.Millisecond)
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
		Size:    100 * time.Millisecond,
		Advance: 100 * time.Millisecond,
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

	intermediate := NewNoopPipe()

	go func() {
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

		Pipe(MarshalAny(ag), intermediate)
		intermediate.Close()
	}()

	state := NewMapState[string, string]()
	table, err := PipeTable(UnmarshalString(intermediate), state)
	if err != nil {
		t.Errorf("error: %v", err)
	}

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

	joined := TableJoinReader(
		rekeyr2,
		table,
		func(r Record[string, string], i string) string {
			return r.Value + ":" + i
		},
	)
	Consume(table)
	Pipe(MarshalAny(joined), &TestWriter{})
}

func TestStreamingJoin(t *testing.T) {
	r1 := &TestReader{st: []string{
		"the quick BROWN",
		"fox JUMPS over",
		"the lazy lazy dog",
	}}

	/*
		r2 := &TestReader{st: []string{
			"the", "the", "the",
			"brown", "brown", "brown",
		}}
	*/
	r2 := &TestReader{st: []string{
		"the",
		"brown",
	}}

	u1 := FlatMap(UnmarshalString(r1), func(r KeyValue[string, string]) []KeyValue[string, int] {
		var out []KeyValue[string, int]
		for _, s := range strings.Split(r.Value, " ") {
			out = append(out, KeyValue[string, int]{
				Key:   strings.ToLower(s),
				Value: 1,
			})
		}

		return out
	})
	u2 := Map(UnmarshalString(r2), func(r KeyValue[string, string]) KeyValue[string, int] {
		return KeyValue[string, int]{
			Key:   strings.ToLower(r.Value),
			Value: 1,
		}
	})

	joined := StreamJoinReader(
		u1,
		u2,
		NewMapWindowState[string, int](),
		NewMapWindowState[string, int](),
		func(r Record[string, int], r2 Record[string, int]) int {
			return r.Value + r2.Value
		},
		JoinWindows{
			Before: 1 * time.Second,
			After:  1 * time.Second,
		},
	)

	Pipe(MarshalAny(joined), &TestWriter{})
}

var splitValues = func(r KeyValue[string, string]) []KeyValue[string, string] {
	var out []KeyValue[string, string]
	for _, s := range strings.Split(r.Value, " ") {
		out = append(out, KeyValue[string, string]{
			Key:   r.Key,
			Value: s,
		})
	}

	return out
}

func TestMerge(t *testing.T) {
	r1 := &TestReader{st: []string{
		"the quick brown",
		"fox jumps over",
	}}

	r2 := &TestReader{st: []string{
		"the lazy lazy dog",
	}}

	u1 := FlatMap(UnmarshalString(r1), splitValues)
	u2 := FlatMap(UnmarshalString(r2), splitValues)

	merged := Merge(u1, u2)

	Pipe(MarshalAny(merged), &TestWriter{})
}
