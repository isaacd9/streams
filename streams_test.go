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

func (m *TestReader) ReadMessage(ctx context.Context) (string, error) {
	if len(m.st) == 0 {
		return "", io.EOF
	}
	r := m.st[0]
	m.st = m.st[1:]
	return r, nil
}

func TestStreams(t *testing.T) {
	r := &TestReader{st: []string{
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
	c := NewCount[string](gb)

	for {
		k, m, err := c.ReadMessage(context.Background())
		if err != nil {
			return
		}
		log.Printf("%q=%d", k, m)
	}
}
