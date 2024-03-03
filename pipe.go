package streams

import (
	"context"
	"io"
)

type NoopPipe struct {
	done chan struct{}
	ch   chan Message
}

func NewNoopPipe() *NoopPipe {
	return &NoopPipe{
		ch:   make(chan Message),
		done: make(chan struct{}),
	}
}

func (n *NoopPipe) Close() error {
	close(n.done)
	return nil
}

func (n *NoopPipe) Read(ctx context.Context) (Message, CommitFunc, error) {
	commit := func() error {
		return nil
	}

	select {
	case msg := <-n.ch:
		return msg, commit, nil
	case <-n.done:
		return Message{}, commit, io.EOF
	case <-ctx.Done():
		return Message{}, commit, ctx.Err()
	}
}

func (n *NoopPipe) Write(ctx context.Context, msg Message) error {
	select {
	case n.ch <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
