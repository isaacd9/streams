package streams

import (
	"context"
)

type NoopPipe struct {
	ch chan Message
}

func NewNoopPipe() *NoopPipe {
	return &NoopPipe{
		ch: make(chan Message),
	}
}

func (n *NoopPipe) Read(ctx context.Context) (Message, error) {
	select {
	case msg := <-n.ch:
		return msg, nil
	case <-ctx.Done():
		return Message{}, ctx.Err()
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
