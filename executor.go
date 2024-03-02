package streams

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type Executor struct {
	root topologyNode
	last topologyNode

	runnables []runnableNode
}

func NewExecutor() *Executor {
	return &Executor{}
}

func (e *Executor) Execute(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	for _, r := range e.runnables {
		rr := r
		g.Go(func() error {
			return rr.run(ctx)
		})
	}

	g.Go(func() error {
		err := e.root.do(ctx, nil)
		cancel()
		return err
	})

	return g.Wait()
}
