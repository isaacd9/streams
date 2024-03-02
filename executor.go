package streams

import "context"

type Executor struct {
	root topologyNode
	last topologyNode
}

func NewExecutor() *Executor {
	return &Executor{}
}

func (e *Executor) Execute(ctx context.Context) error {
	return e.root.do(ctx, nil)
}
