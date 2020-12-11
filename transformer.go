package etl

import (
	"context"
)

type Transformer interface {
	Runner
	OutputCh() <-chan Message
}

type transformer struct {
	t TransformerDemux
}

func NewTransformer(inputCh <-chan Message, handler TransformerHandler, optsSetters ...TransformerOption) Transformer {
	return &transformer{t: NewTransformerDemux(inputCh, handler, 1, optsSetters...)}
}

func (t *transformer) OutputCh() <-chan Message {
	return t.t.OutputCh(0)
}

func (t *transformer) Run(ctx context.Context) error {
	return t.t.Run(ctx)
}
