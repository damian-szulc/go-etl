package etl

import (
	"context"
)

type Transformer struct {
	t *TransformerDemux
}

func NewTransformer(inputCh <-chan Message, handler TransformerHandler, optsSetters ...TransformerOption) *Transformer {
	return &Transformer{t: NewTransformerDemux(inputCh, handler, 1, optsSetters...)}
}

func (t *Transformer) OutputCh() <-chan Message {
	return t.t.OutputCh(0)
}

func (t *Transformer) Run(ctx context.Context) error {
	return t.t.Run(ctx)
}
