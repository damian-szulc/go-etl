package etl

import (
	"context"
)

type TransformerUnary struct {
	t *Transformer
}

func NewTransformerUnary(inputCh <-chan Message, handler TransformerHandler, optsSetters ...TransformerOption) *TransformerUnary {
	return &TransformerUnary{t: NewTransformer(inputCh, handler, 1, optsSetters...)}
}

func (t *TransformerUnary) OutputCh() <-chan Message {
	return t.t.OutputCh(0)
}

func (t *TransformerUnary) Run(ctx context.Context) error {
	return t.t.Run(ctx)
}
