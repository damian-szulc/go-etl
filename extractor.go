package etl

import (
	"context"
	"github.com/pkg/errors"
)

type ExtractorHandler func(ctx context.Context, sender ExtractorSender) error


type Extractor struct {
	handler  ExtractorHandler
	outputCh chan Message

	opts *extractorOptions
}

func NewExtractor(handler ExtractorHandler, optsSetters ...ExtractorOption) *Extractor {
	opts := newExtractorOptions(optsSetters...)

	return &Extractor{
		handler:  handler,
		outputCh: make(chan Message, opts.outputChannelBufferSize),
		opts:     opts,
	}
}

func (e *Extractor) OutputCh() <-chan Message {
	return e.outputCh
}

func (e *Extractor) preRunHooks(ctx context.Context) error {
	var err error
	for _, hook := range e.opts.hooksPreRun {
		err = hook(ctx, e.outputCh)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Extractor) Run(ctx context.Context) error {
	err := e.preRunHooks(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to run extractor preRunHooks")
	}

	err = e.handler(ctx, newExtractorSender(e.outputCh))
	if err != nil {
		return err
	}

	close(e.outputCh)

	return nil
}
