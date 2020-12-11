package etl

import (
	"context"
	"github.com/pkg/errors"
)

type ExtractorHandler func(ctx context.Context, sender Sender) error

type Extractor interface {
	Runner
	OutputCh() <-chan Message
}

type extractor struct {
	handler  ExtractorHandler
	outputCh chan Message

	opts *extractorOptions
}

func NewExtractor(handler ExtractorHandler, optsSetters ...ExtractorOption) Extractor {
	opts := newExtractorOptions(optsSetters...)

	return &extractor{
		handler:  handler,
		outputCh: make(chan Message, opts.outputChannelBufferSize),
		opts:     opts,
	}
}

func (e *extractor) OutputCh() <-chan Message {
	return e.outputCh
}

func (e *extractor) preRunHooks(ctx context.Context) error {
	var err error
	for _, hook := range e.opts.hooksPreRun {
		err = hook(ctx, e.outputCh)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *extractor) Run(ctx context.Context) error {
	err := e.preRunHooks(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to run extractor preRunHooks")
	}

	err = e.handler(ctx, newSender([]chan Message{e.outputCh}, nil, nil))
	if err != nil {
		return err
	}

	close(e.outputCh)

	return nil
}
