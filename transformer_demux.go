package etl

import (
	"context"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type TransformerHandler func(ctx context.Context, inMsg Message, sender TransformerSender) error

type TransformerDemux struct {
	handler TransformerHandler

	inputCh     <-chan Message
	outputChsNr uint
	outputChs   []chan Message

	opts *transformerOptions
}

func NewTransformerDemux(inputCh <-chan Message, handler TransformerHandler, outputChannelsNr uint, optsSetters ...TransformerOption) *TransformerDemux {
	opts := newTransformerOptions(optsSetters...)

	var outputChs = make([]chan Message, outputChannelsNr)
	for i := 0; i < int(outputChannelsNr); i++ {
		outputChs[i] = make(chan Message, opts.outputChannelBufferSize)
	}

	return &TransformerDemux{
		handler: handler,

		inputCh:     inputCh,
		outputChsNr: outputChannelsNr,
		outputChs:   outputChs,

		opts: opts,
	}
}

func (t *TransformerDemux) OutputCh(i int) <-chan Message {
	if i > len(t.outputChs) || i < 0 {
		panic(ErrOutputMessageOutOfChannelsRange)
	}

	return t.outputChs[i]
}

func (t *TransformerDemux) preRunHooks(ctx context.Context) error {
	var err error
	for _, hook := range t.opts.hooksPreRun {
		err = hook(ctx, t.inputCh)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *TransformerDemux) closeChannels(chs []chan Message) {
	for _, ch := range chs {
		close(ch)
	}
}

func (t *TransformerDemux) Run(ctx context.Context) error {
	err := t.preRunHooks(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to run transformer preRunHooks")
	}

	defer t.closeChannels(t.outputChs)

	if t.opts.concurrency == 1 {
		return t.runWorker(ctx)
	}

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i <= t.opts.concurrency; i++ {
		g.Go(func() error {
			return t.runWorker(ctx)
		})
	}
	return g.Wait()
}

func (t *TransformerDemux) onErrorHook(ctx context.Context, inMsg Message, opErr error) error {
	var err error
	for _, hook := range t.opts.hooksOnError {
		err = hook(ctx, inMsg, opErr)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *TransformerDemux) runWorker(ctx context.Context) error {
	var (
		sender transformerSender

		inMsg Message
		ok    bool
		opErr error
		err   error
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case inMsg, ok = <-t.inputCh:
			if !ok {
				return nil
			}

			sender = newTransformerSender(inMsg, t.outputChs, t.opts.hooksOnComplete)

			opErr = t.handler(ctx, inMsg, sender)
			if opErr != nil {
				err = t.onErrorHook(ctx, inMsg, opErr)
				if err != nil {
					return errors.Wrap(err, "running on error hook has failed")
				}

				if opErr == ErrCastingFailed || t.opts.failOnErr {
					return opErr
				}
			}
		}
	}
}
