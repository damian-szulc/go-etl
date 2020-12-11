package etl

import (
	"context"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type LoaderBatchedHandler func(ctx context.Context, messages []Message) error

type LoaderBatched interface {
	Runner
}

type loaderBatched struct {
	handler LoaderBatchedHandler

	inputCh <-chan Message

	opts *loaderBatchedOptions
}

func NewLoaderBatched(inputCh <-chan Message, handler LoaderBatchedHandler, optsSetters ...LoaderBatchedOption) LoaderBatched {
	opts := newLoaderBatchedOptions(optsSetters...)

	return &loaderBatched{
		handler: handler,

		inputCh: inputCh,

		opts: opts,
	}
}

func (l *loaderBatched) preRunHooks(ctx context.Context) error {
	var err error
	for _, hook := range l.opts.hooksPreRun {
		err = hook(ctx, l.inputCh)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *loaderBatched) Run(ctx context.Context) error {
	err := l.preRunHooks(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to run batched loader preRunHooks")
	}

	if l.opts.concurrency == 1 {
		return l.runWorker(ctx)
	}

	g, ctx := errgroup.WithContext(ctx)

	for i := 0; i <= l.opts.concurrency; i++ {
		g.Go(func() error {
			return l.runWorker(ctx)
		})
	}

	return g.Wait()
}

func (l *loaderBatched) onErrorHook(ctx context.Context, inMsgs []Message, opErr error) error {
	var err error
	for _, hook := range l.opts.hooksOnError {
		err = hook(ctx, inMsgs, opErr)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *loaderBatched) onCompleteHook(ctx context.Context, inMsgs []Message) error {
	var err error
	for _, hook := range l.opts.hooksOnComplete {
		err = hook(ctx, inMsgs)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *loaderBatched) runWorker(ctx context.Context) error {
	var (
		inMsgs []Message
		opErr  error
		err    error
	)
	for {
		inMsgs, err = l.opts.batcher(ctx, l.inputCh)
		if err != nil {
			return err
		}

		if len(inMsgs) == 0 {
			return nil
		}

		opErr = l.handler(ctx, inMsgs)
		if opErr != nil {
			err = l.onErrorHook(ctx, inMsgs, opErr)
			if err != nil {
				return errors.Wrap(err, "running batched loader on error hook has failed")
			}

			if l.opts.failOnErr {
				return opErr
			}
		}

		err = l.onCompleteHook(ctx, inMsgs)
		if err != nil {
			return errors.Wrap(err, "failed to run loader onComplete hook")
		}
	}
}
