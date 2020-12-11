package etl

import (
	"context"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type LoaderHandler func(ctx context.Context, message Message) error

type Loader interface {
	Runner
}

type loader struct {
	handler LoaderHandler

	inputCh <-chan Message

	opts *loaderOptions
}

func NewLoader(inputCh <-chan Message, handler LoaderHandler, optsSetters ...LoaderOption) Loader {
	opts := newLoaderOptions(optsSetters...)

	return &loader{
		handler: handler,

		inputCh: inputCh,

		opts: opts,
	}
}

func (l *loader) preRunHooks(ctx context.Context) error {
	var err error
	for _, hook := range l.opts.hooksPreRun {
		err = hook(ctx, l.inputCh)
		if err != nil {
			return err
		}
	}

	return nil
}

// Run loader with a specified context. Note that execution of this function is blocking, until processing is finished.
func (l *loader) Run(ctx context.Context) error {
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

func (l *loader) onErrorHook(ctx context.Context, inMsg Message, opErr error) error {
	var err error
	for _, hook := range l.opts.hooksOnError {
		err = hook(ctx, inMsg, opErr)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *loader) onCompleteHook(ctx context.Context, inMsg Message) error {
	var err error
	for _, hook := range l.opts.hooksOnComplete {
		err = hook(ctx, inMsg)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *loader) runWorker(ctx context.Context) error {
	var (
		inMsg Message
		ok    bool
		opErr error
		err   error
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case inMsg, ok = <-l.inputCh:
			if !ok {
				return nil
			}

			opErr = l.handler(ctx, inMsg)
			if opErr != nil {
				err = l.onErrorHook(ctx, inMsg, opErr)
				if err != nil {
					return errors.Wrap(err, "running loader on error hook has failed")
				}

				if l.opts.failOnErr {
					return opErr
				}
			}

			err = l.onCompleteHook(ctx, inMsg)
			if err != nil {
				return errors.Wrap(err, "failed to run loader onComplete hook")
			}

		}
	}
}
