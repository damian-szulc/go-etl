package etl

import (
	"context"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type LoaderHandler func(ctx context.Context, messages []Message) error

type Loader struct {
	handler LoaderHandler

	inputCh <-chan Message

	opts *loaderOptions
}

func NewLoader(inputCh <-chan Message, handler LoaderHandler, optsSetters ...LoaderOption) *Loader {
	opts := newLoaderOptions(optsSetters...)

	return &Loader{
		handler: handler,

		inputCh: inputCh,

		opts: opts,
	}
}

func (l *Loader) preRunHooks(ctx context.Context) error {
	var err error
	for _, hook := range l.opts.hooksPreRun {
		err = hook(ctx, l.inputCh)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *Loader) Run(ctx context.Context) error {
	err := l.preRunHooks(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to run loader preRunHooks")
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

func (l *Loader) onErrorHook(ctx context.Context, inMsgs []Message, opErr error) error {
	var err error
	for _, hook := range l.opts.hooksOnError {
		err = hook(ctx, inMsgs, opErr)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *Loader) onCompleteHook(ctx context.Context, inMsgs []Message) error {
	var err error
	for _, hook := range l.opts.hooksOnComplete {
		err = hook(ctx, inMsgs)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *Loader) runWorker(ctx context.Context) error {
	var (
		inMsgs []Message
		inMsg  Message
		ok     bool
		opErr  error
		err    error
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case inMsg, ok = <-l.inputCh:
			if !ok {
				return nil
			}

			inMsgs = l.opts.drainer(ctx, l.inputCh)
			inMsgs = append(inMsgs, inMsg)

			opErr = l.handler(ctx, inMsgs)
			if opErr != nil {
				err = l.onErrorHook(ctx, inMsgs, opErr)
				if err != nil {
					return errors.Wrap(err, "running loader on error hook has failed")
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
}
