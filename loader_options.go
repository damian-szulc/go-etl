package etl

import (
	"context"
)

type loaderOptions struct {
	hooksPreRun     []LoaderPreRunHook
	hooksOnError    []LoaderOnErrorHook
	hooksOnComplete []LoaderOnComplete

	concurrency int

	failOnErr bool
}

func newLoaderOptions(optsSetters ...LoaderOption) *loaderOptions {
	opts := &loaderOptions{
		concurrency: 1,

		failOnErr: true,
	}

	for _, setter := range optsSetters {
		if setter != nil {
			setter(opts)
		}
	}

	return opts
}

type LoaderOption func(o *loaderOptions)

type LoaderPreRunHook func(ctx context.Context, inputCh <-chan Message) error

func LoaderWithPreRunHook(hook LoaderPreRunHook) LoaderOption {
	return func(o *loaderOptions) { o.hooksPreRun = append(o.hooksPreRun, hook) }
}

type LoaderOnErrorHook func(ctx context.Context, inMsg Message, err error) error

func LoaderWithOnErrorHook(hook LoaderOnErrorHook) LoaderOption {
	return func(o *loaderOptions) { o.hooksOnError = append(o.hooksOnError, hook) }
}

type LoaderOnComplete func(ctx context.Context, inMsg Message) error

func LoaderWithOnCompleteHook(hook LoaderOnComplete) LoaderOption {
	return func(o *loaderOptions) { o.hooksOnComplete = append(o.hooksOnComplete, hook) }
}

func LoaderWithConcurrency(concurrency int) LoaderOption {
	return func(o *loaderOptions) { o.concurrency = concurrency }
}

func LoaderWithFailOnError(failOnErr bool) LoaderOption {
	return func(o *loaderOptions) { o.failOnErr = failOnErr }
}
