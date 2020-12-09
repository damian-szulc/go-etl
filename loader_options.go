package etl

import (
	"context"
	"time"
)

type loaderOptions struct {
	hooksPreRun     []LoaderPreRunHook
	hooksOnError    []LoaderOnErrorHook
	hooksOnComplete []LoaderOnComplete
	drainer         LoaderDrainer

	concurrency int

	failOnErr bool
}

func newLoaderOptions(optsSetters ...LoaderOption) *loaderOptions {
	opts := &loaderOptions{
		drainer:     func(ctx context.Context, inMsgCh <-chan Message) []Message { return nil },
		concurrency: 1,
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

type LoaderOnErrorHook func(ctx context.Context, inMsgs []Message, err error) error

func LoaderWithOnErrorHook(hook LoaderOnErrorHook) LoaderOption {
	return func(o *loaderOptions) { o.hooksOnError = append(o.hooksOnError, hook) }
}

type LoaderOnComplete func(ctx context.Context, inMsgs []Message) error

func LoaderWithOnCompleteHook(hook LoaderOnComplete) LoaderOption {
	return func(o *loaderOptions) { o.hooksOnComplete = append(o.hooksOnComplete, hook) }
}

func LoaderWithConcurrency(concurrency int) LoaderOption {
	return func(o *loaderOptions) { o.concurrency = concurrency }
}

func LoaderWithFailOnError(failOnErr bool) LoaderOption {
	return func(o *loaderOptions) { o.failOnErr = failOnErr }
}

type LoaderDrainer func(ctx context.Context, inMsgCh <-chan Message) []Message

func LoaderWithChannelDrainer(maxItems int) LoaderOption {
	return func(o *loaderOptions) {
		o.drainer = func(ctx context.Context, inMsgCh <-chan Message) []Message {
			var (
				messages = make([]Message, 0, maxItems-1)
				inMsg    Message
				ok       bool
			)
			for {
				if len(messages) >= maxItems-1 {
					return messages
				}

				select {
				case <-ctx.Done():
					return messages
				case inMsg, ok = <-inMsgCh:
					if !ok {
						return messages
					}

					messages = append(messages, inMsg)
				default:
					return messages
				}
			}

			return messages
		}
	}
}

func LoaderWithChannelTimeDrainer(tickerInterval time.Duration, maxItems int) LoaderOption {
	return func(o *loaderOptions) {
		o.drainer = func(ctx context.Context, inMsgCh <-chan Message) []Message {
			var (
				messages = make([]Message, 0, maxItems-1)
				ticker   = time.NewTimer(tickerInterval)
				inMsg    Message
				ok       bool
			)
			for {
				if len(messages) >= maxItems-1 {
					return messages
				}

				select {
				case <-ctx.Done():
					return messages
				case inMsg, ok = <-inMsgCh:
					if !ok {
						return messages
					}

					messages = append(messages, inMsg)
				case <-ticker.C:
					return messages
				}
			}

			return messages
		}
	}
}
