package etl

import (
	"context"
	"time"
)

type loaderBatchedOptions struct {
	hooksPreRun     []LoaderBatchedBatchedPreRunHook
	hooksOnError    []LoaderBatchedOnErrorHook
	hooksOnComplete []LoaderBatchedOnComplete
	batcher         LoaderBatcher

	concurrency int

	failOnErr bool
}

func newLoaderBatchedOptions(optsSetters ...LoaderBatchedOption) *loaderBatchedOptions {
	opts := &loaderBatchedOptions{
		batcher:     func(ctx context.Context, inMsgCh <-chan Message) []Message { return nil },
		concurrency: 1,
		failOnErr:   true,
	}

	for _, setter := range optsSetters {
		if setter != nil {
			setter(opts)
		}
	}

	return opts
}

type LoaderBatchedOption func(o *loaderBatchedOptions)

type LoaderBatchedBatchedPreRunHook func(ctx context.Context, inputCh <-chan Message) error

func LoaderBatchedWithPreRunHook(hook LoaderBatchedBatchedPreRunHook) LoaderBatchedOption {
	return func(o *loaderBatchedOptions) { o.hooksPreRun = append(o.hooksPreRun, hook) }
}

type LoaderBatchedOnErrorHook func(ctx context.Context, inMsgs []Message, err error) error

func LoaderBatchedWithOnErrorHook(hook LoaderBatchedOnErrorHook) LoaderBatchedOption {
	return func(o *loaderBatchedOptions) { o.hooksOnError = append(o.hooksOnError, hook) }
}

type LoaderBatchedOnComplete func(ctx context.Context, inMsgs []Message) error

func LoaderBatchedWithOnCompleteHook(hook LoaderBatchedOnComplete) LoaderBatchedOption {
	return func(o *loaderBatchedOptions) { o.hooksOnComplete = append(o.hooksOnComplete, hook) }
}

func LoaderBatchedWithConcurrency(concurrency int) LoaderBatchedOption {
	return func(o *loaderBatchedOptions) { o.concurrency = concurrency }
}

func LoaderBatchedWithFailOnError(failOnErr bool) LoaderBatchedOption {
	return func(o *loaderBatchedOptions) { o.failOnErr = failOnErr }
}

type LoaderBatcher func(ctx context.Context, inMsgCh <-chan Message) []Message

func LoaderBatchedWithChannelBatcher(maxItems int) LoaderBatchedOption {
	return func(o *loaderBatchedOptions) {
		o.batcher = func(ctx context.Context, inMsgCh <-chan Message) []Message {
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

func LoaderBatchedWithChannelTimeBatcher(tickerInterval time.Duration, maxItems int) LoaderBatchedOption {
	return func(o *loaderBatchedOptions) {
		o.batcher = func(ctx context.Context, inMsgCh <-chan Message) []Message {
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
