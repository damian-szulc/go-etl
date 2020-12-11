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
		batcher:     defaultLoaderBatcher,
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

type LoaderBatcher func(ctx context.Context, inMsgCh <-chan Message) ([]Message, error)

func LoaderBatchedWithBatcher(batcher LoaderBatcher) LoaderBatchedOption {
	return func(o *loaderBatchedOptions) {
		o.batcher = batcher
	}
}

func LoaderBatchedWithFixedSizeBatches(maxItems int) LoaderBatchedOption {
	return LoaderBatchedWithBatcher(func(ctx context.Context, inMsgCh <-chan Message) ([]Message, error) {
		var (
			messages = make([]Message, 0, maxItems)
			inMsg    Message
			ok       bool
		)
		for {
			if len(messages) >= maxItems {
				return messages, nil
			}

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case inMsg, ok = <-inMsgCh:
				if !ok {
					return messages, nil
				}

				messages = append(messages, inMsg)
			}
		}

		return messages, nil
	})
}

func LoaderBatchedWithDrainedChannelBatches(maxItems int) LoaderBatchedOption {
	return LoaderBatchedWithBatcher(func(ctx context.Context, inMsgCh <-chan Message) ([]Message, error) {
		var (
			messages = make([]Message, 0, maxItems)
			inMsg    Message
			ok       bool
		)
		for {
			if len(messages) >= maxItems {
				return messages, nil
			}

			if len(messages) == 0 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case inMsg, ok = <-inMsgCh:
					if !ok {
						return messages, nil
					}

					messages = append(messages, inMsg)
				}

				continue
			}

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case inMsg, ok = <-inMsgCh:
				if !ok {
					return messages, nil
				}

				messages = append(messages, inMsg)
			default:
				return messages, nil
			}
		}

		return messages, nil
	})
}

func LoaderBatchedWithThrottledBatches(interval time.Duration, maxItems int) LoaderBatchedOption {
	return LoaderBatchedWithBatcher(func(ctx context.Context, inMsgCh <-chan Message) ([]Message, error) {
		var (
			messages = make([]Message, 0, maxItems)
			inMsg    Message
			ticker   *time.Timer
			ok       bool
		)
		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case inMsg, ok = <-inMsgCh:
				if !ok {
					return nil, nil
				}

				messages = append(messages, inMsg)

				ticker = time.NewTimer(interval)

				for {
					if len(messages) >= maxItems {
						return messages, nil
					}

					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case inMsg, ok = <-inMsgCh:
						if !ok {
							// channel closed. There's not going to be any new messages. But return what we have buffered
							return messages, nil
						}

						messages = append(messages, inMsg)
					case <-ticker.C:
						return messages, nil
					}
				}

				return messages, nil
			}
		}
	})
}

func LoaderBatchedWithDebouncedBatches(interval time.Duration, maxItems int) LoaderBatchedOption {
	return LoaderBatchedWithBatcher(func(ctx context.Context, inMsgCh <-chan Message) ([]Message, error) {
		var (
			messages = make([]Message, 0, maxItems)
			inMsg    Message
			ticker   *time.Timer
			ok       bool
		)
		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case inMsg, ok = <-inMsgCh:
				if !ok {
					return nil, nil
				}

				messages = append(messages, inMsg)

				ticker = time.NewTimer(interval)

				for {
					if len(messages) >= maxItems {
						return messages, nil
					}

					ticker.Reset(interval)

					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case inMsg, ok = <-inMsgCh:
						if !ok {
							// channel closed. There's not going to be any new messages. But return what we have buffered
							return messages, nil
						}

						messages = append(messages, inMsg)
					case <-ticker.C:
						return messages, nil
					}
				}

				return messages, nil
			}
		}
	})
}

func defaultLoaderBatcher(ctx context.Context, inMsgCh <-chan Message) ([]Message, error) {
	var (
		inMsg Message
		ok    bool
	)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case inMsg, ok = <-inMsgCh:
			if !ok {
				return nil, nil
			}

			return []Message{inMsg}, nil
		}
	}

	return nil, nil
}
