package etl

import "context"

type transformerOptions struct {
	hooksPreRun     []TransformerPreRunHook
	hooksOnError    []TransformerOnErrorHook
	hooksOnComplete []TransformerOnComplete

	outputChannelBufferSize int
	concurrency             int
	failOnErr               bool
}

func newTransformerOptions(optsSetters ...TransformerOption) *transformerOptions {
	opts := &transformerOptions{
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

type TransformerOption func(o *transformerOptions)

type TransformerPreRunHook func(ctx context.Context, inputCh <-chan Message) error

func TransformerWithPreRunHook(hook TransformerPreRunHook) TransformerOption {
	return func(o *transformerOptions) { o.hooksPreRun = append(o.hooksPreRun, hook) }
}

type TransformerOnErrorHook func(ctx context.Context, inMsg Message, err error) error

func TransformerWithOnErrorHook(hook TransformerOnErrorHook) TransformerOption {
	return func(o *transformerOptions) { o.hooksOnError = append(o.hooksOnError, hook) }
}

type TransformerOnComplete func(ctx context.Context, inMsg Message, outMsg Message, chNr uint) error

func TransformerWithOnCompleteHook(hook TransformerOnComplete) TransformerOption {
	return func(o *transformerOptions) { o.hooksOnComplete = append(o.hooksOnComplete, hook) }
}

func TransformerWithOutputChannelBufferSize(size int) TransformerOption {
	return func(o *transformerOptions) { o.outputChannelBufferSize = size }
}

func TransformerWithConcurrency(concurrency int) TransformerOption {
	return func(o *transformerOptions) { o.concurrency = concurrency }
}

func TransformerWithFailOnError(failOnErr bool) TransformerOption {
	return func(o *transformerOptions) { o.failOnErr = failOnErr }
}
