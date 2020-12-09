package etl

import "context"

type extractorOptions struct {
	hooksPreRun             []ExtractorPreRunHook
	outputChannelBufferSize int
}

func newExtractorOptions(optsSetters ...ExtractorOption) *extractorOptions {
	opts := &extractorOptions{}

	for _, setter := range optsSetters {
		if setter != nil {
			setter(opts)
		}
	}

	return opts
}

type ExtractorOption func(o *extractorOptions)

type ExtractorPreRunHook func(ctx context.Context, outputCh chan<- Message) error

func ExtractorWithPreRunHook(hook ExtractorPreRunHook) ExtractorOption {
	return func(o *extractorOptions) {
		o.hooksPreRun = append(o.hooksPreRun, hook)
	}
}

func ExtractorWithOutputChannelBufferSize(size int) ExtractorOption {
	return func(o *extractorOptions) {
		o.outputChannelBufferSize = size
	}
}
