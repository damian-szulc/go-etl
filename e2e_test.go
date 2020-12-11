package etl_test

import (
	"context"
	"github.com/damian-szulc/go-etl"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func fakeTransformer(ctx context.Context, inMsg etl.Message, sender etl.TransformerSender) error {
	data, ok := inMsg.Payload().(int)
	if !ok {
		return etl.ErrCastingFailed
	}

	return sender.Send(ctx, data*2)
}

func TestETL_BasePipeline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	extractor := etl.NewExtractor(newFakeExtractor(1, 2))
	transformer := etl.NewTransformer(extractor.OutputCh(), fakeTransformer)
	l := &fakeLoaderBatched{}
	loader := etl.NewLoaderBatched(transformer.OutputCh(), l.Handle)

	err := etl.RunAll(ctx, extractor, transformer, loader)
	require.NoError(t, err)

	require.Equal(t, 2, len(l.calls), "expected loader handler to be called two times")
	require.Equal(t, 2, l.calls[0][0].Payload())
	require.Equal(t, 4, l.calls[1][0].Payload())
}

func TestETL_PipelineWithBatchedLoader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	extractor := etl.NewExtractor(newFakeExtractor(1, 2))
	transformer := etl.NewTransformer(extractor.OutputCh(), fakeTransformer)
	l := &fakeLoaderBatched{}
	loader := etl.NewLoaderBatched(transformer.OutputCh(), l.Handle, etl.LoaderBatchedWithChannelTimeBatcher(time.Second, 2))

	err := etl.RunAll(ctx, extractor, transformer, loader)
	require.NoError(t, err)

	require.Equal(t, 1, len(l.calls), "expected loader handler to be called only once")
	require.Equal(t, 4, l.calls[0][0].Payload())
	require.Equal(t, 2, l.calls[0][1].Payload())
}

func TestETL_PipelineWithIncreaseConcurrency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	extractor := etl.NewExtractor(newFakeExtractor(1, 2))
	transformer := etl.NewTransformer(extractor.OutputCh(), fakeTransformer, etl.TransformerWithConcurrency(2))
	l := &fakeLoaderBatched{}
	loader := etl.NewLoaderBatched(transformer.OutputCh(), l.Handle)

	err := etl.RunAll(ctx, extractor, transformer, loader)
	require.NoError(t, err)

	require.Equal(t, 2, len(l.calls), "expected loader handler to be called two times")
	require.Equal(t, 2, l.calls[0][0].Payload())
	require.Equal(t, 4, l.calls[1][0].Payload())
}
