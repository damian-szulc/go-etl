package etl_test

import (
	"context"
	"errors"
	"github.com/damian-szulc/go-etl"
	"github.com/stretchr/testify/require"
	"testing"
)

func newFakeExtractor(payload ...interface{}) etl.ExtractorHandler {
	return func(ctx context.Context, sender etl.ExtractorSender) error {
		for _, p := range payload {
			err := sender.Send(ctx, p)
			if err != nil {
				return err
			}
		}

		return nil
	}
}

type fakeExtractorHook struct {
	preRunCalls []struct{}
}

func (f *fakeExtractorHook) PreRun(ctx context.Context, outputCh chan<- etl.Message) error {
	f.preRunCalls = append(f.preRunCalls, struct{}{})
	return nil
}

type fakeDrainer struct {
	inMsgCh <-chan etl.Message
}

func (f *fakeDrainer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-f.inMsgCh:
			if !ok {
				return nil
			}
		}
	}
}

func TestExtractor_FailsWhenHandlerReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errTest := errors.New("test")
	extractor := etl.NewExtractor(func(ctx context.Context, sender etl.ExtractorSender) error {
		return errTest
	})

	err := extractor.Run(ctx)
	require.Equal(t, err, errTest)
}

func TestExtractor_CallsPreRunHooks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fakeHook := fakeExtractorHook{}
	extractor := etl.NewExtractor(newFakeExtractor(1, 2), etl.ExtractorWithPreRunHook(fakeHook.PreRun))
	drainer := &fakeDrainer{inMsgCh: extractor.OutputCh()}
	err := etl.RunAll(ctx, drainer, extractor)
	require.NoError(t, err)
	require.Equal(t, 1, len(fakeHook.preRunCalls))
}

func TestExtractor_DefaultOutputChBufferSize(t *testing.T) {
	extractor := etl.NewExtractor(newFakeExtractor(1, 2))
	require.Equal(t, 0, cap(extractor.OutputCh()))
}

func TestExtractor_CustomOutputChBufferSize(t *testing.T) {
	extractor := etl.NewExtractor(newFakeExtractor(1, 2), etl.ExtractorWithOutputChannelBufferSize(5))
	require.Equal(t, 5, cap(extractor.OutputCh()))
}
