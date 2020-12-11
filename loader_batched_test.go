package etl_test

import (
	"context"
	"errors"
	"github.com/damian-szulc/go-etl"
	"github.com/stretchr/testify/require"
	"testing"
)

type fakeLoaderBatched struct {
	calls [][]etl.Message
}

func (f *fakeLoaderBatched) Handle(ctx context.Context, messages []etl.Message) error {
	f.calls = append(f.calls, messages)
	return nil
}

type fakeLoaderBatchedHook struct {
	preRunCalls     []struct{}
	onErrorCalls    []error
	onCompleteCalls []struct{}
}

func (f *fakeLoaderBatchedHook) PreRun(ctx context.Context, inputCh <-chan etl.Message) error {
	f.preRunCalls = append(f.preRunCalls, struct{}{})
	return nil
}

func (f *fakeLoaderBatchedHook) OnError(ctx context.Context, inMsgs []etl.Message, err error) error {
	f.onErrorCalls = append(f.onErrorCalls, err)
	return nil
}

func (f *fakeLoaderBatchedHook) OnComplete(ctx context.Context, inMsgs []etl.Message) error {
	f.onCompleteCalls = append(f.onCompleteCalls, struct{}{})
	return nil
}

func TestLoaderBatched_FailsWhenHandlerReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errTest := errors.New("test")

	extractor := etl.NewExtractor(newFakeExtractor(1, 2))
	loader := etl.NewLoaderBatched(extractor.OutputCh(), func(ctx context.Context, messages []etl.Message) error {
		return errTest
	})

	err := etl.RunAll(ctx, extractor, loader)
	require.Equal(t, err, errTest)
}

func TestLoaderBatched_NotFailsWhenHandlerReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errTest := errors.New("test")

	extractor := etl.NewExtractor(newFakeExtractor(1, 2))
	loader := etl.NewLoaderBatched(extractor.OutputCh(), func(ctx context.Context, messages []etl.Message) error {
		return errTest
	}, etl.LoaderBatchedWithFailOnError(false))

	err := etl.RunAll(ctx, extractor, loader)
	require.NoError(t, err)
}

func TestLoaderBatched_CallsPreRunHooks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fakeL := fakeLoaderBatched{}
	fakeHook := fakeLoaderBatchedHook{}

	extractor := etl.NewExtractor(newFakeExtractor(1))
	loader := etl.NewLoaderBatched(extractor.OutputCh(), fakeL.Handle, etl.LoaderBatchedWithPreRunHook(fakeHook.PreRun))

	err := etl.RunAll(ctx, extractor, loader)

	require.NoError(t, err)
	require.Equal(t, 1, len(fakeHook.preRunCalls))
}

func TestLoaderBatched_CallsOnErrorHook(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testErr := errors.New("test error")
	fakeHook := fakeLoaderBatchedHook{}

	extractor := etl.NewExtractor(newFakeExtractor(1))
	loader := etl.NewLoaderBatched(extractor.OutputCh(),
		func(ctx context.Context, messages []etl.Message) error {
			return testErr
		},
		etl.LoaderBatchedWithFailOnError(false),
		etl.LoaderBatchedWithOnErrorHook(fakeHook.OnError),
	)

	err := etl.RunAll(ctx, extractor, loader)

	require.NoError(t, err)
	require.Equal(t, 1, len(fakeHook.onErrorCalls))
	require.Equal(t, testErr, fakeHook.onErrorCalls[0])
}

func TestLoaderBatched_CallsOnCompleteHook(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fakeHook := fakeLoaderBatchedHook{}

	extractor := etl.NewExtractor(newFakeExtractor(1))
	loader := etl.NewLoaderBatched(extractor.OutputCh(),
		func(ctx context.Context, messages []etl.Message) error {
			return nil
		},
		etl.LoaderBatchedWithOnCompleteHook(fakeHook.OnComplete),
	)

	err := etl.RunAll(ctx, extractor, loader)

	require.NoError(t, err)
	require.Equal(t, 1, len(fakeHook.onCompleteCalls))
}

func TestLoaderBatched_Concurrency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fakeLoader := fakeLoaderBatched{}

	extractor := etl.NewExtractor(newFakeExtractor(1, 2))
	loader := etl.NewLoaderBatched(extractor.OutputCh(),
		fakeLoader.Handle,
		etl.LoaderBatchedWithConcurrency(2),
	)

	err := etl.RunAll(ctx, extractor, loader)

	require.NoError(t, err)
	require.Equal(t, 2, len(fakeLoader.calls))
}
