package etl_test

import (
	"context"
	"errors"
	"github.com/damian-szulc/go-etl"
	"github.com/stretchr/testify/require"
	"testing"
)

type fakeLoader struct {
	calls [][]etl.Message
}

func (f *fakeLoader) Handle(ctx context.Context, messages []etl.Message) error {
	f.calls = append(f.calls, messages)
	return nil
}

type fakeLoaderHook struct {
	preRunCalls     []struct{}
	onErrorCalls    []error
	onCompleteCalls []struct{}
}

func (f *fakeLoaderHook) PreRun(ctx context.Context, inputCh <-chan etl.Message) error {
	f.preRunCalls = append(f.preRunCalls, struct{}{})
	return nil
}

func (f *fakeLoaderHook) OnError(ctx context.Context, inMsgs []etl.Message, err error) error {
	f.onErrorCalls = append(f.onErrorCalls, err)
	return nil
}

func (f *fakeLoaderHook) OnComplete(ctx context.Context, inMsgs []etl.Message) error {
	f.onCompleteCalls = append(f.onCompleteCalls, struct{}{})
	return nil
}

func TestLoader_FailsWhenHandlerReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errTest := errors.New("test")

	extractor := etl.NewExtractor(newFakeExtractor(1, 2))
	loader := etl.NewLoader(extractor.OutputCh(), func(ctx context.Context, messages []etl.Message) error {
		return errTest
	})

	err := etl.RunAll(ctx, extractor, loader)
	require.Equal(t, err, errTest)
}

func TestLoader_NotFailsWhenHandlerReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errTest := errors.New("test")

	extractor := etl.NewExtractor(newFakeExtractor(1, 2))
	loader := etl.NewLoader(extractor.OutputCh(), func(ctx context.Context, messages []etl.Message) error {
		return errTest
	}, etl.LoaderWithFailOnError(false))

	err := etl.RunAll(ctx, extractor, loader)
	require.NoError(t, err)
}

func TestLoader_CallsPreRunHooks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fakeL := fakeLoader{}
	fakeHook := fakeLoaderHook{}

	extractor := etl.NewExtractor(newFakeExtractor(1))
	loader := etl.NewLoader(extractor.OutputCh(), fakeL.Handle, etl.LoaderWithPreRunHook(fakeHook.PreRun))

	err := etl.RunAll(ctx, extractor, loader)

	require.NoError(t, err)
	require.Equal(t, 1, len(fakeHook.preRunCalls))
}

func TestLoader_CallsOnErrorHook(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testErr := errors.New("test error")
	fakeHook := fakeLoaderHook{}

	extractor := etl.NewExtractor(newFakeExtractor(1))
	loader := etl.NewLoader(extractor.OutputCh(),
		func(ctx context.Context, messages []etl.Message) error {
			return testErr
		},
		etl.LoaderWithFailOnError(false),
		etl.LoaderWithOnErrorHook(fakeHook.OnError),
	)

	err := etl.RunAll(ctx, extractor, loader)

	require.NoError(t, err)
	require.Equal(t, 1, len(fakeHook.onErrorCalls))
	require.Equal(t, testErr, fakeHook.onErrorCalls[0])
}

func TestLoader_CallsOnCompleteHook(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fakeHook := fakeLoaderHook{}

	extractor := etl.NewExtractor(newFakeExtractor(1))
	loader := etl.NewLoader(extractor.OutputCh(),
		func(ctx context.Context, messages []etl.Message) error {
			return nil
		},
		etl.LoaderWithOnCompleteHook(fakeHook.OnComplete),
	)

	err := etl.RunAll(ctx, extractor, loader)

	require.NoError(t, err)
	require.Equal(t, 1, len(fakeHook.onCompleteCalls))
}

func TestLoader_Concurrency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fakeLoader := fakeLoader{}

	extractor := etl.NewExtractor(newFakeExtractor(1, 2))
	loader := etl.NewLoader(extractor.OutputCh(),
		fakeLoader.Handle,
		etl.LoaderWithConcurrency(2),
	)

	err := etl.RunAll(ctx, extractor, loader)

	require.NoError(t, err)
	require.Equal(t, 2, len(fakeLoader.calls))
}
