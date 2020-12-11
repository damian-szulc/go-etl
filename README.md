# go-etl

Go utility to develop and run ETL pipelines. Remove boilerplate related to creating new goroutines, closing channels, graceful exits, etc. It also provides primitives to handle errors, measure and monitor execution time.

## Why

When building ETL pipeline (or fan in/out pipeline), one must take following considerations into an account:

* **concurrency** - ability to control concurrency,
* **error handling** - handle panics and any arbitrary errors, without affecting the entire processor,
* **graceful exit** - ability to exit if necessary, without leaving processor stuck,
* **observability** - measuring and exposing throughput, processing time, error rates etc.

Usually there is a lot of boilerplate code involved. This package aims at addressing issues above.

## Example usage

```go
import (
    "github.com/damian-szulc/go-etl"
)

// Controller contains use-case specific logic. All methods should be safe to run concurrently
type Controller struct {}

func (c *Controller) Extract(ctx context.Context, sender etl.ExtractorSender) error {
	return sender.Send(ctx, 1)
}

func (c *Controller) Transform(ctx context.Context, inMsg etl.Message, sender etl.TransformerSender) error {
	data, ok := inMsg.Payload().(int)
	if !ok {
		return etl.ErrCastingFailed
	}

	// process
	data = process(data)

	// send
	return sender.Send(ctx, data)
}

func (c *Controller) Load(ctx context.Context, message etl.Message) error {
	// store
}

....

// Runner starts processing
func Run(ctx context.Context) error {
	controller := &Controller{}

	extractor := etl.NewExtractor(
		controller.Extract,
	)

	transformer := etl.NewTransformer(
		extractor.OutputCh(),
		controller.Transform,
		etl.TransformerWithConcurrency(10),
        etl.TransformerWithFailOnErr(true),
		etl.TransformerWithOnErrorHook(controller.OnTransformerError),
		etl.TransformerWithOnCompleteHook(controller.OnTransformerComplete),
		etl.TransformerWithOutputChannelBufferSize(10),
	)

	loader := etl.NewLoader(
		transformer.OutputCh(),
		controller.Load,
		etl.LoaderWithConcurrency(10),
		etl.LoaderWithFailOnErr(true),
		etl.LoaderWithOnCompleteHook(controller.OnLoaderCompleteHook),
		etl.LoaderWithOnErrorHook(controller.OnLoaderErrorHook),
	)

	return etl.RunAll(ctx, extractor, transformer, loader)
}
```

## Storing result in batches

Sometimes it is not efficient to store every processing result efficiently. To address this issue, you can leverage `LoaderBatched` as follows:

```
    ...

    loader := etl.NewLoaderBatched(
    		transformer.OutputCh(),
    		controller.Load,
    		etl.LoaderBatchedWithConcurrency(10),
		    etl.LoaderWithFailOnErr(true),
    		etl.LoaderBatchedWithOnCompleteHook(controller.OnLoaderBatchedCompleteHook),
    		etl.LoaderBatchedWithOnErrorHook(controller.OnLoaderBatchedErrorHook),
            etl.LoaderBatchedWithMaxItemsBatcher(100),
    	)

    ...
```

Following batching strategies are available:

1. `etl.LoaderBatchedWithFixedSizeBatches` - collects incoming messages until specified amount is reached. Last batch before closing might contain less item. 
2. `etl.LoaderBatchedWithDrainedChannelBatches` - after receiving first message drains input channel, up to `maxItems` in batch. If channel is empty, returns collected messages right away. It works specifically well with buffered incoming channels.
3. `etl.LoaderBatchedWithThrottledBatches` - performs throttling on received messages, up until it collected maximum items per batch. (In other words, after receiving first message, it collects incoming messages for specified amount of time) 
4. `etl.LoaderBatchedWithDebouncedBatches` - performs a debouncing on received messages, up until it collected maximum items per batch. (In other words, it collects messages until encountered inactivity for a specified amount of time, or maximum batch size has been reached) 
5. You can implement your own batching strategy and pass it down to loader via `etl.LoaderBatchedWithBatcher` option.

If none of those option has been selected, `etl.LoaderBatchedWithFixedSizeBatches` will be used with a maximum of 1 message per batch. 

## ToDo

- [ ] Add logger support
- [ ] When "failing" by default is disabled and there are no error hooks, log errors
- [ ] Add docs about Metrics, advanced Transformers
- [ ] Add transformer tests
- [ ] Add loader tests
- [ ] Add queue tests
- [ ] Add GoDoc comments
