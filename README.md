# go-etl

Go utility to develop and run ETL pipelines. Remove boilerplate related to creating new goroutines, closing channels, graceful exits, etc. It also provides primitives to handle errors, measure and monitor execution time.

## Example usage

```go
import (
    "github.com/damian-szulc/go-etl"
)

// Controller

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
	data = data * 2

	// send
	return sender.Send(ctx, data)
}

func (c *Controller) Load(ctx context.Context, messages []etl.Message) error {
	// store
}
....

// Runner

func Run(ctx context.Context) {
	controller := &Controller{}

	extractor := etl.NewExtractor(
		controller.Extract,
	)

	transformer := etl.NewTransformer(
		extractor.OutputCh(),
		controller.Transform,
		etl.TransformerWithConcurrency(10),
		etl.TransformerWithOnErrorHook(controller.OnTransformerError),
		etl.TransformerWithOnCompleteHook(controller.OnTransformerComplete),
		etl.TransformerWithOutputChannelBufferSize(10),
	)

	loader := etl.NewLoader(
		transformer.OutputCh(),
		controller.Load,
		etl.LoaderWithConcurrency(10),
		etl.LoaderWithOnCompleteHook(controller.OnLoaderCompleteHook),
		etl.LoaderWithOnErrorHook(controller.OnLoaderErrorHook),
		etl.LoaderWithChannelTimeDrainer(time.Second, 100),
	)

	return etl.RunAll(ctx, extractor, transformer, loader)
}

```
## ToDo

- [ ] Add logger support
- [ ] When "failing" by default is disabled and there are no error hooks, log errors
- [ ] Add transformer tests
- [ ] Add queue tests
- [ ] Add GoDoc comments
