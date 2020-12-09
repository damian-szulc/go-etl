package queue

import (
	"context"
	etl "github.com/damian-szulc/go-etl"
	"golang.org/x/sync/errgroup"
)

type Queue struct {
	driver  Driver
	inputCh <-chan etl.Message
}

func New(inputCh <-chan etl.Message, opts ...Option) *Queue {
	queue := &Queue{
		inputCh: inputCh,
	}

	queue = applyQueueOptions(queue, opts...)

	if queue.driver == nil {
		queue.driver = NewDriverDefault()
	}

	return queue
}

func (q *Queue) OutputCh() <-chan etl.Message {
	return q.driver.OutputCh()
}

func (q *Queue) enquer(ctx context.Context) error {
	var (
		msg etl.Message
		ok  bool
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok = <-q.inputCh:
			if !ok {
				return nil
			}

			q.driver.Enqueue(msg)
		}
	}

	return nil
}

func (q *Queue) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return q.driver.Run(ctx)
	})
	g.Go(func() error {
		return q.enquer(ctx)
	})

	return g.Wait()
}
