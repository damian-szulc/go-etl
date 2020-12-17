package queue

import (
	"context"
	"github.com/damian-szulc/go-etl"
	"github.com/karalabe/cookiejar/collections/queue"
	"sync"
)

type queueDriverDefault struct {
	sync.Mutex
	q *queue.Queue

	enqueuedCh chan struct{}
	outputCh   chan etl.Message

	opts *queueDriverDefaultOptions
}

func NewDriverDefault(opts ...DefaultDriverOption) Driver {
	return &queueDriverDefault{
		Mutex:      sync.Mutex{},
		q:          queue.New(),
		enqueuedCh: make(chan struct{}, 1),
		outputCh:   make(chan etl.Message),

		opts: newQueueDriverDefaultOptions(opts...),
	}
}

func (q *queueDriverDefault) OutputCh() <-chan etl.Message {
	return q.outputCh
}

func (q *queueDriverDefault) hasEnqueueHooks() bool {
	return q.opts != nil && len(q.opts.onEnqueueHook) != 0
}
func (q *queueDriverDefault) hasDequeueHooks() bool {
	return q.opts != nil && len(q.opts.onDequeueHook) != 0
}

func (q *queueDriverDefault) callEnqueueHooks(ctx context.Context, size int) error {
	if !q.hasEnqueueHooks() {
		return nil
	}

	var err error
	for _, hook := range q.opts.onEnqueueHook {
		if hook != nil {
			err = hook(ctx, size)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (q *queueDriverDefault) callDequeueHooks(ctx context.Context, size int) error {
	if !q.hasDequeueHooks() {
		return nil
	}

	var err error
	for _, hook := range q.opts.onDequeueHook {
		if hook != nil {
			err = hook(ctx, size)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Enqueue adds item to
func (q *queueDriverDefault) Enqueue(ctx context.Context, msg etl.Message) error {
	var (
		size           int
		hasEnqueueHook = q.hasEnqueueHooks()
	)

	q.Lock()
	// enqueue message
	q.q.Push(msg)

	// read queue size
	if hasEnqueueHook {
		size = q.q.Size()
	}
	q.Unlock()

	// notify dequeuer if necessary
	select {
	case q.enqueuedCh <- struct{}{}:
	default:
	}

	return q.callEnqueueHooks(ctx, size)
}

func (q *queueDriverDefault) dequeue() (etl.Message, int, bool) {
	var size int
	q.Lock()
	defer q.Unlock()

	size = q.q.Size()
	if size <= 0 {
		return nil, size, false
	}

	rawMsg := q.q.Pop()
	// allow to panic if it's not a etl.Message
	msg := rawMsg.(etl.Message)

	return msg, size - 1, true
}

// Run starts driver main loop
func (q *queueDriverDefault) Run(ctx context.Context) error {
	var (
		msg  etl.Message
		size int
		err  error
		ok   bool
	)
	for {
		for true {
			// dequeue message
			msg, size, ok = q.dequeue()
			// if no message was dequeued, break and wait for new message in a queue
			if !ok {
				break
			}

			// send dequeued message to output channel
			select {
			case <-ctx.Done():
				return ctx.Err()
			case q.outputCh <- msg:
			}

			// call dequeue hooks
			err = q.callDequeueHooks(ctx, size)
			if err != nil {
				return err
			}
		}

		// wait until new item is enqueued
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.enqueuedCh:
		}
	}

	return nil
}
