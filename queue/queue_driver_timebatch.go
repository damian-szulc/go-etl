package queue

import (
	"context"
	"github.com/damian-szulc/go-etl"
	"github.com/karalabe/cookiejar/collections/queue"
	"sync"
	"time"
)

type queueTimeBatchEntry struct {
	msg       etl.Message
	expiresAt time.Time
}

type queueDriverTimeBatch struct {
	sync.Mutex
	q *queue.Queue

	enqueuedCh    chan struct{}
	outputCh      chan etl.Message
	blockInterval time.Duration

	opts *queueDriverTimeBatchOptions
}

func NewDriverTimeBatch(blockInterval time.Duration, opts ...TimeBatchDriverOption) Driver {
	return &queueDriverTimeBatch{
		Mutex:         sync.Mutex{},
		q:             queue.New(),
		enqueuedCh:    make(chan struct{}, 1),
		blockInterval: blockInterval,

		opts: newQueueDriverTimeBatchOptions(opts...),
	}
}

func (q *queueDriverTimeBatch) OutputCh() <-chan etl.Message {
	return q.outputCh
}

func (q *queueDriverTimeBatch) hasEnqueueHooks() bool {
	return q.opts != nil && len(q.opts.onEnqueueHook) != 0
}
func (q *queueDriverTimeBatch) hasDequeueHooks() bool {
	return q.opts != nil && len(q.opts.onDequeueHook) != 0
}

func (q *queueDriverTimeBatch) callEnqueueHooks(ctx context.Context, size int) error {
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

func (q *queueDriverTimeBatch) callDequeueHooks(ctx context.Context, size int) error {
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

func (q *queueDriverTimeBatch) Enqueue(ctx context.Context, msg etl.Message) error {
	var (
		size           int
		hasEnqueueHook = q.hasEnqueueHooks()
	)
	q.Lock()

	// enqueue message
	q.q.Push(queueTimeBatchEntry{
		msg:       msg,
		expiresAt: time.Now().Add(q.blockInterval),
	})

	// read queue size
	if hasEnqueueHook {
		size = q.q.Size()
	}

	q.Unlock()

	select {
	case q.enqueuedCh <- struct{}{}:
	default:
	}

	return q.callEnqueueHooks(ctx, size)
}

func (q *queueDriverTimeBatch) dequeue() (etl.Message, *time.Duration, int, bool) {
	q.Lock()
	defer q.Unlock()

	size := q.q.Size()
	if size == 0 {
		return nil, nil, size, false
	}

	entry := q.q.Front().(queueTimeBatchEntry)
	if !time.Now().After(entry.expiresAt) {
		dur := entry.expiresAt.Sub(time.Now())

		return nil, &dur, size, true
	}

	entry = q.q.Pop().(queueTimeBatchEntry)
	return entry.msg, nil, size - 1, true
}

func (q *queueDriverTimeBatch) Run(ctx context.Context) error {
	var (
		ticker = time.NewTicker(q.blockInterval)
		msg    etl.Message
		dur    *time.Duration
		size   int
		ok     bool
		err    error
	)
	for {
		for true {
			msg, dur, size, ok = q.dequeue()
			if !ok {
				break
			}

			// if there is no duration to wait, send dequeued message into the output channel
			if dur == nil {
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

				continue
			}

			ticker.Reset(*dur)

			select {
			case <-ctx.Done():
				ticker.Stop()
				return ctx.Err()
			case <-ticker.C:
			}

			ticker.Stop()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.enqueuedCh:
		}
	}
}
