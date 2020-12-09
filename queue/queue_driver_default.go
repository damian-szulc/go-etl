package queue

import (
	"context"
	"github.com/damian-szulc/go-etl"
	"github.com/karalabe/cookiejar/collections/queue"
	"sync"
)

type queueDriverDefault struct {
	sync.Mutex
	q          *queue.Queue
	enqueuedCh chan struct{}
	outputCh   chan etl.Message
}

func NewDriverDefault() Driver {
	return &queueDriverDefault{
		Mutex:      sync.Mutex{},
		q:          queue.New(),
		enqueuedCh: make(chan struct{}, 1),
		outputCh:   make(chan etl.Message),
	}
}

func (q *queueDriverDefault) OutputCh() <-chan etl.Message {
	return q.outputCh
}

func (q *queueDriverDefault) Enqueue(msg etl.Message) {
	q.Lock()

	q.q.Push(msg)

	q.Unlock()

	select {
	case q.enqueuedCh <- struct{}{}:
	default:
	}
}

func (q *queueDriverDefault) dequeue() (etl.Message, bool) {
	q.Lock()
	defer q.Unlock()

	if q.q.Size() > 0 {
		return nil, false
	}

	rawMsg := q.q.Pop()
	// allow to panic if it's not a etl.Message
	msg := rawMsg.(etl.Message)

	return msg, true
}

func (q *queueDriverDefault) Run(ctx context.Context) error {
	var (
		msg etl.Message
		ok  bool
	)
	for {
		for true {
			msg, ok = q.dequeue()
			if !ok {
				break
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case q.outputCh <- msg:
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.enqueuedCh:
		}
	}

	return nil
}
