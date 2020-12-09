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
	q             *queue.Queue
	enqueuedCh    chan struct{}
	outputCh      chan etl.Message
	blockInterval time.Duration
}

func NewDriverTimeBatch(blockInterval time.Duration) Driver {
	return &queueDriverTimeBatch{
		Mutex:         sync.Mutex{},
		q:             queue.New(),
		enqueuedCh:    make(chan struct{}, 1),
		blockInterval: blockInterval,
	}
}

func (q *queueDriverTimeBatch) OutputCh() <-chan etl.Message {
	return q.outputCh
}

func (q *queueDriverTimeBatch) Enqueue(msg etl.Message) {
	q.Lock()

	q.q.Push(queueTimeBatchEntry{
		msg:       msg,
		expiresAt: time.Now().Add(q.blockInterval),
	})

	q.Unlock()

	select {
	case q.enqueuedCh <- struct{}{}:
	default:
	}
}

func (q *queueDriverTimeBatch) dequeue() (etl.Message, *time.Duration, bool) {
	q.Lock()
	defer q.Unlock()

	if q.q.Size() == 0 {
		return nil, nil, false
	}

	entry := q.q.Front().(queueTimeBatchEntry)
	if !time.Now().After(entry.expiresAt) {
		dur := entry.expiresAt.Sub(time.Now())

		return nil, &dur, true
	}

	entry = q.q.Pop().(queueTimeBatchEntry)
	return entry.msg, nil, true
}

func (q *queueDriverTimeBatch) Run(ctx context.Context) error {
	var (
		ticker = time.NewTicker(q.blockInterval)
		msg    etl.Message
		dur    *time.Duration
		ok     bool
	)
	for {
		for true {
			msg, dur, ok = q.dequeue()
			if !ok {
				break
			}

			if dur == nil {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case q.outputCh <- msg:
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
