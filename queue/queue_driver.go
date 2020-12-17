package queue

import (
	"context"
	"github.com/damian-szulc/go-etl"
)

type Driver interface {
	etl.Runner
	OutputCh() <-chan etl.Message
	Enqueue(ctx context.Context, data etl.Message) error
}
