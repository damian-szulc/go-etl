package queue

import "github.com/damian-szulc/go-etl"

type Driver interface {
	etl.Runner
	OutputCh() <-chan etl.Message
	Enqueue(data etl.Message)
}
