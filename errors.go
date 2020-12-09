package etl

import "errors"

var (
	ErrCastingFailed                   = errors.New("casting incomming message failed")
	ErrOutputMessageOutOfChannelsRange = errors.New("tried to get an output chan out of range")
)
