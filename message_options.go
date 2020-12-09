package etl

import "time"

func applyMessageOptions(msg *message, optsSetters ...MessageOption) {
	for _, setter := range optsSetters {
		if setter != nil {
			setter(msg)
		}
	}
}

type MessageOption func(o *message)

func MessageWithProcessingStartedAt(tm time.Time) MessageOption {
	return func(o *message) {
		o.processingStartedAt = tm
	}
}
