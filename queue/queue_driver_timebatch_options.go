package queue

type queueDriverTimeBatchOptions struct {
	onEnqueueHook []OnEnqueueHook
	onDequeueHook []OnDequeueHook
}

func newQueueDriverTimeBatchOptions(opts ...TimeBatchDriverOption) *queueDriverTimeBatchOptions {
	o := &queueDriverTimeBatchOptions{}
	for _, setter := range opts {
		if setter != nil {
			setter(o)
		}
	}

	return o
}

type TimeBatchDriverOption func(o *queueDriverTimeBatchOptions)

func TimeBatchDriverWithEnqueueHook(hook OnEnqueueHook) TimeBatchDriverOption {
	return func(o *queueDriverTimeBatchOptions) {
		o.onEnqueueHook = append(o.onEnqueueHook, hook)
	}
}

func TimeBatchDriverWithDequeueHook(hook OnDequeueHook) TimeBatchDriverOption {
	return func(o *queueDriverTimeBatchOptions) {
		o.onDequeueHook = append(o.onDequeueHook, hook)
	}
}
