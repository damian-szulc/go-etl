package queue

type queueDriverDefaultOptions struct {
	onEnqueueHook []OnEnqueueHook
	onDequeueHook []OnDequeueHook
}

func newQueueDriverDefaultOptions(opts ...DefaultDriverOption) *queueDriverDefaultOptions {
	o := &queueDriverDefaultOptions{}
	for _, setter := range opts {
		if setter != nil {
			setter(o)
		}
	}

	return o
}

type DefaultDriverOption func(o *queueDriverDefaultOptions)

func DefaultDriverWithEnqueueHook(hook OnEnqueueHook) DefaultDriverOption {
	return func(o *queueDriverDefaultOptions) {
		o.onEnqueueHook = append(o.onEnqueueHook, hook)
	}
}

func DefaultDriverWithDequeueHook(hook OnDequeueHook) DefaultDriverOption {
	return func(o *queueDriverDefaultOptions) {
		o.onDequeueHook = append(o.onDequeueHook, hook)
	}
}
