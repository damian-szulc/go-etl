package queue

type Option func(o *Queue)

func applyQueueOptions(queue *Queue, optsSetters ...Option) *Queue {
	for _, setter := range optsSetters {
		if setter != nil {
			setter(queue)
		}
	}

	return queue
}

func WithDriver(driver Driver) Option {
	return func(o *Queue) {
		o.driver = driver
	}
}
