package etl

import "context"

type ExtractorSender interface {
	Send(ctx context.Context, payload interface{}) error
	SendSlc(ctx context.Context, payloadSlc []interface{}) error
}

type extractorSender struct {
	ch chan<- Message
}

func newExtractorSender(ch chan<- Message) *extractorSender {
	return &extractorSender{ch: ch}
}

func (e *extractorSender) Send(ctx context.Context, payload interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case e.ch <- NewMessage(payload):
	}

	return nil
}

func (e *extractorSender) SendSlc(ctx context.Context, payloadSlc []interface{}) error {
	for _, payload := range payloadSlc {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e.ch <- NewMessage(payload):
		}
	}

	return nil
}
